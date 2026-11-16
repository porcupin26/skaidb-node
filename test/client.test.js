'use strict';
// Client end to end against the in-process fake server: handshake, Hello,
// every request opcode, streaming (including the abandon/drain rule),
// batches, failover, reconnect and the pool.
const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('net');
const { Client, Pool, SkaidbError, _internal: I } = require('../skaidb');
const pkg = require('../package.json');
const { FakeServer, F } = require('./fake-server');

// A scripted handler: `script[sql]` answers OP_QUERY / OP_QUERY_STREAM text
// and OP_QUERY_STREAM_PARAMS (by its `?` text), `prepared[sql]` makes
// OP_PREPARE succeed with that arity, executes answer through `onExecute`.
function scripted(script, extra = {}) {
  const prepared = extra.prepared || {};
  let nextId = 100;
  const ids = new Map();
  return (req, conn) => {
    switch (req.op) {
      case 1: case 5: case 9: {
        const a = script[req.sql];
        if (a === undefined) return F.error(`fake server: unknown statement ${req.sql}`);
        return typeof a === 'function' ? a(req, conn) : a;
      }
      case 2: {
        if (!(req.sql in prepared)) return F.error('cannot prepare this statement');
        const id = nextId++; ids.set(id, req.sql);
        return F.prepared(id, prepared[req.sql]);
      }
      case 3: return extra.onExecute(ids.get(req.id), req);
      case 7: return extra.onBatch ? extra.onBatch(ids.get(req.id), req) : F.mutation(req.rows.length);
      default: return undefined;                                     // Hello -> Ddl
    }
  };
}

async function withServer(opts, fn) {
  const srv = await new FakeServer(opts).start();
  try { return await fn(srv); } finally { await srv.stop(); }
}

const until = async (pred, ms = 2000) => {
  const t0 = Date.now();
  while (!pred()) {
    if (Date.now() - t0 > ms) throw new Error('timeout waiting');
    await new Promise((r) => setTimeout(r, 5));
  }
};

// `promise`, or a rejection when it has not settled within `ms`: a call that
// hangs must fail its test, not stall the suite.
const settles = (promise, ms = 2000) => {
  let timer;
  const late = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`still pending after ${ms} ms`)), ms); });
  return Promise.race([promise, late]).finally(() => clearTimeout(timer));
};

test('connect runs SCRAM, verifies the server signature and sends Hello with the package version', async () => {
  await withServer({}, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, user: 'ada', password: 'secret' });
    await c.connect();
    assert.equal(c.isUsable(), true);
    assert.equal(srv.hellos.length, 1);
    assert.equal(srv.hellos[0].name, 'nodejs');
    assert.equal(srv.hellos[0].version, pkg.version);
    assert.equal(pkg.version, '1.5.0');
    await c.end();
    assert.equal(c.isUsable(), false);
  });
});

test('a wrong password is denied; a forged server signature is refused', async () => {
  await withServer({}, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, user: 'ada', password: 'nope' });
    await assert.rejects(c.connect(), /authentication denied: bad password/);
    srv.corruptServerSignature = true;
    const d = new Client({ host: '127.0.0.1', port: srv.port, user: 'ada', password: 'secret' });
    await assert.rejects(d.connect(), /server signature mismatch/);
  });
});

test('a failed handshake closes its socket instead of leaking an open connection', async () => {
  await withServer({}, async (srv) => {
    srv.corruptServerSignature = true;           // the server keeps the connection open
    const c = new Client({ host: '127.0.0.1', port: srv.port, user: 'ada', password: 'secret' });
    await assert.rejects(c.connect(), /server signature mismatch/);
    await until(() => srv.sockets.size === 0);
    assert.equal(c.isUsable(), false);
  });
});

test('query decodes Rows, Mutation, Ddl and Error; rowMode array; per-statement consistency', async () => {
  const script = {
    'SELECT id, name FROM t': F.rows(['id', 'name'], [[1, 'Ada'], [2, 'Linus']]),
    "INSERT INTO t VALUES (1)": F.mutation(3),
    'CREATE TABLE t (PRIMARY KEY (id))': F.ddl(),
    'SELECT nope': F.error('no such column nope'),
    'SELECT 1 AT ALL': (req) => F.rows(['c'], [[req.consistency]]),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', consistency: 'ONE' });
    await c.connect();
    const r = await c.query('SELECT id, name FROM t');
    assert.equal(r.command, 'SELECT');
    assert.equal(r.rowCount, 2);
    assert.deepEqual(r.rows, [{ id: 1, name: 'Ada' }, { id: 2, name: 'Linus' }]);
    assert.deepEqual(r.fields, [{ name: 'id' }, { name: 'name' }]);
    assert.deepEqual(r.columns, ['id', 'name']);
    const arr = await c.query({ text: 'SELECT id, name FROM t', rowMode: 'array' });
    assert.deepEqual(arr.rows, [[1, 'Ada'], [2, 'Linus']]);
    const m = await c.query("INSERT INTO t VALUES (1)");
    assert.deepEqual([m.command, m.rowCount, m.rows], ['MUTATION', 3, []]);
    const d = await c.query('CREATE TABLE t (PRIMARY KEY (id))');
    assert.deepEqual([d.command, d.rowCount], ['DDL', null]);
    await assert.rejects(c.query('SELECT nope'), (e) => e instanceof SkaidbError && /no such column nope/.test(e.message));
    assert.equal(c.isUsable(), true);                             // a statement error keeps the connection
    assert.equal((await c.query('SELECT 1 AT ALL')).rows[0].c, 0);                       // client default ONE
    assert.equal((await c.query({ text: 'SELECT 1 AT ALL', consistency: 'ALL' })).rows[0].c, 2);
    const plainQueries = srv.requests.filter((q) => q.op === 1);
    assert.ok(plainQueries.every((q) => q.sql.length > 0));
    await c.end();
  });
});

test('parameters go through server-side prepare with typed values, cached per connection', async () => {
  const executed = [];
  const handle = scripted({}, {
    prepared: { 'SELECT * FROM t WHERE id = ? AND tags = ? AND meta = ?': 3 },
    onExecute: (sql, req) => { executed.push(req); return F.rows(['ok'], [[true]]); },
  });
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const params = [2n ** 62n, ['a', 'b'], { k: new Date(5), n: null, b: Buffer.from('z') }];
    await c.query('SELECT * FROM t WHERE id = $1 AND tags = $2 AND meta = $3', params);
    await c.query('SELECT * FROM t WHERE id = $1 AND tags = $2 AND meta = $3', [1, [], {}]);
    assert.equal(srv.requests.filter((q) => q.op === 2).length, 1);   // prepared once
    assert.equal(executed.length, 2);
    assert.deepEqual(executed[0].params, params);                       // typed, lossless
    assert.deepEqual(executed[1].params, [1, [], {}]);
    await assert.rejects(
      c.query('SELECT * FROM t WHERE id = $1 AND tags = $2 AND meta = $3', [1, 2]),
      /no parameter for \$3/);
    await c.end();
  });
});

test('an unpreparable statement falls back to client-side literals', async () => {
  const seen = [];
  const handle = scripted({}, { onExecute: () => F.error('unexpected') });
  const wrapped = (req, conn) => {
    if (req.op === 1) { seen.push(req.sql); return F.ddl(); }
    return handle(req, conn);
  };
  await withServer({ handle: wrapped }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    await c.query('USE $1', ["app'db"]);
    assert.deepEqual(seen, ["USE 'app''db'"]);
    await c.end();
  });
});

// The server's prepared-statement table as the real one keeps it: `max` slots
// per connection, a closed slot is reused, and a prepare that finds the table
// full is refused. Statements sent as text are recorded in `texts`.
function statementTable(max = 256) {
  const slots = [];                                    // id -> the statement's text, null once closed
  const texts = [];
  const stats = { peak: 0, refused: 0 };
  const open = () => slots.filter((s) => s !== null).length;
  const unknown = (id) => F.error(`unknown prepared statement id ${id}`);
  const handle = (req) => {
    switch (req.op) {
      case 1: texts.push(req.sql); return F.ddl();
      case 2: {
        let id = slots.indexOf(null);
        if (id < 0) {
          if (slots.length >= max) {
            stats.refused++;
            return F.error(`too many prepared statements (max ${max}); close some first`);
          }
          id = slots.length;
          slots.push(null);
        }
        slots[id] = req.sql;
        stats.peak = Math.max(stats.peak, open());
        // `AS OF ?` is not a parameter to the server: it counts the others.
        return F.prepared(id, (req.sql.replace(/AS OF \?/g, '').match(/\?/g) || []).length);
      }
      case 3: return slots[req.id] ? F.mutation(1) : unknown(req.id);
      case 4: {
        if (!slots[req.id]) return unknown(req.id);
        slots[req.id] = null;
        return F.ddl();
      }
      case 7: return slots[req.id] ? F.mutation(req.rows.length) : unknown(req.id);
      default: return undefined;                       // Hello -> Ddl
    }
  };
  return { handle, texts, stats, open };
}

test('a statement the prepared cache has no room for is closed after it ran, so the server\'s statement table never fills', async () => {
  const table = statementTable();
  await withServer({ handle: table.handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    // More distinct parameterised texts than the server keeps per connection.
    for (let i = 0; i < 300; i++) {
      assert.equal((await c.query(`UPDATE t SET v = $1 WHERE ${i} = ${i}`, [i])).rowCount, 1);
    }
    assert.equal(table.stats.refused, 0);
    assert.equal(table.open(), 240);                   // the cache, and nothing else
    assert.ok(table.stats.peak <= 241, `peak ${table.stats.peak}`);
    // Each of the 60 that did not fit was closed, by its own id, right after it ran.
    const closes = srv.requests.filter((q) => q.op === 4);
    assert.equal(closes.length, 60);
    for (const close of closes) {
      const before = srv.requests[srv.requests.indexOf(close) - 1];
      assert.deepEqual([before.op, before.id], [3, close.id]);
    }
    // A statement that is new to the connection still binds typed values: a
    // Date as a Timestamp, a Buffer as Bytes, an array as an Array.
    const params = [1, new Date(5), Buffer.from('z'), ['a']];
    await c.query('INSERT INTO t (id, at, raw, tags) VALUES ($1, $2, $3, $4)', params);
    assert.deepEqual(srv.requests.filter((q) => q.op === 3).pop().params, params);
    // … and so does a batch, which closes its statement as well.
    const rows = [[1, new Date(5)], [2, new Date(6)]];
    assert.equal(await c.batch('INSERT INTO t (id, at) VALUES ($1, $2)', rows), 2);
    assert.deepEqual(srv.requests.filter((q) => q.op === 7).pop().rows, rows);
    assert.equal(table.open(), 240);
    assert.deepEqual(table.texts, []);                 // nothing was ever sent as text
    // A cached statement is not closed: it runs again on the id it has.
    const prepares = srv.requests.filter((q) => q.op === 2).length;
    await c.query('UPDATE t SET v = $1 WHERE 0 = 0', [7]);
    assert.equal(srv.requests.filter((q) => q.op === 2).length, prepares);
    assert.equal(srv.requests[srv.requests.length - 1].op, 3);
    // A statement that fails after its prepare still gives its slot back.
    const closed = () => srv.requests.filter((q) => q.op === 4).length;
    const before = closed();
    await assert.rejects(c.query('SELECT v FROM t AS OF $1', [new Date(5)]), /statement expects 0 parameters, got 1/);
    await assert.rejects(c.query('UPDATE t SET v = $1 WHERE id = 9', [NaN]), /cannot bind NaN\/Infinity/);
    await assert.rejects(c.batch('UPDATE t SET v = $1 WHERE id = $2', [[1, 2], [1]]), /no parameter for \$2/);
    assert.equal(closed(), before + 3);
    assert.equal(table.open(), 240);
    assert.equal(c.isUsable(), true);
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('a prepare refused because the statement table is full is that error, not a fallback to literals', async () => {
  const FULL = 'too many prepared statements (max 256); close some first';
  let refusal = FULL;
  const texts = [];
  const handle = (req) => {
    if (req.op === 2) return F.error(refusal);
    if (req.op === 1) { texts.push(req.sql); return F.ddl(); }
    return undefined;
  };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const when = new Date(5);
    // As text the Date would have gone out as the number 5 and the Buffer as
    // the string '7a': a statement that can be prepared is never sent that way.
    const err = await c.query('INSERT INTO t (id, at, raw) VALUES ($1, $2, $3)', [1, when, Buffer.from('z')]).catch((e) => e);
    assert.ok(err instanceof SkaidbError);
    assert.equal(err.message, FULL);
    assert.equal(I.memberFault(err), false);           // the server answered: a statement error
    await assert.rejects(c.batch('INSERT INTO t (id, at) VALUES ($1, $2)', [[1, when]]),
      (e) => e instanceof SkaidbError && e.message === FULL);
    assert.deepEqual(texts, []);
    assert.equal(c.isUsable(), true);
    // Every refusal that is about the statement still falls back: a kind the
    // server does not prepare, a statement whose grammar takes no `?` (the
    // parse error of the `?` form), a server without the opcode.
    for (refusal of ['statement kind cannot be prepared', 'expected a quoted string, found Question',
      'protocol error: unknown opcode']) {
      texts.length = 0;
      assert.equal((await c.query('CREATE USER u PASSWORD $1', ["p'w"])).command, 'DDL', refusal);
      assert.deepEqual(texts, ["CREATE USER u PASSWORD 'p''w'"], refusal);
    }
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('ResultSets from a CALL that EMITs land under resultSets with the last set as rows', async () => {
  const script = {
    'CALL report()': F.resultSets([[['a'], [[1], [2]]], [['b', 'c'], [['x', 'y']]]]),
    'CALL empty()': F.resultSets([]),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const r = await c.query('CALL report()');
    assert.equal(r.command, 'CALL');
    assert.equal(r.resultSets.length, 2);
    assert.deepEqual(r.resultSets[0].rows, [{ a: 1 }, { a: 2 }]);
    assert.deepEqual(r.rows, [{ b: 'x', c: 'y' }]);
    assert.deepEqual(r.columns, ['b', 'c']);
    assert.equal(r.rowCount, 1);
    const e = await c.query('CALL empty()');
    assert.deepEqual([e.rows, e.rowCount, e.resultSets], [[], 0, []]);
    await c.end();
  });
});

test('database is selected with USE after the handshake', async () => {
  const seen = [];
  const handle = (req) => { if (req.op === 1) seen.push(req.sql); return F.ddl(); };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', database: 'my"db' });
    await c.connect();
    assert.deepEqual(seen, ['USE "my""db"']);
    await c.end();
  });
});

test('stream yields rows chunk by chunk, exposes columns, and ends cleanly', async () => {
  const script = {
    'SELECT id FROM big': [F.header(['id']), F.chunk([[1], [2]]), F.chunk([]), F.chunk([[3]]), F.end()],
    'SELECT id FROM t': F.rows(['id'], [[9]]),
    'INSERT INTO t VALUES (1)': F.mutation(1),
    'SELECT boom': [F.header(['id']), F.chunk([[1]]), F.error('scan budget exceeded')],
    'SELECT early': F.error('no such table'),
    'CALL emits()': F.resultSets([[['a'], [[1]]]]),
    'CREATE TABLE s (PRIMARY KEY (id))': F.ddl(),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const got = [];
    for await (const row of c.stream('SELECT id FROM big')) got.push(row);
    assert.deepEqual(got, [{ id: 1 }, { id: 2 }, { id: 3 }]);
    assert.deepEqual(c.stream.columns, ['id']);
    assert.equal(c.stream.command, 'SELECT');
    assert.equal(c.stream.affected, null);
    const arr = [];
    for await (const row of c.stream('SELECT id FROM big', { rowMode: 'array' })) arr.push(row);
    assert.deepEqual(arr, [[1], [2], [3]]);
    // Streaming a non-row statement yields nothing and does not desync.
    const none = [];
    for await (const row of c.stream('INSERT INTO t VALUES (1)')) none.push(row);
    assert.deepEqual(none, []);
    // ...and reports what it did: nothing stale from the previous stream.
    assert.equal(c.stream.command, 'MUTATION');
    assert.equal(c.stream.affected, 1);
    assert.equal(c.stream.columns, null);
    for await (const row of c.stream('CREATE TABLE s (PRIMARY KEY (id))')) none.push(row);
    assert.deepEqual(none, []);
    assert.equal(c.stream.command, 'DDL');
    assert.equal(c.stream.affected, null);
    assert.deepEqual((await c.query('SELECT id FROM t')).rows, [{ id: 9 }]);
    // Error before the header: a plain statement failure.
    await assert.rejects((async () => { for await (const _ of c.stream('SELECT early')) {} })(), /no such table/);
    assert.equal(c.stream.columns, null);
    assert.equal(c.stream.command, null);
    // Error after the header: rows so far are valid, the stream ends, connection is fine.
    const partial = [];
    await assert.rejects((async () => { for await (const r of c.stream('SELECT boom')) partial.push(r); })(),
      /scan budget exceeded/);
    assert.deepEqual(partial, [{ id: 1 }]);
    assert.equal(c.isUsable(), true);
    assert.deepEqual((await c.query('SELECT id FROM t')).rows, [{ id: 9 }]);
    // A ResultSets reply on the stream opcode is one whole frame: its sets
    // come back (see the CALL test below) and nothing is left to drain.
    assert.deepEqual(await collect(c.stream('CALL emits()')), [{ a: 1 }]);
    assert.equal(c.stream.command, 'CALL');
    assert.deepEqual((await c.query('SELECT id FROM t')).rows, [{ id: 9 }]);
    await c.end();
  });
});

test('a streamed CALL whose body EMITs yields its final set and puts every set under stream.resultSets', async () => {
  // One ResultSets frame, as query() receives it: [a: x, y] then [b: z].
  const sets = F.resultSets([[['a'], [['x'], ['y']]], [['b'], [['z']]]]);
  const script = {
    'CALL report(?)': sets,      // with values: OP_QUERY_STREAM_PARAMS, $1 sent as ?
    'CALL report()': sets,       // without: OP_QUERY_STREAM
    'SELECT id FROM big': [F.header(['id']), F.chunk([[1]]), F.end()],
    'SELECT 1': F.rows(['x'], [[1]]),
  };
  const joined = (rs) => rs.map((s) => s.rows.map((r) => Object.values(r).join('')).join('')).join('|');
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    for (const [sql, values] of [['CALL report($1)', [5]], ['CALL report()', undefined]]) {
      // The rows query() returns: the call's final result, the last set ...
      assert.deepEqual(await collect(c.stream(sql, values)), [{ b: 'z' }], sql);
      assert.equal(c.stream.command, 'CALL');
      assert.deepEqual(c.stream.columns, ['b']);
      assert.equal(c.stream.affected, null);
      // ... and every set, in emission order, as query() puts them under resultSets.
      assert.equal(joined(c.stream.resultSets), 'xy|z', sql);
      assert.deepEqual(c.stream.resultSets.map((s) => s.columns), [['a'], ['b']]);
      assert.equal(c.isUsable(), true);
    }
    assert.deepEqual(srv.requests.filter((r) => r.op !== 8).map((r) => r.op), [9, 5]);
    assert.deepEqual(srv.requests[srv.requests.length - 2].params, [5]);
    // Array rows, and leaving early: one frame, so there is nothing to drain.
    assert.deepEqual(await collect(c.stream('CALL report()', { rowMode: 'array' })), [['z']]);
    assert.deepEqual(c.stream.resultSets[0].rows, [['x'], ['y']]);
    for await (const _ of c.stream('CALL report()')) break;
    // Nothing is in flight, so a statement the loop body awaits runs instead
    // of queueing behind the stream (which would wait on the body forever).
    const queued = () => new Promise((_, reject) => {
      setTimeout(() => reject(new Error('the statement queued behind a finished CALL')), 2000).unref();
    });
    for await (const row of c.stream('CALL report()')) {
      assert.deepEqual(row, { b: 'z' });
      assert.deepEqual((await Promise.race([c.query('SELECT 1'), queued()])).rows, [{ x: 1 }]);
    }
    // Nothing was left on the socket: the same connection reads its own replies.
    assert.deepEqual((await c.query('SELECT 1')).rows, [{ x: 1 }]);
    assert.deepEqual(await collect(c.stream('SELECT id FROM big')), [{ id: 1 }]);
    assert.equal(c.stream.resultSets, null, 'a later stream starts clean');
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('abandoning a stream drains the rest so the next statement reads its own reply', async () => {
  const chunks = [];
  for (let i = 0; i < 10; i++) chunks.push(F.chunk([[i]]));
  const script = {
    'SELECT id FROM big': [F.header(['id']), ...chunks, F.end()],
    'SELECT 42': F.rows(['v'], [[42]]),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    for await (const row of c.stream('SELECT id FROM big')) { if (row.id === 1) break; }
    assert.equal(c.isUsable(), true);                       // drained, still the same connection
    assert.deepEqual((await c.query('SELECT 42')).rows, [{ v: 42 }]);
    // Explicit .return() and a throw out of the loop body both close it too.
    const it = c.stream('SELECT id FROM big');
    await it.next();
    await it.return();
    assert.deepEqual((await c.query('SELECT 42')).rows, [{ v: 42 }]);
    await assert.rejects((async () => { for await (const _ of c.stream('SELECT id FROM big')) throw new Error('mine'); })(), /mine/);
    assert.deepEqual((await c.query('SELECT 42')).rows, [{ v: 42 }]);
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('abandoning a stream with too much left drops the connection, and the next statement re-dials', async () => {
  const bigRow = ['x'.repeat(200 * 1024)];
  const chunks = [];
  for (let i = 0; i < 60; i++) chunks.push(F.chunk([bigRow]));   // ~12 MB > 8 MB drain budget
  const script = {
    'SELECT blob FROM big': [F.header(['blob']), ...chunks, F.end()],
    'SELECT 42': F.rows(['v'], [[42]]),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    for await (const _ of c.stream('SELECT blob FROM big')) break;
    assert.equal(c.isUsable(), false);
    assert.deepEqual((await c.query('SELECT 42')).rows, [{ v: 42 }]);
    assert.equal(srv.connections, 2);
    assert.equal(srv.hellos.length, 2);                      // Hello again after the reconnect
    await c.end();
  });
});

test('statements on one client are serialized, even around a stream', async () => {
  const script = {
    'SELECT id FROM big': [F.header(['id']), F.chunk([[1]]), F.chunk([[2]]), F.end()],
    'SELECT 42': F.rows(['v'], [[42]]),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const it = c.stream('SELECT id FROM big');
    assert.deepEqual((await it.next()).value, { id: 1 });
    const queued = c.query('SELECT 42');                     // queues behind the open stream
    let settled = false; queued.then(() => { settled = true; });
    await new Promise((r) => setTimeout(r, 30));
    assert.equal(settled, false);
    await it.return();
    assert.deepEqual((await queued).rows, [{ v: 42 }]);
    await c.end();
  });
});

// Every row a stream yields (a rejection if it throws).
const collect = async (it) => { const rows = []; for await (const r of it) rows.push(r); return rows; };

// OP_QUERY_STREAM exactly as a stream without values has always sent it.
const queryStreamBytes = (consistency, sql) => {
  const b = Buffer.from(sql, 'utf8');
  return Buffer.concat([Buffer.from([5, consistency]), I.u32le(b.length), b]);
};

test('stream with values sends one OP_QUERY_STREAM_PARAMS: the ? text, then the parameter vector OP_EXECUTE carries', async () => {
  const sql = 'SELECT id FROM t WHERE ts >= ? AND tag = ? AND tags = ? AND meta = ? AND gone = ? AND big = ?';
  const script = {
    [sql]: [F.header(['id']), F.chunk([[7], [8]]), F.chunk([[9]]), F.end()],
    'SELECT b, a, c FROM t WHERE b = ? AND a = ? OR c = ?': (req) => [F.header(['b', 'a', 'c']), F.chunk([req.params]), F.end()],
    'SELECT 42': F.rows(['v'], [[42]]),
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const text = 'SELECT id FROM t WHERE ts >= $1 AND tag = $2 AND tags = $3 AND meta = $4 AND gone = $5 AND big = $6';
    const values = [new Date(5), "o'neil", ['a', 'b'], { k: 1.5, n: null }, null, 2n ** 62n];
    assert.deepEqual(await collect(c.stream(text, values)), [{ id: 7 }, { id: 8 }, { id: 9 }]);
    assert.deepEqual(c.stream.columns, ['id']);
    assert.equal(c.stream.command, 'SELECT');
    const req = srv.requests.find((q) => q.op === 9);
    // Built independently: opcode 9, the consistency (the client's QUORUM),
    // the u32 length and the `?` text, the u16 count, each value length-prefixed.
    const body = Buffer.from(sql, 'utf8');
    const count = Buffer.alloc(2); count.writeUInt16LE(values.length, 0);
    const want = [Buffer.from([9, 1]), I.u32le(body.length), body, count];
    for (const v of values) { const b = I.encodeValue(v); want.push(I.u32le(b.length), b); }
    assert.deepEqual(req.raw, Buffer.concat(want));
    assert.deepEqual(req.params, values);                            // typed, lossless
    assert.equal(srv.requests.filter((q) => q.op === 2 || q.op === 3).length, 0);   // one request, no prepare
    // `$N` in wire order (a repeat travels twice); a per-statement consistency and rowMode.
    assert.deepEqual(await collect(c.stream('SELECT b, a, c FROM t WHERE b = $2 AND a = $1 OR c = $2', ['x', 'y'],
      { consistency: 'ONE', rowMode: 'array' })), [['y', 'x', 'y']]);
    assert.equal(srv.requests[srv.requests.length - 1].consistency, 0);
    // Abandoned early, it drains like any stream: same connection, the next reply its own.
    for await (const row of c.stream(text, values)) { assert.deepEqual(row, { id: 7 }); break; }
    assert.deepEqual((await c.query('SELECT 42')).rows, [{ v: 42 }]);
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('stream without values sends the original OP_QUERY_STREAM byte for byte, in every call form', async () => {
  const script = {
    'SELECT id FROM t': (req) => [F.header(['id', 'c']), F.chunk([[1, req.consistency]]), F.end()],
    "SELECT '$1', $1": [F.header(['a']), F.end()],
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', consistency: 'ALL' });
    await c.connect();
    const sent = () => srv.requests[srv.requests.length - 1].raw;
    // The forms from before stream() took values: (sql) and (sql, opts).
    assert.deepEqual(await collect(c.stream('SELECT id FROM t')), [{ id: 1, c: 2 }]);
    assert.deepEqual(sent(), queryStreamBytes(2, 'SELECT id FROM t'));
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', { consistency: 'ONE' })), [{ id: 1, c: 0 }]);
    assert.deepEqual(sent(), queryStreamBytes(0, 'SELECT id FROM t'));
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', { rowMode: 'array' })), [[1, 2]]);
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', { consistency: 'quorum', rowMode: 'array' })), [[1, 1]]);
    assert.deepEqual(sent(), queryStreamBytes(1, 'SELECT id FROM t'));
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', {})), [{ id: 1, c: 2 }]);
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', undefined)), [{ id: 1, c: 2 }]);
    assert.deepEqual(sent(), queryStreamBytes(2, 'SELECT id FROM t'));
    // Nothing to bind is the same request: no values, or values the text
    // never references (ignored, as query() ignores them).
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', [])), [{ id: 1, c: 2 }]);
    assert.deepEqual(sent(), queryStreamBytes(2, 'SELECT id FROM t'));
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', null, { rowMode: 'array' })), [[1, 2]]);
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', [], { consistency: 'ONE' })), [{ id: 1, c: 0 }]);
    assert.deepEqual(sent(), queryStreamBytes(0, 'SELECT id FROM t'));
    assert.deepEqual(await collect(c.stream('SELECT id FROM t', [5])), [{ id: 1, c: 2 }]);
    assert.deepEqual(sent(), queryStreamBytes(2, 'SELECT id FROM t'));
    // Without values `$N` is left alone: the text goes out exactly as given.
    assert.deepEqual(await collect(c.stream("SELECT '$1', $1")), []);
    assert.deepEqual(sent(), queryStreamBytes(2, "SELECT '$1', $1"));
    assert.equal(srv.requests.filter((q) => q.op === 9).length, 0);
    await c.end();
  });
});

test('stream takes values as its second argument or in opts; opts.values wins, as config.values does for query()', async () => {
  const script = { 'SELECT v FROM t WHERE id > ?': (req) => [F.header(['v']), F.chunk([[req.params[0]]]), F.end()] };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const text = 'SELECT v FROM t WHERE id > $1';
    assert.deepEqual(await collect(c.stream(text, [3])), [{ v: 3 }]);
    assert.deepEqual(await collect(c.stream(text, { values: [4], rowMode: 'array' })), [[4]]);
    assert.deepEqual(await collect(c.stream(text, [1], { values: [5] })), [{ v: 5 }]);
    assert.deepEqual(await collect(c.stream(text, null, { values: [6], consistency: 'ALL' })), [{ v: 6 }]);
    const last = srv.requests[srv.requests.length - 1];
    assert.deepEqual([last.op, last.consistency], [9, 2]);
    await c.end();
  });
});

test('a server older than 0.330 refuses a stream with values; the error says so and the connection stays usable', async () => {
  const unknown = 'protocol error: malformed message: unknown opcode';
  const knows = { params: false, streaming: true };        // what the server understands
  const handle = (req) => {
    if ((req.op === 9 && !knows.params) || (req.op === 5 && !knows.streaming)) return F.error(unknown);
    if (req.op === 9) return F.error('statement expects 2 parameters, got 1');
    if (req.op === 5) return [F.header(['id']), F.chunk([[1]]), F.end()];
    if (req.op === 1) return F.rows(['v'], [[42]]);
    return undefined;                                       // Hello -> Ddl
  };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    let refused;
    await assert.rejects(collect(c.stream('SELECT id FROM t WHERE id > $1', [1])), (e) => {
      refused = e;
      return e instanceof SkaidbError
        && e.message === `server does not support parameterised streaming (skaidb >= 0.330): ${unknown}`;
    });
    assert.equal(I.memberFault(refused), false);            // a statement error, not the member's fault
    assert.equal(c.isUsable(), true);
    assert.equal(c.stream.columns, null);
    // The same connection carries on: a stream without values works, so does a query.
    assert.deepEqual(await collect(c.stream('SELECT id FROM t')), [{ id: 1 }]);
    assert.deepEqual((await c.query('SELECT 42')).rows, [{ v: 42 }]);
    // A server that knows the opcode: its errors arrive as they are.
    knows.params = true;
    await assert.rejects(collect(c.stream('SELECT id FROM t WHERE id > $1', [1])),
      (e) => e instanceof SkaidbError && e.message === 'statement expects 2 parameters, got 1');
    // One without streaming at all keeps its own message.
    knows.streaming = false;
    await assert.rejects(collect(c.stream('SELECT id FROM t')),
      (e) => e instanceof SkaidbError && e.message === `server does not support streaming: ${unknown}`);
    assert.equal(c.isUsable(), true);
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('stream rejects values it cannot bind before sending anything', async () => {
  await withServer({}, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const before = srv.requests.length;
    await assert.rejects(collect(c.stream('SELECT * FROM t WHERE id = ?', [1])),
      (e) => e instanceof SkaidbError
        && e.message === "this driver uses $1, $2 … placeholders; '?' is not a placeholder");
    await assert.rejects(collect(c.stream('SELECT $1, $3', [1, 2])), /no parameter for \$3/);
    await assert.rejects(collect(c.stream('SELECT $1', [NaN])), /cannot bind NaN\/Infinity/);
    await assert.rejects(collect(c.stream('SELECT $1', { consistency: 'ONE' }, { rowMode: 'array' })),
      (e) => e instanceof SkaidbError && e.message === 'stream() values must be an array');
    await assert.rejects(collect(c.stream(`SELECT ${Array(65536).fill('$1').join(', ')}`, [1])),
      (e) => e instanceof SkaidbError && e.message.startsWith('too many parameters: 65536'));
    assert.equal(srv.requests.length, before);                   // nothing reached the server
    assert.equal(c.isUsable(), true);
    assert.equal((await c.query('SELECT 1')).command, 'DDL');    // the chain is alive
    await c.end();
  });
});

test('batch sends one OP_EXECUTE_BATCH with typed rows and returns the total', async () => {
  const batches = [];
  const handle = scripted({}, {
    prepared: { 'INSERT INTO t (id, tags) VALUES (?, ?)': 2 },
    onExecute: () => F.error('unexpected'),
    onBatch: (sql, req) => { batches.push({ sql, req }); return F.mutation(req.rows.length); },
  });
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', consistency: 'ALL' });
    await c.connect();
    const rows = [[1, ['a']], [2, []], [3, ['b', 'c']]];
    assert.equal(await c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', rows), 3);
    assert.equal(batches.length, 1);
    assert.equal(batches[0].sql, 'INSERT INTO t (id, tags) VALUES (?, ?)');
    assert.equal(batches[0].req.consistency, 2);
    assert.deepEqual(batches[0].req.rows, rows);
    // A per-batch consistency overrides the client's.
    assert.equal(await c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', rows, { consistency: 'ONE' }), 3);
    assert.equal(batches[1].req.consistency, 0);
    assert.throws(() => c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', rows, { consistency: 'SOME' }),
      /bad consistency/);
    assert.equal(await c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', []), 0);
    await assert.rejects(c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', [[1, ['a']], [2]]), /no parameter for \$2/);
    await assert.rejects(c.batch('CREATE TABLE x (PRIMARY KEY (id))', [[1]]), /cannot be prepared/);
    await c.end();
  });
});

test('connectTimeout bounds the handshake too: a peer that accepts the connection and never answers is given up on', async () => {
  // Accepts the connection, reads what it is sent and never answers.
  const accepted = [];
  const mute = (sock) => { accepted.push(sock); sock.on('error', () => {}); sock.resume(); };
  const silent = net.createServer(mute);
  await new Promise((r) => silent.listen(0, '127.0.0.1', r));
  const silentPort = silent.address().port;
  const timedOut = /^no reachable endpoint in 127\.0\.0\.1:\d+: connect timeout$/;
  try {
    const c = new Client({ host: '127.0.0.1', port: silentPort, password: 'secret', connectTimeout: 150 });
    const t0 = Date.now();
    await assert.rejects(settles(c.connect()), (e) => e instanceof SkaidbError && timedOut.test(e.message));
    assert.ok(Date.now() - t0 >= 140, 'not before the timeout');
    // The socket is closed, not left to the peer, and the client is as it
    // is after any failed connect.
    await until(() => accepted.length === 1 && accepted[0].destroyed);
    assert.equal(c.isUsable(), false);
    await assert.rejects(c.query('SELECT 1'), /not connected: await connect\(\) first/);

    // The timer ends with the attempt: a connect that failed at once (here
    // refused) does not keep the process alive for the rest of the timeout.
    const timers = () => process.getActiveResourcesInfo().filter((r) => r === 'Timeout').length;
    const before = timers();
    const dead = net.createServer();
    await new Promise((r) => dead.listen(0, '127.0.0.1', r));
    const deadPort = dead.address().port;
    await new Promise((r) => dead.close(r));
    const refused = new Client({ host: '127.0.0.1', port: deadPort, password: 'secret', connectTimeout: 60000 });
    await assert.rejects(refused.connect(), /connect failed/);
    assert.equal(timers(), before);

    await withServer({ handle: (req) => (req.op === 1 ? F.rows(['v'], [[1]]) : undefined) }, async (srv) => {
      // The walk goes on to the next seed, and the socket it gave up on —
      // whose close arrives later — is not taken for the new connection's.
      for (let i = 0; i < 6; i++) {
        const d = new Client({ seeds: [`127.0.0.1:${silentPort}`, `127.0.0.1:${srv.port}`], password: 'secret', connectTimeout: 150 });
        await settles(d.connect());
        assert.equal(d.port, srv.port);
        await new Promise((r) => setTimeout(r, 20));
        assert.equal(d.isUsable(), true);
        assert.deepEqual((await d.query('SELECT 1')).rows, [{ v: 1 }]);
        await d.end();
      }
      // A re-dial is a connect like any other: a statement on a client whose
      // server stopped answering handshakes fails after the timeout, each time.
      const e = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', connectTimeout: 150 });
      await e.connect();
      const answer = srv._onConnection;
      srv._onConnection = mute;
      srv.closeAll();
      await until(() => !e.isUsable());
      for (let i = 0; i < 2; i++) {
        await assert.rejects(settles(e.query('SELECT 1')), (x) => x instanceof SkaidbError && timedOut.test(x.message));
      }
      srv._onConnection = answer;
      assert.deepEqual((await settles(e.query('SELECT 1'))).rows, [{ v: 1 }]);
      await e.end();
    });
  } finally {
    for (const sock of accepted) sock.destroy();
    await new Promise((r) => silent.close(r));
  }
});

test('end() lets go of a connection a stream still holds: later statements reject instead of waiting behind it', async () => {
  const handle = (req) => (req.op === 5 ? [F.header(['id']), F.chunk([[1], [2]]), F.chunk([[3]]), F.end()] : F.rows(['v'], [[1]]));
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const it = c.stream('SELECT id FROM big');
    assert.deepEqual((await it.next()).value, { id: 1 });
    const queued = c.query('SELECT 1');                        // waits behind the open stream
    queued.catch(() => {});
    await settles(c.end());
    const closed = (e) => e instanceof SkaidbError && e.message === 'connection is closed';
    await assert.rejects(settles(queued), closed);             // … and was never sent
    await assert.rejects(settles(c.query('SELECT 1')), closed);
    await assert.rejects(settles(c.batch('INSERT INTO t (id) VALUES ($1)', [[1]])), closed);
    await assert.rejects(settles(collect(c.stream('SELECT 1'))), closed);
    await assert.rejects(settles(c.transaction(async () => {})), closed);
    assert.equal(srv.requests.filter((q) => q.op === 1).length, 0);
    // The stream itself is not cut short by end(): what the server had sent is still read.
    assert.deepEqual(await settles(collect(it)), [{ id: 2 }, { id: 3 }]);
  });
});

test('seeds: a dead endpoint is skipped, a live one wins', async () => {
  const dead = net.createServer();
  await new Promise((r) => dead.listen(0, '127.0.0.1', r));
  const deadPort = dead.address().port;
  await new Promise((r) => dead.close(r));                   // port now refuses connections
  await withServer({}, async (srv) => {
    const c = new Client({ seeds: [`127.0.0.1:${deadPort}`, `127.0.0.1:${srv.port}`], password: 'secret', connectTimeout: 2000 });
    await c.connect();
    assert.equal(c.port, srv.port);
    await c.end();
    const d = new Client({ seeds: [`127.0.0.1:${deadPort}`], password: 'secret', connectTimeout: 500 });
    await assert.rejects(d.connect(), /no reachable endpoint in 127\.0\.0\.1:\d+: connect failed/);
  });
});

test('seeds: one that closes the connection mid-handshake does not fail the rest of the walk', async () => {
  await withServer({}, async (bad) => {
    bad.refuse = true;                                        // accepts TCP, then hangs up
    await withServer({}, async (srv) => {
      // Shuffled per connect: over ten tries the refusing seed comes first
      // in all but a 1-in-1024 run, and the next seed must still win.
      for (let i = 0; i < 10; i++) {
        const c = new Client({ seeds: [`127.0.0.1:${bad.port}`, `127.0.0.1:${srv.port}`], password: 'secret' });
        await c.connect();
        assert.equal(c.port, srv.port);
        await c.end();
      }
    });
  });
});

test('a connection the server dropped is re-dialled on the next statement; prepared cache is reset', async () => {
  let prepares = 0;
  const handle = (req) => {
    if (req.op === 2) { prepares++; return F.prepared(1, 1); }
    if (req.op === 3) return F.rows(['v'], [[req.params[0]]]);
    return F.ddl();
  };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', database: 'app' });
    await c.connect();
    assert.deepEqual((await c.query('SELECT $1', [1])).rows, [{ v: 1 }]);
    srv.closeAll();
    await until(() => !c.isUsable());
    assert.deepEqual((await c.query('SELECT $1', [2])).rows, [{ v: 2 }]);
    assert.equal(c.isUsable(), true);
    assert.equal(srv.connections, 2);
    assert.equal(prepares, 2);                                 // re-prepared on the new socket
    assert.equal(srv.requests.filter((q) => q.op === 1 && q.sql === 'USE "app"').length, 2);
    await c.end();
    await assert.rejects(c.query('SELECT 1'), /connection is closed/);
  });
});

// A server that keeps each connection's session database, as the real one
// does: a new connection is in `default`, `USE <name>` enters a database
// (refused for a name in `refuse`), and every other statement is recorded in
// `ran` with the database it ran in and the socket it arrived on.
function sessionServer() {
  const ran = [];
  const refuse = new Set();
  const used = (sql) => {
    const m = /^\s*use\s+(?:database\s+)?"?([^";\s]+)"?\s*;?\s*(?:--.*)?$/i.exec(sql);
    return m ? m[1] : null;
  };
  const handle = (req, conn) => {
    if (conn.db === undefined) { conn.db = 'default'; conn.statements = []; }
    const record = (sql) => ran.push({ sql, db: conn.db, sock: conn.sock });
    switch (req.op) {
      case 1: case 5: {
        const name = used(req.sql);
        if (name !== null) {
          if (refuse.has(name)) return F.error(`database "${name}" does not exist`);
          conn.db = name;
          return F.ddl();
        }
        record(req.sql);
        const select = /^select/i.test(req.sql);
        if (req.op === 5) return select ? [F.header(['db']), F.chunk([[conn.db]]), F.chunk([[conn.db]]), F.end()] : F.ddl();
        return select ? F.rows(['db'], [[conn.db]]) : F.mutation(1);
      }
      case 2:
        conn.statements.push(req.sql);
        return F.prepared(conn.statements.length - 1, (req.sql.match(/\?/g) || []).length);
      case 3: record(conn.statements[req.id]); return F.mutation(1);
      default: return undefined;                       // Hello -> Ddl
    }
  };
  return { handle, ran, refuse, where: () => ran.map((r) => `${r.db}: ${r.sql}`) };
}

// Check E's scenario: the connection is lost while the client is idle, then a
// count and an insert. Resolves to what the two statements answered.
async function countThenInsertAfterALoss(c, srv) {
  srv.closeAll();
  await until(() => !c.isUsable());
  const count = await c.query('SELECT count(*) FROM notes');
  const insert = await c.query('INSERT INTO notes (id) VALUES ($1)', [7]);
  return [count.rows[0].db, insert.rowCount];
}

test('the database option is re-entered on a re-dialled connection before anything else is sent', async () => {
  const session = sessionServer();
  await withServer({ handle: session.handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', database: 'app' });
    await c.connect();
    assert.equal(c.database, 'app');
    assert.deepEqual(await countThenInsertAfterALoss(c, srv), ['app', 1]);
    assert.deepEqual(session.where(), ['app: SELECT count(*) FROM notes', 'app: INSERT INTO notes (id) VALUES (?)']);
    assert.equal(srv.connections, 2);
    // On the new connection the USE is the first statement after the Hello.
    const second = srv.requests.slice(srv.requests.findIndex((q, i) => i > 0 && q.op === 8));
    assert.deepEqual(second.slice(0, 2).map((q) => q.sql || q.op), [8, 'USE "app"']);

    // The database cannot be entered on the new connection: the statement
    // fails with that error and runs nowhere, each time, until it can.
    session.ran.length = 0;
    session.refuse.add('app');
    srv.closeAll();
    await until(() => !c.isUsable());
    for (let i = 0; i < 2; i++) {
      await assert.rejects(c.query('INSERT INTO notes (id) VALUES (8)'),
        (e) => e instanceof SkaidbError && /database "app" does not exist/.test(e.message));
    }
    assert.deepEqual(session.where(), []);
    session.refuse.clear();
    assert.equal((await c.query('INSERT INTO notes (id) VALUES (8)')).rowCount, 1);
    assert.deepEqual(session.where(), ['app: INSERT INTO notes (id) VALUES (8)']);
    await c.end();
  });
});

test('a database entered with a USE statement is re-entered on a re-dialled connection', async () => {
  const session = sessionServer();
  await withServer({ handle: session.handle }, async (srv) => {
    // Every form the server takes for it, through query() and through stream().
    const forms = [
      (c) => c.query('USE app'),
      (c) => c.query('use database app'),
      (c) => c.query('  USE "app" ;  '),
      (c) => c.query('USE app -- the application\'s own'),
      (c) => c.query({ text: 'USE app' }),
      (c) => collect(c.stream('USE app')),
    ];
    for (const [i, enter] of forms.entries()) {
      session.ran.length = 0;
      const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
      await c.connect();
      assert.equal(c.database, null);
      await enter(c);
      assert.equal(c.database, 'app', `form ${i}`);
      assert.deepEqual(await countThenInsertAfterALoss(c, srv), ['app', 1], `form ${i}`);
      assert.deepEqual(session.where(), ['app: SELECT count(*) FROM notes', 'app: INSERT INTO notes (id) VALUES (?)'], `form ${i}`);
      await c.end();
    }

    // The last database entered is the one that counts, over the option too;
    // a USE the server refused changes nothing.
    session.ran.length = 0;
    session.refuse.add('nope');
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', database: 'first' });
    await c.connect();
    await c.query('USE second');
    await c.query('USE app');
    await assert.rejects(c.query('USE nope'), /database "nope" does not exist/);
    await assert.rejects(collect(c.stream('USE nope')), /database "nope" does not exist/);
    assert.equal(c.database, 'app');
    assert.deepEqual(await countThenInsertAfterALoss(c, srv), ['app', 1]);

    // It cannot be entered on the new connection: the statement fails with
    // that error and runs nowhere — not in the option's database either.
    session.ran.length = 0;
    session.refuse.add('app');
    srv.closeAll();
    await until(() => !c.isUsable());
    await assert.rejects(c.query('INSERT INTO notes (id) VALUES (8)'),
      (e) => e instanceof SkaidbError && /database "app" does not exist/.test(e.message));
    assert.deepEqual(session.where(), []);
    await c.end();
  });
});

test('Pool: a borrower\'s USE survives a re-dial of its connection and is not the next borrower\'s database', async () => {
  const session = sessionServer();
  await withServer({ handle: session.handle }, async (srv) => {
    const pool = new Pool({ host: '127.0.0.1', port: srv.port, password: 'secret', database: 'app', maxsize: 1 });
    try {
      // A checked-out connection that is re-dialled re-enters the pool's database …
      const conn = await pool.acquire();
      assert.deepEqual(await countThenInsertAfterALoss(conn, srv), ['app', 1]);
      // … or the one its borrower entered since.
      await conn.query('USE other');
      assert.deepEqual(await countThenInsertAfterALoss(conn, srv), ['other', 1]);
      await pool.release(conn);
      // Back in the pool it is in the pool's database again: the next borrower gets that.
      const next = await pool.acquire();
      assert.equal(next, conn);
      assert.equal(next.database, 'app');
      assert.equal((await next.query('SELECT count(*) FROM notes')).rows[0].db, 'app');
      await pool.release(next);
      assert.equal(srv.connections, 3);
      // The pool's database cannot be entered again: the connection is closed, not pooled.
      await pool.withConnection((c) => c.query('USE other'));
      assert.equal(pool.members()[0].idle, 1);
      session.refuse.add('app');
      const stuck = await pool.acquire();
      await stuck.query('USE other');
      await pool.release(stuck);
      assert.equal(pool.members()[0].idle, 0);
      assert.equal(stuck.isUsable(), false);
      // Nor can it be while a stream the borrower left open holds the
      // connection: closed as well, without waiting behind the stream.
      session.refuse.clear();
      const busy = await pool.acquire();
      await busy.query('USE other');
      assert.deepEqual((await busy.stream('SELECT 1').next()).value, { db: 'other' });   // one row read, never closed
      await settles(pool.release(busy));
      assert.equal(pool.members()[0].idle, 0);
      assert.equal(busy.isUsable(), false);
    } finally {
      await pool.end();
    }
  });
});

test('a connection reset under a statement rejects it with a SkaidbError, as any lost connection does', async () => {
  // The socket reports a reset as an 'error' (ECONNRESET), not as a close.
  const sent = [];
  const handle = (req, conn) => {
    if (req.op !== 1) return undefined;
    sent.push(req.sql);
    if (req.sql === 'UPDATE resets') { conn.sock.resetAndDestroy(); return []; }
    return F.rows(['v'], [[1]]);
  };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const lost = (e) => e instanceof SkaidbError && /^connection lost: .*ECONNRESET/.test(e.message)
      && e.cause instanceof Error && e.cause.code === 'ECONNRESET' && I.memberFault(e);
    await assert.rejects(c.query('UPDATE resets'), lost);
    assert.equal(c.isUsable(), false);
    assert.deepEqual((await c.query('SELECT 1')).rows, [{ v: 1 }]);    // re-dialled, as after any loss
    assert.equal(srv.connections, 2);
    // Inside a transaction: the statement gets that error, the rest of the
    // body the usual one, and nothing more is sent.
    sent.length = 0;
    const caught = [];
    await assert.rejects(c.transaction(async (tx) => {
      caught.push(await tx.query('UPDATE resets').catch((e) => e));
      caught.push(await tx.query('UPDATE after').catch((e) => e));
      throw caught[0];
    }), (e) => e === caught[0] && lost(e));
    assert.ok(caught[1] instanceof SkaidbError && caught[1].message === 'connection lost during a transaction');
    assert.deepEqual(sent, ['BEGIN', 'UPDATE resets']);
    await c.end();
  });
});

// Every way a statement leaves a client, each as a call returning a promise.
// `ran` collects a mark each time a transaction body is entered.
const entryPoints = (c, ran) => ({
  'query(text)': () => c.query('SELECT 1'),
  'query(text, values)': () => c.query('SELECT $1', [1]),
  'query(config)': () => c.query({ text: 'SELECT $1', values: [1] }),
  'batch': () => c.batch('INSERT INTO t (id) VALUES ($1)', [[1], [2]]),
  'batch without rows': () => c.batch('INSERT INTO t (id) VALUES ($1)', []),
  'stream': () => collect(c.stream('SELECT 1')),
  'stream with values': () => collect(c.stream('SELECT $1', [1])),
  'transaction': () => c.transaction(async (tx) => { ran.push('body'); await tx.query('SELECT 1'); }),
  'subscribe': () => collect(c.subscribe('events')),
});

// Each entry point of `c` rejects with a SkaidbError whose message is `message`.
async function rejectsEverywhere(c, message, what) {
  const ran = [];
  for (const [name, call] of Object.entries(entryPoints(c, ran))) {
    await assert.rejects(call(), (e) => e instanceof SkaidbError && e.message === message, `${what}: ${name}`);
  }
  assert.deepEqual(ran, [], `${what}: the transaction body never runs`);
}

test('a client that is not connected rejects every statement with a SkaidbError saying so', async () => {
  const NOT_CONNECTED = 'not connected: await connect() first';
  const handle = (req) => (req.op === 1 ? F.rows(['v'], [[1]]) : req.op === 5 ? F.ddl() : undefined);
  await withServer({ handle }, async (srv) => {
    // Constructed, never connected: nothing is dialled and nothing is sent.
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await rejectsEverywhere(c, NOT_CONNECTED, 'never connected');
    assert.equal(srv.connections, 0);
    assert.equal(c.isUsable(), false);
    // While connect() is still running.
    const connecting = c.connect();
    await rejectsEverywhere(c, NOT_CONNECTED, 'connect() in flight');
    await connecting;
    // Once connected the same client works: the refusals left nothing behind.
    assert.deepEqual((await c.query('SELECT 1')).rows, [{ v: 1 }]);
    assert.equal(await c.transaction(async (tx) => (await tx.query('SELECT 1')).rows[0].v), 1);
    assert.deepEqual(await collect(c.stream('SELECT 1')), []);
    assert.equal(srv.connections, 1);
    await c.end();

    // connect() failed: refused credentials, and nothing listening at all.
    const denied = new Client({ host: '127.0.0.1', port: srv.port, password: 'nope' });
    await assert.rejects(denied.connect(), /authentication denied/);
    await rejectsEverywhere(denied, NOT_CONNECTED, 'connect() denied');
    const dead = net.createServer();
    await new Promise((r) => dead.listen(0, '127.0.0.1', r));
    const deadPort = dead.address().port;
    await new Promise((r) => dead.close(r));
    const unreachable = new Client({ host: '127.0.0.1', port: deadPort, password: 'secret', connectTimeout: 500 });
    await assert.rejects(unreachable.connect(), /no reachable endpoint/);
    await rejectsEverywhere(unreachable, NOT_CONNECTED, 'connect() refused');
    await unreachable.end();
  });
  // A handshake that has not finished: the socket is there, the connection
  // is not. A statement must not be written into the middle of the handshake.
  const frames = [];
  const silent = net.createServer((sock) => { sock.on('data', (d) => frames.push(d)); sock.on('error', () => {}); });
  await new Promise((r) => silent.listen(0, '127.0.0.1', r));
  const stalled = new Client({ host: '127.0.0.1', port: silent.address().port, password: 'secret' });
  const never = stalled.connect();                    // AuthStart goes out; no challenge ever comes
  never.catch(() => {});
  await until(() => frames.length === 1);
  await rejectsEverywhere(stalled, NOT_CONNECTED, 'handshake in flight');
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(frames.length, 1);                     // still only the AuthStart
  stalled._sock.destroy();
  await new Promise((r) => silent.close(r));
});

test('a closed client rejects every statement with "connection is closed", a closed pool with "pool is closed"', async () => {
  await withServer({ handle: (req) => (req.op === 1 ? F.rows(['v'], [[1]]) : undefined) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    await c.end();
    const sent = srv.requests.length;
    await rejectsEverywhere(c, 'connection is closed', 'closed');
    // Closed without ever having been connected: closed all the same.
    const never = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await never.end();
    await rejectsEverywhere(never, 'connection is closed', 'closed before connecting');
    assert.equal(srv.requests.length, sent);                   // nothing reached the server
    assert.equal(srv.connections, 1);

    // A pool has no such state: it dials when a connection is asked for …
    const pool = new Pool({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    assert.deepEqual((await pool.withConnection((p) => p.query('SELECT 1'))).rows, [{ v: 1 }]);
    assert.equal(await pool.transaction(async (tx) => (await tx.query('SELECT 1')).rows[0].v), 1);
    const held = await pool.acquire();
    await pool.end();
    // … and once closed refuses to hand one out, whichever way it is asked.
    const closed = (e) => e instanceof SkaidbError && e.message === 'pool is closed';
    const ran = [];
    await assert.rejects(pool.acquire(), closed);
    await assert.rejects(pool.withConnection(() => { ran.push('fn'); }), closed);
    await assert.rejects(pool.transaction(async () => { ran.push('body'); }), closed);
    assert.deepEqual(ran, []);
    // A connection checked out before the pool closed works until it is released; then it is closed.
    assert.deepEqual((await held.query('SELECT 1')).rows, [{ v: 1 }]);
    await pool.release(held);
    await rejectsEverywhere(held, 'connection is closed', 'released to a closed pool');
  });
});

test('end() resolves to nothing, the first time and when it is called again', async () => {
  await withServer({}, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    assert.equal(await c.end(), undefined);
    assert.equal(await c.end(), undefined);                    // not the socket's "already finished" error
    await until(() => srv.sockets.size === 0);
    const never = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    assert.equal(await never.end(), undefined);
  });
});

test('Pool reuses idle connections, bounds the idle set, and discards broken ones', async () => {
  await withServer({ handle: (req) => (req.op === 1 ? F.rows(['v'], [[1]]) : F.ddl()) }, async (srv) => {
    const pool = new Pool({ host: '127.0.0.1', port: srv.port, password: 'secret', maxsize: 1 });
    const a = await pool.acquire();
    const b = await pool.acquire();                            // burst above maxsize is allowed
    assert.equal(srv.connections, 2);
    await pool.release(a);
    await pool.release(b);                                     // surplus: closed
    assert.equal(b.isUsable(), false);
    const again = await pool.acquire();
    assert.equal(again, a);                                    // the idle one came back
    await pool.release(again);
    srv.closeAll();
    await until(() => !a.isUsable());
    const v = await pool.withConnection((c) => c.query('SELECT 1'));
    assert.deepEqual(v.rows, [{ v: 1 }]);
    assert.equal(srv.connections, 3);                          // the broken idle one was discarded
    await pool.end();
    assert.equal(pool.closed, true);
    await assert.rejects(pool.acquire(), /pool is closed/);
    assert.throws(() => new Pool({ maxsize: 0 }), /maxsize must be >= 1/);
  });
});

// Stream log ids as the server really issues them: opaque strings that sort
// in log order. A driver that treated them as numbers (`after: 0`) would ask
// for `id > 0`, which no such id satisfies.
const ID1 = '00000001789857620741-0000000000-0902800000000000000100';
const ID2 = '00000001789857620741-0000000000-0902800000000000000101';
const ID3 = '00000001789857620742-0000000000-0902800000000000000100';

test('subscribe pages a stream log with a resumable cursor', async () => {
  const pages = [
    F.rows(['id', 'op', 'k', 'ts', 'doc'], [[ID1, 'insert', 'a', new Date(1), { x: 1 }], [ID2, 'update', 'a', new Date(2), { x: 2 }]]),
    F.rows(['id', 'op', 'k', 'ts', 'doc'], []),
    F.rows(['id', 'op', 'k', 'ts', 'doc'], [[ID3, 'delete', 'a', new Date(3), null]]),
  ];
  let calls = 0;
  const handle = (req) => {
    if (req.op === 2) return F.prepared(5, 1);
    if (req.op === 1 || req.op === 3) return pages[Math.min(calls++, pages.length - 1)];
    return F.ddl();
  };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const got = [];
    for await (const ev of c.subscribe('orders', { pollMs: 5 })) {
      got.push(ev);
      if (got.length === 3) break;
    }
    assert.deepEqual(got.map((e) => [e.id, e.op]), [[ID1, 'insert'], [ID2, 'update'], [ID3, 'delete']]);
    assert.equal(typeof got[0].id, 'string');
    assert.ok(got[0].ts instanceof Date);
    const first = srv.requests.find((q) => q.op === 1 && /_stream_orders/.test(q.sql));
    assert.match(first.sql, /^SELECT id, op, k, ts, doc FROM _stream_orders ORDER BY id LIMIT 500$/);
    const resumed = srv.requests.find((q) => q.op === 2);
    assert.match(resumed.sql, /WHERE id > \? ORDER BY id LIMIT 500$/);
    // The cursor travels as the string it is, not coerced to a number.
    const exec = srv.requests.find((q) => q.op === 3);
    assert.deepEqual(exec.params, [ID2]);
    await c.end();
  });
});

// An idle subscription answered with empty pages, polling so slowly that a
// stop which waited out the sleep would time the test out.
const idleLog = (req) => (req.op === 1 || req.op === 3) ? F.rows(['id', 'op', 'k', 'ts', 'doc'], [])
  : req.op === 2 ? F.prepared(5, 1) : F.ddl();
const withinTick = (p, what) => Promise.race([
  p, new Promise((_, rej) => setTimeout(() => rej(new Error(`${what} did not settle within 50 ms`)), 50)),
]);

test('subscribe: return() on an idle iterator resolves promptly instead of waiting out the poll', async () => {
  await withServer({ handle: idleLog }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const it = c.subscribe('orders', { pollMs: 60_000 });
    const pending = it.next();                                   // fetches an empty page, then sleeps
    await until(() => srv.requests.some((q) => q.op === 1 && /_stream_orders/.test(q.sql)));
    await new Promise((r) => setTimeout(r, 20));                 // let it settle into the sleep
    const t0 = Date.now();
    const ret = await withinTick(it.return(), 'return()');
    assert.deepEqual(ret, { value: undefined, done: true });
    assert.deepEqual(await withinTick(pending, 'the pending next()'), { value: undefined, done: true });
    assert.ok(Date.now() - t0 < 50);
    const polls = srv.requests.filter((q) => q.op === 1 && /_stream_orders/.test(q.sql)).length;
    assert.equal(polls, 1);                                      // nothing was sent after the stop
    assert.equal(c.isUsable(), true);                            // the connection is untouched
    assert.deepEqual(await it.next(), { value: undefined, done: true });
    await c.end();
  });
});

test('subscribe: aborting the signal ends an idle iteration cleanly within a tick', async () => {
  await withServer({ handle: idleLog }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const ac = new AbortController();
    let events = 0;
    const loop = (async () => {
      for await (const ev of c.subscribe('orders', { pollMs: 60_000, signal: ac.signal })) { events++; void ev; }
      return 'ended';
    })();
    await until(() => srv.requests.some((q) => q.op === 1 && /_stream_orders/.test(q.sql)));
    await new Promise((r) => setTimeout(r, 20));
    ac.abort();
    assert.equal(await withinTick(loop, 'the for-await loop'), 'ended');   // no AbortError
    assert.equal(events, 0);
    assert.equal(srv.requests.filter((q) => q.op === 1 && /_stream_orders/.test(q.sql)).length, 1);

    // A signal that is already aborted yields nothing and sends nothing.
    const before = srv.requests.length;
    const dead = new AbortController(); dead.abort();
    assert.deepEqual(await c.subscribe('orders', { signal: dead.signal }).next(), { value: undefined, done: true });
    assert.equal(srv.requests.length, before);
    assert.throws(() => c.subscribe('orders', { signal: 'nope' }), /signal must be an AbortSignal/);
    await c.end();
  });
});

test('subscribe: a stop requested while a page is in flight takes effect after that page, without yielding', async () => {
  let release;
  const gate = new Promise((r) => { release = r; });
  const handle = (req, conn) => {
    if (req.op === 1 && /_stream_orders/.test(req.sql)) {
      // Answer the page only when the test says so, with one event in it.
      gate.then(() => conn.send(F.rows(['id', 'op', 'k', 'ts', 'doc'], [[ID1, 'insert', 'a', new Date(1), null]])));
      return [];
    }
    return F.ddl();
  };
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const it = c.subscribe('orders', { pollMs: 60_000 });
    const pending = it.next();
    await until(() => srv.requests.some((q) => q.op === 1 && /_stream_orders/.test(q.sql)));
    const ret = it.return();                                     // cannot interrupt the fetch itself
    release();
    assert.deepEqual(await withinTick(pending, 'the pending next()'), { value: undefined, done: true });
    assert.deepEqual(await withinTick(ret, 'return()'), { value: undefined, done: true });
    assert.equal(await c.query('SELECT 1').then(() => 'ok'), 'ok');   // connection at a request boundary
    await c.end();
  });
});

test('subscribe: aborting mid-page stops at the next event instead of draining the page', async () => {
  const page = F.rows(['id', 'op', 'k', 'ts', 'doc'],
    [[ID1, 'insert', 'a', new Date(1), null], [ID2, 'insert', 'b', new Date(2), null], [ID3, 'insert', 'c', new Date(3), null]]);
  const handle = (req) => (req.op === 1 && /_stream_orders/.test(req.sql)) ? page : F.ddl();
  await withServer({ handle }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const ac = new AbortController();
    const seen = [];
    for await (const ev of c.subscribe('orders', { pollMs: 60_000, signal: ac.signal })) {
      seen.push(ev.id);
      ac.abort();                                                // while suspended at the yield
    }
    assert.deepEqual(seen, [ID1]);
    assert.equal(srv.requests.filter((q) => q.op === 1 && /_stream_orders/.test(q.sql)).length, 1);
    await c.end();
  });
});

test('query rejects ? placeholders before sending anything', async () => {
  await withServer({}, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const before = srv.requests.length;
    await assert.rejects(c.query('SELECT * FROM t WHERE id = ?', [1]),
      (e) => e instanceof SkaidbError
        && e.message === "this driver uses $1, $2 … placeholders; '?' is not a placeholder");
    await assert.rejects(c.batch('INSERT INTO t VALUES (?)', [[1], [2]]), /'\?' is not a placeholder/);
    assert.equal(srv.requests.length, before);                   // nothing reached the server
    assert.equal(c.isUsable(), true);
    assert.equal((await c.query('SELECT 1')).command, 'DDL');    // the chain is alive
    await c.end();
  });
});
