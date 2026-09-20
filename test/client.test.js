'use strict';
// Client end to end against the in-process fake server: handshake, Hello,
// every request opcode, streaming (including the abandon/drain rule),
// batches, failover, reconnect and the pool.
const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('net');
const { Client, Pool, SkaidbError } = require('../skaidb');
const pkg = require('../package.json');
const { FakeServer, F } = require('./fake-server');

// A scripted handler: `script[sql]` answers OP_QUERY / OP_QUERY_STREAM text,
// `prepared[sql]` makes OP_PREPARE succeed with that arity, executes answer
// through `onExecute`.
function scripted(script, extra = {}) {
  const prepared = extra.prepared || {};
  let nextId = 100;
  const ids = new Map();
  return (req, conn) => {
    switch (req.op) {
      case 1: case 5: {
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

test('connect runs SCRAM, verifies the server signature and sends Hello with the package version', async () => {
  await withServer({}, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, user: 'ada', password: 'secret' });
    await c.connect();
    assert.equal(c.isUsable(), true);
    assert.equal(srv.hellos.length, 1);
    assert.equal(srv.hellos[0].name, 'nodejs');
    assert.equal(srv.hellos[0].version, pkg.version);
    assert.equal(pkg.version, '1.0.2');
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
  };
  await withServer({ handle: scripted(script) }, async (srv) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const got = [];
    for await (const row of c.stream('SELECT id FROM big')) got.push(row);
    assert.deepEqual(got, [{ id: 1 }, { id: 2 }, { id: 3 }]);
    assert.deepEqual(c.stream.columns, ['id']);
    const arr = [];
    for await (const row of c.stream('SELECT id FROM big', { rowMode: 'array' })) arr.push(row);
    assert.deepEqual(arr, [[1], [2], [3]]);
    // Streaming a non-row statement yields nothing and does not desync.
    const none = [];
    for await (const row of c.stream('INSERT INTO t VALUES (1)')) none.push(row);
    assert.deepEqual(none, []);
    assert.deepEqual((await c.query('SELECT id FROM t')).rows, [{ id: 9 }]);
    // Error before the header: a plain statement failure.
    await assert.rejects((async () => { for await (const _ of c.stream('SELECT early')) {} })(), /no such table/);
    // Error after the header: rows so far are valid, the stream ends, connection is fine.
    const partial = [];
    await assert.rejects((async () => { for await (const r of c.stream('SELECT boom')) partial.push(r); })(),
      /scan budget exceeded/);
    assert.deepEqual(partial, [{ id: 1 }]);
    assert.equal(c.isUsable(), true);
    assert.deepEqual((await c.query('SELECT id FROM t')).rows, [{ id: 9 }]);
    // A ResultSets reply on the stream opcode is one whole frame: refuse the CALL, keep the connection.
    await assert.rejects((async () => { for await (const _ of c.stream('CALL emits()')) {} })(), /unexpected response tag 8/);
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
    assert.equal(await c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', []), 0);
    await assert.rejects(c.batch('INSERT INTO t (id, tags) VALUES ($1, $2)', [[1, ['a']], [2]]), /no parameter for \$2/);
    await assert.rejects(c.batch('CREATE TABLE x (PRIMARY KEY (id))', [[1]]), /cannot be prepared/);
    await c.end();
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
