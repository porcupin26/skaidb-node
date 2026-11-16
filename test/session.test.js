'use strict';
// A connection's session — its database, its SET CONSISTENCY override, its
// scan budget and an open transaction — across what a client does behind the
// application's back: re-dialling a lost connection, and handing a pooled
// connection from one borrower to the next. Against an in-process server that
// keeps each connection's session, as the real one does.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Client, Pool, SkaidbError, _internal: I } = require('../skaidb');
const { FakeServer, F } = require('./fake-server');

const BUDGET_EXCEEDED = 'resource limit: scan budget exceeded (2 rows examined)';

/**
 * A scripted server with per-connection session state.
 *
 * - A new connection is in database `default`, with no consistency
 *   override, no scan budget and no transaction.
 * - `USE [DATABASE] <name>`, `SET CONSISTENCY …`, `SET SCAN BUDGET …`,
 *   `BEGIN …`, `COMMIT`, `ROLLBACK` act on the connection's session, as the
 *   real server's do (`ROLLBACK` with nothing open succeeds, `COMMIT` with
 *   nothing open is an error).
 * - `SELECT …` answers one row describing the session it ran in — `db`,
 *   `consistency` (the override, or null), `txn` — and `ids`, the committed
 *   rows; with a scan budget set it is refused, `scan budget exceeded`.
 * - `INSERT INTO <t> (id) VALUES (<n>)` adds a row: at once, or at COMMIT
 *   when a transaction is open on the connection.
 *
 * `log` holds every statement as `<n>: <sql>`, n numbering the connections
 * in the order they were made. `refuse` maps a statement (as logged, without
 * the number) to the error it is answered with; `gone` names databases that
 * do not exist; `dies` names statements under which the connection is cut.
 * Only OP_QUERY and OP_QUERY_STREAM are served: a statement with values
 * falls back to client-side literals and is logged as text.
 */
function sessionServer() {
  const log = [];
  const refuse = new Map();
  const gone = new Set();
  const dies = new Set();
  const ids = new Set();
  let connections = 0;
  const run = (sql, s) => {
    let m;
    if (refuse.has(sql)) return F.error(refuse.get(sql));
    if ((m = /^use (?:database )?"?([^"\s]+)"?$/i.exec(sql))) {
      if (gone.has(m[1])) return F.error(`database "${m[1]}" does not exist`);
      s.db = m[1];
      return F.ddl();
    }
    if (/^set scan budget default$/i.test(sql)) { s.budget = null; return F.ddl(); }
    if (/^set scan budget (rows|bytes) \d+/i.test(sql)) { s.budget = sql; return F.ddl(); }
    if ((m = /^set consistency (one|quorum|all|default)$/i.exec(sql))) {
      s.consistency = m[1].toUpperCase() === 'DEFAULT' ? null : m[1].toUpperCase();
      return F.ddl();
    }
    if (/^set consistency/i.test(sql)) return F.error('parse error: SET CONSISTENCY takes ONE, QUORUM, ALL, or DEFAULT');
    if (/^begin\b/i.test(sql)) {
      if (s.txn !== null) return F.error('constraint violation: a transaction is already in progress');
      s.txn = [];
      return F.ddl();
    }
    if (/^commit( transaction)?$/i.test(sql)) {
      if (s.txn === null) return F.error('constraint violation: no transaction in progress');
      for (const id of s.txn) ids.add(id);
      s.txn = null;
      return F.ddl();
    }
    if (/^rollback( transaction)?$/i.test(sql)) { s.txn = null; return F.ddl(); }
    if ((m = /^insert into \w+ \(id\) values \((\d+)\)$/i.exec(sql))) {
      if (s.txn !== null) s.txn.push(Number(m[1])); else ids.add(Number(m[1]));
      return F.mutation(1);
    }
    if (/^select\b/i.test(sql)) {
      if (s.budget !== null) return F.error(BUDGET_EXCEEDED);
      return F.rows(['db', 'consistency', 'txn', 'ids'], [[s.db, s.consistency, s.txn !== null, [...ids].sort((a, b) => a - b)]]);
    }
    return F.error(`session server: unknown statement ${sql}`);
  };
  const handle = (req, conn) => {
    if (conn.session === undefined) {
      conn.session = { n: ++connections, db: 'default', consistency: null, budget: null, txn: null };
    }
    if (req.op === 2) return F.error('statement kind cannot be prepared');
    if (req.op !== 1 && req.op !== 5) return undefined;                // Hello -> Ddl
    // As the server reads it: comments gone, one trailing `;` gone.
    const sql = req.sql.replace(/--[^\n]*/g, ' ').trim().replace(/\s*;$/, '').replace(/\s+/g, ' ');
    log.push(`${conn.session.n}: ${sql}`);
    if (dies.has(sql)) { conn.sock.destroy(); return []; }
    const answer = run(sql, conn.session);
    // A stream request is answered with stream frames when there are rows.
    if (req.op === 5 && answer[0] === 0) {
      const s = conn.session;
      return [F.header(['db', 'consistency', 'txn', 'ids']),
        F.chunk([[s.db, s.consistency, s.txn !== null, [...ids].sort((a, b) => a - b)]]), F.end()];
    }
    return answer;
  };
  return {
    handle, log, refuse, gone, dies, ids,
    /** What connection `n` was sent, in order. */
    on: (n) => log.filter((l) => l.startsWith(`${n}: `)).map((l) => l.slice(`${n}: `.length)),
  };
}

// Runs `fn(server, srv)` with a session server listening.
async function withSession(fn) {
  const server = sessionServer();
  const srv = await new FakeServer({ handle: server.handle }).start();
  try { return await fn(server, srv); } finally { await srv.stop(); }
}

const connected = async (srv, opts = {}) => {
  const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret', ...opts });
  await c.connect();
  return c;
};

const until = async (pred, ms = 2000) => {
  const t0 = Date.now();
  while (!pred()) {
    if (Date.now() - t0 > ms) throw new Error('timeout waiting');
    await new Promise((r) => setTimeout(r, 5));
  }
};

// The connection is lost while the client is idle, and the client knows.
async function lose(c, srv) {
  srv.closeAll();
  await until(() => !c.isUsable());
}

const collect = async (it) => { const rows = []; for await (const r of it) rows.push(r); return rows; };

// The session a SELECT ran in, as the server reports it.
const where = async (c) => (await c.query('SELECT count(*) FROM notes')).rows[0];

const isStatementError = (message) => (e) => e instanceof SkaidbError && e.message === message && !I.memberFault(e);

// ---- H: session settings survive a re-dial ------------------------------------

test('SET SCAN BUDGET and SET CONSISTENCY sent as statements are sent again on a re-dialled connection, after the database and before the statement', async () => {
  await withSession(async (server, srv) => {
    const c = await connected(srv);
    await c.query('USE app');
    await c.query('SET SCAN BUDGET ROWS 1');
    await c.query('SET CONSISTENCY ALL');
    await assert.rejects(where(c), isStatementError(BUDGET_EXCEEDED));
    await lose(c, srv);
    // The limit the application set still holds: the same scan fails the same way.
    await assert.rejects(where(c), isStatementError(BUDGET_EXCEEDED));
    assert.deepEqual(server.on(2), ['USE "app"', 'SET CONSISTENCY ALL', 'SET SCAN BUDGET ROWS 1', 'SELECT count(*) FROM notes']);

    // A setting cleared with DEFAULT before the loss is not sent again.
    await c.query('SET SCAN BUDGET DEFAULT');
    assert.deepEqual(await where(c), { db: 'app', consistency: 'ALL', txn: false, ids: [] });
    await lose(c, srv);
    assert.deepEqual(await where(c), { db: 'app', consistency: 'ALL', txn: false, ids: [] });
    assert.deepEqual(server.on(3), ['USE "app"', 'SET CONSISTENCY ALL', 'SELECT count(*) FROM notes']);
    await c.query('set consistency default ;');
    await lose(c, srv);
    assert.deepEqual(await where(c), { db: 'app', consistency: null, txn: false, ids: [] });
    assert.deepEqual(server.on(4), ['USE "app"', 'SELECT count(*) FROM notes']);

    // One note per kind, the last accepted statement wins; it is kept as it
    // was written, without its comments. Through stream() as well.
    await c.query('SET SCAN BUDGET ROWS 5');
    await c.query('  set scan budget\n  bytes 1000 rows 7 ; -- both');
    await c.query('SET CONSISTENCY ONE');
    await collect(c.stream('SET CONSISTENCY QUORUM -- back'));
    await c.query({ text: 'SET -- the level\n CONSISTENCY ALL' });
    // A statement the server refused changes nothing.
    await assert.rejects(c.query('SET CONSISTENCY SOME'), /SET CONSISTENCY takes/);
    server.refuse.set('SET SCAN BUDGET ROWS 9', 'permission denied');
    await assert.rejects(c.query('SET SCAN BUDGET ROWS 9'), /permission denied/);
    await lose(c, srv);
    await assert.rejects(where(c), isStatementError(BUDGET_EXCEEDED));
    assert.deepEqual(server.on(5), ['USE "app"', 'SET CONSISTENCY ALL', 'set scan budget bytes 1000 rows 7', 'SELECT count(*) FROM notes']);

    // connect() called again by the application is a new connection too.
    await c.connect();
    await assert.rejects(where(c), isStatementError(BUDGET_EXCEEDED));
    assert.deepEqual(server.on(6).slice(0, 3), ['USE "app"', 'SET CONSISTENCY ALL', 'set scan budget bytes 1000 rows 7']);
    await c.end();
  });
});

test('a setting the server refuses on the re-dialled connection fails the statement: it never runs under other settings than the application set', async () => {
  await withSession(async (server, srv) => {
    const c = await connected(srv, { database: 'app' });
    for (const [setting, sent] of [['SET CONSISTENCY ALL', ['USE "app"', 'SET CONSISTENCY ALL']],
      ['SET SCAN BUDGET ROWS 2 BYTES 4096', ['USE "app"', 'SET CONSISTENCY ALL', 'SET SCAN BUDGET ROWS 2 BYTES 4096']]]) {
      await c.query(setting);
      server.refuse.set(setting, `refused: ${setting}`);
      await lose(c, srv);
      server.log.length = 0;
      // The statement fails with the server's answer to the setting and is
      // sent nowhere, each time, until the setting is taken again.
      for (const run of [() => c.query('INSERT INTO notes (id) VALUES (1)'), () => where(c),
        () => collect(c.stream('SELECT count(*) FROM notes'))]) {
        await assert.rejects(run(), isStatementError(`refused: ${setting}`));
        assert.equal(c.isUsable(), false);
      }
      assert.deepEqual([...new Set(server.log.map((l) => l.replace(/^\d+: /, '')))], sent);
      assert.deepEqual([...server.ids], []);
      server.refuse.clear();
      await c.query('SET SCAN BUDGET DEFAULT');
      assert.deepEqual(await where(c), { db: 'app', consistency: 'ALL', txn: false, ids: [] });
    }
    await c.end();
  });
});

test('a pooled connection that is re-dialled while a borrower holds it gets that borrower\'s settings back', async () => {
  await withSession(async (server, srv) => {
    const pool = new Pool({ host: '127.0.0.1', port: srv.port, password: 'secret', database: 'app', maxsize: 1 });
    try {
      const conn = await pool.acquire();
      await conn.query('SET SCAN BUDGET ROWS 1');
      await conn.query('SET CONSISTENCY ONE');
      await lose(conn, srv);
      await assert.rejects(where(conn), isStatementError(BUDGET_EXCEEDED));
      assert.deepEqual(server.on(2), ['USE "app"', 'SET CONSISTENCY ONE', 'SET SCAN BUDGET ROWS 1', 'SELECT count(*) FROM notes']);
      await conn.query('SET SCAN BUDGET DEFAULT');
      assert.deepEqual(await where(conn), { db: 'app', consistency: 'ONE', txn: false, ids: [] });
      await conn.end();
    } finally {
      await pool.end();
    }
  });
});

test('sessionSetting reads exactly one SET CONSISTENCY or SET SCAN BUDGET statement, and nothing else', () => {
  const set = (kind, text) => ({ kind, text });
  const cases = [
    ['SET CONSISTENCY ALL', set('consistency', 'SET CONSISTENCY ALL')],
    ['set consistency one;', set('consistency', 'set consistency one')],
    ['  Set   Consistency\tQuorum  ;  ', set('consistency', 'Set Consistency Quorum')],
    ['SET CONSISTENCY ALL -- every replica', set('consistency', 'SET CONSISTENCY ALL')],
    ['-- first\nSET CONSISTENCY ALL', set('consistency', 'SET CONSISTENCY ALL')],
    ['SET -- a\n CONSISTENCY -- b\n ALL -- c\n ; -- d', set('consistency', 'SET CONSISTENCY ALL')],
    ['SET CONSISTENCY DEFAULT', set('consistency', null)],
    ['set consistency default ;', set('consistency', null)],
    ['SET SCAN BUDGET ROWS 1', set('scanBudget', 'SET SCAN BUDGET ROWS 1')],
    ['SET SCAN BUDGET ROWS 5000 BYTES 1048576;', set('scanBudget', 'SET SCAN BUDGET ROWS 5000 BYTES 1048576')],
    ['set scan budget bytes 10 rows 2', set('scanBudget', 'set scan budget bytes 10 rows 2')],
    ['SET SCAN BUDGET DEFAULT', set('scanBudget', null)],
    ['SET SCAN BUDGET default -- off', set('scanBudget', null)],
    // What the server will make of the rest is its business: the text is kept as written.
    ['SET CONSISTENCY', set('consistency', 'SET CONSISTENCY')],
    ['SET SCAN BUDGET', set('scanBudget', 'SET SCAN BUDGET')],
    // Not exactly one such statement: left alone.
    ['SET CONSISTENCY ALL; SELECT 1', null],
    ['SET SCAN BUDGET ROWS 1; SET CONSISTENCY ALL', null],
    ['SET CONSISTENCY ALL;;', null],
    ['SET CONFIG a.b = \'SET CONSISTENCY ALL\'', null],
    ['SET SCAN ROWS 1', null],
    ['SETCONSISTENCY ALL', null],
    ['"SET" CONSISTENCY ALL', null],
    ['-- SET CONSISTENCY ALL', null],
    ['SELECT \'SET CONSISTENCY ALL\'', null],
    ['RESET CONSISTENCY', null],
    ['', null],
    [';', null],
    [null, null],
    [undefined, null],
    [42, null],
  ];
  for (const [sql, want] of cases) assert.deepEqual(I.sessionSetting(sql), want, JSON.stringify(sql));
});

// ---- K: a USE is never held hostage by the database it leaves ---------------

test('a USE that re-dials goes out without re-entering the database it leaves, so a dropped database cannot hold the client', async () => {
  await withSession(async (server, srv) => {
    const gone = (name) => isStatementError(`database "${name}" does not exist`);
    const c = await connected(srv);
    await c.query('USE app');
    await c.query('SET CONSISTENCY ALL');
    server.gone.add('app');                               // dropped from another connection
    await lose(c, srv);
    // Any other statement fails with the USE's error, and runs nowhere: it
    // must not run in another database.
    await assert.rejects(c.query('SELECT 1'), gone('app'));
    await assert.rejects(c.query('INSERT INTO notes (id) VALUES (1)'), gone('app'));
    assert.deepEqual(server.on(2).concat(server.on(3)), ['USE "app"', 'USE "app"']);
    // A USE gets out: the new connection is given the settings, not the old database.
    await c.query('USE other');
    assert.deepEqual(server.on(4), ['SET CONSISTENCY ALL', 'USE other']);
    assert.equal(c.database, 'other');
    assert.deepEqual(await where(c), { db: 'other', consistency: 'ALL', txn: false, ids: [] });
    // … and other is where a later re-dial goes back to.
    await lose(c, srv);
    assert.deepEqual(await where(c), { db: 'other', consistency: 'ALL', txn: false, ids: [] });
    assert.deepEqual(server.on(5), ['USE "other"', 'SET CONSISTENCY ALL', 'SELECT count(*) FROM notes']);

    // Every form the client reads as a USE, through query() and stream().
    const forms = [(x) => x.query('use database app2;'), (x) => x.query('  USE "app2" -- back'),
      (x) => x.query({ text: 'USE app2' }), (x) => collect(x.stream('USE app2'))];
    for (const [i, use] of forms.entries()) {
      await c.query('USE other');
      server.gone.add('other');
      await lose(c, srv);
      server.log.length = 0;
      await use(c);
      assert.ok(!server.log.some((l) => /USE "other"/.test(l)), `form ${i}: ${server.log}`);
      assert.equal((await where(c)).db, 'app2', `form ${i}`);
      server.gone.delete('other');
    }

    // A USE the server refuses there leaves the client where it was — in a
    // database it cannot enter — and not in the new connection's default
    // one: the next statement fails as before, and still runs nowhere.
    await c.query('USE other');
    server.gone.add('other').add('nope');
    await lose(c, srv);
    server.log.length = 0;
    await assert.rejects(c.query('USE nope'), gone('nope'));
    await assert.rejects(collect(c.stream('USE nope')), gone('nope'));
    assert.equal(c.database, 'other');
    await assert.rejects(where(c), gone('other'));
    await assert.rejects(c.query('INSERT INTO notes (id) VALUES (2)'), gone('other'));
    assert.ok(!server.log.some((l) => /SELECT|INSERT/.test(l)), String(server.log));
    assert.deepEqual([...server.ids], []);
    // The database is there again: the client is back in it.
    server.gone.delete('other');
    assert.equal((await where(c)).db, 'other');
    await c.end();

    // Inside transaction() nothing changes: a lost connection is a lost
    // transaction for a USE too.
    const t = await connected(srv, { database: 'app2' });
    let caught;
    await assert.rejects(t.transaction(async (tx) => {
      await lose(tx, srv);
      caught = await tx.query('USE other').catch((e) => e);
      throw caught;
    }), /connection lost during a transaction/);
    assert.equal(t.database, 'app2');
    assert.equal((await where(t)).db, 'app2');
    await t.end();
  });
});

// ---- L: a hand-written transaction is a transaction --------------------------

const LOST = 'connection lost during a transaction';
const NOT_COMMITTED = 'connection lost during a transaction: COMMIT was not sent and nothing was committed';
// A transport failure as the driver reports one, and the two it reports for
// a transaction the application opened itself.
const isTransport = (e) => e instanceof SkaidbError && I.memberFault(e);
const isLost = (e) => isTransport(e) && e.message === LOST;
const diedInFlight = (e) => isTransport(e) && /^connection (closed$|lost(:|$))/.test(e.message);
const DDL = { command: 'DDL', rowCount: null, rows: [], fields: [] };
const insert = (c, id) => c.query(`INSERT INTO t (id) VALUES (${id})`);

test('a transaction the application opens with BEGIN is not carried to a new connection: after a loss its statements fail unsent, COMMIT says nothing was committed, ROLLBACK succeeds unsent', async () => {
  // PROTOCOL.md section 6 rule 7, for a BEGIN the application sends itself.
  for (const end of ['COMMIT', 'ROLLBACK', 'commit transaction ;', 'ROLLBACK TRANSACTION -- give up']) {
    await withSession(async (server, srv) => {
      const c = await connected(srv);
      await c.query('BEGIN');
      await insert(c, 1);
      await lose(c, srv);
      // The transaction died with its connection: nothing more of it is
      // sent — on a new session each statement would commit on its own.
      assert.ok(isLost(await insert(c, 2).catch((e) => e)), end);
      assert.ok(isLost(await insert(c, 3).catch((e) => e)), end);
      if (/^commit/i.test(end)) {
        await assert.rejects(c.query(end), (e) => isTransport(e) && e.message === NOT_COMMITTED, end);
      } else {
        assert.deepEqual(await c.query(end), DDL, end);     // the server rolled back when the connection went
      }
      assert.deepEqual(server.log, ['1: BEGIN', '1: INSERT INTO t (id) VALUES (1)'], end);
      assert.equal(srv.connections, 1, end);
      assert.equal(c.isUsable(), false, end);
      // Ended, the client is what it is after any lost connection: the next
      // statement re-dials, and none of the three rows exists.
      assert.deepEqual(await where(c), { db: 'default', consistency: null, txn: false, ids: [] }, end);
      assert.equal(srv.connections, 2, end);
      await insert(c, 4);                                   // in autocommit again
      assert.deepEqual((await where(c)).ids, [4], end);
      await c.end();
    });
  }
});

test('a hand-written transaction lost under a statement, under its COMMIT, or under its ROLLBACK', async () => {
  await withSession(async (server, srv) => {
    // Under statement 2 itself: it gets the usual connection error, the rest the transaction's.
    const c = await connected(srv);
    server.dies.add('INSERT INTO t (id) VALUES (2)');
    await c.query('BEGIN');
    await insert(c, 1);
    assert.ok(diedInFlight(await insert(c, 2).catch((e) => e)));
    assert.ok(isLost(await insert(c, 3).catch((e) => e)));
    assert.deepEqual(await c.query('ROLLBACK'), DDL);
    assert.deepEqual(server.on(1), ['BEGIN', 'INSERT INTO t (id) VALUES (1)', 'INSERT INTO t (id) VALUES (2)']);
    assert.deepEqual((await where(c)).ids, []);

    // Under the COMMIT itself the outcome is unknown, and the error says so
    // instead of claiming either. The transaction is over all the same.
    server.dies.clear();
    server.dies.add('COMMIT');
    await c.query('BEGIN');
    await insert(c, 5);
    const unknown = await c.query('COMMIT').catch((e) => e);
    assert.ok(isTransport(unknown), String(unknown));
    assert.match(unknown.message, /^connection lost under COMMIT: it is not known whether the transaction was committed \(connection (closed|lost.*)\)$/);
    assert.ok(diedInFlight(unknown.cause), String(unknown.cause));
    assert.equal(server.log.filter((l) => /COMMIT/.test(l)).length, 1);        // sent once, never again
    server.dies.clear();
    assert.equal((await where(c)).txn, false);                                  // re-dialled: the note is gone

    // Under the ROLLBACK itself: the connection error, and the transaction is over.
    server.dies.add('ROLLBACK');
    await c.query('BEGIN');
    await insert(c, 6);
    assert.ok(diedInFlight(await c.query('ROLLBACK').catch((e) => e)));
    server.dies.clear();
    assert.deepEqual(await where(c), { db: 'default', consistency: null, txn: false, ids: [] });
    await c.end();
  });
});

test('a hand-written transaction with no loss behaves as it always did, and what the server answers decides when it is open', async () => {
  await withSession(async (server, srv) => {
    const c = await connected(srv);
    // BEGIN, INSERT, COMMIT: sent as written, on one connection.
    await c.query('BEGIN');
    await insert(c, 1);
    assert.equal((await where(c)).txn, true);
    assert.deepEqual(await c.query('COMMIT'), DDL);
    assert.deepEqual(await where(c), { db: 'default', consistency: null, txn: false, ids: [1] });
    assert.deepEqual(server.on(1), ['BEGIN', 'INSERT INTO t (id) VALUES (1)', 'SELECT count(*) FROM notes', 'COMMIT', 'SELECT count(*) FROM notes']);
    // Committed, the transaction is over: a loss now is any lost connection.
    await lose(c, srv);
    await insert(c, 2);
    assert.deepEqual((await where(c)).ids, [1, 2]);

    // A COMMIT the server refuses has ended the transaction too …
    server.refuse.set('COMMIT', 'transaction conflict: a row of table t was changed by another writer');
    await c.query('begin transaction;');
    await assert.rejects(c.query('COMMIT'), /transaction conflict/);
    server.refuse.clear();
    await lose(c, srv);
    await insert(c, 3);                                    // … so this re-dials and runs
    // … and so has a ROLLBACK, whatever it is answered.
    server.refuse.set('ROLLBACK', 'no transaction in progress');
    await collect(c.stream('BEGIN -- through stream()'));
    await assert.rejects(c.query('ROLLBACK'), /no transaction in progress/);
    server.refuse.clear();
    await c.query('ROLLBACK');
    await lose(c, srv);
    await insert(c, 4);
    // A BEGIN the server refused opened nothing.
    server.refuse.set('BEGIN ATOMIC', 'unsupported: BEGIN ATOMIC PARTITION is a cluster statement');
    await assert.rejects(c.query('BEGIN ATOMIC'), /unsupported/);
    server.refuse.clear();
    await lose(c, srv);
    await insert(c, 5);
    assert.deepEqual((await where(c)).ids, [1, 2, 3, 4, 5]);

    // Open, every way a statement leaves the client fails once the
    // connection is gone — a BEGIN and a USE included — and so does a
    // connect() by hand: only COMMIT or ROLLBACK ends it.
    for (const begin of ['BEGIN', 'Begin Transaction', 'BEGIN ATOMIC PARTITION (tenant = \'a;b\')', '-- open\nbegin']) {
      await c.query(begin);
      await insert(c, 9);
      await lose(c, srv);
      const sent = server.log.length;
      const ways = [() => insert(c, 9), () => c.query('INSERT INTO t (id) VALUES ($1)', [9]),
        () => c.batch('INSERT INTO t (id) VALUES ($1)', [[9]]), () => collect(c.stream('INSERT INTO t (id) VALUES (9)')),
        () => c.query('SELECT 1'), () => c.query('USE other'), () => c.query('BEGIN'), () => collect(c.stream('BEGIN')),
        () => c.query('SET SCAN BUDGET ROWS 1'), () => c.transaction(async () => 'ran')];
      for (const [i, way] of ways.entries()) assert.ok(isLost(await way().catch((e) => e)), `${begin}, way ${i}`);
      await c.connect();                                    // a new connection, by the application's own hand
      assert.equal(c.isUsable(), true);
      assert.ok(isLost(await insert(c, 9).catch((e) => e)), begin);
      assert.equal(server.log.length, sent, begin);         // nothing of all that was sent anywhere
      assert.deepEqual(await collect(c.stream('ROLLBACK')), [], begin);
      assert.equal(server.log.length, sent, begin);
      assert.deepEqual(await where(c), { db: 'default', consistency: null, txn: false, ids: [1, 2, 3, 4, 5] }, begin);
    }
    await c.end();
  });
});

test('transaction() does not note its own BEGIN, and a hand-written transaction ended on a lost connection re-dials into the session it had', async () => {
  await withSession(async (server, srv) => {
    const c = await connected(srv);
    await c.query('USE app');
    await c.query('SET CONSISTENCY ALL');
    // The helper is what it was: its transaction is its own.
    assert.equal(await c.transaction(async (tx) => { await insert(tx, 1); return 'done'; }), 'done');
    await lose(c, srv);
    await insert(c, 2);                                     // no transaction is open: re-dialled
    await assert.rejects(c.transaction(async (tx) => { await lose(tx, srv); await insert(tx, 3); }), isLost);
    assert.deepEqual((await where(c)).ids, [1, 2]);         // … and after the helper lost one, too
    // By hand, lost, ended client-side: the next statement is back in the
    // database and at the consistency the session had.
    await c.query('BEGIN');
    await lose(c, srv);
    await assert.rejects(c.query('COMMIT'), (e) => e.message === NOT_COMMITTED);
    server.log.length = 0;
    assert.deepEqual(await where(c), { db: 'app', consistency: 'ALL', txn: false, ids: [1, 2] });
    assert.deepEqual(server.log.map((l) => l.replace(/^\d+: /, '')), ['USE "app"', 'SET CONSISTENCY ALL', 'SELECT count(*) FROM notes']);
    await c.end();
  });
});

test('transactionStatement reads a BEGIN, and exactly COMMIT or ROLLBACK', () => {
  const cases = [
    ['BEGIN', 'begin'], ['begin;', 'begin'], ['BEGIN TRANSACTION', 'begin'], ['  Begin  Transaction ; ', 'begin'],
    ['BEGIN ATOMIC', 'begin'], ['BEGIN ATOMIC PARTITION (tenant = 7)', 'begin'],
    ['BEGIN ATOMIC PARTITION (tenant = \'a;b\')', 'begin'], ['-- open\nBEGIN -- now', 'begin'],
    ['COMMIT', 'commit'], ['commit;', 'commit'], ['COMMIT TRANSACTION', 'commit'], ['Commit   transaction ;', 'commit'],
    ['COMMIT -- done', 'commit'],
    ['ROLLBACK', 'rollback'], ['rollback ;', 'rollback'], ['ROLLBACK TRANSACTION', 'rollback'], ['-- undo\nROLLBACK', 'rollback'],
    // Not exactly one of them: left alone.
    ['COMMIT WORK', null], ['COMMIT; BEGIN', null], ['ROLLBACK TO x', null], ['ROLLBACK TRANSACTION now', null],
    ['BEGINNING', null], ['"BEGIN"', null], ['START TRANSACTION', null], ['-- BEGIN', null], ['SELECT \'BEGIN\'', null],
    ['BEGIN; INSERT INTO t (id) VALUES (1)', null], ['', null], [null, null], [7, null],
  ];
  for (const [sql, want] of cases) assert.equal(I.transactionStatement(sql), want, JSON.stringify(sql));
});
