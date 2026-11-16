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
