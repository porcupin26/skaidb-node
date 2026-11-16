'use strict';
// A connection the peer closed while it sat idle costs no statement. A
// member restart, a failover or an idle timeout closes idle connections; on
// a real network a request written to such a socket is accepted by the
// kernel and only the read fails — a statement lost to a connection that
// died minutes ago. The scripted server here half-closes: it ends its
// sending side and keeps reading, so the client sees EOF while its writes
// succeed, and whatever it writes to the dead connection is counted.
const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('net');
const tls = require('tls');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const { Client, Pool, SkaidbError } = require('../skaidb');
const { FakeServer, F } = require('./fake-server');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const collect = async (it) => { const rows = []; for await (const r of it) rows.push(r); return rows; };

// `promise`, or a rejection when it has not settled within `ms`: a statement
// written to a dead connection waits for an answer that never comes, and
// must fail its test, not stall the suite.
const settles = (promise, ms = 2000) => {
  let timer;
  const late = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`still pending after ${ms} ms`)), ms); });
  return Promise.race([promise, late]).finally(() => clearTimeout(timer));
};

// Every statement is answered; `seen` holds each one with its socket.
function answering() {
  const seen = [];
  const handle = (req, conn) => {
    if (req.op === 8) return undefined;                                // Hello -> Ddl
    seen.push({ op: req.op, sql: req.sql, sock: conn.sock });
    if (req.op === 2) return F.prepared(0, (req.sql.match(/\?/g) || []).length);
    if (req.op === 5 || req.op === 9) return [F.header(['v']), F.chunk([[1]]), F.end()];
    if (req.op === 3 || req.op === 7) return F.mutation(1);
    return /^select/i.test(req.sql) ? F.rows(['v'], [[1]]) : F.mutation(1);
  };
  return { handle, seen, on: (sock) => seen.filter((s) => s.sock === sock).length };
}

// Close `sock`, the server's end of a connection, as `how` says — 'fin': end
// the sending side only and go on reading; 'reset': a TCP reset — and
// resolve to a counter of the bytes that arrive on it afterwards.
function closeIdle(sock, how) {
  const late = { bytes: 0 };
  sock.on('data', (d) => { late.bytes += d.length; });
  if (how === 'reset') sock.resetAndDestroy(); else sock.end();
  return late;
}

async function withServer(fn) {
  const server = answering();
  const srv = await new FakeServer({ handle: server.handle, allowHalfOpen: true }).start();
  try { return await fn(srv, server); } finally { await srv.stop(); }
}

// Every way a statement leaves a client: a read and a write alike, since
// nothing was sent on the dead connection.
const STATEMENTS = {
  'a read': (c) => c.query('SELECT 1'),
  'a write': (c) => c.query('INSERT INTO t (id) VALUES (1)'),
  'a write with values': (c) => c.query('INSERT INTO t (id) VALUES ($1)', [1]),
  'a batch': (c) => c.batch('INSERT INTO t (id) VALUES ($1)', [[1], [2]]),
  'a stream': (c) => collect(c.stream('SELECT 1')),
  'a stream with values': (c) => collect(c.stream('SELECT $1', [1])),
  'a transaction': (c) => c.transaction((tx) => tx.query('INSERT INTO t (id) VALUES (1)')),
};

test('a connection the server closed while it was idle costs no statement: the next one runs on a new connection and nothing is written to the dead one', async () => {
  for (const how of ['fin', 'reset']) {
    for (const [name, run] of Object.entries(STATEMENTS)) {
      await withServer(async (srv, server) => {
        const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
        await c.connect();
        await c.query('SELECT 1');
        const first = [...srv.sockets][0];
        const sent = server.on(first);
        const late = closeIdle(first, how);
        await sleep(100);                                   // the EOF (or the reset) arrives
        await settles(run(c));                              // no error reaches the application
        assert.equal(srv.connections, 2, `${how}, ${name}`);
        assert.equal(server.on(first), sent, `${how}, ${name}: no statement on the dead connection`);
        assert.equal(late.bytes, 0, `${how}, ${name}: no bytes on the dead connection`);
        assert.ok(server.seen.length > sent, `${how}, ${name}: it ran on the new one`);
        await c.end();
      });
    }
  }
});

test('looking at an idle connection costs no round trip', async () => {
  await withServer(async (srv, server) => {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    for (let i = 0; i < 3; i++) {
      await sleep(30);                                      // idle, and alive
      await c.query('SELECT 1');
    }
    assert.deepEqual(srv.requests.map((q) => q.op), [8, 1, 1, 1]);     // Hello, then the statements alone
    assert.equal(srv.connections, 1);
    await c.end();
  });
});

test('Pool: a connection the server closed while it sat idle in the pool is not handed out', async () => {
  for (const how of ['fin', 'reset']) {
    await withServer(async (srv, server) => {
      const pool = new Pool({ host: '127.0.0.1', port: srv.port, password: 'secret', maxsize: 2 });
      try {
        const idle = await pool.acquire();
        await idle.query('SELECT 1');
        await pool.release(idle);
        const first = [...srv.sockets][0];
        const sent = server.on(first);
        const late = closeIdle(first, how);
        await sleep(100);
        const conn = await pool.acquire();
        assert.notEqual(conn, idle, how);
        assert.equal(conn.isUsable(), true, how);
        assert.deepEqual((await settles(conn.query('SELECT 1'))).rows, [{ v: 1 }], how);
        await pool.release(conn);
        assert.equal(srv.connections, 2, how);
        assert.equal(server.on(first), sent, how);
        assert.equal(late.bytes, 0, how);
        assert.deepEqual([pool.members()[0].idle, pool.members()[0].inUse], [1, 0], how);
      } finally {
        await pool.end();
      }
    });
  }
});

test('inside transaction() a connection the server closed is a lost transaction, not a re-dial', async () => {
  // PROTOCOL.md section 6 rule 7 holds: nothing of the body is sent on a new
  // session, and nothing more on the dead one.
  for (const how of ['fin', 'reset']) {
    await withServer(async (srv, server) => {
      const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
      await c.connect();
      const caught = [];
      let late;
      const out = await settles(c.transaction(async (tx) => {
        await tx.query('INSERT INTO t (id) VALUES (1)');
        late = closeIdle([...srv.sockets][0], how);
        await sleep(100);
        caught.push(await tx.query('INSERT INTO t (id) VALUES (2)').catch((e) => e));
        caught.push(await tx.query('INSERT INTO t (id) VALUES (3)').catch((e) => e));
        return 'the body returned';
      })).catch((e) => e);
      const lost = (e) => e instanceof SkaidbError && e.message === 'connection lost during a transaction';
      assert.ok(lost(caught[0]) && lost(caught[1]) && lost(out), `${how}: ${caught[0]} / ${caught[1]} / ${out}`);
      assert.deepEqual(server.seen.map((s) => s.sql), ['BEGIN', 'INSERT INTO t (id) VALUES (1)'], how);
      assert.equal(late.bytes, 0, how);
      assert.equal(srv.connections, 1, how);
      // The transaction over, the next statement re-dials as usual.
      assert.deepEqual((await c.query('SELECT 1')).rows, [{ v: 1 }], how);
      assert.equal(srv.connections, 2, how);
      await c.end();
    });
  }
});

// ---- under TLS ---------------------------------------------------------------

function haveOpenssl() {
  try { execFileSync('openssl', ['version'], { stdio: 'ignore' }); return true; } catch { return false; }
}

test('under TLS it is the transport that is looked at: a TCP half-close with no TLS alert costs no statement either',
  { skip: haveOpenssl() ? false : 'openssl CLI not installed' }, async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'skaidb-node-idle-'));
    const key = path.join(dir, 'server.key');
    const cert = path.join(dir, 'server.crt');
    execFileSync('openssl', ['req', '-x509', '-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes',
      '-keyout', key, '-out', cert, '-days', '2', '-subj', '/CN=skaidb', '-addext', 'subjectAltName=DNS:skaidb'],
    { stdio: 'pipe' });
    // The scripted server speaks TLS; in front of it a TCP relay, which can
    // end its side toward the client without the TLS layer saying anything.
    const server = answering();
    const fake = new FakeServer({ handle: server.handle });
    const secure = tls.createServer({ key: fs.readFileSync(key), cert: fs.readFileSync(cert) }, (sock) => fake._onConnection(sock));
    const links = [];
    const relay = net.createServer({ allowHalfOpen: true }, (down) => {
      const up = net.connect(secure.address().port, '127.0.0.1');
      const link = { down, up, cut: false };
      links.push(link);
      for (const s of [down, up]) s.on('error', () => {});
      down.on('data', (d) => up.write(d));                  // still relayed after the cut: a statement would arrive
      up.on('data', (d) => { if (!link.cut) down.write(d); });
      down.on('close', () => up.destroy());
      up.on('close', () => down.destroy());
    });
    try {
      await new Promise((r) => secure.listen(0, '127.0.0.1', r));
      await new Promise((r) => relay.listen(0, '127.0.0.1', r));
      const c = new Client({ host: '127.0.0.1', port: relay.address().port, password: 'secret', tlsCa: cert });
      await c.connect();
      assert.deepEqual((await c.query('SELECT 1')).rows, [{ v: 1 }]);
      const first = server.seen[0].sock;
      const sent = server.on(first);
      links[0].cut = true;
      links[0].down.end();                                  // FIN to the client; no close_notify
      await sleep(100);
      assert.deepEqual((await settles(c.query('SELECT 1'))).rows, [{ v: 1 }]);
      assert.equal(links.length, 2);
      assert.equal(server.on(first), sent, 'no statement on the dead connection');
      assert.notEqual(server.seen[server.seen.length - 1].sock, first);
      await c.end();
    } finally {
      for (const l of links) { l.down.destroy(); l.up.destroy(); }
      await new Promise((r) => relay.close(r));
      await new Promise((r) => secure.close(r));
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
