'use strict';
// Pool load balancing: policies, idle order, health ejection and
// re-admission, member discovery (SHOW ENDPOINTS) and read-your-writes
// affinity — against fake members that each serve many connections.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Pool, SkaidbError, _internal: I } = require('../skaidb');
const { FakeServer, F } = require('./fake-server');

/**
 * A fake skaidb member: `SELECT …` answers one row naming the member (after
 * `delay` ms), `SHOW ENDPOINTS` answers `listing()`, `SHOW DATABASES` (the
 * pool's probe) answers no rows, anything else is a one-row mutation.
 * `srv.refuse` closes each new connection before the handshake; `oldServer`
 * (or no `listing`) answers SHOW ENDPOINTS with a parse error.
 */
async function member(name, opts = {}) {
  const m = { name, delay: 0, oldServer: false, listing: null, statements: [] };
  m.srv = await new FakeServer({
    password: opts.password,
    host: opts.host,
    handle: (req, conn) => {
      if (req.op !== 1) return undefined;                          // Hello -> Ddl
      m.statements.push(req.sql);
      if (req.sql === 'SHOW ENDPOINTS') {
        if (m.oldServer || m.listing === null) {
          return F.error('parse error: expected TABLES, found ENDPOINTS');
        }
        return F.rows(['endpoint', 'state', 'local'], m.listing());
      }
      if (req.sql === 'SHOW DATABASES') return F.rows(['name'], []);
      if (req.sql.startsWith('SELECT')) {
        if (m.delay) {
          setTimeout(() => conn.send(F.rows(['member'], [[name]])), m.delay);
          return [];
        }
        return F.rows(['member'], [[name]]);
      }
      return F.mutation(1);
    },
  }).start();
  m.endpoint = `127.0.0.1:${m.srv.port}`;
  return m;
}

// Runs `fn(members, pool)` against fresh members named by `names`; `pool`
// builds a Pool over the given members (extra options win) and everything
// is torn down afterwards.
async function withMembers(names, fn) {
  const ms = [];
  const pools = [];
  const pool = (list, opts = {}) => {
    const p = new Pool({ seeds: list.map((m) => m.endpoint), password: 'secret', connectTimeout: 5000, ...opts });
    pools.push(p);
    return p;
  };
  try {
    for (const n of names) ms.push(await member(n));
    await fn(ms, pool);
  } finally {
    for (const p of pools) await p.end();
    for (const m of ms) await m.srv.stop();
  }
}

async function who(pool, affinity) {
  return pool.withConnection({ affinity }, async (c) => (await c.query('SELECT member()')).rows[0].member);
}

async function serve(pool, n, affinity) {
  const out = [];
  for (let i = 0; i < n; i++) out.push(await who(pool, affinity));
  return out;
}

const stateOf = (pool, m) => pool.members().find((r) => r.endpoint === m.endpoint).state;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const until = async (pred, ms, what) => {
  const t0 = Date.now();
  while (!pred()) {
    if (Date.now() - t0 > ms) throw new Error(`timeout waiting: ${what}`);
    await sleep(10);
  }
};

// ---- policies ---------------------------------------------------------------

test('pool: round_robin rotates over every member', () => withMembers('abc', async (ms, pool) => {
  const p = pool(ms, { loadBalancing: 'round_robin' });
  const served = await serve(p, 9);
  assert.deepEqual([...served].sort(), [...'aaabbbccc']);
  // Strict rotation: three in a row are three different members.
  for (let i = 0; i < 9; i += 3) assert.equal(new Set(served.slice(i, i + 3)).size, 3, served.join(''));
}));

test('pool: least_latency prefers the faster member', () => withMembers('ab', async ([a, b], pool) => {
  b.delay = 20;
  const p = pool([a, b], { loadBalancing: 'least_latency', ejectLatencyFactor: 0 });
  const served = await serve(p, 60);
  assert.ok(served.filter((s) => s === 'a').length > 48, served.join(''));
  assert.ok(served.includes('b'), 'b was measured at least once');
}));

test('pool: least_latency re-measures a member it has not used lately', () => withMembers('ab', async ([a, b], pool) => {
  b.delay = 20;
  const p = pool([a, b], { loadBalancing: 'least_latency', ejectLatencyFactor: 0 });
  await serve(p, 20);
  // b is slow, then recovers: an idle pool notices once b's average goes
  // stale instead of never asking b again.
  b.delay = 0;
  p._members.get(b.endpoint).lastSample -= 10000;
  assert.ok((await serve(p, 3)).includes('b'));
}));

test('pool: the random policy keeps one reused connection', () => withMembers('abc', async (ms, pool) => {
  const p = pool(ms);
  // One connection, reused: random never targets a member.
  assert.equal(new Set(await serve(p, 5)).size, 1);
}));

test('pool: an unknown policy or idle order is refused', () => {
  assert.throws(() => new Pool({ loadBalancing: 'nearest' }), /unknown loadBalancing nearest/);
  assert.throws(() => new Pool({ idleOrder: 'random' }), /unknown idleOrder random/);
  assert.throws(() => new Pool({ ejectErrors: 0 }), /ejectErrors must be >= 1/);
  assert.throws(() => new Pool({ loadBalancing: 'nearest' }), SkaidbError);
});

// ---- idle order and introspection ------------------------------------------

async function threeReturned(p) {
  const conns = [await p.acquire(), await p.acquire(), await p.acquire()];
  for (const c of conns) await p.release(c);
  return conns;
}

test('pool: lifo hands out the most recently returned connection', () => withMembers('a', async ([a], pool) => {
  const p = pool([a]);
  const conns = await threeReturned(p);
  assert.equal(await p.acquire(), conns[2]);
}));

test('pool: fifo hands out the longest idle connection', () => withMembers('a', async ([a], pool) => {
  const p = pool([a], { idleOrder: 'fifo' });
  const conns = await threeReturned(p);
  for (const c of conns) assert.equal(await p.acquire(), c);
}));

test('pool: members() reports state, latency and connections', () => withMembers('a', async ([a], pool) => {
  const p = pool([a]);
  assert.deepEqual(p.members(), [{
    endpoint: a.endpoint, canonical: null, state: 'ready', latencyMs: null, inUse: 0, idle: 0, seed: true,
  }]);
  const conn = await p.acquire();
  await conn.query('SELECT member()');
  let [row] = p.members();
  assert.equal(row.inUse, 1);
  assert.equal(row.idle, 0);
  assert.equal(row.latencyMs, null, 'not measured until its first three round trips');
  await conn.query('SELECT member()');
  await conn.query('SELECT member()');
  assert.equal(typeof p.members()[0].latencyMs, 'number');
  await p.release(conn);
  [row] = p.members();
  assert.equal(row.inUse, 0);
  assert.equal(row.idle, 1);
  // Closed while checked out: no longer counted against the member.
  const lost = await p.acquire();
  await lost.end();
  assert.equal(p.members()[0].inUse, 0);
}));

test('pool: a checked-out connection that re-dials is counted on its new member', () => withMembers('ab', async (ms, pool) => {
  const p = pool(ms, { loadBalancing: 'round_robin' });
  const conn = await p.acquire();
  const lost = ms.find((m) => m.endpoint === conn._connected);
  const other = ms.find((m) => m !== lost);
  lost.srv.refuse = true;
  lost.srv.closeAll();
  await until(() => !conn.isUsable(), 2000, 'the connection noticed');
  // The pool's order puts the member just lost last.
  assert.equal((await conn.query('SELECT member()')).rows[0].member, other.name);
  const rows = Object.fromEntries(p.members().map((r) => [r.endpoint, r]));
  assert.equal(rows[lost.endpoint].inUse, 0);
  assert.equal(rows[other.endpoint].inUse, 1);
  assert.equal(p._members.get(lost.endpoint).errors, 0, 'a connection closed while idle is not a failure');
  await p.release(conn);
}));

// ---- health ejection --------------------------------------------------------

test('pool: a failing member is ejected, then re-admitted by the probe', () => withMembers('abc', async ([a, b, c], pool) => {
  b.srv.refuse = true;
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectErrors: 2, ejectBackoff: 300 });
  const events = [];
  p.on('eject', (e) => events.push(`eject ${e.endpoint}`));
  p.on('readmit', (e) => events.push(`readmit ${e.endpoint}`));
  assert.ok(!(await serve(p, 12)).includes('b'));
  assert.equal(stateOf(p, b), 'ejected');
  // Healthy again: the background probe re-admits it after the backoff.
  b.srv.refuse = false;
  await until(() => stateOf(p, b) === 'ready', 5000, 'b re-admitted');
  assert.ok((await serve(p, 6)).includes('b'));
  assert.deepEqual(events, [`eject ${b.endpoint}`, `readmit ${b.endpoint}`]);
}));

test('pool: a probe that fails doubles the backoff; the timer is unref()d and stops with the pool', () => withMembers('abc', async ([a, b, c], pool) => {
  b.srv.refuse = true;
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectErrors: 1, ejectBackoff: 200 });
  await serve(p, 6);
  const m = p._members.get(b.endpoint);
  await until(() => m.ejections >= 2, 5000, 'the probe ran');
  assert.equal(stateOf(p, b), 'ejected');
  assert.ok(p._backoffFor(m) >= 400);
  assert.equal(p._timer.hasRef(), false);
  await p.end();
  assert.equal(p._timer, null);
}));

test('pool: a consistently slow member is ejected', () => withMembers('abc', async ([a, b, c], pool) => {
  b.delay = 50;
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectLatencyFloor: 20, ejectBackoff: 30000 });
  await serve(p, 45);
  assert.equal(stateOf(p, b), 'ejected');
  assert.ok(!(await serve(p, 6)).includes('b'));
}));

test('pool: an ejected member is not ejected again by late round trips', () => withMembers('abc', async ([a, b, c], pool) => {
  b.delay = 50;
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectLatencyFloor: 20, ejectBackoff: 30000 });
  await serve(p, 45);
  const m = p._members.get(b.endpoint);
  assert.equal(stateOf(p, b), 'ejected');
  const until = m.ejectedUntil;
  // Slow round trips still finishing on connections to b that were checked
  // out before the ejection.
  const late = { _connected: m.key, _poolEp: m.key, _poolOut: false };
  for (let i = 0; i < 5; i++) p._roundTrip(late, 50);
  assert.deepEqual([m.ejections, m.ejectedUntil], [1, until]);
}));

test('pool: one slow statement does not eject a member', () => withMembers('abc', async ([a, b, c], pool) => {
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectLatencyFloor: 50 });
  await serve(p, 36);
  b.delay = 500;
  while (await who(p) !== 'b') { /* until b took the slow one */ }
  b.delay = 0;
  await serve(p, 6);
  assert.equal(stateOf(p, b), 'ready');
}));

// Stand-ins for connections to each member, by name, to feed the pool's
// latency measurements round trips of a chosen length.
const fakes = (ms) => Object.fromEntries(
  ms.map((m) => [m.name, { _connected: m.endpoint, _poolEp: m.endpoint, _poolOut: false }]));

test('pool: a slow first round trip does not eject a member', () => withMembers('abc', async ([a, b, c], pool) => {
  // The first statements on b stall in the client — a CPU-bound application
  // starting up blocks the event loop — and b answers fast from then on: the
  // fastest of its first round trips seeds its average, so b stays in
  // rotation. (Seeded by its first round trip alone, b's average still read
  // ~3.8 s once another member had 10 samples, and b was ejected.)
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectLatencyFloor: 50 });
  const fake = fakes([a, b, c]);
  p._roundTrip(fake.b, 6600);
  p._roundTrip(fake.b, 3000);
  for (let i = 0; i < 12; i++) for (const name of 'abc') p._roundTrip(fake[name], 0.2);
  assert.equal(stateOf(p, b), 'ready');
  assert.ok(p._members.get(b.endpoint).ewma < 50);
}));

test('pool: a member slow from its first round trip is ejected and the reason names its slowest round trip', () => withMembers('abc', async ([a, b, c], pool) => {
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectLatencyFloor: 50, ejectBackoff: 30000 });
  const reasons = [];
  p.on('eject', (e) => reasons.push(e.reason));
  const fake = fakes([a, b, c]);
  for (let i = 0; i < 12; i++) {
    p._roundTrip(fake.a, 0.2);
    p._roundTrip(fake.c, 0.2);
    p._roundTrip(fake.b, i === 5 ? 900 : 500);
  }
  assert.equal(stateOf(p, b), 'ejected');
  await sleep(0);                                     // listeners run on a later tick
  assert.equal(reasons.length, 1, reasons.join(' | '));
  assert.match(reasons[0], /^latency 5\d\d\.\d ms vs median 0\.2 ms; slowest of the last 10 round trips 900\.0 ms$/);
}));

test('pool: a re-admitted member is seeded afresh', () => withMembers('abc', async ([a, b, c], pool) => {
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectErrors: 1 });
  const m = p._members.get(b.endpoint);
  const fake = fakes([a, b, c]);
  for (const t of [4, 3, 2]) p._roundTrip(fake.b, t);
  assert.equal(m.ewma, 2);
  p._failed(b.endpoint);
  assert.equal(stateOf(p, b), 'ejected');
  await p._probe(m);                                  // b answers: re-admitted
  assert.equal(stateOf(p, b), 'ready');
  assert.deepEqual([m.ewma, m.firsts, m.recent, m.samples], [null, [], [], 0]);
  p._roundTrip(fake.b, 7);
  assert.equal(m.ewma, null, 'not measured until its first three round trips');
}));

test('pool: the last usable member is never ejected', () => withMembers('abc', async ([a, b, c], pool) => {
  b.srv.refuse = true;
  c.srv.refuse = true;
  const p = pool([a, b, c], { loadBalancing: 'round_robin', ejectErrors: 1 });
  for (const s of await serve(p, 6)) assert.equal(s, 'a');
  a.srv.refuse = true;
  // Idle connections to a still work (how many depends on the dial order
  // above); once they are used up, every dial fails.
  const held = [];
  await assert.rejects(async () => {
    for (let i = 0; i < 12; i++) held.push(await p.acquire());
  }, /no reachable endpoint/);
  assert.equal(stateOf(p, a), 'ready');
}));

test('pool: a refused login is not a member fault', async () => {
  assert.equal(I.memberFault(new SkaidbError('authentication denied: bad password')), false);
  assert.equal(I.memberFault(new SkaidbError('authentication denied: too many connections: 500')), true);
  assert.equal(I.memberFault(new SkaidbError('server signature mismatch (mutual auth failed)')), false);
  assert.equal(I.memberFault(new SkaidbError('connection closed')), true);
  assert.equal(I.memberFault(new SkaidbError('connect failed: connect ECONNREFUSED 127.0.0.1:1')), true);
  assert.equal(I.memberFault(new Error('connect ECONNREFUSED 127.0.0.1:1')), true);
  // End to end: a member that refuses these credentials stays in rotation.
  await withMembers('a', async ([a], pool) => {
    const x = await member('x', { password: 'other' });
    try {
      const p = pool([a, x], { loadBalancing: 'round_robin', ejectErrors: 1 });
      for (const s of await serve(p, 6)) assert.equal(s, 'a');
      assert.equal(stateOf(p, x), 'ready');
      assert.equal(p._members.get(x.endpoint).errors, 0);
    } finally {
      await x.srv.stop();
    }
  });
});

// ---- discovery --------------------------------------------------------------

// Every member lists a and b ready and c resyncing (mutable through the
// returned map), each naming itself as `local`.
function discoverable(ms) {
  const [a, b, c] = ms;
  const states = { [a.endpoint]: 'ready', [b.endpoint]: 'ready', [c.endpoint]: 'resyncing' };
  for (const m of ms) {
    m.listing = () => Object.keys(states).sort().map((ep) => [ep, states[ep], ep === m.endpoint]);
  }
  return states;
}

async function relist(p) {
  const conn = await p.acquire();
  await p._listThrough(conn);
  await p.release(conn);
}

test('pool: members are discovered and resyncing ones get no connections', () => withMembers('abc', async (ms, pool) => {
  const [a, , c] = ms;
  const states = discoverable(ms);
  const p = pool([a], { loadBalancing: 'round_robin', discover: true });
  assert.deepEqual(new Set(await serve(p, 8)), new Set(['a', 'b']));
  assert.equal(stateOf(p, c), 'resyncing');
  // c catches up: the next refresh lets it in, measured afresh.
  const m = p._members.get(c.endpoint);
  m.ewma = 9.9;
  m.firsts = [9.9];
  m.recent = [9.9];
  m.samples = 50;
  states[c.endpoint] = 'ready';
  await relist(p);
  assert.deepEqual([m.ewma, m.firsts, m.recent, m.samples], [null, [], [], 0]);
  assert.ok((await serve(p, 6)).includes('c'));
}));

test('pool: a listing with a member not ready refreshes sooner', () => withMembers('abc', async (ms, pool) => {
  const [a, , c] = ms;
  const states = discoverable(ms);
  const p = pool([a], { loadBalancing: 'round_robin', discover: true, refreshInterval: 60000 });
  await who(p);
  assert.ok(p._nextRefresh - performance.now() <= 5000, 'c is resyncing');
  states[c.endpoint] = 'ready';
  await relist(p);
  assert.ok(p._nextRefresh - performance.now() > 50000);
}));

test('pool: a seed given by another name is not counted twice', () => withMembers('bc', async ([b, c], pool) => {
  // a listens on whichever address `localhost` resolves to first and calls
  // itself 127.0.0.1:<port> in the listing; the seed names it `localhost`.
  const a = await member('a', { host: 'localhost' });
  try {
    discoverable([a, b, c]);
    const p = pool([a], { seeds: [`localhost:${a.srv.port}`], loadBalancing: 'round_robin', discover: true });
    await who(p);
    const rows = p.members();
    assert.equal(rows.length, 3, JSON.stringify(rows));
    const seed = rows.find((r) => r.seed);
    assert.equal(seed.endpoint, `localhost:${a.srv.port}`);
    assert.equal(seed.canonical, a.endpoint);
    assert.deepEqual(new Set(await serve(p, 6)), new Set(['a', 'b']));
  } finally {
    await a.srv.stop();
  }
}));

test('pool: a member that leaves the listing is dropped', () => withMembers('abc', async (ms, pool) => {
  const [a, b] = ms;
  const states = discoverable(ms);
  const p = pool([a], { loadBalancing: 'round_robin', discover: true });
  await who(p);
  delete states[b.endpoint];
  await relist(p);
  assert.ok(!p.members().some((r) => r.endpoint === b.endpoint));
}));

test('pool: a seed whose member left the ring is kept as unlisted', () => withMembers('abc', async (ms, pool) => {
  const [a, b] = ms;
  const states = discoverable(ms);
  const p = pool([a, b], { loadBalancing: 'round_robin', discover: true });
  await serve(p, 2);                                  // both seeds named themselves
  delete states[b.endpoint];
  await relist(p);
  assert.equal(stateOf(p, b), 'unlisted');
  assert.ok(!(await serve(p, 4)).includes('b'));
}));

test('pool: an old server turns discovery off and keeps the seeds', () => withMembers('abc', async (ms, pool) => {
  const [a] = ms;
  a.oldServer = true;
  const p = pool([a], { loadBalancing: 'round_robin', discover: true });
  assert.deepEqual(new Set(await serve(p, 4)), new Set(['a']));
  assert.equal(p._discoveryOff, true);
  assert.equal(p.members().length, 1);
}));

test('pool: a standalone server turns discovery off and keeps the seeds', () => withMembers('ab', async (ms, pool) => {
  for (const m of ms) m.listing = () => [];
  const p = pool(ms, { loadBalancing: 'round_robin', discover: true });
  assert.deepEqual(new Set(await serve(p, 4)), new Set(['a', 'b']));
  assert.equal(p._discoveryOff, true);
  // Asked once, on the first connection; never again on the second seed.
  const asked = ms.reduce((n, m) => n + m.statements.filter((s) => s === 'SHOW ENDPOINTS').length, 0);
  assert.equal(asked, 1);
  assert.deepEqual(p.members().map((r) => [r.state, r.canonical]), [['ready', null], ['ready', null]]);
}));

// ---- read-your-writes -------------------------------------------------------

test('pool: reads follow the member that took the write', () => withMembers('abc', async (ms, pool) => {
  const p = pool(ms, { loadBalancing: 'round_robin', readYourWrites: 400 });
  const conn = await p.acquire({ affinity: 'alice' });
  await conn.query('INSERT INTO t VALUES (1)');
  const writer = ms.find((m) => m.endpoint === conn._connected).name;
  await p.release(conn);
  assert.deepEqual(new Set(await serve(p, 6, 'alice')), new Set([writer]));
  // Another key, or no key, still rotates.
  assert.equal(new Set(await serve(p, 3, 'bob')).size, 3);
  await sleep(500);
  assert.equal(new Set(await serve(p, 3, 'alice')).size, 3);
}));

test('pool: reads do not set affinity', () => withMembers('abc', async (ms, pool) => {
  const p = pool(ms, { loadBalancing: 'round_robin', readYourWrites: 5000 });
  await who(p, 'alice');
  assert.equal(new Set(await serve(p, 3, 'alice')).size, 3);
}));

test('pool: statement classification', () => {
  for (const sql of ['SELECT 1', '  select 1', '(SELECT 1)', '-- c\nSHOW TABLES',
    '/* x */ EXPLAIN SELECT 1', 'WITH a AS (SELECT 1) SELECT * FROM a',
    'DESCRIBE t', 'VALUES (1)', 'use app']) {
    assert.equal(I.isWrite(sql), false, sql);
  }
  for (const sql of ['INSERT INTO t VALUES (1)', 'update t set a = 1', 'DELETE FROM t',
    'CREATE TABLE t (id INT PRIMARY KEY)', 'COMMIT', 'CALL p()', 'selectx']) {
    assert.equal(I.isWrite(sql), true, sql);
  }
});
