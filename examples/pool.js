// A connection pool balanced across the cluster's members, with TLS against
// a CA and a session database. Every Client option passes through the Pool.
// With loadBalancing 'least_latency' and discover, the pool spreads its
// checkouts over every member it learns from SHOW ENDPOINTS, skipping
// members that are resyncing and ejecting ones that fail or turn slow.
//
//   node examples/pool.js host1:7000,host2:7000,host3:7000 [user] [password] [caPath]
const { Pool } = require('../skaidb');

const [seedList = 'localhost:7000', user = 'anonymous', password = '', tlsCa] = process.argv.slice(2);

(async () => {
  const pool = new Pool({
    seeds: seedList.split(','),        // the starting members; any node serves
    user, password,
    ...(tlsCa ? { tlsCa } : {}),       // verify the server against this CA (implies TLS)
    consistency: 'QUORUM',
    maxsize: 8,                        // idle connections kept; bursts open extras
    loadBalancing: 'least_latency',    // or 'round_robin'; 'random' is the default
    discover: true,                    // learn the members from the cluster
  });
  pool.on('eject', (e) => console.warn(`ejected ${e.endpoint}: ${e.reason}`));
  pool.on('readmit', (e) => console.warn(`re-admitted ${e.endpoint}`));

  // withConnection returns the connection however the callback ends.
  const rows = await pool.withConnection((c) => c.query('SELECT 1 AS one'));
  console.log(rows.rows);            // [ { one: 1 } ]

  // Many concurrent users share the pool.
  await Promise.all(Array.from({ length: 20 }, (_, i) =>
    pool.withConnection(async (c) => {
      const r = await c.query('SELECT $1 AS i', [i]);
      return r.rows[0].i;
    })));

  // Manual checkout when you need the same connection for several statements.
  const conn = await pool.acquire();
  try {
    await conn.query('SELECT 1');
  } finally {
    await pool.release(conn);
  }

  // What the pool sees per member.
  for (const m of pool.members()) {
    console.log(m.endpoint, m.state, m.latencyMs, m.inUse, m.idle);
  }

  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
