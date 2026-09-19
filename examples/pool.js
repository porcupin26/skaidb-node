// A connection pool with multi-seed failover, TLS against a CA, and a
// session database. Every Client option passes through the Pool.
//
//   node examples/pool.js host1:7000,host2:7000,host3:7000 [user] [password] [caPath]
const { Pool } = require('../skaidb');

const [seedList = 'localhost:7000', user = 'anonymous', password = '', tlsCa] = process.argv.slice(2);

(async () => {
  const pool = new Pool({
    seeds: seedList.split(','),        // shuffled per connect; any node serves
    user, password,
    ...(tlsCa ? { tlsCa } : {}),       // verify the server against this CA (implies TLS)
    consistency: 'QUORUM',
    maxsize: 8,                        // idle connections kept; bursts open extras
  });

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

  await pool.end();
})().catch((e) => { console.error(e); process.exit(1); });
