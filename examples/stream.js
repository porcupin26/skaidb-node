// Stream a large result set row by row with bounded memory, and stop early.
//
//   node examples/stream.js [host] [port] [user] [password]
//
// The abandon rule: the connection is busy until the iterator is exhausted
// or CLOSED. `break`, `return`, a throw and `.return()` all close it; the
// driver then drains what is left (or drops the connection if that is
// cheaper) so the next statement reads its own reply. Never let a half-read
// iterator go out of scope without closing it.
const { Client } = require('../skaidb');

const [host = 'localhost', port = '7000', user = 'anonymous', password = ''] = process.argv.slice(2);

(async () => {
  const client = new Client({ host, port: Number(port), user, password });
  await client.connect();

  await client.query('DROP TABLE IF EXISTS readings');
  await client.query('CREATE TABLE readings (PRIMARY KEY (id))');
  const rows = [];
  for (let i = 1; i <= 5000; i++) rows.push([i, Math.sin(i / 100)]);
  await client.batch('INSERT INTO readings (id, v) VALUES ($1, $2)', rows);

  // Full scan, one chunk in memory at a time. Name the columns: on a cluster
  // a bare `SELECT *` only streams page by page at consistency ONE.
  let n = 0, sum = 0;
  for await (const row of client.stream('SELECT id, v FROM readings')) { n++; sum += row.v; }
  console.log('columns', client.stream.columns, 'rows', n, 'mean', sum / n);

  // Stop early: `break` closes the iterator and the driver cleans up.
  for await (const row of client.stream('SELECT id, v FROM readings', { rowMode: 'array' })) {
    if (row[0] >= 3) break;
  }
  // The connection is at a request boundary again.
  console.log((await client.query('SELECT count(*) AS n FROM readings')).rows[0]);

  await client.query('DROP TABLE readings');
  await client.end();
})().catch((e) => { console.error(e); process.exit(1); });
