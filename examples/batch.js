// Bulk insert in ONE round-trip with batch(), then read it back.
//
//   node examples/batch.js [host] [port] [user] [password]
const { Client } = require('../skaidb');

const [host = 'localhost', port = '7000', user = 'anonymous', password = ''] = process.argv.slice(2);

(async () => {
  const client = new Client({ host, port: Number(port), user, password });
  await client.connect();

  await client.query('DROP TABLE IF EXISTS events');
  await client.query('CREATE TABLE events (PRIMARY KEY (id))');

  // Each row is a parameter list for $1, $2, $3. Arrays and objects travel
  // typed (they have no SQL literal form), so a JSON-like document is fine.
  const rows = [];
  for (let i = 1; i <= 1000; i++) {
    rows.push([i, new Date(Date.now() - i * 1000), { kind: i % 2 ? 'click' : 'view', tags: ['web', `u${i % 7}`] }]);
  }
  const affected = await client.batch('INSERT INTO events (id, at, payload) VALUES ($1, $2, $3)', rows);
  console.log('inserted', affected);

  const res = await client.query('SELECT count(*) AS n FROM events');
  console.log(res.rows[0]);       // { n: 1000 }

  await client.query('DROP TABLE events');
  await client.end();
})().catch((e) => { console.error(e); process.exit(1); });
