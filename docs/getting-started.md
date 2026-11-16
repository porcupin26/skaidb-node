# Getting started

## Requirements

- Node.js 18 or newer.
- A reachable skaidb node (default client port **7000**). Any node of a
  cluster will do: skaidb is leaderless.

The driver has no dependencies and no build step.

## Install

```sh
npm install @skaidb/client
```

The package is `@skaidb/client`, so it is required as `@skaidb/client`. To
install from GitHub instead, `npm install github:porcupin26/skaidb-node#v1.5.1`
pins a tag and `#main` follows the development branch; the module name is
the same either way.

## Connect and query

```js
const { Client } = require('@skaidb/client');

async function main() {
  const client = new Client({
    host: 'localhost', port: 7000,
    user: 'skaidb', password: 'secret',
    database: 'app',                 // optional: selects the session database
  });
  await client.connect();

  await client.query('CREATE TABLE IF NOT EXISTS users (PRIMARY KEY (id))');
  await client.query('INSERT INTO users (id, name, tags) VALUES ($1, $2, $3)', [1, 'Ada', ['admin']]);

  const res = await client.query('SELECT id, name, tags FROM users WHERE id = $1', [1]);
  console.log(res.rows);             // [ { id: 1, name: 'Ada', tags: [ 'admin' ] } ]

  await client.end();
}

main().catch((e) => { console.error(e); process.exit(1); });
```

Placeholders are `$1, $2, …`. With parameters the statement is prepared on
the server and the values are sent typed — that is how the array above gets
through; it has no SQL literal form.

ESM and TypeScript use `import { Client } from '@skaidb/client'`.

## Anonymous connections

A server with authentication disabled accepts `user: 'anonymous'` (the
default) with an empty password. The SCRAM handshake still runs; the server
verifies nothing, and the driver skips mutual authentication when the
password is empty.

## TLS

A server with `client_tls = required` refuses plaintext, so one of these is
needed there:

```js
new Client({ ..., tlsCa: '/etc/skaidb/skai-ca.crt' });    // verify against your CA — recommended
new Client({ ..., tls: true });                            // verify against the system trust store
new Client({ ..., tlsInsecure: true });                    // encrypt only, no verification — development
```

`tlsServerName` (default `skaidb`) is the SNI name and the name the
certificate is verified against. It must match a SAN on the server
certificate — which is normally *not* the host you dialled; skaidb's own
certificates carry `DNS:skaidb`.

## Several nodes

```js
new Client({ seeds: ['db1:7000', 'db2:7000', 'db3:7000'], user, password });
```

The seeds are shuffled and tried until one connects **and authenticates**.
The same walk runs when a connection is lost later: the next statement on the
client re-dials, re-authenticates, re-enters the database it was in (the
`database` option, or one entered since with a `USE` statement), sets the
consistency override and the scan budget it was given with `SET CONSISTENCY`
/ `SET SCAN BUDGET` statements again, and runs. The
statement that was in flight when the connection died is rejected and never
retried by the driver (it may have executed).

## Many concurrent queries

Statements on one client run one at a time. For concurrency use a pool:

```js
const { Pool } = require('@skaidb/client');
const pool = new Pool({ seeds: ['db1:7000', 'db2:7000'], user, password, maxsize: 8 });

const res = await pool.withConnection((c) => c.query('SELECT count(*) AS n FROM users'));

await pool.end();
```

To spread one process's connections evenly over the cluster, pick a policy
and let the pool learn the members:

```js
const pool = new Pool({ seeds: ['db1:7000'], user, password,
                        loadBalancing: 'least_latency', discover: true });
```

It skips members that are resyncing, ejects ones that fail or turn slow and
probes them back in. See [the pool reference](api.md#new-pooloptions).

## Big results

```js
for await (const row of client.stream('SELECT id, name FROM users')) {
  // one row at a time, one chunk in memory
}
for await (const row of client.stream('SELECT id, name FROM users WHERE id > $1', [1000])) {
  // values bind $1, $2, … as in query() (server ≥ 0.330)
}
```

Read the [abandon rule](api.md#the-abandon-rule) before stopping a stream
early: the connection is busy until the iterator is closed.

## Bulk inserts

```js
await client.batch('INSERT INTO users (id, name) VALUES ($1, $2)', [[1, 'Ada'], [2, 'Linus']]);
```

One round-trip for all rows; each row autocommits on its own.

## Transactions

```js
const left = await client.transaction(async (tx) => {
  const { rows } = await tx.query('SELECT qty FROM stock WHERE id = $1', [7]);
  await tx.query('UPDATE stock SET qty = $2 WHERE id = $1', [7, rows[0].qty - 1]);
  return rows[0].qty - 1;
});
```

`BEGIN`, your function, `COMMIT` — on one connection — or `ROLLBACK` if the
function throws. When the server refuses the `COMMIT` because another writer
changed a row the transaction writes, the whole function runs again (three
runs in all by default), so it must be safe to run more than once. On a
cluster pass the opening statement, `{ begin: 'BEGIN ATOMIC' }`; on a pool
use `pool.transaction()`. See
[the reference](api.md#clienttransactionbody-opts-promiset).

## Next

- [API reference](api.md)
- [Types and TypeScript](types.md)
- [Examples](../examples/): `basic.js`, `batch.js`, `stream.js`, `pool.js`, `subscribe.js`, `typescript.ts`
