# skaidb — Node.js / TypeScript driver

The official [skaidb](https://skaidb.org/) driver for Node.js. The API is
modeled on [node-postgres (`pg`)](https://node-postgres.com/): `new Client(…)`,
`await client.query(…)`, `{ rows, rowCount, fields }`. **Zero dependencies**
(pure `net`, `tls`, `crypto`), one file, TypeScript declarations included.

- Node.js **18 or newer**, CommonJS (`require`) and ESM/TypeScript (`import`) both work.
- Speaks skaidb's binary protocol directly: SCRAM-SHA-256 auth, server-side
  prepared statements with typed parameters, streaming result sets, one-round-trip
  batches, full type fidelity (bigint, Decimal, UUID, Bytes, Timestamp, Array, Document).
- Multi-seed failover, transparent reconnect, TLS, connection pooling.

Full documentation: this README, the [`docs/`](docs/) folder
([getting started](docs/getting-started.md) · [API reference](docs/api.md) ·
[types and TypeScript](docs/types.md)), runnable [`examples/`](examples/), the
[wire protocol specification](https://skaidb.org/docs/PROTOCOL.html) and the
[skaidb documentation](https://skaidb.org/docs/).

## Install

```sh
npm install github:porcupin26/skaidb-node#v1.0.1
```

The package is named `skaidb`, so it is required as `skaidb` whatever the
install source. (Once published to the npm registry, `npm install skaidb`
installs the same package.)

## Quick start

```js
const { Client } = require('skaidb');

const client = new Client({
  host: 'localhost', port: 7000,
  user: 'skaidb', password: 'secret',
});
await client.connect();

await client.query('CREATE TABLE users (PRIMARY KEY (id))');
await client.query('INSERT INTO users (id, name) VALUES ($1, $2)', [1, 'Ada']);

const res = await client.query('SELECT id, name FROM users WHERE id = $1', [1]);
console.log(res.rows);      // [ { id: 1, name: 'Ada' } ]
console.log(res.rowCount);  // 1

await client.end();
```

TypeScript:

```ts
import { Client, QueryResult } from 'skaidb';

interface User { id: number; name: string }
const client = new Client({ seeds: ['db1:7000', 'db2:7000'], user: 'app', password: 'secret' });
await client.connect();
const res: QueryResult<User> = await client.query<User>('SELECT id, name FROM users');
```

## Connecting

```js
new Client({
  // Where. Either host/port, or a seed list (which wins when both are given).
  host: 'localhost',            // default 'localhost'
  port: 7000,                   // default 7000; also the port for seeds given without one
  seeds: ['db1:7000', 'db2:7000', 'db3'],

  // Who.
  user: 'skaidb',               // default 'anonymous'
  password: 'secret',           // default '' (anonymous)
  database: 'app',              // optional: `USE app` right after the handshake

  // How.
  consistency: 'QUORUM',        // 'ONE' | 'QUORUM' | 'ALL' (or 0 | 1 | 2); default QUORUM
  connectTimeout: 10000,        // ms per seed, TCP + TLS handshake

  // TLS (see below).
  tls: false, tlsCa: undefined, tlsInsecure: false, tlsServerName: 'skaidb',
});
```

Every option is also accepted by `Pool`.

### Seeds and failover

skaidb is leaderless: every node accepts reads and writes, so a seed list is
only "somewhere to land". `connect()` shuffles the seeds and tries them in
turn until one **connects and authenticates** (a node that accepts TCP while
unhealthy does not swallow the attempt). If none works it throws
`no reachable endpoint in <list>: <last error>`. Bare hosts take `port`.

The same walk runs again on reconnect (below), so a client survives the node
it was talking to going away.

### TLS modes

| Option | Meaning |
|---|---|
| *(none)* | Plaintext. A server with `client_tls = required` refuses this. |
| `tls: true` | TLS, server certificate verified against the system trust store. |
| `tlsCa: '/path/ca.pem'` | TLS, certificate verified against this PEM bundle. Implies `tls`. |
| `tlsInsecure: true` | TLS with **no** certificate verification: encrypts, authenticates nothing. Development only. Implies `tls`. |
| `tlsServerName` | SNI / hostname to verify (default `skaidb`). Must match a SAN on the server certificate, which is usually *not* the address you dialled — skaidb's own certificates carry `DNS:skaidb`. |

```js
new Client({ seeds, user, password, tlsCa: '/etc/skaidb/skai-ca.crt' });  // recommended
```

### Consistency

`consistency` selects how many replicas must acknowledge a write or be
consulted for a read: `ONE`, `QUORUM` (the default) or `ALL`. Set it per client
or per statement:

```js
const client = new Client({ ..., consistency: 'ONE' });
await client.query({ text: 'SELECT ...', consistency: 'ALL' });
for await (const row of client.stream('SELECT ...', { consistency: 'ONE' })) { ... }
```

`batch()` uses the client's level. The `CONSISTENCY` export holds the
numeric constants (`{ ONE: 0, QUORUM: 1, ALL: 2 }`).

## Queries

### `client.query(text, values?)` / `client.query({ text, values, consistency, rowMode })`

Runs one statement and resolves to a result:

```js
{
  command:  'SELECT' | 'MUTATION' | 'DDL' | 'CALL',
  rowCount: number | null,     // rows returned, rows affected, or null for DDL
  rows:     [...],             // objects keyed by column name (default) or arrays
  fields:   [{ name }],        // column descriptors, pg-style
  columns:  ['id', 'name'],    // column names, for row-producing statements
  resultSets?: [...]           // only for a CALL whose body EMITs; see below
}
```

`rowMode: 'array'` yields each row as a cell array in column order (like `pg`).

Statements on one client are **serialized**: one request/response is in
flight at a time and further calls queue. Use a `Pool` (or several clients)
for concurrency.

### Parameters and prepared statements

Placeholders are `$1, $2, …` (pg style). A parameter may be referenced more
than once; `$N` inside a string literal is left alone.

```js
await client.query('UPDATE users SET name = $2, seen = $3 WHERE id = $1', [1, 'Ada', new Date()]);
```

With parameters, the driver **prepares the statement on the server** (once per
distinct SQL text per connection, cached up to 240 entries) and sends the
values as **typed binary** — no string interpolation, no injection surface,
and JavaScript arrays and plain objects travel as skaidb `Array` and
`Document` values, which have no SQL literal form. The statement's parameter
count must match what you pass (missing ones throw before anything is sent).

There is no separate `prepare()`/`execute()` API: preparing is automatic.
Statements the server declines to prepare (DDL, `USE` and other session
statements) fall back to safe client-side quoting of the same `$N`
parameters, so every statement kind accepts parameters. Prepared ids are
scoped to a connection and the cache is dropped on reconnect.

### `client.batch(sql, rows)` — many rows, one round-trip

```js
const n = await client.batch('INSERT INTO t (id, tags) VALUES ($1, $2)',
                             [[1, ['a']], [2, []], [3, ['b', 'c']]]);   // n === 3
```

Runs a prepared statement once per row in a single request and returns the
**total** affected count. Rows **autocommit individually**: on the first
failing row the server answers with an error naming the row index and how
many rows applied before it, and those earlier rows stay applied — so use
idempotent statements. Only preparable statements (`SELECT`/`INSERT`/
`UPDATE`/`DELETE`) can be batched; the whole request must fit one 64 MiB
frame. An empty `rows` resolves to 0 without a round-trip.

### Multiple result sets

A `CALL` of a procedure whose body runs `EMIT <select>` answers with every
emitted set plus the call's final result:

```js
const r = await client.query('CALL report()');
r.resultSets   // every set, in emission order, each { rows, fields, columns, rowCount }
r.rows         // the LAST set (the call's own result)
```

### Transactions

Every statement autocommits; the driver adds no transaction API of its own.
Whatever transaction statements your server version supports are plain SQL
sent through `query()` on one connection (statements on a client are
serialized, so they run in order). `batch()` rows autocommit one by one, as
described above.

## Streaming large results — `client.stream(sql, opts?)`

`query()` buffers the whole result; a large scan is also bounded by the
server's scan budgets. `stream()` asks the server to deliver the rows in
chunks and yields them one at a time, holding one chunk in memory:

```js
for await (const row of client.stream('SELECT id, v FROM readings')) {
  process(row);
}
console.log(client.stream.columns);   // column names, set once the header arrives
```

Options: `{ consistency, rowMode }`. It takes **no parameters** (the streaming
opcode carries SQL text only). A non-row statement streamed this way yields
nothing. An error before any row is an ordinary statement error; an error
partway through (a node dying mid-scan, a scan budget tripping) is thrown
after the rows already yielded, which are valid.

On a cluster, name the columns: a bare `SELECT *` is only executed page by
page at consistency `ONE`; a named column list streams at any level.

### The abandon rule

**The connection is busy for the whole stream, and closing the iterator is
what frees it.** `break`, `return`, a throw out of the loop body and an
explicit `iterator.return()` all close it. On an early close the driver reads
the rest of the result out (up to 8 MB) so the socket is left at a request
boundary; past that it drops the connection instead, and the next statement
transparently reconnects. Either way the next statement reads its own reply.

An iterator that is merely dropped half-read is closed by nobody — JavaScript
has no finalizer that could do it — and that connection stays busy for good,
with every queued statement behind it. Always iterate to the end or close it.

```js
const it = client.stream('SELECT ...');
try {
  const first = await it.next();
  ...
} finally {
  await it.return();     // releases the connection
}
```

Other `query()`/`batch()` calls made while a stream is open **queue** behind
it rather than failing.

## Following a stream — `client.subscribe(name, { after, pollMs })`

For tables with a `CREATE STREAM`, `subscribe()` yields the stream's events
as they arrive, forever:

```js
for await (const ev of client.subscribe('big_orders', { after: lastSeenId, pollMs: 500 })) {
  // ev = { id, op, k, ts, doc }
  save(ev.id);            // pass it back as `after` to resume exactly here
}
```

It polls the stream's log (`_stream_<name>`) with a keyset cursor, 500 events
per page; `id` is the position — an opaque **string** that sorts in log order
(`"00000001789857620741-0000000000-0902800000000000000100"`), not a number, so
`after` takes a saved `id` string or `null`. For push delivery subscribe to
`$stream/<db>/<name>` with any MQTT client instead — the events are identical.

To stop, `break` out of the loop, call `.return()` on the iterator, or pass an
`AbortSignal` as `signal` and abort it; each ends the iteration within a tick
while it is idle between polls (an aborted signal ends it cleanly, without an
error):

```js
const ac = new AbortController();
setTimeout(() => ac.abort(), 60_000);
for await (const ev of client.subscribe('big_orders', { signal: ac.signal })) {
  handle(ev);
}                          // the loop exits when the signal fires
```

## Pooling — `new Pool(options)`

```js
const { Pool } = require('skaidb');
const pool = new Pool({ seeds: ['h1:7000', 'h2:7000'], user, password, database: 'app', maxsize: 8 });

const res = await pool.withConnection((c) => c.query('SELECT 1'));

const conn = await pool.acquire();
try { await conn.query('...'); } finally { await pool.release(conn); }

await pool.end();
```

`maxsize` (default 10) bounds the connections kept **idle**, not the number
checked out: a burst opens extras and the surplus is closed on release.
`acquire()` validates an idle connection before handing it out and discards
one the server closed meanwhile; `release()` closes a broken connection
instead of pooling it. Every `Client` option passes through.

## Reconnect and errors

Every error the driver raises is a `SkaidbError` (an `Error` subclass).

- **Statement errors** (`SELECT nope` → `no such column …`) reject the call;
  the connection stays usable.
- **Transport errors** reject the in-flight statement with `connection lost`
  / `connection closed` and mark the client *broken*, not closed. The next
  statement **re-dials** (through the seed walk), re-authenticates, re-sends
  `USE <database>` and clears the prepared-statement cache. Because the failed
  statement *may* have executed, the driver never retries it for you: retry a
  write only if it is idempotent. `isUsable()` reports the state.
- **Protocol desync** (an unexpected frame): the driver drops the connection
  (`isUsable()` → false) rather than hand the next caller a misaligned reply,
  and the next statement reconnects.
- `end()` is terminal: a closed client never reconnects (`connection is closed`).

Common messages: `connect failed: …`, `connect timeout`, `no reachable
endpoint in …`, `authentication denied: …`, `server signature mismatch
(mutual auth failed)`, `cannot read tlsCa …`, `server does not support
streaming: …`, `statement expects N parameters, got M`, `no parameter for
$N`, `cannot bind value of type …`, `cannot bind NaN/Infinity`, `bigint …
does not fit a 64-bit integer`, `stream abandoned with too much data left to
drain`.

## Type mapping

| skaidb | JavaScript (results) | Bind (parameters) |
|---|---|---|
| Null | `null` | `null`, `undefined` |
| Bool | `boolean` | `boolean` |
| Int (i64) | `number`, or `bigint` when outside ±2^53 | integer `number`, `bigint` |
| Float | `number` | non-integer `number` (NaN/Infinity refused) |
| Decimal | `string` (exact, e.g. `'123.45'`) | bind as `string` |
| String | `string` | `string` |
| Bytes | `Buffer` | `Buffer` |
| Uuid | `string` (canonical `8-4-4-4-12`) | bind as `string` |
| Timestamp | `Date` (millisecond precision) | `Date` |
| Array | `Array` | `Array` (nested) |
| Document | plain object, key order preserved | plain object |

An integer-valued `number` binds as Int and a fractional one as Float; pass a
`bigint` to force Int. In the client-side fallback (unpreparable statements)
a `Date` renders as its epoch milliseconds, a `Buffer` as a hex string, and
arrays/objects cannot be bound at all.

## Compatibility

- **Node.js** ≥ 18. No build step, no native code.
- **Server**: any skaidb. Prepared statements need server ≥ 0.17.0 (older
  servers get the client-side fallback automatically), `batch()` ≥ 0.87.0,
  `stream()` a server with the streaming opcode (an older one answers `server
  does not support streaming`), multiple result sets a server with `EMIT`.
  The Hello frame that fills the server's `drivers` table (`client_name`
  `nodejs`, `client_version` = this package's version) is ignored by older
  servers.
- **Wire protocol**: <https://skaidb.org/docs/PROTOCOL.html>.

## Development

```sh
npm test            # node:test unit tests, no server needed (~1 s)
npm run typecheck   # compiles test/types/consumer.ts against skaidb.d.ts
npm run pack:check  # what a publish would ship
node examples/basic.js host 7000 user password   # against a real node
```

CI runs the suite on Node 18, 20 and 22 and the TypeScript check on every
push and pull request. Tagging `vX.Y.Z` runs the publish workflow, which
publishes to npm when the `NPM_TOKEN` secret is present and otherwise exits
with a notice.

## License

[SSPL-1.0](LICENSE).
