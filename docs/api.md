# API reference

```js
const { Client, Pool, SkaidbError, CONSISTENCY } = require('skaidb');
```

Everything is exported from the package root. Types for TypeScript are in
[`skaidb.d.ts`](../skaidb.d.ts); see [types.md](types.md).

## `new Client(options?)`

| Option | Type | Default | Meaning |
|---|---|---|---|
| `host` | `string` | `'localhost'` | Host to dial when `seeds` is not given. |
| `port` | `number` | `7000` | Port to dial; also the port for a seed given without one. |
| `seeds` | `string[]` | — | `['h1:7000', 'h2:7000', 'h3']`. Overrides `host`/`port`. Shuffled on every connect. |
| `user` | `string` | `'anonymous'` | User name. |
| `password` | `string` | `''` | Password. Empty means anonymous; mutual authentication is skipped. |
| `database` | `string` | — | Selected with `USE` after every (re)connect. |
| `consistency` | `'ONE' \| 'QUORUM' \| 'ALL' \| 0 \| 1 \| 2` | `'QUORUM'` | Default for every statement on this client. Names are case-insensitive. |
| `connectTimeout` | `number` (ms) | `10000` | Per seed, covering TCP connect and the TLS handshake. |
| `tls` | `boolean` | `false` | TLS with the system trust store. |
| `tlsCa` | `string` (path) | — | PEM CA bundle to verify the server against. Implies `tls`. |
| `tlsInsecure` | `boolean` | `false` | TLS without certificate verification. Implies `tls`. Development only. |
| `tlsServerName` | `string` | `'skaidb'` | SNI and verification name; must match a SAN on the server certificate. |

The constructor only stores the configuration; nothing is dialled until
`connect()`.

Public fields after construction: `host`, `port` (the endpoint most recently
dialled), `seeds` (parsed to `{ host, port }`), `user`, `password`,
`consistency` (resolved to `0 | 1 | 2`), `connectTimeout`, `tls`, `tlsCa`,
`tlsInsecure`, `tlsServerName`, `database`.

### `client.connect(): Promise<void>`

Shuffles the seeds and tries each until one connects and authenticates.
Then sends the Hello frame (driver name `nodejs`, version = the package
version; ignored by servers that predate it) and `USE "<database>"` if
`database` was given. Rejects with `SkaidbError('no reachable endpoint in
<seeds>: <last error>')` when every seed failed, or with the authentication
error (`authentication denied: …`, `server signature mismatch (mutual auth
failed)`) when the handshake failed.

### `client.query(text, values?)` · `client.query(config, values?)`

`config = { text, values?, consistency?, rowMode? }`. `config.values` wins
over the second argument. Returns `Promise<QueryResult>`:

| Field | Meaning |
|---|---|
| `command` | `'SELECT'` (rows), `'MUTATION'` (INSERT/UPDATE/DELETE), `'DDL'` (CREATE/DROP/USE/…), `'CALL'` (a procedure that EMITs). |
| `rowCount` | Rows returned for `SELECT`/`CALL`, rows affected for `MUTATION`, `null` for `DDL`. |
| `rows` | Row objects keyed by column name; with `rowMode: 'array'`, cell arrays in column order. |
| `fields` | `[{ name }]`, one per column (empty for non-row statements). |
| `columns` | Column names in order; present for row-producing statements. |
| `resultSets` | Only for `'CALL'`: every emitted set in order, each a `QueryResult`, the call's own result last. `rows`/`fields`/`columns`/`rowCount` mirror that last set. |

Behaviour:

- Statements on one client are serialized in call order; a call made while
  another (or a stream) is in flight waits.
- A broken connection is re-dialled before the statement is sent
  ([reconnect](#reconnect)).
- A statement error rejects with `SkaidbError(<server message>)` and leaves
  the connection usable.

#### Parameters

Placeholders are `$1, $2, …`. Rules:

- A placeholder may be used more than once; missing ones (`$3` with two
  values) throw `no parameter for $3` before anything is sent. Extra values
  are ignored.
- `$N` inside a single-quoted string literal is text, not a placeholder.
- `?` is **not** a placeholder. With values given, a statement that contains
  a bare `?` (outside string literals) and no `$N` at all rejects with
  `this driver uses $1, $2 … placeholders; '?' is not a placeholder` before
  anything is sent, instead of the server's puzzling `statement expects N
  parameters, got 0`.
- With one or more values the statement is **prepared on the server** and
  executed with typed bindings; see [types.md](types.md) for the mapping.
  The server's parameter count must equal the number of placeholders used
  (`statement expects N parameters, got M`).
- Prepared statements are cached per connection by SQL text (240 entries).
  The cache is dropped on reconnect since ids are connection-scoped.
- When the server refuses to prepare the statement (DDL, `USE`, other
  session statements), the driver falls back to interpolating the same
  values as **correctly quoted SQL literals**: strings with `'` doubled,
  numbers as is, booleans `TRUE`/`FALSE`, `null`/`undefined` → `NULL`, `Date`
  → epoch milliseconds, `Buffer` → hex string. Arrays and objects cannot be
  bound this way (`cannot bind value of type object`).

### `client.batch(sql, rows): Promise<number>`

`rows` is an array of parameter arrays for `$1, $2, …`. The statement is
prepared once and executed once per row in a **single round-trip**; the
result is the total affected count.

- Rows autocommit individually. On the first failing row the server rejects
  the call with a message naming the row index and how many rows applied
  before it; those rows stay applied. Use idempotent statements.
- Each row must bind exactly the statement's parameter count.
- Only preparable statements (`SELECT`/`INSERT`/`UPDATE`/`DELETE`, and
  `EXPLAIN` of those) can be batched; others reject with `statement cannot be
  prepared, so it cannot be batched`.
- Uses the client's default consistency.
- The whole request must fit one 64 MiB frame; split very large batches.
- `rows = []` resolves to `0` without a round-trip.

### `client.stream(sql, opts?)` → `AsyncGenerator<Row>`

`opts = { consistency?, rowMode? }`. Yields the statement's rows one at a
time while holding a single server chunk in memory. Takes no parameters:
interpolate values yourself, or select by key with `query()`.

- `client.stream.columns` holds the column names once the header has
  arrived. The slot is on the method, not the iterator, so it describes the
  stream started most recently.
- A non-row statement (INSERT, DDL, USE) yields nothing.
- An error before any row rejects like `query()` would. An error after some
  rows (a node dying mid-scan, a scan budget tripping) is thrown from the
  iteration after those rows, which are valid; the connection stays usable.
- A `CALL` that EMITs cannot be streamed (the reply is one frame); it throws
  `unexpected response tag 8`, and the connection is dropped and re-dialled
  on the next statement.
- On a cluster, a bare `SELECT *` streams page by page only at consistency
  `ONE`; a named column list streams at any level.
- `query()`/`batch()` calls made while a stream is open queue behind it.

#### The abandon rule

The connection is busy for the whole stream. Closing the iterator frees it:
`break`, `return` out of the loop, a throw from the loop body, or an explicit
`iterator.return()`. On an early close the driver:

1. reads the remaining frames out, up to 8 MB, leaving the socket at a
   request boundary — the connection is reused; or
2. past 8 MB, drops the connection (`isUsable()` becomes false) because
   transferring the rest would cost more than a reconnect — the next
   statement re-dials transparently.

An iterator that is dropped half-read without being closed keeps the
connection busy forever, and every statement queued behind it hangs.
JavaScript offers no finalizer to rescue this. Always consume or close.

### `client.subscribe(name, opts?)` → `AsyncGenerator<StreamEvent>`

`opts = { after?: string | null, pollMs?: number, signal?: AbortSignal }`
(defaults `null`, `500`, none).
Yields the events of the stream `name` (created with `CREATE STREAM`) forever,
as `{ id, op, k, ts, doc }`: `id` is the log position, `op` the operation,
`k` the key, `ts` a `Date`, `doc` the document. It runs
`SELECT id, op, k, ts, doc FROM _stream_<name> [WHERE id > $after] ORDER BY id LIMIT 500`
repeatedly, sleeping `pollMs` when a page is empty. Persist the last `id` and
pass it as `after` to resume. Push delivery is available over MQTT on
`$stream/<db>/<name>` with identical events.

- `id` is an opaque **string** that sorts in log order, such as
  `"00000001789857620741-0000000000-0902800000000000000100"`, never a
  number: `after: 0` would issue `WHERE id > 0`, which no id satisfies, and
  the subscription would yield nothing.
- Stopping: `break` out of the `for await`, call `.return()` on the
  iterator, or abort `signal`. Each ends the iteration within a tick while
  the iterator is idle in its poll sleep (the sleep is woken, not waited
  out), or as soon as the page fetch in flight completes. An aborted
  `signal` ends the iteration cleanly, with no error; a signal that is
  already aborted yields nothing and sends nothing.

### `client.isUsable(): boolean`

`true` while connected, `false` after `end()` or once a transport error broke
the socket (until the next statement reconnects it).

### `client.end(): Promise<void>`

Closes the connection. Terminal: any later statement rejects with
`connection is closed`.

### Reconnect

A transport failure (socket error or close) rejects the in-flight statement
with `connection lost` or `connection closed` and marks the client broken.
The next `query()`, `batch()` or `stream()` first re-dials — the full seed
walk, handshake, Hello and `USE` — then runs. The prepared-statement cache
is cleared. The driver never re-runs the failed statement: it may have
executed on the server, and retrying a write could duplicate it. A protocol
desync (an unexpected frame) is handled the same way: the connection is
dropped rather than reused misaligned.

## `new Pool(options?)`

All `Client` options plus `maxsize` (default `10`, must be ≥ 1): the number
of connections kept **idle**. Checking out beyond it opens extra connections;
the surplus is closed when released.

| Member | Meaning |
|---|---|
| `pool.acquire(): Promise<Client>` | An idle connection validated with `isUsable()` (a dead one is discarded and the next tried), or a freshly connected one. Rejects with `pool is closed` after `end()`. |
| `pool.release(conn): Promise<void>` | Returns a connection; closes it instead when it is broken, the pool is closed, or `maxsize` idle connections already exist. |
| `pool.withConnection(fn): Promise<T>` | `acquire()`, run `fn(conn)`, `release()` however `fn` ends. |
| `pool.end(): Promise<void>` | Closes every idle connection; checked-out ones close on release. |
| `pool.maxsize`, `pool.closed` | The configured bound; whether `end()` was called. |

A connection checked out with `acquire()` is an ordinary `Client`: the same
serialization, streaming and reconnect rules apply.

## `SkaidbError`

`class SkaidbError extends Error`. Every error the driver raises — statement
errors carrying the server's message, protocol and codec errors, connection
and authentication errors, binding errors — is an instance. Node's own
socket errors are wrapped (`connect failed: <message>`).

## `CONSISTENCY`

`{ ONE: 0, QUORUM: 1, ALL: 2 }`. Anywhere a consistency is accepted, a name
(any case) or one of these numbers works.

## Hello and the `drivers` table

After each successful handshake the driver identifies itself to the server
(`client_name = 'nodejs'`, `client_version = <package version>`), which
appears in the server's `drivers` table. A server without this opcode answers
with an error that the driver ignores.

## Wire protocol

The driver implements the binary protocol specified at
<https://skaidb.org/docs/PROTOCOL.html>: big-endian length-prefixed frames,
the four-frame SCRAM-SHA-256 handshake with mutual authentication, opcodes
`QUERY`, `PREPARE`, `EXECUTE`, `QUERY_STREAM`, `EXECUTE_BATCH` and `HELLO`,
and the self-describing value encoding.
