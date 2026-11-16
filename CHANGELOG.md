# Changelog

All notable changes to the skaidb Node.js driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [1.5.1] - 2026-11-16

### Fixed
- A connection that had run more than 256 different parameterised
  statements stopped binding typed values without saying so. The driver
  caches 240 prepared statements per connection and the server keeps 256;
  a statement that did not fit the cache was prepared, run and left open,
  so the 257th was refused (`too many prepared statements`) and the driver
  answered that as it answers a statement that cannot be prepared, by
  sending the text with client-side literals. From then on every statement
  new to that connection stored a `Date` as its epoch milliseconds (a
  number, not a timestamp) and a `Buffer` as a hex string, rejected arrays
  and objects with `cannot bind value of type object`, and `batch()`
  failed with `statement cannot be prepared, so it cannot be batched`. A
  statement the cache has no room for is now closed on the server once it
  has run, so the server's limit is never reached.
- A prepare the server refuses because the connection's statement table
  is full rejects with the server's message instead of falling back to
  literals. The fallback is for refusals that are about the statement —
  DDL, `USE` and other session statements, and servers without prepared
  statements — and those are unchanged.
- A statement on a client that is not connected rejects with
  `SkaidbError('not connected: await connect() first')`. `query()`,
  `batch()`, `stream()`, `transaction()` and `subscribe()` on a client
  whose `connect()` was never called, had failed or was still running
  rejected with a raw `TypeError: Cannot read properties of null (reading
  'write')`, and one sent while the handshake was in flight was written
  into the middle of it. Nothing is sent now, and a transaction's body
  does not run.
- `transaction()` no longer hangs when its body leaves a stream open. A
  stream holds its connection until its iterator is finished or closed, so
  a body that opened one and returned or threw without closing it — taking
  only the first row with `.next()` is enough — left the `COMMIT` or
  `ROLLBACK` waiting behind it for good: `transaction()` never settled,
  the body's error was lost, later statements on the client hung, a pool
  never got the connection back, and the server kept the transaction
  open. The client now closes the connection, which makes the server drop
  the transaction, and `transaction()` rejects — with the body's own error
  if it threw, otherwise with `the transaction body left a stream open: …`
  — without a retry. The next statement reconnects; a pool closes that
  connection instead of pooling it. A stream read to its end or left by a
  `for await` loop is closed already and changes nothing.
- A `number` too large for a 64-bit integer — `2 ** 63`, `1e21`,
  `Number.MAX_VALUE`, all integer-valued to JavaScript — binds as a Float
  instead of rejecting with Node's `RangeError: The value of "value" is
  out of range`, and an invalid `Date` rejects with `SkaidbError('cannot
  bind an invalid Date')` instead of a `RangeError` as well. In the
  client-side fallback a number is now the literal of the same value: an
  integer past 2^53 was written as a nearby, different integer, one from
  2^63 up as a literal the server refuses, and `-0` as `0`.
- The session database survives a reconnect when it was entered with a
  `USE` statement. A client re-dials a lost connection on its next
  statement and a new connection starts in the server's default database;
  the `database` option was re-entered there, a database entered with
  `client.query('USE app')` was not, so the next statement ran in the
  default database (or the option's) against whatever table of the same
  name is there — refused for a role without rights on it, otherwise
  reading or writing the wrong table. The client now notes a `USE
  [DATABASE] <name>` the server accepted (through `query()` or
  `stream()`, as one statement of its own), keeps it in `client.database`,
  and enters that database on every dial before anything else is sent. If
  it cannot be entered on the new connection, the statement fails with the
  server's error and is not sent.
- A pool with a `database` option hands every borrower a connection in
  that database. One whose previous borrower had sent `USE other` came
  back from the pool still in `other`; `release()` now puts it back into
  the pool's database first, and closes it if that cannot be done. A pool
  without a `database` option leaves a connection where its borrower left
  it, as before.
- `connect()` called again on a client empties its prepared-statement
  cache, as the client's own re-dial of a lost connection always did. An
  application that reconnects by hand kept the ids of the old connection,
  and an id is only an index on the connection that issued it: on the new
  one a cached statement failed with `unknown prepared statement id N` or,
  once another statement had taken that number there, ran as that
  statement with its own values — an `UPDATE` executed as the `DELETE`
  holding its old id. Every dial now starts with an empty cache, and a
  cached id is used, and closed, only on the connection that issued it.
- `connectTimeout` bounds the whole connect, the login handshake included.
  It covered only the TCP connect and TLS, so a peer that accepts the
  connection and then says nothing held `connect()` — and a statement
  re-dialling a lost connection — for good, and the next seed was never
  tried. Such a seed now fails with `connect timeout` and the walk goes
  on. The timer also ends with the attempt: a script whose `connect()`
  was refused at once no longer stays alive for the rest of
  `connectTimeout`.
- `client.end()` rejects the statements waiting behind a stream that is
  still open, and every statement sent afterwards, with `connection is
  closed`. They waited for good: an open stream holds the client's
  statement queue, and `end()` did not let it go.
- A connection that ends with a socket error instead of a clean close — a
  reset, a broken pipe, a TLS alert — rejects the statement in flight with
  a `SkaidbError`, `connection lost: read ECONNRESET` and the like, whose
  `cause` is the socket's error. It rejected with Node's own `Error`, so
  `instanceof SkaidbError` did not see a lost connection there.
- `client.end()` resolves to `undefined` however often it is called. It
  resolved to `null`, and on a client that was already ended to the
  socket's `ERR_STREAM_ALREADY_FINISHED` error object.

## [1.5.0] - 2026-11-15

### Added
- **`client.transaction(body, { attempts, begin })`** runs `body` in a
  transaction on the client's connection and resolves to its value: `BEGIN`
  (or the `begin` text, sent verbatim — `BEGIN ATOMIC …` on a cluster),
  `body(client)`, `COMMIT`. A `COMMIT` the server refuses for a conflict
  runs the whole transaction again from `BEGIN`, at once, until it has run
  `attempts` times in all (default 3); the last conflict is thrown, so
  `body` must be safe to run more than once. If `body` throws, `ROLLBACK`
  is sent and `body`'s error is thrown unchanged, whatever the `ROLLBACK`
  answers, with no retry. If `BEGIN` fails, `body` never runs and nothing
  else is sent. Any other `COMMIT` error is thrown without a retry or a
  `ROLLBACK`. `attempts` below 1 is refused before anything is sent.
  While the transaction is open the client does not reconnect: a statement
  that finds the connection gone fails with `connection lost during a
  transaction` rather than running on a new connection, outside the
  transaction. A second `transaction()` on a client that has one running is
  refused.
- **`pool.transaction(body, { attempts, begin, affinity })`**: the same on
  one checked-out connection, held for the whole transaction and returned
  afterwards.
- **`TransactionConflictError`**, a `SkaidbError` subclass, for a statement
  error whose message starts with `transaction conflict:` — a standalone
  server ≥ 0.346.0 refusing a `COMMIT` because another writer changed a row
  the transaction writes after its `BEGIN`. `query()`, `batch()` and
  `stream()` raise it; the message is the server's, the connection stays
  usable, and code that catches `SkaidbError` keeps catching it.
- The declarations gain `Client.transaction`, `Pool.transaction`,
  `TransactionOptions`, `PoolTransactionOptions` and
  `TransactionConflictError`.

### Changed
- `pool.release()` closes, instead of pooling, a connection that a
  `transaction()` may still be open on: one released while its transaction
  is running, or after one whose `ROLLBACK` failed.

## [1.4.1] - 2026-10-31

### Fixed
- One slow round trip at the start can no longer get a healthy member
  ejected for latency. A member's first round trip set its average
  outright, so a statement that stalled in the client — a CPU-bound
  application blocking the event loop while it starts — put seconds into
  it; ten round trips later the member was compared with the others, and
  the average had barely decayed: ejected (`latency 4153.7 ms vs median
  0.2 ms`) while it answered in milliseconds. The fastest of a member's
  first three round trips now seeds its average (a round trip is only ever
  lengthened), and later ones move it as before. Until then the member
  counts as not measured, as it did before its first round trip:
  `pool.members()` reports `latencyMs: null` and `'least_latency'` scores
  it 0.
- A latency ejection's `'eject'` event names the slowest of the member's
  last 10 round trips in its `reason`, alongside its average and the
  median (`latency 516.3 ms vs median 0.2 ms; slowest of the last 10 round
  trips 900.0 ms`), which tells a slow member from one slow statement.

## [1.4.0] - 2026-10-29

### Fixed
- A streamed `CALL` whose procedure body `EMIT`s result sets returns them.
  The server answers such a call with every set in one `ResultSets` frame
  (tag 8) even to a stream request, and `client.stream()`, with or without
  values, rejected that frame (`unexpected response tag 8 to stream
  request`) and dropped the connection. The frame is now decoded by the
  code `query()` uses: the stream yields the rows `query()` would return
  (the call's final result, the last set), `client.stream.command` is
  `'CALL'`, and the new `client.stream.resultSets` slot holds every set in
  emission order, exactly as `query()`'s `resultSets` does (`null` for
  every other statement). The exchange is complete in that one frame, so
  nothing is drained and the connection is free before the first row is
  yielded: a statement awaited inside the loop runs rather than queueing
  behind the stream. The declarations gain
  `StreamFunction.resultSets` and `'CALL'` in `StreamFunction.command`.

## [1.3.0] - 2026-10-29

### Added
- **Parameterised streaming**: `client.stream(sql, values, opts?)` binds
  `$1, $2, …` exactly as `query(text, values)` does — typed, arrays and
  documents included — over `OP_QUERY_STREAM_PARAMS` (server ≥ 0.330), so a
  range-bounded export no longer needs its literals interpolated into the
  SQL. The values may also go in the options (`opts.values`, which wins
  over the argument, as `config.values` does for `query()`). Without values
  the request is the original `OP_QUERY_STREAM`, byte for byte, so it works
  against every server, and `stream(sql, opts)` is unchanged. Against an
  older server a stream with values fails with `server does not support
  parameterised streaming (skaidb >= 0.330): …`, and the connection stays
  usable. The declarations gain `StreamOptions.values` and the
  `(sql, values?, opts?)` call form.

## [1.2.0] - 2026-10-25

### Added
- **Pool load balancing across members** (the rules every skaidb driver
  shares, PROTOCOL.md §6.1): `loadBalancing: 'round_robin'` /
  `'least_latency'` (power of two choices on the round-trip average ×
  checked-out connections, re-measuring a member not used for 2 s;
  `'random'` stays the default), `idleOrder: 'fifo'`, health ejection
  (consecutive transport failures, or a latency average far above the other
  members') with a background probe that re-admits the member,
  `discover: true` (members from `SHOW ENDPOINTS`, server ≥ 0.327: joiners
  added, leavers dropped, resyncing/unreachable members skipped and
  re-checked every 5 s), and `readYourWrites: <ms>` with
  `acquire({ affinity })` / `withConnection({ affinity }, fn)` — reads
  follow the member that took the key's last write. `pool.members()`
  reports each member's state, latency and connections, and the pool emits
  `'eject'` and `'readmit'`. The probe/discovery timer is `unref()`'d and
  stops with `pool.end()`.

### Changed
- A pooled connection that re-dials does so in the pool's order (usable
  members first, the one it lost last) instead of walking the shuffled
  seeds.
- `Pool` is an `EventEmitter`, and `release()` closes a connection whose
  member is ejected, not `ready` or unknown to the pool (such as a client
  the pool did not create) instead of keeping it idle.

### Fixed
- A seed that accepted the TCP connection and then closed it during the
  handshake made every later seed in the same walk fail with
  `connection lost`, so `connect()` (and a reconnect) gave up while a
  healthy seed was still untried. Each attempt now starts clean.

## [1.1.0] - 2026-09-26

### Added
- Certificate login: `authMechanism: 'certificate'` authenticates with the
  TLS client certificate (wire mechanism EXTERNAL, PROTOCOL.md §2.4). The
  certificate's Common Name is the username and no password is sent; `user`,
  when given, asserts the expected identity.
- `tlsClientCert` / `tlsClientKey` (PEM paths) present a client certificate
  in the TLS handshake, for servers that verify clients; either implies TLS.
- `client.stream.command` (`'SELECT'`, `'MUTATION'` or `'DDL'`) and
  `client.stream.affected` (a mutation's affected-row count) describe a
  streamed statement that yields no rows.
- `batch(sql, rows, { consistency })`: a per-batch consistency level.
- The shared skaidb wire-protocol conformance suite
  (`conformance/vectors.json`, run by `test/conformance.test.js` as part of
  `npm test`); CI fails when the vendored vectors differ from
  <https://skaidb.org/conformance/vectors.json>.

### Fixed
- A streamed non-row statement (`UPDATE`, `DELETE`) gave no way to read its
  affected-row count, and `client.stream.columns` kept the previous
  stream's column names; it is now `null` for a statement without rows.
  Found by the conformance suite.
- A connect attempt that failed the handshake (denied, a server signature
  mismatch, a protocol error) left its socket open: every failed seed or
  retry leaked a connection. The socket is now closed and its unread frames
  dropped before the next seed is tried.
- `-0` was bound as Int `0`, losing its sign; it now binds as Float `-0.0`.

## [1.0.3] - 2026-09-20

### Changed
- Release automation: published from GitHub Actions. Pushing a `vX.Y.Z`
  tag now runs the tests, publishes `@skaidb/client` to npm and creates
  the GitHub Release from this changelog section (no code change; see
  `RELEASING.md`).

## [1.0.2] - 2026-09-20

### Changed
- Published to npm as `@skaidb/client` (package renamed; no code change).
  Install with `npm install @skaidb/client` and require it as
  `@skaidb/client`. Installing from GitHub
  (`npm install github:porcupin26/skaidb-node#v1.0.2`) yields the same
  module name.

## [1.0.1] - 2026-09-19

### Fixed
- `StreamEvent.id` and `SubscribeOptions.after` were declared as `number`,
  but a stream log id is an opaque **string** that sorts in log order
  (`"00000001789857620741-0000000000-0902800000000000000100"`). A consumer
  that followed the types and resumed with `after: 0` issued `WHERE id > 0`
  and never received an event. The declarations now say `id: string` and
  `after?: string | null`; the docs, README and example say so too, and the
  TypeScript consumer compiled in CI asserts that a numeric id or `after`
  no longer type-checks.
- `.return()` on an idle `subscribe()` iterator did not resolve until the
  whole `pollMs` sleep had elapsed (the generator was suspended inside the
  sleep and could not see the request). The sleep is now woken by
  `.return()`, so `break`/`.return()` take effect within a tick while the
  iterator is idle; a stop requested while a page fetch is in flight takes
  effect as soon as that page arrives, without yielding it.
- `?` placeholders were accepted silently and failed on the server with
  `statement expects N parameters, got 0`. With values given, a statement
  containing a bare `?` (outside string literals) and no `$N` now rejects
  before anything is sent: `this driver uses $1, $2 … placeholders; '?' is
  not a placeholder`. `batch()` applies the same rule.

### Added
- `subscribe(name, { signal })`: an `AbortSignal` that ends the iteration
  cleanly (no error), within a tick while idle. A signal that is already
  aborted yields nothing and sends nothing.

### Changed
- Install coordinate: `npm install github:porcupin26/skaidb-node#v1.0.1`.

## [1.0.0] - 2026-09-19

First release as a standalone package (`github.com/porcupin26/skaidb-node`),
carrying its full history over from the skaidb monorepo's `drivers/nodejs`.

### Added
- `Client`: SCRAM-SHA-256 handshake, `query()` with `$1`-style parameters,
  server-side prepared statements with typed bindings, `batch()`,
  `stream()` with the abandon/drain rule, `subscribe()` over stream logs,
  multi-seed failover, transparent reconnect, TLS (`tls`, `tlsCa`,
  `tlsInsecure`, `tlsServerName`), per-connection `database`, and
  per-client or per-statement consistency (`ONE`/`QUORUM`/`ALL`).
- `Pool`: a bounded idle pool with `acquire()`/`release()`/`withConnection()`.
- Multiple result sets (`resultSets`) from a `CALL` whose body `EMIT`s.
- Hello self-identification: the version sent to the server is the
  package version.
- TypeScript declarations covering the whole public surface (`Pool`,
  `seeds`, `stream`, `batch`, `subscribe`, `isUsable`, `resultSets`).
- Unit tests (`node:test`) for the value codec, parameter binding, framing,
  and a full in-process fake server exercising connect/query/stream/batch/
  pool/failover; CI on Node 18, 20 and 22; a TypeScript consumer compiled
  in CI against the declarations.
- Documentation: README plus `docs/` (getting started, API reference,
  types and TypeScript usage), and runnable `examples/`.

### Fixed
- A client with `database` set deadlocked on its first statement after a
  lost connection: the reconnect issued `USE` through the serialized query
  chain, behind the very statement that was reconnecting. `USE` now goes
  straight to the socket.
- A destroyed socket's late `close` event could mark the replacement
  connection broken (and destroy it) when a statement re-dialled right
  after an abandoned stream was dropped. Socket events are now ignored once
  the socket is no longer the client's.
- Binding a `bigint` outside the 64-bit range threw a raw `RangeError`; it
  is now a `SkaidbError`.

### Changed
- Package version series restarts at 1.0.0; `engines.node` is `>=18`.
- The example moved to `examples/basic.js`.

[1.0.2]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.2
[1.0.1]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.1
[1.0.0]: https://github.com/porcupin26/skaidb-node/releases/tag/v1.0.0
