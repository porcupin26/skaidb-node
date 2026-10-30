# Changelog

All notable changes to the skaidb Node.js driver. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

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
