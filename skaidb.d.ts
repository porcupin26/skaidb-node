// TypeScript declarations for the skaidb Node.js driver (skaidb.js).
//
// The runtime is plain CommonJS; these declarations cover its whole public
// surface: Client, Pool, SkaidbError, CONSISTENCY and the option/result shapes.

/// <reference types="node" />

import { EventEmitter } from "events";

/** A consistency level, by number or by name (case-insensitive at runtime). */
export type Consistency = 0 | 1 | 2 | "ONE" | "QUORUM" | "ALL" | "one" | "quorum" | "all";

/** Every value the wire codec can carry in either direction. */
export type Value =
  | null
  | boolean
  | number
  | bigint
  | string
  | Buffer
  | Date
  | Value[]
  | { [key: string]: Value };

/**
 * How the client logs in. `'scram'` (default): SCRAM-SHA-256 with
 * `user`/`password`. `'certificate'`: the TLS client certificate is the
 * credential (wire mechanism EXTERNAL) and its Common Name the username.
 */
export type AuthMechanism = "scram" | "certificate";

/** A `host:port` seed, or a bare host that takes the client's `port`. */
export type Seed = string;

export interface ClientOptions {
  /** Host to dial when `seeds` is not given. Default `localhost`. */
  host?: string;
  /** Port to dial, and the default port for seeds given without one. Default `7000`. */
  port?: number;
  /**
   * Endpoints to try, `['db1:7000', 'db2:7000', 'db3']`, shuffled on every
   * connect. skaidb is leaderless, so any node serves; the first seed that
   * connects AND authenticates wins. Overrides `host`/`port`.
   */
  seeds?: Seed[];
  /**
   * User name. Default `anonymous`. With `authMechanism: 'certificate'` it
   * only asserts the identity (it must equal the certificate's CN) and
   * defaults to empty.
   */
  user?: string;
  /** Password. Empty for anonymous connections. */
  password?: string;
  /** Default consistency for every statement on this client. Default `QUORUM`. */
  consistency?: Consistency;
  /** Milliseconds allowed for TCP connect (+ TLS handshake) per seed. Default `10000`. */
  connectTimeout?: number;
  /** Use TLS with the system trust store. Implied by `tlsCa` or `tlsInsecure`. */
  tls?: boolean;
  /** Path to a PEM CA bundle used to verify the server certificate. Implies `tls`. */
  tlsCa?: string;
  /** Encrypt without verifying the certificate. Development only. Implies `tls`. */
  tlsInsecure?: boolean;
  /** SNI name; must match a SAN on the server certificate. Default `skaidb`. */
  tlsServerName?: string;
  /**
   * Path to a PEM client certificate presented in the TLS handshake. Needs
   * `tlsClientKey`; implies `tls`.
   */
  tlsClientCert?: string;
  /** Path to the PEM private key of `tlsClientCert`. */
  tlsClientKey?: string;
  /**
   * `'certificate'` logs in with the client certificate instead of a
   * password (requires `tlsClientCert`/`tlsClientKey`). Default `'scram'`.
   */
  authMechanism?: AuthMechanism;
  /** Session database, selected with `USE` right after the handshake (and after every reconnect). */
  database?: string;
}

export interface Field {
  name: string;
}

export type RowMode = "object" | "array";

export interface QueryConfig {
  text: string;
  /** Parameters for `$1, $2, …` placeholders. Overrides the second argument of `query()`. */
  values?: Value[];
  /** Consistency for this statement only. */
  consistency?: Consistency;
  /** `'array'` yields each row as a cell array in column order. Default `'object'`. */
  rowMode?: RowMode;
}

export interface QueryResult<Row = any> {
  /** `'SELECT'`, `'MUTATION'`, `'DDL'` or `'CALL'`. */
  command: "SELECT" | "MUTATION" | "DDL" | "CALL" | string;
  /** Rows returned (SELECT/CALL), rows affected (MUTATION), `null` for DDL. */
  rowCount: number | null;
  rows: Row[];
  fields: Field[];
  /** Column names in order; present for row-producing statements. */
  columns?: string[];
  /**
   * Every result set of a `CALL` whose body `EMIT`s, in emission order, the
   * call's final result last. `rows`/`fields`/`columns` are that last set.
   * Absent for everything else.
   */
  resultSets?: QueryResult<Row>[];
}

export interface StreamOptions {
  /**
   * Parameters for `$1, $2, …` placeholders, as in `QueryConfig.values`.
   * Overrides the `values` argument of `stream()`.
   */
  values?: Value[];
  /** Consistency for this statement only. */
  consistency?: Consistency;
  /** `'array'` yields cell arrays instead of objects. Default `'object'`. */
  rowMode?: RowMode;
}

export interface BatchOptions {
  /** Consistency for this batch only. Default: the client's. */
  consistency?: Consistency;
}

/**
 * `client.stream(sql, values?, opts?)` — an async generator over the rows of
 * one statement; `stream(sql, opts)` is the same without values. The slots
 * below describe the stream started MOST RECENTLY (they live on the method,
 * not the iterator).
 */
export interface StreamFunction {
  <Row = any>(sql: string, opts?: StreamOptions): AsyncGenerator<Row, void, undefined>;
  /**
   * `values` bind `$1, $2, …` as `query(text, values)` binds them, sent with
   * the statement in one request (server ≥ 0.330). Without any, the request
   * is the one every server with streaming understands.
   */
  <Row = any>(sql: string, values?: Value[] | null, opts?: StreamOptions): AsyncGenerator<Row, void, undefined>;
  /** Column names once the header arrives; `null` for a statement without rows. */
  columns?: string[] | null;
  /**
   * `'SELECT'` for rows, `'MUTATION'` or `'DDL'` for a statement that yields
   * none, `'CALL'` for a `CALL` whose body `EMIT`s result sets.
   */
  command?: "SELECT" | "MUTATION" | "DDL" | "CALL" | null;
  /** A mutation's affected-row count; `null` otherwise. */
  affected?: number | bigint | null;
  /**
   * Every result set of a `CALL` whose body `EMIT`s, in emission order, as
   * `QueryResult.resultSets` holds them: the server answers such a call in
   * one frame even when streamed, and the stream yields the last set's rows.
   * `null` for every other statement.
   */
  resultSets?: QueryResult[] | null;
}

/** One event from a `CREATE STREAM` log, as yielded by `subscribe()`. */
export interface StreamEvent {
  /**
   * Position in the log: an opaque string that sorts in log order, such as
   * `"00000001789857620741-0000000000-0902800000000000000100"`. Pass the
   * last one seen as `after` to resume. It is not a number.
   */
  id: string;
  op: string;
  k: Value;
  ts: Date;
  doc: Value;
}

export interface SubscribeOptions {
  /**
   * Resume after this event id (a `StreamEvent.id` string). `null` (default)
   * starts from the earliest retained event.
   */
  after?: string | null;
  /** Poll interval when the log is idle, in milliseconds. Default `500`. */
  pollMs?: number;
  /**
   * Aborting it ends the iteration cleanly (no error) within a tick while
   * the iterator is idle, else once the page fetch in flight completes.
   */
  signal?: AbortSignal;
}

export class Client {
  constructor(options?: ClientOptions);

  /** Host of the endpoint most recently dialled. */
  host: string;
  /** Port of the endpoint most recently dialled. */
  port: number;
  /** Parsed seed list. */
  seeds: Array<{ host: string; port: number }>;
  user: string;
  password: string;
  /** Resolved default consistency, as a number. */
  consistency: 0 | 1 | 2;
  connectTimeout: number;
  tls: boolean;
  tlsCa: string | null;
  tlsInsecure: boolean;
  tlsServerName: string;
  tlsClientCert: string | null;
  tlsClientKey: string | null;
  authMechanism: AuthMechanism;
  database: string | null;

  /**
   * Dial the seeds in shuffled order until one connects and authenticates,
   * then send Hello and `USE <database>` if one was given.
   */
  connect(): Promise<void>;

  /**
   * Run one statement. With parameters, the statement is prepared on the
   * server and the values travel typed; without, the text is sent as is.
   * Statements on one client are serialized.
   */
  query<Row = any>(text: string, values?: Value[]): Promise<QueryResult<Row>>;
  query<Row = any>(config: QueryConfig, values?: Value[]): Promise<QueryResult<Row>>;

  /**
   * Run `sql` once per parameter row in ONE round-trip and return the total
   * affected count. Rows autocommit individually; the statement must be
   * preparable (SELECT/INSERT/UPDATE/DELETE) and idempotent.
   */
  batch(sql: string, rows: Value[][], opts?: BatchOptions): Promise<number>;

  /**
   * Stream a result set row by row while holding one chunk in memory:
   * `stream(sql, values?, opts?)`, `values` binding `$1, $2, …` as in
   * `query()` (server ≥ 0.330). The connection is busy until the iterator is
   * exhausted or closed (`break`, `return`, a throw, or `.return()`);
   * closing early drains or drops the connection so it is left at a request
   * boundary. Never leak an iterator.
   */
  stream: StreamFunction;

  /**
   * Yield a stream's events as they arrive, forever, by polling its log with
   * a keyset cursor. Stop it with `break`, `.return()` or `opts.signal`; each
   * takes effect promptly, even while the iterator is idle in its poll
   * sleep. Push delivery is available over MQTT instead.
   */
  subscribe(stream: string, opts?: SubscribeOptions): AsyncGenerator<StreamEvent, void, undefined>;

  /** False once `end()` was called or a transport error broke the socket. */
  isUsable(): boolean;

  /** Close the connection. Terminal: a closed client never reconnects. */
  end(): Promise<void>;
}

/**
 * Where a checkout goes. `'random'` (default): any idle connection, and a
 * new connection dials the usable members in random order. `'round_robin'`:
 * the next usable member in endpoint order. `'least_latency'`: of two usable
 * members picked at random, the one with the lower round-trip average ×
 * (checked-out connections + 1).
 */
export type LoadBalancing = "random" | "round_robin" | "least_latency";

/** Which idle connection a checkout reuses: the most recently returned (`'lifo'`) or the longest idle (`'fifo'`). */
export type IdleOrder = "lifo" | "fifo";

/** A member's state as the pool sees it. Only `ready` members get new connections. */
export type MemberState = "ready" | "ejected" | "resyncing" | "unreachable" | "unlisted";

export interface PoolOptions extends ClientOptions {
  /** Connections kept IDLE (not a cap on checked-out ones). Default `10`. */
  maxsize?: number;
  /** How checkouts spread over the members. Default `'random'`. */
  loadBalancing?: LoadBalancing;
  /** Default `'lifo'`; `'fifo'` keeps every idle connection in use. */
  idleOrder?: IdleOrder;
  /**
   * Learn the members from `SHOW ENDPOINTS` (server ≥ 0.327) when a member
   * is first reached and every `refreshInterval` after (every 5 s while one
   * is `resyncing` or `unreachable`). Default `false`.
   */
  discover?: boolean;
  /** Milliseconds between discovery refreshes. Default `60000`. */
  refreshInterval?: number;
  /**
   * Milliseconds after a write during which checkouts with the same
   * `affinity` go to the member that took it. Default `0` (off).
   */
  readYourWrites?: number;
  /** Consecutive transport failures that eject a member. Default `3`. */
  ejectErrors?: number;
  /** Eject a member whose latency average is above this × the others' median. Default `3`; `0` disables. */
  ejectLatencyFactor?: number;
  /** …and more than this many milliseconds above it. Default `50`. */
  ejectLatencyFloor?: number;
  /** Milliseconds an ejection lasts, doubled per repeat. Default `5000`. */
  ejectBackoff?: number;
  /** Cap on the doubled backoff, in milliseconds. Default `60000`. */
  ejectBackoffMax?: number;
}

export interface AcquireOptions {
  /**
   * Read-your-writes scope — a user id, a session id. With `readYourWrites`
   * set, checkouts under the same key follow the member that took the key's
   * last write. Left out, checkouts share one pool-wide key.
   */
  affinity?: unknown;
}

/** One member as `pool.members()` reports it. */
export interface PoolMember {
  /** The endpoint the pool dials, `host:port`. */
  endpoint: string;
  /** The member's own name from `SHOW ENDPOINTS`, once discovered. */
  canonical: string | null;
  state: MemberState;
  /** Round-trip average in milliseconds; `null` until measured. */
  latencyMs: number | null;
  /** Connections checked out to it. */
  inUse: number;
  /** Idle connections to it. */
  idle: number;
  /** Whether it was configured (a seed is never dropped by discovery). */
  seed: boolean;
}

/** Payload of the pool's `'eject'` event. */
export interface EjectEvent {
  endpoint: string;
  /**
   * Why: consecutive failures, or its latency average against the median
   * and the slowest of its last 10 round trips.
   */
  reason: string;
}

/** Payload of the pool's `'readmit'` event. */
export interface ReadmitEvent {
  endpoint: string;
}

/**
 * A pool of connections balanced across the cluster's members. Every
 * `ClientOptions` field passes through, so pooled connections inherit TLS,
 * consistency and the database; `seeds` (or `host`/`port`) name the
 * starting members. Emits `'eject'` and `'readmit'` as members leave and
 * rejoin the rotation.
 */
export class Pool extends EventEmitter {
  constructor(options?: PoolOptions);
  readonly maxsize: number;
  /** True after `end()`. */
  readonly closed: boolean;
  /**
   * Check out a usable connection: an idle one to the member the policy
   * picks, or a new one dialed to it (falling back to the other members).
   */
  acquire(options?: AcquireOptions): Promise<Client>;
  /** Return a connection; it is closed instead if broken, its member is not usable, or the pool is full. */
  release(conn: Client): Promise<void>;
  /** Run `fn` with a checked-out connection and return it however `fn` ends. */
  withConnection<T>(fn: (conn: Client) => Promise<T> | T): Promise<T>;
  withConnection<T>(options: AcquireOptions, fn: (conn: Client) => Promise<T> | T): Promise<T>;
  /** Every member as the pool sees it, in endpoint order. */
  members(): PoolMember[];
  /** Close the pool and every idle connection, and stop its probe/discovery timer. */
  end(): Promise<void>;

  on(event: "eject", listener: (e: EjectEvent) => void): this;
  on(event: "readmit", listener: (e: ReadmitEvent) => void): this;
  on(event: string | symbol, listener: (...args: any[]) => void): this;
  once(event: "eject", listener: (e: EjectEvent) => void): this;
  once(event: "readmit", listener: (e: ReadmitEvent) => void): this;
  once(event: string | symbol, listener: (...args: any[]) => void): this;
}

/** Every error the driver raises: statement errors, protocol errors, connection errors. */
export class SkaidbError extends Error {}

export const CONSISTENCY: { readonly ONE: 0; readonly QUORUM: 1; readonly ALL: 2 };
