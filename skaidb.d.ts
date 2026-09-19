// TypeScript declarations for the skaidb Node.js driver (skaidb.js).
//
// The runtime is plain CommonJS; these declarations cover its whole public
// surface: Client, Pool, SkaidbError, CONSISTENCY and the option/result shapes.

/// <reference types="node" />

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
  /** User name. Default `anonymous`. */
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
  /** Consistency for this statement only. */
  consistency?: Consistency;
  /** `'array'` yields cell arrays instead of objects. Default `'object'`. */
  rowMode?: RowMode;
}

/**
 * `client.stream(sql)` — an async generator over the rows of one statement.
 * `columns` is filled once the header arrives and describes the stream
 * started MOST RECENTLY (the slot lives on the method, not the iterator).
 */
export interface StreamFunction {
  <Row = any>(sql: string, opts?: StreamOptions): AsyncGenerator<Row, void, undefined>;
  columns?: string[];
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
  batch(sql: string, rows: Value[][]): Promise<number>;

  /**
   * Stream a result set row by row while holding one chunk in memory. The
   * connection is busy until the iterator is exhausted or closed (`break`,
   * `return`, a throw, or `.return()`); closing early drains or drops the
   * connection so it is left at a request boundary. Never leak an iterator.
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

export interface PoolOptions extends ClientOptions {
  /** Connections kept IDLE (not a cap on checked-out ones). Default `10`. */
  maxsize?: number;
}

/**
 * A pool of connections. Every `ClientOptions` field passes through, so
 * pooled connections inherit seeds, TLS, consistency and the database.
 */
export class Pool {
  constructor(options?: PoolOptions);
  readonly maxsize: number;
  /** True after `end()`. */
  readonly closed: boolean;
  /** Check out a usable connection, opening a fresh one when none is idle. */
  acquire(): Promise<Client>;
  /** Return a connection; it is closed instead if broken or the pool is full. */
  release(conn: Client): Promise<void>;
  /** Run `fn` with a checked-out connection and return it however `fn` ends. */
  withConnection<T>(fn: (conn: Client) => Promise<T> | T): Promise<T>;
  /** Close the pool and every idle connection. */
  end(): Promise<void>;
}

/** Every error the driver raises: statement errors, protocol errors, connection errors. */
export class SkaidbError extends Error {}

export const CONSISTENCY: { readonly ONE: 0; readonly QUORUM: 1; readonly ALL: 2 };
