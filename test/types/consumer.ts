// A TypeScript consumer of the driver. It is compiled (never run) in CI to
// prove skaidb.d.ts matches skaidb.js: every public member the runtime has
// is used here with the types it really takes and returns. If a declaration
// goes stale, this file stops compiling.

import {
  Client, Pool, SkaidbError, CONSISTENCY,
  ClientOptions, PoolOptions, QueryResult, QueryConfig, StreamEvent, Value, Consistency,
} from '@skaidb/client';

interface User { id: number; name: string; tags: string[] }

const opts: ClientOptions = {
  seeds: ['db1:7000', 'db2:7000', 'db3'],
  user: 'app',
  password: 'secret',
  database: 'app',
  consistency: 'QUORUM',
  connectTimeout: 5000,
  tlsCa: '/etc/skaidb/ca.pem',
  tlsServerName: 'skaidb',
};

async function main(): Promise<void> {
  const client = new Client(opts);
  await client.connect();

  // query(): both call forms, generic row type, every result field.
  const r1: QueryResult<User> = await client.query<User>('SELECT id, name, tags FROM users WHERE id = $1', [1]);
  const name: string = r1.rows[0].name;
  const count: number | null = r1.rowCount;
  const cols: string[] | undefined = r1.columns;
  const fieldName: string = r1.fields[0].name;
  const cfg: QueryConfig = { text: 'SELECT id FROM users', rowMode: 'array', consistency: CONSISTENCY.ONE };
  const r2 = await client.query<Value[]>(cfg);
  const firstCell: Value = r2.rows[0][0];

  // Multiple result sets from a CALL that EMITs.
  const call = await client.query('CALL report()');
  const sets: QueryResult[] | undefined = call.resultSets;
  if (sets) { const n: number = sets.length; void n; }

  // batch(): typed rows, total affected count.
  const affected: number = await client.batch(
    'INSERT INTO users (id, name, tags) VALUES ($1, $2, $3)',
    [[1, 'Ada', ['x']], [2, 'Linus', []]]);

  // stream(): async iteration, options, columns slot.
  for await (const row of client.stream<User>('SELECT id, name, tags FROM users', { consistency: 'ONE' })) {
    const id: number = row.id;
    if (id > 10) break;                      // closing early is allowed; the driver drains
  }
  const streamCols: string[] | undefined = client.stream.columns;
  for await (const cells of client.stream<Value[]>('SELECT id FROM users', { rowMode: 'array' })) {
    void cells[0];
    break;
  }

  // subscribe(): stream events with a resumable cursor. Ids are opaque
  // STRINGS that sort in log order, never numbers.
  let cursor: string | null = null;
  const ac = new AbortController();
  for await (const ev of client.subscribe('big_orders', { after: cursor, pollMs: 250, signal: ac.signal })) {
    const e: StreamEvent = ev;
    cursor = e.id;
    const id: string = e.id;
    const ts: Date = e.ts;
    void [id, ts];
    break;
  }
  const idle = client.subscribe('big_orders');
  const ret: IteratorResult<StreamEvent, void> = await idle.return();
  void ret;
  // The declarations must REFUSE numeric ids: a consumer saving `ev.id` into
  // a number, or resuming with `after: 0`, would issue `WHERE id > 0` and
  // never see an event. @ts-expect-error fails the build if either compiles.
  for await (const ev of client.subscribe('big_orders')) {
    // @ts-expect-error a stream id is a string
    const wrong: number = ev.id;
    void wrong;
    break;
  }
  // @ts-expect-error `after` takes an id string or null, not a number
  void client.subscribe('big_orders', { after: 0 });

  // Connection state and lifecycle.
  const usable: boolean = client.isUsable();
  const level: 0 | 1 | 2 = client.consistency;
  const host: string = client.host;
  const seeds: Array<{ host: string; port: number }> = client.seeds;
  await client.end();

  // Pool: every ClientOptions field plus maxsize.
  const poolOpts: PoolOptions = { ...opts, maxsize: 4 };
  const pool = new Pool(poolOpts);
  const viaPool: number = await pool.withConnection(async (c: Client) => {
    const res = await c.query<{ n: number }>('SELECT count(*) AS n FROM users');
    return res.rows[0].n;
  });
  const conn: Client = await pool.acquire();
  await pool.release(conn);
  const closed: boolean = pool.closed;
  const max: number = pool.maxsize;
  await pool.end();

  // Errors are SkaidbError instances (and Errors).
  try {
    await client.query('SELECT 1');
  } catch (e) {
    if (e instanceof SkaidbError) { const msg: string = e.message; void msg; }
  }

  // Consistency accepts numbers and names.
  const levels: Consistency[] = [0, 1, 2, 'ONE', 'QUORUM', 'ALL', CONSISTENCY.ALL];

  void [name, count, cols, fieldName, firstCell, affected, streamCols, usable, level, host, seeds,
        viaPool, closed, max, levels];
}

void main;
