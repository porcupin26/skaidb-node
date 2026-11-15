// A TypeScript consumer of the driver. It is compiled (never run) in CI to
// prove skaidb.d.ts matches skaidb.js: every public member the runtime has
// is used here with the types it really takes and returns. If a declaration
// goes stale, this file stops compiling.

import {
  Client, Pool, SkaidbError, TransactionConflictError, CONSISTENCY,
  ClientOptions, PoolOptions, QueryResult, QueryConfig, StreamEvent, StreamOptions, Value, Consistency,
  AuthMechanism, BatchOptions, LoadBalancing, IdleOrder, MemberState, PoolMember, AcquireOptions,
  EjectEvent, ReadmitEvent, TransactionOptions, PoolTransactionOptions,
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
  const batchOpts: BatchOptions = { consistency: 'ALL' };
  const affectedAll: number = await client.batch('DELETE FROM users WHERE id = $1', [[1]], batchOpts);
  void affectedAll;

  // transaction(): the body gets the client and may be async or not; the
  // call resolves to the body's value.
  const txOpts: TransactionOptions = { attempts: 5, begin: 'BEGIN ATOMIC PARTITION (tenant = 7)' };
  const left: number = await client.transaction(async (tx: Client) => {
    const res = await tx.query<{ qty: number }>('SELECT qty FROM stock WHERE id = $1', [7]);
    await tx.query('UPDATE stock SET qty = $2 WHERE id = $1', [7, res.rows[0].qty - 1]);
    return res.rows[0].qty - 1;
  }, txOpts);
  const label: string = await client.transaction(() => 'no statements');
  const nothing: void = await client.transaction(async (tx) => { await tx.query('DELETE FROM stock WHERE qty = 0'); });
  // @ts-expect-error attempts is a number
  void client.transaction(async () => 1, { attempts: 'three' });
  // @ts-expect-error the body is a function, not a statement
  void client.transaction('BEGIN');
  // @ts-expect-error affinity is a checkout option: the pool's transaction() takes it, a client's does not
  const noAffinity: TransactionOptions = { affinity: 'user-42' };
  void [left, label, nothing, noAffinity];

  // stream(): async iteration, options, columns slot.
  for await (const row of client.stream<User>('SELECT id, name, tags FROM users', { consistency: 'ONE' })) {
    const id: number = row.id;
    if (id > 10) break;                      // closing early is allowed; the driver drains
  }
  const streamCols: string[] | null | undefined = client.stream.columns;
  for await (const _ of client.stream('DELETE FROM users WHERE id = 0')) { /* yields nothing */ }
  const streamCommand: 'SELECT' | 'MUTATION' | 'DDL' | 'CALL' | null | undefined = client.stream.command;
  const streamAffected: number | bigint | null | undefined = client.stream.affected;
  const streamSets: QueryResult[] | null | undefined = client.stream.resultSets;
  void [streamCommand, streamAffected, streamSets];
  for await (const cells of client.stream<Value[]>('SELECT id FROM users', { rowMode: 'array' })) {
    void cells[0];
    break;
  }

  // stream() with values: `$1, $2, …` as in query(), as the second argument
  // (with or without options) or in the options.
  for await (const row of client.stream<User>('SELECT id, name, tags FROM users WHERE id > $1', [10])) {
    const id: number = row.id;
    if (id > 10) break;
  }
  for await (const cells of client.stream<Value[]>(
    'SELECT id FROM users WHERE tags = $1 AND seen < $2', [['a'], new Date()], { rowMode: 'array', consistency: 'ONE' })) {
    void cells[0];
    break;
  }
  const streamOpts: StreamOptions = { values: [1n, null, { k: 'v' }], consistency: 'ALL' };
  for await (const _ of client.stream('SELECT id FROM users WHERE id = $1', streamOpts)) break;
  for await (const _ of client.stream('SELECT id FROM users', null, { rowMode: 'array' })) break;
  // @ts-expect-error values are an array, not a single value
  void client.stream('SELECT id FROM users WHERE id = $1', 1);

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

  // Certificate login: the TLS client certificate is the credential.
  const mech: AuthMechanism = 'certificate';
  const certOpts: ClientOptions = {
    seeds: ['db1:7000'], tlsCa: '/etc/skaidb/ca.pem',
    tlsClientCert: '/etc/app/app.crt', tlsClientKey: '/etc/app/app.key', authMechanism: mech,
  };
  const certClient = new Client(certOpts);
  const certPath: string | null = certClient.tlsClientCert;
  const keyPath: string | null = certClient.tlsClientKey;
  const usedMech: AuthMechanism = certClient.authMechanism;
  void [certPath, keyPath, usedMech];
  // @ts-expect-error only 'scram' and 'certificate' are mechanisms
  void new Client({ authMechanism: 'kerberos' });

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
  // transaction() on the pool: one checked-out connection, plus the checkout's affinity.
  const poolTx: PoolTransactionOptions = { attempts: 2, begin: 'BEGIN', affinity: 'user-42' };
  const moved: number = await pool.transaction(async (c: Client) => {
    await c.query('UPDATE accounts SET balance = balance - $2 WHERE id = $1', [1, 10]);
    return 10;
  }, poolTx);
  const viaPoolTx: boolean = await pool.transaction(() => true);
  void [moved, viaPoolTx];
  const closed: boolean = pool.closed;
  const max: number = pool.maxsize;
  await pool.end();

  // Load balancing across members: every option, affinity on checkout,
  // introspection and the eject/readmit events.
  const policy: LoadBalancing = 'least_latency';
  const order: IdleOrder = 'fifo';
  const balanced = new Pool({
    ...opts, loadBalancing: policy, idleOrder: order, discover: true, refreshInterval: 30000,
    readYourWrites: 2000, ejectErrors: 3, ejectLatencyFactor: 3, ejectLatencyFloor: 50,
    ejectBackoff: 5000, ejectBackoffMax: 60000,
  });
  const affinity: AcquireOptions = { affinity: 'user-42' };
  const mine: Client = await balanced.acquire(affinity);
  await balanced.release(mine);
  const sameMember: number = await balanced.withConnection({ affinity: 42 }, async (c) => {
    await c.query('INSERT INTO notes (id) VALUES ($1)', [1]);
    return 1;
  });
  const rows: PoolMember[] = balanced.members();
  const memberState: MemberState = rows[0].state;
  const latency: number | null = rows[0].latencyMs;
  const canonical: string | null = rows[0].canonical;
  const counts: number = rows[0].inUse + rows[0].idle;
  const isSeed: boolean = rows[0].seed;
  balanced.on('eject', (e: EjectEvent) => { const why: string = e.reason; void [e.endpoint, why]; });
  balanced.once('readmit', (e: ReadmitEvent) => { void e.endpoint; });
  await balanced.end();
  void [sameMember, memberState, latency, canonical, counts, isSeed];
  // @ts-expect-error not a policy
  void new Pool({ loadBalancing: 'nearest' });

  // Errors are SkaidbError instances (and Errors).
  try {
    await client.query('SELECT 1');
  } catch (e) {
    if (e instanceof SkaidbError) { const msg: string = e.message; void msg; }
  }
  // A refused COMMIT is a TransactionConflictError, which is a SkaidbError.
  try {
    await client.query('COMMIT');
  } catch (e) {
    if (e instanceof TransactionConflictError) {
      const asBase: SkaidbError = e;
      const asError: Error = e;
      const msg: string = e.message;
      void [asBase, asError, msg];
    }
  }
  const conflict: SkaidbError = new TransactionConflictError('transaction conflict: …');
  void conflict;

  // Consistency accepts numbers and names.
  const levels: Consistency[] = [0, 1, 2, 'ONE', 'QUORUM', 'ALL', CONSISTENCY.ALL];

  void [name, count, cols, fieldName, firstCell, affected, streamCols, usable, level, host, seeds,
        viaPool, closed, max, levels];
}

void main;
