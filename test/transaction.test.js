'use strict';
// The typed conflict error and the retrying transaction helper, against the
// in-process fake server. What a transaction did is asserted from the
// statements the server received, in order, and the socket each arrived on.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Client, Pool, SkaidbError, TransactionConflictError, _internal: I } = require('../skaidb');
const { FakeServer, F } = require('./fake-server');

// The server's message today. Only the prefix is the contract.
const CONFLICT = 'transaction conflict: a row of table accounts was changed by another writer '
  + 'after the transaction began — nothing was committed; run the transaction again';

const isConflict = (e) => e instanceof TransactionConflictError && e instanceof SkaidbError
  && e instanceof Error && e.message === CONFLICT;

/**
 * A handler that records every statement text with the socket it arrived
 * on. `answers[sql]` scripts a statement: one payload for every time it is
 * sent, or an array giving the answer per time (the last one repeats).
 * Unscripted statements answer Ddl. Only OP_QUERY is served, so a statement
 * with values falls back to client-side literals and is recorded as text.
 */
function recorder(answers = {}) {
  const seen = [];
  const times = new Map();
  const handle = (req, conn) => {
    if (req.op !== 1) return undefined;                // Hello -> Ddl, prepare -> Error
    const n = times.get(req.sql) || 0;
    times.set(req.sql, n + 1);
    seen.push({ sql: req.sql, sock: conn.sock });
    const a = answers[req.sql];
    if (a === undefined) return F.ddl();
    return Array.isArray(a) ? a[Math.min(n, a.length - 1)] : a;
  };
  return {
    handle,
    seen,
    sqls: () => seen.map((s) => s.sql),
    sockets: () => new Set(seen.map((s) => s.sock)).size,
  };
}

// Runs `fn(client, rec, srv)` with a connected client against a recorder.
async function withClient(answers, fn) {
  const rec = recorder(answers);
  const srv = await new FakeServer({ handle: rec.handle }).start();
  const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
  try {
    await c.connect();
    return await fn(c, rec, srv);
  } finally {
    await c.end();
    await srv.stop();
  }
}

const until = async (pred, ms = 2000) => {
  const t0 = Date.now();
  while (!pred()) {
    if (Date.now() - t0 > ms) throw new Error('timeout waiting');
    await new Promise((r) => setTimeout(r, 5));
  }
};

const collect = async (it) => { const rows = []; for await (const r of it) rows.push(r); return rows; };

// ---- the typed error --------------------------------------------------------

test('a server Error starting with "transaction conflict:" is a TransactionConflictError on every path, and the connection stays usable', async () => {
  const handle = (req) => {
    switch (req.op) {
      case 1:
        if (req.sql === 'SELECT 1') return F.rows(['v'], [[1]]);
        if (req.sql === 'SELECT nope') return F.error('no such column nope');
        if (req.sql === 'SELECT late') return F.error(`commit failed: ${CONFLICT}`);
        if (req.sql === 'SELECT short') return F.error('transaction conflict: x');
        return F.error(CONFLICT);
      case 2: return F.prepared(7, 1);
      case 3: case 7: case 9: return F.error(CONFLICT);
      case 5:
        return req.sql === 'SELECT id FROM big'
          ? [F.header(['id']), F.chunk([[1]]), F.error(CONFLICT)] : F.error(CONFLICT);
      default: return undefined;
    }
  };
  const srv = await new FakeServer({ handle }).start();
  try {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    const usable = async () => {
      assert.equal(c.isUsable(), true);
      assert.deepEqual((await c.query('SELECT 1')).rows, [{ v: 1 }]);
    };
    await assert.rejects(c.query('COMMIT'), isConflict);                               // OP_QUERY
    await usable();
    // A statement error like any other to a pool: never a fault of the member that sent it.
    assert.equal(I.memberFault(await c.query('COMMIT').catch((e) => e)), false);
    await assert.rejects(c.query('UPDATE t SET v = $1', [1]), isConflict);             // OP_EXECUTE
    await usable();
    await assert.rejects(c.batch('INSERT INTO t VALUES ($1)', [[1], [2]]), isConflict); // OP_EXECUTE_BATCH
    await usable();
    await assert.rejects(collect(c.stream('COMMIT')), isConflict);                     // OP_QUERY_STREAM
    await usable();
    await assert.rejects(collect(c.stream('SELECT id FROM t WHERE id > $1', [1])), isConflict);   // …_PARAMS
    await usable();
    const partial = [];                                                                // an Error after rows
    await assert.rejects((async () => { for await (const r of c.stream('SELECT id FROM big')) partial.push(r); })(),
      isConflict);
    assert.deepEqual(partial, [{ id: 1 }]);
    await usable();
    // The prefix is the whole test: the rest of the message may be anything …
    await assert.rejects(c.query('SELECT short'),
      (e) => e instanceof TransactionConflictError && e.message === 'transaction conflict: x');
    // … and it must START the message. Any other statement error is the plain one.
    for (const sql of ['SELECT nope', 'SELECT late']) {
      await assert.rejects(c.query(sql), (e) => e instanceof SkaidbError && !(e instanceof TransactionConflictError), sql);
    }
    await usable();
    assert.equal(srv.connections, 1);                    // never dropped, never re-dialled
    await c.end();
  } finally {
    await srv.stop();
  }
});

// ---- the helper -------------------------------------------------------------

test('transaction: BEGIN, the body\'s statements, COMMIT — once — and the body\'s value', async () => {
  const answers = { 'SELECT balance FROM accounts WHERE id = 1': F.rows(['balance'], [[50]]) };
  await withClient(answers, async (c, rec, srv) => {
    let got;
    const value = await c.transaction(async (tx) => {
      got = tx;
      const { rows } = await tx.query('SELECT balance FROM accounts WHERE id = $1', [1]);
      await tx.query('UPDATE accounts SET balance = $2 WHERE id = $1', [1, rows[0].balance - 10]);
      return rows[0].balance - 10;
    });
    assert.equal(value, 40);
    assert.equal(got, c);                                // the body runs on this very connection
    assert.deepEqual(rec.sqls(), [
      'BEGIN',
      'SELECT balance FROM accounts WHERE id = 1',
      'UPDATE accounts SET balance = 40 WHERE id = 1',
      'COMMIT',
    ]);
    assert.equal(rec.sockets(), 1);
    // A body that is not async, and one that returns nothing.
    assert.equal(await c.transaction(() => 'plain'), 'plain');
    assert.equal(await c.transaction(async () => {}), undefined);
    assert.deepEqual(rec.sqls().slice(4), ['BEGIN', 'COMMIT', 'BEGIN', 'COMMIT']);
    assert.equal(srv.connections, 1);
  });
});

test('transaction: a conflict on COMMIT twice, then success, with attempts 3 — the body ran three times and the last value is returned', async () => {
  await withClient({ COMMIT: [F.error(CONFLICT), F.error(CONFLICT), F.ddl()] }, async (c, rec, srv) => {
    let runs = 0;
    // Every timer armed while the transaction runs: a sleep between attempts would be one.
    const realSetTimeout = global.setTimeout;
    let timers = 0;
    global.setTimeout = (...args) => { timers++; return realSetTimeout(...args); };
    let value;
    try {
      value = await c.transaction(async (tx) => {
        runs++;
        await tx.query(`INSERT INTO log VALUES (${runs})`);
        return `run ${runs}`;
      }, { attempts: 3 });
    } finally {
      global.setTimeout = realSetTimeout;
    }
    assert.equal(runs, 3);
    assert.equal(value, 'run 3');
    assert.equal(timers, 0);                             // no sleep or backoff between attempts
    assert.deepEqual(rec.sqls(), [
      'BEGIN', 'INSERT INTO log VALUES (1)', 'COMMIT',
      'BEGIN', 'INSERT INTO log VALUES (2)', 'COMMIT',
      'BEGIN', 'INSERT INTO log VALUES (3)', 'COMMIT',
    ]);                                                  // from BEGIN each time; never a ROLLBACK
    assert.equal(rec.sockets(), 1);
    assert.equal(srv.connections, 1);                    // a conflict is not a reason to reconnect
  });
});

test('transaction: a conflict on every COMMIT with attempts 2 is the typed error after exactly two runs; the default is three', async () => {
  await withClient({ COMMIT: F.error(CONFLICT) }, async (c, rec) => {
    let runs = 0;
    const body = async (tx) => { runs++; await tx.query('UPDATE t SET v = v + 1'); };
    await assert.rejects(c.transaction(body, { attempts: 2 }), isConflict);
    assert.equal(runs, 2);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE t SET v = v + 1', 'COMMIT', 'BEGIN', 'UPDATE t SET v = v + 1', 'COMMIT']);
    assert.equal(c.isUsable(), true);
    // attempts 1: one run, no retry at all.
    runs = 0;
    await assert.rejects(c.transaction(body, { attempts: 1 }), isConflict);
    assert.equal(runs, 1);
    // No options: three runs.
    runs = 0;
    rec.seen.length = 0;
    await assert.rejects(c.transaction(body), isConflict);
    assert.equal(runs, 3);
    assert.equal(rec.sqls().filter((s) => s === 'COMMIT').length, 3);
    assert.ok(!rec.sqls().includes('ROLLBACK'));
    // The connection is back in autocommit and takes the next statement.
    assert.equal((await c.query('SELECT 1')).command, 'DDL');
  });
});

test('transaction: a body error sends ROLLBACK and is thrown unchanged — also when the ROLLBACK is answered with an Error', async () => {
  for (const rollback of [F.ddl(), F.error('no transaction in progress')]) {
    await withClient({ ROLLBACK: rollback, 'SELECT nope': F.error('no such column nope') }, async (c, rec) => {
      const mine = new Error('mine');
      let runs = 0;
      await assert.rejects(c.transaction(async (tx) => {
        runs++;
        await tx.query('UPDATE t SET v = 1');
        throw mine;
      }), (e) => e === mine);
      assert.equal(runs, 1);
      assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE t SET v = 1', 'ROLLBACK']);   // and no COMMIT

      // A statement error raised by the body's own statement: the same object comes out.
      rec.seen.length = 0;
      let raised;
      await assert.rejects(c.transaction(async (tx) => {
        try { await tx.query('SELECT nope'); } catch (e) { raised = e; throw e; }
      }), (e) => e === raised && /no such column nope/.test(e.message));
      assert.deepEqual(rec.sqls(), ['BEGIN', 'SELECT nope', 'ROLLBACK']);

      // A body that throws synchronously, and one that throws a non-Error.
      rec.seen.length = 0;
      await assert.rejects(c.transaction(() => { throw mine; }), (e) => e === mine);
      await assert.rejects(c.transaction(async () => { throw 'a string'; }), (e) => e === 'a string');
      assert.deepEqual(rec.sqls(), ['BEGIN', 'ROLLBACK', 'BEGIN', 'ROLLBACK']);
      assert.equal(c.isUsable(), true);
    });
  }
});

test('transaction: a conflict error thrown by the body is not retried', async () => {
  // The body commits by hand and the server refuses it: that is the body's
  // error, so ROLLBACK, throw it, and do not run the body again.
  await withClient({ COMMIT: F.error(CONFLICT), ROLLBACK: F.error('no transaction in progress') }, async (c, rec) => {
    let runs = 0;
    await assert.rejects(c.transaction(async (tx) => {
      runs++;
      await tx.query('COMMIT');
    }, { attempts: 5 }), isConflict);
    assert.equal(runs, 1);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'COMMIT', 'ROLLBACK']);
  });
});

test('transaction: a refused BEGIN is thrown; the body never runs and neither COMMIT nor ROLLBACK is sent', async () => {
  await withClient({ BEGIN: F.error('a transaction is already in progress') }, async (c, rec) => {
    let runs = 0;
    await assert.rejects(c.transaction(async () => { runs++; }),
      (e) => e instanceof SkaidbError && e.message === 'a transaction is already in progress');
    assert.equal(runs, 0);
    assert.deepEqual(rec.sqls(), ['BEGIN']);
  });
  // Even a BEGIN refused with a conflict message is not a reason to go on or to try again.
  await withClient({ BEGIN: F.error(CONFLICT) }, async (c, rec) => {
    let runs = 0;
    await assert.rejects(c.transaction(async () => { runs++; }), isConflict);
    assert.equal(runs, 0);
    assert.deepEqual(rec.sqls(), ['BEGIN']);
  });
});

test('transaction: a COMMIT error that is not a conflict is thrown — no retry, no ROLLBACK', async () => {
  await withClient({ COMMIT: F.error('quorum not reached') }, async (c, rec) => {
    let runs = 0;
    await assert.rejects(c.transaction(async (tx) => { runs++; await tx.query('UPDATE t SET v = 1'); }, { attempts: 3 }),
      (e) => e instanceof SkaidbError && !(e instanceof TransactionConflictError) && e.message === 'quorum not reached');
    assert.equal(runs, 1);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE t SET v = 1', 'COMMIT']);
    assert.equal(c.isUsable(), true);
  });
});

test('transaction: attempts below 1, or a body that is not a function, is refused before anything is sent', async () => {
  await withClient({}, async (c, rec, srv) => {
    const before = srv.requests.length;
    let runs = 0;
    const body = async () => { runs++; };
    for (const attempts of [0, -1, NaN, null]) {
      await assert.rejects(c.transaction(body, { attempts }),
        (e) => e instanceof SkaidbError && e.message === 'attempts must be >= 1', String(attempts));
    }
    await assert.rejects(c.transaction(undefined), /transaction\(\) takes the function to run/);
    await assert.rejects(c.transaction('BEGIN'), /transaction\(\) takes the function to run/);
    await assert.rejects(c.transaction(body, { begin: 7 }), /begin must be the opening statement, a string/);
    assert.equal(runs, 0);
    assert.equal(srv.requests.length, before);           // nothing reached the server
    assert.deepEqual(rec.sqls(), []);
    // … and nothing is left behind: the next transaction runs.
    assert.equal(await c.transaction(async () => 'ok', { attempts: 1 }), 'ok');
  });
});

test('transaction: a custom begin text is sent verbatim, on every attempt', async () => {
  const begin = 'BEGIN ATOMIC PARTITION (tenant = 7)';
  await withClient({ COMMIT: [F.error(CONFLICT), F.ddl()] }, async (c, rec) => {
    await c.transaction(async (tx) => { await tx.query('UPDATE t SET v = 1'); }, { begin });
    assert.deepEqual(rec.sqls(), [begin, 'UPDATE t SET v = 1', 'COMMIT', begin, 'UPDATE t SET v = 1', 'COMMIT']);
    rec.seen.length = 0;
    await c.transaction(async () => {}, { begin: 'BEGIN ATOMIC', attempts: 1 });
    assert.deepEqual(rec.sqls(), ['BEGIN ATOMIC', 'COMMIT']);
  });
});

test('transaction: a second transaction() on a client that has one running is refused before anything is sent', async () => {
  await withClient({}, async (c, rec) => {
    let release;
    const gate = new Promise((r) => { release = r; });
    let inner;
    const first = c.transaction(async (tx) => {
      // From inside the body too: the server has one transaction per connection.
      inner = await tx.transaction(async () => 'nested').catch((e) => e);
      await gate;
      return 'first';
    });
    await assert.rejects(c.transaction(async () => 'second'), /a transaction is already running on this connection/);
    release();
    assert.equal(await first, 'first');
    assert.match(inner.message, /a transaction is already running on this connection/);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'COMMIT']);
    assert.equal(await c.transaction(async () => 'third'), 'third');      // free again
  });
});

// ---- a lost connection ------------------------------------------------------

test('transaction: a connection lost between statements is not re-dialled — the rest is never sent to another connection', async () => {
  await withClient({}, async (c, rec, srv) => {
    let runs = 0;
    let raised;
    await assert.rejects(c.transaction(async (tx) => {
      runs++;
      await tx.query('UPDATE a SET v = 1');
      srv.closeAll();                                    // the node restarts mid-transaction
      await until(() => !c.isUsable());
      try { await tx.query('UPDATE b SET v = 1'); } catch (e) { raised = e; throw e; }
    }), (e) => e === raised && e instanceof SkaidbError && e.message === 'connection lost during a transaction');
    assert.equal(runs, 1);                               // a transport failure is not retried
    // Neither the second UPDATE (it would have autocommitted) nor a ROLLBACK went anywhere.
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1']);
    assert.equal(srv.connections, 1);
    // Once the transaction is over the client reconnects as it always does.
    assert.equal((await c.query('SELECT 1')).command, 'DDL');
    assert.equal(srv.connections, 2);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1', 'SELECT 1']);
    // … and a new transaction runs whole on the new connection.
    assert.equal(await c.transaction(async (tx) => { await tx.query('UPDATE a SET v = 2'); return 'again'; }), 'again');
    assert.deepEqual(rec.sqls().slice(3), ['BEGIN', 'UPDATE a SET v = 2', 'COMMIT']);
    assert.equal(new Set(rec.seen.slice(2).map((s) => s.sock)).size, 1);
  });
});

test('transaction: a connection lost before COMMIT fails the COMMIT instead of sending it to a new connection', async () => {
  await withClient({}, async (c, rec, srv) => {
    let runs = 0;
    await assert.rejects(c.transaction(async (tx) => {
      runs++;
      await tx.query('UPDATE a SET v = 1');
      srv.closeAll();
      await until(() => !c.isUsable());
    }), /connection lost during a transaction/);
    assert.equal(runs, 1);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1']);     // no COMMIT, no ROLLBACK
    assert.equal(srv.connections, 1);
  });
});

test('transaction: a connection that dies under a statement is the usual connection error, not retried', async () => {
  const rec = recorder();
  const handle = (req, conn) => {
    const out = rec.handle(req, conn);
    if (req.op === 1 && req.sql === 'UPDATE dies') { conn.sock.destroy(); return []; }
    return out;
  };
  const srv = await new FakeServer({ handle }).start();
  try {
    const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
    await c.connect();
    let runs = 0;
    await assert.rejects(c.transaction(async (tx) => { runs++; await tx.query('UPDATE dies'); }),
      (e) => e instanceof SkaidbError && !(e instanceof TransactionConflictError) && /connection (closed|lost)/.test(e.message));
    assert.equal(runs, 1);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE dies']);            // the ROLLBACK had nowhere to go
    assert.equal(srv.connections, 1);
    assert.equal((await c.query('SELECT 1')).command, 'DDL');          // reconnects afterwards
    assert.equal(srv.connections, 2);
    await c.end();
  } finally {
    await srv.stop();
  }
});

// A transport failure as the driver reports one: a SkaidbError the server
// did not answer with (a statement error is never a member's fault).
const isTransport = (e) => e instanceof SkaidbError && !(e instanceof TransactionConflictError) && I.memberFault(e);
const lostInTransaction = (e) => isTransport(e) && e.message === 'connection lost during a transaction';

test('transaction: after the connection is lost, a body that catches the error and goes on sends nothing more — not its next write, not the COMMIT', async () => {
  // PROTOCOL.md section 6 rule 7: no statement of the body leaves the
  // transaction's session. Lost between statements, noticed before the next.
  await withClient({}, async (c, rec, srv) => {
    const caught = [];
    const out = await c.transaction(async (tx) => {
      await tx.query('UPDATE a SET v = 1');                                           // statement 1
      srv.closeAll();                                                                 // the node restarts
      await until(() => !c.isUsable());
      caught.push(await tx.query('UPDATE b SET v = 1').catch((e) => e));              // statement 2, caught
      caught.push(await tx.query('INSERT INTO c VALUES (3)').catch((e) => e));        // statement 3, a write
      caught.push(await tx.query('INSERT INTO c VALUES ($1)', [4]).catch((e) => e));  // … with values
      caught.push(await tx.batch('INSERT INTO c VALUES ($1)', [[5]]).catch((e) => e));
      caught.push(await collect(tx.stream('INSERT INTO c VALUES (6)')).catch((e) => e));
      return 'the body returned';
    }).catch((e) => e);
    assert.equal(caught.length, 5);
    for (const e of caught) assert.ok(lostInTransaction(e), String(e));
    assert.ok(lostInTransaction(out), String(out));     // the loss, although the body returned normally
    // Nothing after statement 1 went anywhere: no write, no COMMIT, no ROLLBACK, no new connection.
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1']);
    assert.deepEqual(srv.requests.filter((q) => q.op !== 8).map((q) => q.op), [1, 1]);
    assert.equal(srv.connections, 1);
    // The transaction over, the client is what it is after any lost connection: the next statement re-dials.
    assert.equal(c.isUsable(), false);
    assert.equal((await c.query('SELECT 1')).command, 'DDL');
    assert.equal(c.isUsable(), true);
    assert.equal(srv.connections, 2);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1', 'SELECT 1']);
  });

  // Lost under statement 2 itself, and under the COMMIT itself.
  for (const dies of ['UPDATE b SET v = 1', 'COMMIT']) {
    const rec = recorder();
    const handle = (req, conn) => {
      const answer = rec.handle(req, conn);
      if (req.op === 1 && req.sql === dies) { conn.sock.destroy(); return []; }
      return answer;
    };
    const srv = await new FakeServer({ handle }).start();
    try {
      const c = new Client({ host: '127.0.0.1', port: srv.port, password: 'secret' });
      await c.connect();
      const caught = [];
      const out = await c.transaction(async (tx) => {
        await tx.query('UPDATE a SET v = 1');
        if (dies !== 'COMMIT') {
          caught.push(await tx.query(dies).catch((e) => e));
          caught.push(await tx.query('INSERT INTO c VALUES (3)').catch((e) => e));
        }
        return 'the body returned';
      }).catch((e) => e);
      if (dies === 'COMMIT') {
        // In flight when the connection went: the usual connection error, which
        // does not say whether the COMMIT was applied. Not sent again, no ROLLBACK.
        assert.ok(isTransport(out) && /^connection (closed|lost)$/.test(out.message), String(out));
        assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1', 'COMMIT']);
      } else {
        assert.ok(isTransport(caught[0]) && /^connection (closed|lost)$/.test(caught[0].message), String(caught[0]));
        assert.ok(lostInTransaction(caught[1]), String(caught[1]));
        assert.ok(lostInTransaction(out), String(out));
        assert.deepEqual(rec.sqls(), ['BEGIN', 'UPDATE a SET v = 1', 'UPDATE b SET v = 1']);
      }
      assert.equal(srv.connections, 1);
      assert.equal((await c.query('SELECT 1')).command, 'DDL');         // usable again, on a new connection
      assert.equal(srv.connections, 2);
      await c.end();
    } finally {
      await srv.stop();
    }
  }
});

// ---- the pool ---------------------------------------------------------------

// Runs `fn(pool, rec, srv)` with a pool over one fake member.
async function withPool(answers, fn, opts = {}) {
  const rec = recorder(answers);
  const srv = await new FakeServer({ handle: rec.handle }).start();
  const pool = new Pool({ host: '127.0.0.1', port: srv.port, password: 'secret', ...opts });
  try {
    return await fn(pool, rec, srv);
  } finally {
    await pool.end();
    await srv.stop();
  }
}

test('Pool.transaction holds one connection for the whole transaction, retries on it, and returns it', async () => {
  await withPool({ COMMIT: [F.error(CONFLICT), F.ddl()] }, async (pool, rec, srv) => {
    const other = await pool.acquire();                  // a second connection is in play
    await other.query('SELECT 1');
    const conns = new Set();
    const value = await pool.transaction(async (tx) => {
      conns.add(tx);
      assert.notEqual(tx, other);
      assert.equal(tx._poolAffinity, 'alice');
      await tx.query('UPDATE t SET v = 1');
      await other.query('SELECT 2');                     // someone else's statement, elsewhere
      return 'done';
    }, { affinity: 'alice' });
    assert.equal(value, 'done');
    assert.equal(conns.size, 1);                         // both attempts on the same connection
    const [tx] = conns;
    const mine = rec.seen.filter((s) => s.sql !== 'SELECT 1' && s.sql !== 'SELECT 2');
    assert.deepEqual(mine.map((s) => s.sql), ['BEGIN', 'UPDATE t SET v = 1', 'COMMIT', 'BEGIN', 'UPDATE t SET v = 1', 'COMMIT']);
    assert.equal(new Set(mine.map((s) => s.sock)).size, 1);            // … and the same socket
    assert.equal(srv.connections, 2);
    // Returned to the pool, usable, and the conflict did not count against the member.
    const [m] = pool.members();
    assert.deepEqual([m.state, m.inUse, m.idle], ['ready', 1, 1]);
    assert.equal(pool._members.get(m.endpoint).errors, 0);
    assert.equal(tx.isUsable(), true);
    await pool.release(other);
    assert.equal(pool.members()[0].idle, 2);
  });
});

test('Pool.transaction: the last conflict is thrown and the connection still goes back to the pool', async () => {
  await withPool({ COMMIT: F.error(CONFLICT) }, async (pool, rec, srv) => {
    let runs = 0;
    await assert.rejects(pool.transaction(async () => { runs++; }, { attempts: 2 }), isConflict);
    assert.equal(runs, 2);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'COMMIT', 'BEGIN', 'COMMIT']);
    assert.deepEqual([pool.members()[0].inUse, pool.members()[0].idle], [0, 1]);
    assert.equal((await pool.withConnection((c) => c.query('SELECT 1'))).command, 'DDL');
    assert.equal(srv.connections, 1);                    // the same connection served it
  });
});

test('Pool.transaction refuses bad arguments before a connection is checked out', async () => {
  await withPool({}, async (pool, rec, srv) => {
    await assert.rejects(pool.transaction(async () => {}, { attempts: 0 }), /attempts must be >= 1/);
    await assert.rejects(pool.transaction(null), /transaction\(\) takes the function to run/);
    assert.equal(srv.connections, 0);
    assert.deepEqual(rec.sqls(), []);
  });
});

test('Pool: a connection whose ROLLBACK did not go through is closed, not handed to the next caller', async () => {
  await withPool({ ROLLBACK: [F.error('internal error'), F.ddl()] }, async (pool, rec, srv) => {
    const mine = new Error('mine');
    let first;
    await assert.rejects(pool.transaction(async (tx) => { first = tx; throw mine; }), (e) => e === mine);
    assert.deepEqual(rec.sqls(), ['BEGIN', 'ROLLBACK']);
    // The transaction may still be open on it: not pooled.
    assert.deepEqual([pool.members()[0].inUse, pool.members()[0].idle], [0, 0]);
    assert.equal(first.isUsable(), false);
    // A body error whose ROLLBACK succeeded leaves a clean connection, which is pooled.
    let second;
    await assert.rejects(pool.transaction(async (tx) => { second = tx; throw mine; }), (e) => e === mine);
    assert.notEqual(second, first);
    assert.equal(srv.connections, 2);
    assert.deepEqual([pool.members()[0].inUse, pool.members()[0].idle], [0, 1]);
    assert.equal(second.isUsable(), true);
  });
});

test('Pool: a connection released while its transaction() is still running is closed, not pooled', async () => {
  await withPool({}, async (pool) => {
    let release;
    const gate = new Promise((r) => { release = r; });
    let running;
    await pool.withConnection(async (c) => {
      running = c.transaction(async () => { await gate; });    // not awaited: released mid-transaction
      await until(() => c._txn === 'open');
    });
    assert.deepEqual([pool.members()[0].inUse, pool.members()[0].idle], [0, 0]);
    release();
    await assert.rejects(running, /connection is closed/);
  });
});
