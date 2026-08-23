'use strict';
// skaidb — Node.js driver. API modeled on node-postgres (`pg`).
// Pure standard library: `net` + `crypto`. No dependencies.
//
//   const { Client } = require('skaidb');
//   const client = new Client({ host: 'localhost', port: 7000,
//                               user: 'skaidb', password: 'secret' });
//   await client.connect();
//   const res = await client.query('SELECT id, name FROM users WHERE id = $1', [1]);
//   console.log(res.rows);          // [ { id: 1, name: 'Ada' } ]
//   await client.end();

const net = require('net');
const tls = require('tls');
const fs = require('fs');
const crypto = require('crypto');

const CONSISTENCY = { ONE: 0, QUORUM: 1, ALL: 2 };

class SkaidbError extends Error {}

// ---- Value codec (§4 of PROTOCOL.md) --------------------------------------

class Reader {
  constructor(buf) { this.buf = buf; this.pos = 0; }
  take(n) {
    const end = this.pos + n;
    if (end > this.buf.length) throw new SkaidbError('truncated server message');
    const s = this.buf.subarray(this.pos, end);
    this.pos = end;
    return s;
  }
  u8() { return this.take(1)[0]; }
  u16() { const b = this.take(2); return b.readUInt16LE(0); }
  u32() { const b = this.take(4); return b.readUInt32LE(0); }
  i64() { return this.take(8).readBigInt64LE(0); }
  u64() { return this.take(8).readBigUInt64LE(0); }
  blob() { return Buffer.from(this.take(this.u32())); }
  text() { return this.blob().toString('utf8'); }
}

function safeInt(big) {
  return (big >= BigInt(Number.MIN_SAFE_INTEGER) && big <= BigInt(Number.MAX_SAFE_INTEGER))
    ? Number(big) : big; // BigInt only when it would lose precision
}

function decodeValue(r) {
  const tag = r.u8();
  switch (tag) {
    case 0: return null;
    case 1: return r.u8() !== 0;
    case 2: return safeInt(r.i64());
    case 3: return r.take(8).readDoubleLE(0);
    case 4: {                                   // Decimal -> string
      const mantissa = bytesToBigIntLE(r.take(16), true);
      const scale = r.u32();
      return decimalToString(mantissa, scale);
    }
    case 5: return r.text();
    case 6: return r.blob();                    // Bytes -> Buffer
    case 7: return formatUuid(r.take(16));      // Uuid -> canonical string
    case 8: return new Date(Number(r.i64()));   // Timestamp(ms) -> Date
    case 9: {                                   // Array
      const n = r.u32(); const out = [];
      for (let i = 0; i < n; i++) out.push(decodeValue(r));
      return out;
    }
    case 10: {                                  // Document -> object
      const n = r.u32(); const out = {};
      for (let i = 0; i < n; i++) { const k = r.text(); out[k] = decodeValue(r); }
      return out;
    }
    default: throw new SkaidbError(`unknown value tag ${tag}`);
  }
}

function bytesToBigIntLE(buf, signed) {
  let v = 0n;
  for (let i = buf.length - 1; i >= 0; i--) v = (v << 8n) | BigInt(buf[i]);
  if (signed && (buf[buf.length - 1] & 0x80)) v -= (1n << BigInt(8 * buf.length));
  return v;
}

function decimalToString(mantissa, scale) {
  if (scale === 0) return mantissa.toString();
  const neg = mantissa < 0n;
  let digits = (neg ? -mantissa : mantissa).toString().padStart(scale + 1, '0');
  const point = digits.length - scale;
  const s = digits.slice(0, point) + '.' + digits.slice(point);
  return neg ? '-' + s : s;
}

function formatUuid(b) {
  const h = Buffer.from(b).toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// ---- Client-side parameter binding (§5, pg-style $1, $2 ...) ---------------

function quote(arg) {
  if (arg === null || arg === undefined) return 'NULL';
  switch (typeof arg) {
    case 'boolean': return arg ? 'TRUE' : 'FALSE';
    case 'number':
      if (!Number.isFinite(arg)) throw new SkaidbError('cannot bind NaN/Infinity');
      return String(arg);
    case 'bigint': return arg.toString();
    case 'string': return "'" + arg.replace(/'/g, "''") + "'";
  }
  if (arg instanceof Date) return String(arg.getTime());
  if (Buffer.isBuffer(arg)) return "'" + arg.toString('hex') + "'";
  throw new SkaidbError(`cannot bind value of type ${typeof arg}`);
}

/**
 * Rewrite pg-style `$N` placeholders to the positional `?` the server's
 * prepared statements use, returning the SQL and the parameters in wire
 * order. A parameter referenced twice is sent twice — `?` is positional and
 * has no way to say "the same one again".
 *
 * `$N` inside a string literal is left alone, same rule as bindParams.
 */
function toQmark(sql, params) {
  let out = '';
  const order = [];
  let inStr = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inStr) {
      out += ch;
      if (ch === "'") {
        if (sql[i + 1] === "'") { out += "'"; i++; } else inStr = false;
      }
      continue;
    }
    if (ch === "'") { inStr = true; out += ch; continue; }
    if (ch === '$' && /[0-9]/.test(sql[i + 1] || '')) {
      let j = i + 1, num = '';
      while (/[0-9]/.test(sql[j] || '')) { num += sql[j]; j++; }
      const idx = parseInt(num, 10) - 1;
      if (idx < 0 || idx >= params.length) throw new SkaidbError(`no parameter for $${num}`);
      order.push(params[idx]);
      out += '?';
      i = j - 1;
      continue;
    }
    out += ch;
  }
  return { sql: out, params: order };
}

/**
 * Encode a JS value as a TYPED skaidb value (tag + payload) — the inverse of
 * decodeValue. Arrays become Array and plain objects become Document, which
 * is the whole point of the prepared path: neither has a SQL literal form.
 */
function encodeValue(v) {
  const parts = [];
  encodeInto(v, parts);
  return Buffer.concat(parts);
}

function u32le(n) { const b = Buffer.alloc(4); b.writeUInt32LE(n >>> 0, 0); return b; }

function encodeInto(v, parts) {
  if (v === null || v === undefined) { parts.push(Buffer.from([0])); return; }
  if (typeof v === 'boolean') { parts.push(Buffer.from([1, v ? 1 : 0])); return; }
  if (typeof v === 'bigint') {
    const b = Buffer.alloc(8); b.writeBigInt64LE(v, 0);
    parts.push(Buffer.from([2]), b); return;
  }
  if (typeof v === 'number') {
    if (Number.isInteger(v)) {
      const b = Buffer.alloc(8); b.writeBigInt64LE(BigInt(v), 0);
      parts.push(Buffer.from([2]), b); return;
    }
    if (!Number.isFinite(v)) throw new SkaidbError('cannot bind NaN/Infinity');
    const b = Buffer.alloc(8); b.writeDoubleLE(v, 0);
    parts.push(Buffer.from([3]), b); return;
  }
  if (typeof v === 'string') {
    const b = Buffer.from(v, 'utf8');
    parts.push(Buffer.from([5]), u32le(b.length), b); return;
  }
  if (Buffer.isBuffer(v)) {
    parts.push(Buffer.from([6]), u32le(v.length), v); return;
  }
  if (v instanceof Date) {
    const b = Buffer.alloc(8); b.writeBigInt64LE(BigInt(v.getTime()), 0);
    parts.push(Buffer.from([8]), b); return;
  }
  if (Array.isArray(v)) {
    parts.push(Buffer.from([9]), u32le(v.length));
    for (const item of v) encodeInto(item, parts);
    return;
  }
  if (typeof v === 'object') {
    const keys = Object.keys(v);
    parts.push(Buffer.from([10]), u32le(keys.length));
    for (const k of keys) {
      const kb = Buffer.from(k, 'utf8');
      parts.push(u32le(kb.length), kb);
      encodeInto(v[k], parts);
    }
    return;
  }
  throw new SkaidbError(`cannot bind value of type ${typeof v}`);
}

function bindParams(sql, params) {
  if (!params || params.length === 0) return sql;
  // Replace $N outside string literals.
  let out = '';
  let inStr = false;
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i];
    if (inStr) {
      out += ch;
      if (ch === "'") {
        if (sql[i + 1] === "'") { out += "'"; i++; } else inStr = false;
      }
      continue;
    }
    if (ch === "'") { inStr = true; out += ch; continue; }
    if (ch === '$' && /[0-9]/.test(sql[i + 1] || '')) {
      let j = i + 1, num = '';
      while (/[0-9]/.test(sql[j] || '')) { num += sql[j]; j++; }
      const idx = parseInt(num, 10) - 1;
      if (idx < 0 || idx >= params.length) throw new SkaidbError(`no parameter for $${num}`);
      out += quote(params[idx]);
      i = j - 1;
      continue;
    }
    out += ch;
  }
  return out;
}

// ---- Connection / Client (pg-style) ---------------------------------------

let nonceCounter = 0;

class Client {
  constructor(opts = {}) {
    this.host = opts.host || 'localhost';
    this.port = opts.port || 7000;
    // Seeds: ['h1', 'h2:7000', ...]. skaidb is leaderless, so any node
    // serves and a seed list is just "somewhere to land" — there is no
    // primary to discover. Shuffled per connect so many clients spread
    // across the cluster instead of stampeding the first entry.
    this.seeds = (opts.seeds && opts.seeds.length ? opts.seeds : [`${this.host}:${this.port}`])
      .map((s) => {
        const i = String(s).lastIndexOf(':');
        return i > 0
          ? { host: String(s).slice(0, i), port: Number(String(s).slice(i + 1)) }
          : { host: String(s), port: this.port };
      });
    this.user = opts.user || 'anonymous';
    this.password = opts.password || '';
    this.consistency = resolveConsistency(opts.consistency);
    this.connectTimeout = opts.connectTimeout || 10000;
    // TLS. A server with client_tls = required refuses plaintext outright,
    // so without this such a cluster is unreachable. Any of the three
    // options turns it on: tls, tlsCa, or tlsInsecure.
    this.tlsCa = opts.tlsCa || null;
    this.tlsInsecure = opts.tlsInsecure === true;
    this.tls = opts.tls === true || this.tlsCa !== null || this.tlsInsecure;
    // SNI must match a SAN on the server certificate, which is usually not
    // the address you dialled — skaidb's own certs carry DNS:skaidb.
    this.tlsServerName = opts.tlsServerName || 'skaidb';
    // Session database, selected with USE right after the handshake.
    this.database = opts.database || null;
    this._sock = null;
    this._buf = Buffer.alloc(0);
    this._waiters = [];       // queue of {resolve, reject} awaiting a frame
    this._frames = [];        // frames that arrived before a reader asked
    this._queryChain = Promise.resolve();
    this._closed = false;      // end() was called: terminal
    this._broken = false;      // transport died: recoverable on next statement
    this._reconnecting = false;
    this._prepared = new Map();
  }

  /**
   * Connect, trying each seed until one connects AND authenticates — a node
   * that accepts TCP while unhealthy must not swallow the attempt.
   */
  async connect() {
    const order = this.seeds.slice();
    for (let i = order.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [order[i], order[j]] = [order[j], order[i]];
    }
    let last;
    for (const seed of order) {
      this.host = seed.host;
      this.port = seed.port;
      try {
        return await this._connectOne();
      } catch (e) {
        last = e;
      }
    }
    throw new SkaidbError(
      `no reachable endpoint in ${order.map((s) => `${s.host}:${s.port}`).join(', ')}: ${last && last.message}`);
  }

  _connectOne() {
    return new Promise((resolve, reject) => {
      let sock;
      let ready;                       // event that means "usable transport"
      if (this.tls) {
        const opts = {
          host: this.host,
          port: this.port,
          servername: this.tlsServerName,
        };
        if (this.tlsInsecure) {
          // Encrypts, but authenticates nothing: a man in the middle can
          // present any certificate. Development against a self-signed node
          // only — pass tlsCa for anything that matters.
          opts.rejectUnauthorized = false;
        } else if (this.tlsCa) {
          try {
            opts.ca = fs.readFileSync(this.tlsCa);
          } catch (e) {
            reject(new SkaidbError(`cannot read tlsCa ${this.tlsCa}: ${e.message}`));
            return;
          }
        }
        sock = tls.connect(opts);
        ready = 'secureConnect';       // fires only after the TLS handshake
      } else {
        sock = net.createConnection({ host: this.host, port: this.port });
        ready = 'connect';
      }
      sock.setNoDelay(true);
      const onErr = (e) => reject(new SkaidbError(`connect failed: ${e.message}`));
      sock.once('error', onErr);
      const to = setTimeout(() => { sock.destroy(); reject(new SkaidbError('connect timeout')); },
        this.connectTimeout);
      sock.once(ready, () => {
        clearTimeout(to);
        sock.removeListener('error', onErr);
        this._sock = sock;
        sock.on('data', (d) => this._onData(d));
        sock.on('error', (e) => this._fail(e));
        sock.on('close', () => this._fail(new SkaidbError('connection closed')));
        this._handshake()
          .then(() => this._sendHello())
          .then(() => this.database
            ? this.query(`USE "${this.database.replace(/"/g, '""')}"`).then(() => undefined)
            : undefined)
          .then(resolve, reject);
      });
    });
  }

  _onData(d) {
    this._buf = Buffer.concat([this._buf, d]);
    // Parse as many complete frames as available.
    for (;;) {
      if (this._buf.length < 4) return;
      const len = this._buf.readUInt32BE(0);
      if (this._buf.length < 4 + len) return;
      const frame = this._buf.subarray(4, 4 + len);
      this._buf = this._buf.subarray(4 + len);
      const w = this._waiters.shift();
      if (w) w.resolve(Buffer.from(frame));
      // No reader waiting yet: QUEUE the frame, never drop it. A streamed
      // result arrives as header + chunks + end, and the server can deliver
      // several of those in one TCP read while the client is still
      // processing the previous one. Dropping them silently truncated the
      // stream and then hung waiting for frames already thrown away.
      else this._frames.push(Buffer.from(frame));
    }
  }

  // A transport failure. The in-flight statement (if any) fails — it may
  // have executed, so retrying it here could duplicate a write — but the
  // connection is only marked BROKEN, not closed: the next statement calls
  // _ensureLive and transparently re-dials. end() is what makes a client
  // terminal.
  _fail(err) {
    if (this._closed || this._broken) return;
    this._broken = true;
    const e = err instanceof Error ? err : new SkaidbError(String(err));
    while (this._waiters.length) this._waiters.shift().reject(e);
  }

  /**
   * Re-dial if the transport died, before anything is prepared or sent.
   *
   * The prepared-statement cache MUST be cleared: an id is only valid on the
   * connection that created it, so reusing one after a reconnect would run a
   * different statement (or fail obscurely). Everything else is per-socket
   * scratch state and is reset with it.
   */
  async _ensureLive() {
    if (this._closed) throw new SkaidbError('connection is closed');
    if (!this._broken || this._reconnecting) return;
    this._reconnecting = true;
    try {
      this._prepared.clear();
      this._waiters = [];
      this._frames = [];
      this._buf = Buffer.alloc(0);
      try { if (this._sock) this._sock.destroy(); } catch (_) { /* already gone */ }
      this._sock = null;
      this._broken = false;
      await this.connect();          // re-runs seed failover + handshake + USE
    } catch (e) {
      this._broken = true;           // still down; the next call tries again
      throw e;
    } finally {
      this._reconnecting = false;
    }
  }

  /**
   * Best-effort self-identification: fills the server's `drivers` table
   * `client_name`/`client_version`. An old server answers the unknown
   * opcode with an error frame, which is ignored — identity is telemetry,
   * never load-bearing.
   */
  async _sendHello() {
    try {
      let version = '0';
      try { version = require('./package.json').version; } catch (_) { /* vendored single-file */ }
      const name = Buffer.from('nodejs');
      const ver = Buffer.from(version);
      this._writeFrame(Buffer.concat([Buffer.from([8]), u32le(name.length), name, u32le(ver.length), ver]));
      await this._readFrame();
    } catch (_) { /* telemetry only */ }
  }

  _writeFrame(payload) {
    const head = Buffer.allocUnsafe(4);
    head.writeUInt32BE(payload.length, 0);
    this._sock.write(Buffer.concat([head, payload]));
  }

  _readFrame() {
    const queued = this._frames.shift();
    if (queued) return Promise.resolve(queued);
    return new Promise((resolve, reject) => this._waiters.push({ resolve, reject }));
  }

  async _handshake() {
    const clientNonce = `js${process.pid}.${nonceCounter++}`;
    this._writeFrame(Buffer.concat([Buffer.from([10]), encStr(this.user), encStr(clientNonce)]));

    const r1 = new Reader(await this._readFrame());
    if (r1.u8() !== 11) throw new SkaidbError('bad handshake challenge');
    const salt = r1.blob();
    const iterations = r1.u32();
    const serverNonce = r1.text();

    const authMessage = Buffer.from(
      [this.user, clientNonce, serverNonce, salt.toString('hex'), String(iterations)].join('\0'),
      'utf8');
    const salted = crypto.pbkdf2Sync(this.password, salt, iterations, 32, 'sha256');
    const clientKey = hmac(salted, Buffer.from('Client Key'));
    const storedKey = sha256(clientKey);
    const clientSig = hmac(storedKey, authMessage);
    const proof = xor(clientKey, clientSig);

    this._writeFrame(Buffer.concat([Buffer.from([12]), proof]));

    const r2 = new Reader(await this._readFrame());
    if (r2.u8() !== 13) throw new SkaidbError('bad handshake outcome');
    if (r2.u8() === 1) {
      const serverSig = r2.take(32);
      if (this.password) {
        const serverKey = hmac(salted, Buffer.from('Server Key'));
        const expected = hmac(serverKey, authMessage);
        if (!crypto.timingSafeEqual(serverSig, expected))
          throw new SkaidbError('server signature mismatch (mutual auth failed)');
      }
    } else {
      throw new SkaidbError(`authentication denied: ${r2.text()}`);
    }
  }

  // pg-style: query(text, [params]) or query({ text, values, consistency, rowMode })
  query(config, values) {
    let text, params, consistency = this.consistency, rowMode = 'object';
    if (typeof config === 'string') { text = config; params = values; }
    else {
      text = config.text; params = config.values || values;
      if (config.consistency !== undefined) consistency = resolveConsistency(config.consistency);
      if (config.rowMode) rowMode = config.rowMode;
    }
    // Serialize queries on this connection (one request/response in flight).
    const run = this._queryChain.then(async () => {
      // Recover a transport that died since the last statement, BEFORE
      // anything is prepared on it (a stale statement id is the hazard).
      await this._ensureLive();
      if (params && params.length) {
        // Server-side prepare so parameters travel as TYPED values; arrays
        // and documents have no SQL literal form. `$N` is rewritten to the
        // positional `?` the server expects.
        const q = toQmark(text, params);
        const p = await this._prepare(q.sql);
        if (p) {
          if (p.nparams !== q.params.length) {
            throw new SkaidbError(
              `statement expects ${p.nparams} parameters, got ${q.params.length}`);
          }
          return this._doPrepared(p.id, q.params, consistency, rowMode);
        }
      }
      return this._doQuery(bindParams(text, params), consistency, rowMode);
    });
    this._queryChain = run.catch(() => {}); // keep the chain alive after errors
    return run;
  }

  async _doQuery(sql, consistency, rowMode) {
    if (this._closed) throw new SkaidbError('connection is closed');
    const body = Buffer.from(sql, 'utf8');
    const head = Buffer.allocUnsafe(6);
    head[0] = 1; head[1] = consistency; head.writeUInt32LE(body.length, 2);
    return this._roundtrip(Buffer.concat([head, body]), rowMode);
  }

  /**
   * Stream a result set: yields rows one at a time while holding a single
   * chunk, instead of buffering the whole set. For exports and large scans.
   *
   *   for await (const row of client.stream('SELECT ...')) { ... }
   *
   * `columns` is available on the returned iterator once iteration starts.
   * Takes no parameters — the streaming opcode carries SQL text. The
   * connection is busy until the stream is exhausted; breaking out early
   * drains the remaining frames so the connection stays usable.
   */
  async *stream(sql, opts = {}) {
    const consistency = opts.consistency === undefined
      ? this.consistency : resolveConsistency(opts.consistency);
    const rowMode = opts.rowMode || 'object';
    // Take the query chain for the whole stream: no other statement may be
    // in flight on this connection until RowsEnd.
    let release;
    let streaming = false;      // a header arrived: frames are still coming
    const held = new Promise((r) => { release = r; });
    const prev = this._queryChain;
    this._queryChain = held;
    await prev;
    try {
      if (this._closed) throw new SkaidbError('connection is closed');
      const body = Buffer.from(sql, 'utf8');
      const head = Buffer.allocUnsafe(6);
      head[0] = 5; head[1] = consistency; head.writeUInt32LE(body.length, 2);
      this._writeFrame(Buffer.concat([head, body]));

      const first = new Reader(await this._readFrame());
      const tag = first.u8();
      if (tag === 3) {
        const msg = first.text();
        throw new SkaidbError(msg.includes('unknown opcode')
          ? `server does not support streaming: ${msg}` : msg);
      }
      if (tag === 1 || tag === 2) return;      // mutation/ddl: no rows
      if (tag !== 5) throw new SkaidbError(`unexpected response tag ${tag} to stream request`);
      streaming = true;
      const ncols = first.u32();
      const fields = [];
      for (let i = 0; i < ncols; i++) fields.push(first.text());
      this.stream.columns = fields;
      for (;;) {
        const r = new Reader(await this._readFrame());
        const t = r.u8();
        if (t === 6) {
          const n = r.u32();
          for (let i = 0; i < n; i++) {
            const ncells = r.u32();
            const cells = [];
            for (let c = 0; c < ncells; c++) cells.push(decodeValue(new Reader(r.blob())));
            if (rowMode === 'array') yield cells;
            else {
              const o = {};
              for (let c = 0; c < fields.length; c++) o[fields[c]] = cells[c];
              yield o;
            }
          }
        } else if (t === 7) {
          streaming = false;
          return;
        } else if (t === 3) {
          streaming = false;
          // Rows already yielded are valid; the statement failed partway.
          throw new SkaidbError(r.text());
        } else {
          throw new SkaidbError(`unexpected frame tag ${t} in stream`);
        }
      }
    } finally {
      // Abandoned early (a `break`, a throw): the server is still sending.
      // Drain to RowsEnd, or the leftovers would be read as the reply to the
      // NEXT statement on this connection.
      if (streaming && !this._closed) {
        try {
          for (;;) {
            const r = new Reader(await this._readFrame());
            const t = r.u8();
            if (t === 7 || t === 3) break;
          }
        } catch { /* connection is gone; nothing to drain */ }
      }
      release();
    }
  }

  /**
   * Execute `sql` once per row in ONE round-trip. Rows autocommit
   * individually: a failure names the row and earlier rows stay applied, so
   * the statement must be idempotent. Returns the total affected count.
   */
  batch(sql, rows) {
    const run = this._queryChain.then(async () => {
      // Recover a transport that died since the last statement, BEFORE
      // anything is prepared on it (a stale statement id is the hazard).
      await this._ensureLive();
      if (!rows || rows.length === 0) return 0;
      const q = toQmark(sql, rows[0]);
      const p = await this._prepare(q.sql);
      if (!p) throw new SkaidbError('statement cannot be prepared, so it cannot be batched');
      const ordered = rows.map((r) => toQmark(sql, r).params);
      for (const r of ordered) {
        if (r.length !== p.nparams) {
          throw new SkaidbError(`batch row expects ${p.nparams} parameters, got ${r.length}`);
        }
      }
      const head = Buffer.allocUnsafe(10);
      head[0] = 7; head[1] = this.consistency;
      head.writeUInt32LE(p.id, 2); head.writeUInt32LE(ordered.length, 6);
      const parts = [head];
      for (const r of ordered) {
        const cnt = Buffer.alloc(2); cnt.writeUInt16LE(r.length, 0);
        parts.push(cnt);
        for (const v of r) { const b = encodeValue(v); parts.push(u32le(b.length), b); }
      }
      const res = await this._roundtrip(Buffer.concat(parts), 'object');
      return res.rowCount || 0;
    });
    this._queryChain = run.catch(() => {});
    return run;
  }

  /**
   * Prepare `sql` on the SERVER, returning {id, nparams}. Cached per
   * connection — a prepared id only means anything on the connection that
   * created it. Returns null when the server declines the statement kind
   * (DDL, session statements), so the caller falls back to text binding.
   */
  async _prepare(sql) {
    const hit = this._prepared.get(sql);
    if (hit) return hit;
    const body = Buffer.from(sql, 'utf8');
    const head = Buffer.allocUnsafe(5);
    head[0] = 2; head.writeUInt32LE(body.length, 1);
    this._writeFrame(Buffer.concat([head, body]));
    const r = new Reader(await this._readFrame());
    const tag = r.u8();
    if (tag === 4) {
      const id = r.u32();
      const nparams = r.u16();
      const v = { id, nparams };
      if (this._prepared.size < 240) this._prepared.set(sql, v);
      return v;
    }
    if (tag === 3) { r.text(); return null; }   // unpreparable: fall back
    throw new SkaidbError(`unexpected prepare response tag ${tag}`);
  }

  /** Execute a prepared statement with TYPED parameters. */
  async _doPrepared(id, params, consistency, rowMode) {
    const head = Buffer.allocUnsafe(8);
    head[0] = 3; head[1] = consistency;
    head.writeUInt32LE(id, 2); head.writeUInt16LE(params.length, 6);
    const parts = [head];
    for (const p of params) {
      const v = encodeValue(p);
      parts.push(u32le(v.length), v);
    }
    return this._roundtrip(Buffer.concat(parts), rowMode);
  }

  async _roundtrip(request, rowMode) {
    if (this._closed) throw new SkaidbError('connection is closed');
    if (this._broken) throw new SkaidbError('connection lost');
    this._writeFrame(request);

    const r = new Reader(await this._readFrame());
    const tag = r.u8();
    if (tag === 0) {                            // Rows
      const ncols = r.u32();
      const fields = [];
      for (let i = 0; i < ncols; i++) fields.push({ name: r.text() });
      const nrows = r.u32();
      const rows = [];
      for (let i = 0; i < nrows; i++) {
        const ncells = r.u32();
        const cells = [];
        for (let c = 0; c < ncells; c++) cells.push(decodeValue(new Reader(r.blob())));
        if (rowMode === 'array') rows.push(cells);
        else {
          const obj = {};
          for (let c = 0; c < ncells; c++) obj[fields[c].name] = cells[c];
          rows.push(obj);
        }
      }
      return { command: 'SELECT', rowCount: rows.length, rows, fields,
               columns: fields.map((f) => f.name) };
    }
    if (tag === 1) {                            // Mutation
      const affected = r.u64();
      return { command: 'MUTATION', rowCount: safeInt(affected), rows: [], fields: [] };
    }
    if (tag === 2) return { command: 'DDL', rowCount: null, rows: [], fields: [] };
    if (tag === 3) throw new SkaidbError(r.text());
    throw new SkaidbError(`unknown response tag ${tag}`);
  }

  /**
   * Yield a stream's events as they arrive, forever.
   *
   * A dependency-free helper over the stream's log: it pages the log with
   * the keyset cursor and yields each event ({id, op, k, ts, doc}). `id` is
   * the position — keep the last one and pass it as `after` to resume
   * exactly where you stopped, across restarts.
   *
   * This polls; for push delivery subscribe to `$stream/<db>/<name>` with
   * any MQTT client instead. The events are identical.
   *
   *   for await (const ev of client.subscribe('big_orders')) { ... }
   */
  async *subscribe(stream, { after = null, pollMs = 500 } = {}) {
    const log = `_stream_${stream}`;
    let cur = after;
    for (;;) {
      const res = cur === null
        ? await this.query(`SELECT id, op, k, ts, doc FROM ${log} ORDER BY id LIMIT 500`)
        : await this.query(
            `SELECT id, op, k, ts, doc FROM ${log} WHERE id > $1 ORDER BY id LIMIT 500`, [cur]);
      for (const row of res.rows) {
        cur = row.id;
        yield row;
      }
      if (res.rows.length === 0) {
        await new Promise((r) => setTimeout(r, pollMs));
      }
    }
  }

  /** False once end() was called or a transport error broke the socket. */
  isUsable() {
    return !this._closed && !this._broken && this._sock !== null;
  }

  end() {
    this._closed = true;
    return new Promise((resolve) => {
      if (!this._sock) return resolve();
      this._sock.end(resolve);
    });
  }
}

// ---- helpers --------------------------------------------------------------

function resolveConsistency(c) {
  if (c === undefined) return CONSISTENCY.QUORUM;
  if (typeof c === 'number') { if ([0, 1, 2].includes(c)) return c; throw new SkaidbError(`bad consistency ${c}`); }
  const v = CONSISTENCY[String(c).toUpperCase()];
  if (v === undefined) throw new SkaidbError(`bad consistency ${c}`);
  return v;
}
function encStr(s) {
  const b = Buffer.from(s, 'utf8');
  const head = Buffer.allocUnsafe(4); head.writeUInt32LE(b.length, 0);
  return Buffer.concat([head, b]);
}
function hmac(key, msg) { return crypto.createHmac('sha256', key).update(msg).digest(); }
function sha256(b) { return crypto.createHash('sha256').update(b).digest(); }
function xor(a, b) { const o = Buffer.allocUnsafe(a.length); for (let i = 0; i < a.length; i++) o[i] = a[i] ^ b[i]; return o; }

/**
 * A pool of skaidb connections.
 *
 * `maxsize` bounds the connections kept IDLE, not the number checked out:
 * a burst creates extras and the surplus is closed on return. Every Client
 * option passes through, so pooled connections inherit seed failover, TLS
 * and the session database.
 *
 *     const pool = new Pool({ seeds: ['h1:7000', 'h2:7000'], database: 'app', maxsize: 8 });
 *     const res = await pool.withConnection((c) => c.query('SELECT 1'));
 *     await pool.end();
 */
class Pool {
  constructor(opts = {}) {
    const { maxsize = 10, ...clientOpts } = opts;
    if (maxsize < 1) throw new SkaidbError('maxsize must be >= 1');
    this.maxsize = maxsize;
    this._opts = clientOpts;
    this._idle = [];
    this.closed = false;
  }

  /** Check out a usable connection, reusing an idle one when possible. */
  async acquire() {
    for (;;) {
      if (this.closed) throw new SkaidbError('pool is closed');
      const conn = this._idle.pop();
      if (!conn) {
        const fresh = new Client(this._opts);
        await fresh.connect();
        return fresh;
      }
      // A connection the server closed while it sat idle still looks fine
      // locally, so validate before handing it out; discard and try again.
      if (conn.isUsable()) return conn;
      await conn.end().catch(() => {});
    }
  }

  /** Return a connection, closing it if it is broken or the pool is full. */
  async release(conn) {
    if (!this.closed && conn.isUsable() && this._idle.length < this.maxsize) {
      this._idle.push(conn);
      return;
    }
    await conn.end().catch(() => {});
  }

  /** Run `fn` with a checked-out connection, returning it however fn ends. */
  async withConnection(fn) {
    const conn = await this.acquire();
    try {
      return await fn(conn);
    } finally {
      await this.release(conn);
    }
  }

  /** Close the pool and every idle connection. Checked-out ones close on release. */
  async end() {
    this.closed = true;
    const idle = this._idle;
    this._idle = [];
    await Promise.all(idle.map((c) => c.end().catch(() => {})));
  }
}

module.exports = { Client, Pool, SkaidbError, CONSISTENCY };
