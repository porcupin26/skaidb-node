'use strict';
// skaidb — Node.js driver. API modeled on node-postgres (`pg`).
// Pure standard library: `net` + `crypto`. No dependencies.
//
//   const { Client } = require('@skaidb/client');
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
const { EventEmitter } = require('events');

const CONSISTENCY = { ONE: 0, QUORUM: 1, ALL: 2 };

// How much of an abandoned stream is worth reading out before dropping the
// connection instead. The server sends ~256 KB row chunks, so this is a few
// dozen of them: past that, transferring the remainder of a large scan costs
// more than the reconnect (TCP + TLS + SCRAM) that replaces the connection.
const DRAIN_BUDGET_BYTES = 8 * 1024 * 1024;

class SkaidbError extends Error {}

// Errors the server answered with (an Error frame): the statement failed and
// the member that sent it is fine. A pool tells them apart from transport
// failures, which count against a member's health.
const statementErrors = new WeakSet();
function statementError(msg) {
  const e = new SkaidbError(msg);
  statementErrors.add(e);
  return e;
}

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
 *
 * A `?` in the text is NOT a placeholder to this driver, but the server
 * would happily take it as one and then complain that the statement
 * "expects N parameters, got 0". So when values are given and the text
 * contains a bare `?` but no `$N` at all, the mistake is reported here,
 * before anything is sent.
 */
function toQmark(sql, params) {
  let out = '';
  const order = [];
  let inStr = false;
  let sawDollar = false, sawQmark = false;
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
    if (ch === '?') sawQmark = true;
    if (ch === '$' && /[0-9]/.test(sql[i + 1] || '')) {
      sawDollar = true;
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
  if (params.length > 0 && sawQmark && !sawDollar) {
    throw new SkaidbError(
      "this driver uses $1, $2 … placeholders; '?' is not a placeholder");
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
    if (v < -(1n << 63n) || v >= (1n << 63n)) throw new SkaidbError(`bigint ${v} does not fit a 64-bit integer`);
    const b = Buffer.alloc(8); b.writeBigInt64LE(v, 0);
    parts.push(Buffer.from([2]), b); return;
  }
  if (typeof v === 'number') {
    // -0 is an integer to JS but has no Int form: binding it as Int 0
    // would lose the sign, so it travels as the Float it is.
    if (Number.isInteger(v) && !Object.is(v, -0)) {
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

/**
 * The request `stream()` sends. When the statement binds at least one value
 * it is OP_QUERY_STREAM_PARAMS (9): the SQL with `$N` rewritten to `?`, then
 * the typed parameter vector OP_EXECUTE carries, in `?` order. Otherwise it
 * is the original OP_QUERY_STREAM (5) with the SQL exactly as given, which
 * every server with streaming understands: `$N` is only rewritten when
 * values are given, as in query().
 */
function streamRequest(sql, values, consistency) {
  if (values !== undefined && values !== null && !Array.isArray(values)) {
    throw new SkaidbError('stream() values must be an array');
  }
  const q = values && values.length ? toQmark(sql, values) : { sql, params: [] };
  const body = Buffer.from(q.sql, 'utf8');
  const head = Buffer.allocUnsafe(6);
  head[0] = q.params.length ? 9 : 5; head[1] = consistency; head.writeUInt32LE(body.length, 2);
  if (q.params.length === 0) return Buffer.concat([head, body]);
  // The count is a u16 on the wire: refuse more as a SkaidbError rather than
  // let writeUInt16LE throw a RangeError. (query() learns the count from its
  // prepare reply, which could not match.)
  if (q.params.length > 0xffff) {
    throw new SkaidbError(`too many parameters: ${q.params.length} (a statement binds at most 65535)`);
  }
  const count = Buffer.allocUnsafe(2);
  count.writeUInt16LE(q.params.length, 0);
  const parts = [head, body, count];
  for (const p of q.params) { const v = encodeValue(p); parts.push(u32le(v.length), v); }
  return Buffer.concat(parts);
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

// ---- result decoding (§3.2) -------------------------------------------------

/** A Rows body, also one set of ResultSets; `r` sits just past the tag. */
function readRows(r, rowMode) {
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

/**
 * A ResultSets body (a CALL whose body EMITted), `r` just past the tag. The
 * buffered path and a stream request both receive this frame, and both
 * decode it here: the LAST set (the call's final result) is the result's own
 * rows; every set, in emission order, is under `resultSets`.
 */
function readResultSets(r, rowMode) {
  const sets = [];
  const nsets = r.u32();
  for (let s = 0; s < nsets; s++) sets.push(readRows(r, rowMode));
  const last = sets[sets.length - 1] || { rows: [], fields: [], columns: [], rowCount: 0 };
  return { command: 'CALL', rowCount: last.rowCount, rows: last.rows, fields: last.fields,
           columns: last.columns, resultSets: sets };
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
      .map((s) => parseEndpoint(s, this.port));
    // Certificate login (wire mechanism EXTERNAL): the TLS client
    // certificate IS the credential and its Common Name the username, so
    // no password is sent. `user`, when given, only asserts the identity
    // (the server refuses a certificate mapped to a different role); left
    // out, the username is sent empty.
    const mech = String(opts.authMechanism === undefined ? 'scram' : opts.authMechanism).toLowerCase();
    if (!['scram', 'password', 'certificate', 'external', 'x509'].includes(mech)) {
      throw new SkaidbError(
        `unknown authMechanism ${opts.authMechanism} (use 'scram' or 'certificate')`);
    }
    this.authMechanism = ['certificate', 'external', 'x509'].includes(mech) ? 'certificate' : 'scram';
    this.user = opts.user || (this.authMechanism === 'certificate' ? '' : 'anonymous');
    this.password = opts.password || '';
    this.consistency = resolveConsistency(opts.consistency);
    this.connectTimeout = opts.connectTimeout || 10000;
    // TLS. A server with client_tls = required refuses plaintext outright,
    // so without this such a cluster is unreachable. Any of the three
    // options turns it on: tls, tlsCa, or tlsInsecure.
    this.tlsCa = opts.tlsCa || null;
    this.tlsInsecure = opts.tlsInsecure === true;
    // A client certificate (PEM paths) presented in the TLS handshake: what
    // a server that verifies clients needs, and the credential itself with
    // authMechanism 'certificate'. Either implies TLS.
    this.tlsClientCert = opts.tlsClientCert || null;
    this.tlsClientKey = opts.tlsClientKey || null;
    if ((this.tlsClientCert === null) !== (this.tlsClientKey === null)) {
      throw new SkaidbError('tlsClientCert and tlsClientKey go together');
    }
    if (this.authMechanism === 'certificate' && this.tlsClientCert === null) {
      throw new SkaidbError(
        "authMechanism 'certificate' needs a client certificate (tlsClientCert and tlsClientKey)");
    }
    this.tls = opts.tls === true || this.tlsCa !== null || this.tlsInsecure
      || this.tlsClientCert !== null;
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
    this._connected = null;    // 'host:port' reached by the last successful dial
    // The pool that owns this client (see Pool): told each round trip's
    // latency, each transport failure and each write, and asked for the dial
    // order when the client re-dials. null outside a pool; the other _pool*
    // slots are the pool's bookkeeping.
    this._pool = null;
    this._poolEp = null;
    this._poolOut = false;
    this._poolAffinity = null;
  }

  /**
   * Connect, trying each seed until one connects AND authenticates — a node
   * that accepts TCP while unhealthy must not swallow the attempt.
   */
  async connect() {
    return this._connectVia(shuffle(this.seeds.slice()));
  }

  /**
   * Dial the endpoints of `order` ({host, port}) in turn until one connects
   * and authenticates. A pooled client reports each failed endpoint whose
   * failure says something about the member's health.
   */
  async _connectVia(order) {
    this._connected = null;
    let last;
    for (const seed of order) {
      this.host = seed.host;
      this.port = seed.port;
      // An attempt that died mid-handshake leaves the client marked broken,
      // and a broken client refuses to read a frame: without the reset every
      // later endpoint failed with "connection lost".
      this._broken = false;
      try {
        await this._connectOne();
        this._connected = `${seed.host}:${seed.port}`;
        return;
      } catch (e) {
        last = e;
        if (this._pool !== null && memberFault(e)) this._pool._failed(`${seed.host}:${seed.port}`);
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
        if (this.tlsClientCert) {
          try {
            opts.cert = fs.readFileSync(this.tlsClientCert);
          } catch (e) {
            reject(new SkaidbError(`cannot read tlsClientCert ${this.tlsClientCert}: ${e.message}`));
            return;
          }
          try {
            opts.key = fs.readFileSync(this.tlsClientKey);
          } catch (e) {
            reject(new SkaidbError(`cannot read tlsClientKey ${this.tlsClientKey}: ${e.message}`));
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
        // Events from a socket that is no longer the client's are ignored.
        // _fail() destroys a socket and nulls the slot, but its 'close'
        // fires on a LATER tick — after a caller may already have re-dialled
        // through _ensureLive(). Without the guard that stale event marked
        // the fresh connection broken (and destroyed it), so the statement
        // right after an abandoned stream failed with "connection lost".
        sock.on('data', (d) => { if (this._sock === sock) this._onData(d); });
        sock.on('error', (e) => { if (this._sock === sock) this._fail(e); });
        sock.on('close', () => { if (this._sock === sock) this._fail(new SkaidbError('connection closed')); });
        // USE goes straight to the socket, not through query(): a reconnect
        // runs INSIDE the statement that triggered it, and query() would
        // queue the USE behind that very statement — a deadlock that hung
        // every client with a session database on its first statement
        // after a lost connection.
        this._handshake()
          .then(() => this._sendHello())
          .then(() => this.database
            ? this._doQuery(`USE "${this.database.replace(/"/g, '""')}"`, this.consistency, 'object')
                .then(() => undefined)
            : undefined)
          .then(resolve, (e) => {
            // A seed that fails the handshake (denied, a bad signature, a
            // protocol error) must not keep its socket: close it and drop
            // anything it left unread, so the next seed starts clean.
            // Without this every failed attempt leaked an open connection.
            if (this._sock === sock) {
              this._sock = null;
              this._buf = Buffer.alloc(0);
              this._frames = [];
              this._waiters = [];
            }
            sock.destroy();
            reject(e);
          });
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
    // Stop the transfer, not just the reader. The socket is in flowing
    // mode, so leaving it attached means the rest of an abandoned result
    // keeps arriving and `_onData` keeps pushing frames onto `_frames`
    // that nothing will ever read — an abandoned 500 MB export was
    // measured buffering 461 MB of it. The connection is already
    // unusable at this point, so there is nothing left to protect by
    // keeping it open, and a half-read stream is precisely the case
    // where the remaining bytes are worth refusing.
    try {
      if (this._sock) this._sock.destroy();
    } catch (_) { /* already gone */ }
    this._sock = null;
    while (this._waiters.length) this._waiters.shift().reject(e);
  }

  /**
   * A frame that cannot be placed in the protocol: after it we no longer
   * know where the next response starts, so the connection must not be
   * reused as-is. Marking it broken is the safe fallback to draining —
   * isUsable() turns false, so a pool discards it instead of handing the
   * desync to the next caller, and a directly held client re-dials in
   * _ensureLive() on its next statement. Returns the error to throw.
   */
  _desync(msg) {
    const e = new SkaidbError(msg);
    this._fail(e);
    return e;
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
      // Pooled: the pool's current order (usable members first, the one
      // just lost last) instead of the seeds.
      const order = this._pool !== null ? this._pool._dialOrder(this._connected) : null;
      this._prepared.clear();
      this._waiters = [];
      this._frames = [];
      this._buf = Buffer.alloc(0);
      try { if (this._sock) this._sock.destroy(); } catch (_) { /* already gone */ }
      this._sock = null;
      this._broken = false;
      if (order !== null) await this._connectVia(order);
      else await this.connect();     // re-runs seed failover + handshake + USE
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
    // Never park a waiter on a socket that is already gone. _fail() rejects
    // the waiters that existed when the transport died and then goes quiet
    // (it returns early once _broken is set), so a waiter queued afterwards
    // would wait for a frame nobody will ever deliver: a stream drained
    // after a node died mid-scan hung here forever.
    if (this._broken) return Promise.reject(new SkaidbError('connection lost'));
    if (this._closed) return Promise.reject(new SkaidbError('connection is closed'));
    return new Promise((resolve, reject) => this._waiters.push({ resolve, reject }));
  }

  /**
   * Send one request and wait for the first frame of its answer. A pooled
   * client reports the round trip's latency to its pool, or the transport
   * failure that cut it short (a connection end()ed meanwhile is neither).
   */
  async _request(payload) {
    const started = performance.now();
    let frame;
    try {
      this._writeFrame(payload);
      frame = await this._readFrame();
    } catch (e) {
      if (this._pool !== null && this._connected !== null && this._broken && !this._closed) {
        this._pool._failed(this._connected);
      }
      throw e;
    }
    if (this._pool !== null && this._connected !== null) {
      this._pool._roundTrip(this, performance.now() - started);
    }
    return frame;
  }

  /** A statement succeeded: a pool with read-your-writes remembers which member took a write. */
  _noteStatement(sql) {
    if (this._pool !== null && isWrite(sql)) this._pool._wrote(this);
  }

  async _handshake() {
    if (this.authMechanism === 'certificate') return this._handshakeCertificate();
    const clientNonce = `js${process.pid}.${nonceCounter++}`;
    this._writeFrame(authStartFrame(this.user, clientNonce));

    const r1 = new Reader(await this._readFrame());
    if (r1.u8() !== 11) throw new SkaidbError('bad handshake challenge');
    const salt = r1.blob();
    const iterations = r1.u32();
    const serverNonce = r1.text();

    const authMessage = Buffer.from(
      [this.user, clientNonce, serverNonce, salt.toString('hex'), String(iterations)].join('\0'),
      'utf8');
    const { proof, serverSignature } = scramProof(this.password, salt, iterations, authMessage);

    this._writeFrame(Buffer.concat([Buffer.from([12]), proof]));

    const serverSig = parseAuthOutcome(await this._readFrame());
    if (this.password && !crypto.timingSafeEqual(serverSig, serverSignature))
      throw new SkaidbError('server signature mismatch (mutual auth failed)');
  }

  /**
   * EXTERNAL: the TLS client certificate is the credential. AuthStart
   * carries the (possibly empty) username, an empty nonce and mechanism 2;
   * the server answers AuthOutcome at once. The outcome's signature is 32
   * zero bytes and is deliberately NOT verified: TLS authenticated the
   * server.
   */
  async _handshakeCertificate() {
    if (!this.tls || !this.tlsClientCert) {
      throw new SkaidbError('certificate authentication needs TLS with a client certificate');
    }
    this._writeFrame(authStartFrame(this.user, '', AUTH_MECHANISM.EXTERNAL));
    parseAuthOutcome(await this._readFrame());
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
    }).then((res) => {
      this._noteStatement(text);
      return res;
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
   *   for await (const row of client.stream('SELECT ... WHERE ts >= $1', [since])) { ... }
   *
   * `values` bind `$1, $2, …` exactly as query(text, values) binds them —
   * typed, arrays and objects included — and travel with the statement in
   * OP_QUERY_STREAM_PARAMS (server >= 0.330; an older one refuses it with
   * "server does not support parameterised streaming"). Without values the
   * request is the original OP_QUERY_STREAM, byte for byte, so a
   * parameterless stream works against every server. `opts` is
   * { values?, consistency?, rowMode? }: `opts.values` wins over the
   * argument, as `config.values` does in query(), and stream(sql, opts) —
   * the form from before stream() took values — is unchanged.
   *
   * Column names land on `client.stream.columns` once the header arrives.
   * A statement that produces no rows (UPDATE, DELETE, DDL) yields nothing;
   * `client.stream.command` then says `'MUTATION'` or `'DDL'` and
   * `client.stream.affected` holds a mutation's affected-row count
   * (`columns` is null). A row result sets `command` to `'SELECT'`. A CALL
   * whose body EMITs result sets is answered with all of them in one frame,
   * streamed or not: the stream then yields the rows query() would return
   * (the call's final result, the last set), `command` is `'CALL'`, and
   * `client.stream.resultSets` holds every set, in emission order, exactly
   * as query()'s `resultSets` does (null for every other statement). Those
   * slots live on the shared method, not on the iterator, so they describe
   * whichever stream started most recently — read them before starting
   * another.
   *
   * The connection is busy for the whole stream, and CLOSING the iterator is
   * what frees it. `break`, `return`, a throw out of the loop body and an
   * explicit `.return()` all close it, running the drain below so the socket
   * is left at a request boundary. An iterator that is merely dropped
   * half-read is closed by nobody — JavaScript has no finalizer that could
   * do it — and that connection stays busy for good, so always iterate to
   * the end or close it.
   */
  async *stream(sql, values, opts) {
    // stream(sql, opts), the call form from before values existed: only an
    // array (or nothing) is values, so any other second argument is still
    // read as the options it always was.
    if (opts === undefined && values !== undefined && values !== null && !Array.isArray(values)) {
      opts = values;
      values = undefined;
    }
    if (opts === undefined || opts === null) opts = {};
    const consistency = opts.consistency === undefined
      ? this.consistency : resolveConsistency(opts.consistency);
    const rowMode = opts.rowMode || 'object';
    // Built before the stream takes the connection: a value that cannot be
    // bound (`$3` with two values, a `?`, NaN) rejects without waiting
    // behind other statements or sending anything.
    const request = streamRequest(sql, opts.values || values, consistency);
    // Hold the query chain for the WHOLE stream rather than per frame, so
    // no other statement can interleave its frames with ours. Concurrency
    // choice: a query()/batch() call made meanwhile QUEUES behind the stream
    // instead of raising "connection busy" — the chain already gives every
    // other call that semantics, and a caller that reaches for the same
    // connection from two places expects serialization, not a failure it
    // has no way to retry against. The price is that an iterator nobody
    // ever closes wedges those queued calls; see the note above.
    let release;
    let streaming = false;      // a header arrived: frames are still coming
    const held = new Promise((r) => { release = r; });
    const prev = this._queryChain;
    this._queryChain = held;
    await prev;
    try {
      // Re-dial a transport that died since the last statement, exactly as
      // query() does. Without this the request goes into a dead socket and
      // the reply never comes.
      await this._ensureLive();
      // This stream now owns the result slots: nothing stale from the
      // previous stream may describe it (a mutation has no columns).
      this.stream.columns = null;
      this.stream.command = null;
      this.stream.affected = null;
      this.stream.resultSets = null;
      const first = new Reader(await this._request(request));
      const tag = first.u8();
      if (tag === 3) {
        // One ordinary Error frame: the connection is at a request boundary.
        const msg = first.text();
        if (!msg.includes('unknown opcode')) throw statementError(msg);
        throw statementError(request[0] === 9            // OP_QUERY_STREAM_PARAMS
          ? `server does not support parameterised streaming (skaidb >= 0.330): ${msg}`
          : `server does not support streaming: ${msg}`);
      }
      this._noteStatement(sql);
      if (tag === 1) {                         // mutation: no rows, a count
        this.stream.command = 'MUTATION';
        this.stream.affected = safeInt(first.u64());
        return;
      }
      if (tag === 2) { this.stream.command = 'DDL'; return; }
      if (tag === 8) {
        // ResultSets: a CALL whose body EMITted is answered with every set
        // in ONE frame, streamed or not — the frame query() receives,
        // decoded by the same code. The exchange is over (nothing to drain),
        // so this yields what query() calls the rows (the call's final
        // result) and puts every set under `resultSets`, as query() does.
        // The chain is let go first: a statement the loop body awaits runs
        // instead of queueing behind a generator that waits on that body.
        const res = readResultSets(first, rowMode);
        this.stream.command = 'CALL';
        this.stream.columns = res.columns;
        this.stream.resultSets = res.resultSets;
        release();
        for (const row of res.rows) yield row;
        return;
      }
      if (tag !== 5) throw this._desync(`unexpected response tag ${tag} to stream request`);
      streaming = true;
      const ncols = first.u32();
      const fields = [];
      for (let i = 0; i < ncols; i++) fields.push(first.text());
      this.stream.columns = fields;
      this.stream.command = 'SELECT';
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
          throw statementError(r.text());
        } else {
          throw this._desync(`unexpected frame tag ${t} in stream`);
        }
      }
    } finally {
      // Abandoned early (a `break`, a throw): the server is still sending.
      // Drain to RowsEnd, or the leftovers would be read as the reply to the
      // NEXT statement on this connection. A broken connection has nothing
      // to drain and nothing left to desync, so it is skipped.
      if (streaming && !this._closed && !this._broken) {
        try {
          let budget = DRAIN_BUDGET_BYTES;
          for (;;) {
            const f = await this._readFrame();
            const t = new Reader(f).u8();
            if (t === 7 || t === 3) break;       // RowsEnd / Error: at a boundary
            if (t !== 6) {                       // nothing else is legal here
              this._desync(`unexpected frame tag ${t} while draining stream`);
              break;
            }
            budget -= f.length;
            if (budget <= 0) {
              // The rest of this result set is big enough that reading it
              // out costs more than a reconnect does. Drop the connection
              // instead — the protocol allows either.
              this._desync('stream abandoned with too much data left to drain');
              break;
            }
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
   * `opts.consistency` sets the level for this batch only (default: the
   * client's).
   */
  batch(sql, rows, opts = {}) {
    const consistency = opts.consistency === undefined
      ? this.consistency : resolveConsistency(opts.consistency);
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
      head[0] = 7; head[1] = consistency;
      head.writeUInt32LE(p.id, 2); head.writeUInt32LE(ordered.length, 6);
      const parts = [head];
      for (const r of ordered) {
        const cnt = Buffer.alloc(2); cnt.writeUInt16LE(r.length, 0);
        parts.push(cnt);
        for (const v of r) { const b = encodeValue(v); parts.push(u32le(b.length), b); }
      }
      const res = await this._roundtrip(Buffer.concat(parts), 'object');
      this._noteStatement(sql);
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
    const r = new Reader(await this._request(Buffer.concat([head, body])));
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
    const r = new Reader(await this._request(request));
    const tag = r.u8();
    if (tag === 0) return readRows(r, rowMode);           // Rows
    if (tag === 8) return readResultSets(r, rowMode);     // ResultSets (a CALL with EMITs)
    if (tag === 1) {                            // Mutation
      const affected = r.u64();
      return { command: 'MUTATION', rowCount: safeInt(affected), rows: [], fields: [] };
    }
    if (tag === 2) return { command: 'DDL', rowCount: null, rows: [], fields: [] };
    if (tag === 3) throw statementError(r.text());
    throw new SkaidbError(`unknown response tag ${tag}`);
  }

  /**
   * Yield a stream's events as they arrive, forever.
   *
   * A dependency-free helper over the stream's log: it pages the log with
   * the keyset cursor and yields each event ({id, op, k, ts, doc}). `id` is
   * the position, an opaque STRING that sorts in log order — keep the last
   * one and pass it as `after` to resume exactly where you stopped, across
   * restarts. (It is not a number: `after: 0` would issue `WHERE id > 0`,
   * which no string id satisfies.)
   *
   * Stopping: `break` out of the loop, call `.return()` on the iterator, or
   * abort `signal`. Each ends the iteration promptly — within a tick when
   * the iterator is idle in its poll sleep, else as soon as the page fetch
   * in flight completes — and an aborted signal ends it cleanly (no error).
   *
   * This polls; for push delivery subscribe to `$stream/<db>/<name>` with
   * any MQTT client instead. The events are identical.
   *
   *   for await (const ev of client.subscribe('big_orders')) { ... }
   */
  subscribe(stream, { after = null, pollMs = 500, signal = null } = {}) {
    if (signal !== null && signal !== undefined
        && (typeof signal !== 'object' || typeof signal.aborted !== 'boolean')) {
      throw new SkaidbError('signal must be an AbortSignal');
    }
    // `state.wake` resolves the poll sleep early. An async generator that is
    // suspended in an `await` cannot see `.return()` until that await
    // settles, so the iterator's `return` is wrapped to wake the sleep
    // first; otherwise `return()` on an idle subscription waited out the
    // whole `pollMs`.
    const state = { stopped: false, wake: null };
    const gen = this._subscribe(stream, after, pollMs, signal || null, state);
    const finish = gen.return.bind(gen);
    gen.return = (value) => {
      state.stopped = true;
      if (state.wake) state.wake();
      return finish(value);
    };
    return gen;
  }

  async *_subscribe(stream, after, pollMs, signal, state) {
    const log = `_stream_${stream}`;
    const stop = () => state.stopped || (signal !== null && signal.aborted);
    let cur = after;
    while (!stop()) {
      const res = cur === null
        ? await this.query(`SELECT id, op, k, ts, doc FROM ${log} ORDER BY id LIMIT 500`)
        : await this.query(
            `SELECT id, op, k, ts, doc FROM ${log} WHERE id > $1 ORDER BY id LIMIT 500`, [cur]);
      if (stop()) return;                 // stopped while the page was in flight
      for (const row of res.rows) {
        if (stop()) return;               // aborted mid-page: the rest stays unread
        cur = row.id;
        yield row;
      }
      if (res.rows.length === 0) {
        await new Promise((resolve) => {
          const wake = () => {
            clearTimeout(timer);
            if (signal !== null) signal.removeEventListener('abort', wake);
            state.wake = null;
            resolve();
          };
          const timer = setTimeout(wake, pollMs);
          state.wake = wake;
          if (signal !== null) signal.addEventListener('abort', wake, { once: true });
        });
      }
    }
  }

  /** False once end() was called or a transport error broke the socket. */
  isUsable() {
    return !this._closed && !this._broken && this._sock !== null;
  }

  end() {
    this._closed = true;
    // Closed while checked out instead of released: the pool stops counting
    // it against its member.
    if (this._pool !== null && this._poolOut) this._pool._closedOut(this);
    return new Promise((resolve) => {
      if (!this._sock) return resolve();
      this._sock.end(resolve);
    });
  }
}

// ---- helpers --------------------------------------------------------------

/** `'host:port'`, or a bare host that takes `defaultPort`, as {host, port}. */
function parseEndpoint(s, defaultPort) {
  const t = String(s);
  const i = t.lastIndexOf(':');
  return i > 0 ? { host: t.slice(0, i), port: Number(t.slice(i + 1)) } : { host: t, port: defaultPort };
}

/** Fisher-Yates, in place; returns `arr`. */
function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

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

// AuthStart's optional trailing mechanism byte (§2.4).
const AUTH_MECHANISM = { SCRAM: 0, GSSAPI: 1, EXTERNAL: 2 };

/**
 * AuthStart (tag 10). With no mechanism the byte is omitted, which every
 * server reads as SCRAM-SHA-256.
 */
function authStartFrame(user, clientNonce, mechanism) {
  const parts = [Buffer.from([10]), encStr(user), encStr(clientNonce)];
  if (mechanism !== undefined) parts.push(Buffer.from([mechanism]));
  return Buffer.concat(parts);
}

/**
 * AuthOutcome (tag 13): returns the 32-byte server signature of an Ok, and
 * throws with the server's reason for a Denied.
 */
function parseAuthOutcome(payload) {
  const r = new Reader(payload);
  if (r.u8() !== 13) throw new SkaidbError('bad handshake outcome');
  if (r.u8() !== 1) throw new SkaidbError(`authentication denied: ${r.text()}`);
  return Buffer.from(r.take(32));
}

/** SCRAM-SHA-256 (§2.1): the client proof and the expected server signature. */
function scramProof(password, salt, iterations, authMessage) {
  const salted = crypto.pbkdf2Sync(Buffer.from(password, 'utf8'), salt, iterations, 32, 'sha256');
  const clientKey = hmac(salted, Buffer.from('Client Key'));
  const storedKey = sha256(clientKey);
  const proof = xor(clientKey, hmac(storedKey, authMessage));
  const serverSignature = hmac(hmac(salted, Buffer.from('Server Key')), authMessage);
  return { salted, proof, serverSignature };
}

function sha256(b) { return crypto.createHash('sha256').update(b).digest(); }
function xor(a, b) { const o = Buffer.allocUnsafe(a.length); for (let i = 0; i < a.length; i++) o[i] = a[i] ^ b[i]; return o; }

// ---- connection pool ------------------------------------------------------
//
// The balancing rules are shared by every skaidb driver (PROTOCOL.md §6.1):
// policies, idle order, latency averages, health ejection with re-probe,
// discovery from SHOW ENDPOINTS and read-your-writes affinity.

// Leading keywords of statements that never write: everything else counts as
// a write for read-your-writes affinity (a read taken for a write only keeps
// the caller on its member a little longer).
const READ_KEYWORDS = new Set(['select', 'show', 'explain', 'describe', 'desc', 'values', 'with', 'use']);

/** Whether `sql` counts as a write: its first keyword, past whitespace, comments and `(`, is not a read. */
function isWrite(sql) {
  const s = String(sql);
  const n = s.length;
  let i = 0;
  while (i < n) {
    if (/\s/.test(s[i]) || s[i] === '(') i++;
    else if (s.startsWith('--', i)) { const j = s.indexOf('\n', i); i = j < 0 ? n : j + 1; }
    else if (s.startsWith('/*', i)) { const j = s.indexOf('*/', i + 2); i = j < 0 ? n : j + 2; }
    else break;
  }
  let j = i;
  while (j < n && (/\p{L}/u.test(s[j]) || s[j] === '_')) j++;
  return !READ_KEYWORDS.has(s.slice(i, j).toLowerCase());
}

/**
 * Whether a failed dial says something about the member's health. A refused
 * login is the caller's problem, not the member's — except a capacity
 * refusal, which is the member being full. Neither is a statement error (a
 * USE of a missing database) or a local certificate file that cannot be read.
 */
function memberFault(e) {
  if (statementErrors.has(e)) return false;
  const msg = String(e && e.message !== undefined ? e.message : e);
  if (msg.startsWith('authentication denied')) return msg.includes('too many connections:');
  if (msg.startsWith('cannot read ')) return false;
  return !msg.startsWith('server signature mismatch');
}

// Latency EWMA: the fastest of a member's first round trips seeds it (a round
// trip is only ever lengthened — by the member, the network or this process
// stalling — so a slow first statement cannot set it); then each round trip
// moves it 5% toward it, and no sample counts for more than 10x the larger of
// the current average and the ejection floor — one slow query does not eject
// a healthy member, a member that stays slow is ejected within a few dozen
// statements.
const EWMA_SEED_SAMPLES = 3;
const EWMA_WEIGHT = 0.05;
const EWMA_CLAMP = 10;
// Round trips a latency ejection's reason reports the slowest of.
const RECENT_SAMPLES = 10;
// Round trips a member (and at least one other) needs before its average
// can eject it.
const EWMA_MIN_SAMPLES = 10;
// least_latency: a member whose last round trip is older than this scores 0,
// so a lightly loaded pool keeps re-measuring every member instead of
// settling forever on whichever answered fastest once.
const STALE_SAMPLE_MS = 2000;
// How often the background timer checks for members due a re-probe.
const MAINTAIN_TICK_MS = 250;
// Discovery refresh while a listed member is not ready: a member is often
// `unreachable` or `resyncing` only for seconds (a peer's circuit breaker
// right after it restarts), and waiting a whole refresh interval to let it
// back in leaves the load on the others.
const REFRESH_NOT_READY_MS = 5000;
// Affinity keys kept before expired ones are swept out.
const AFFINITY_SWEEP_AT = 4096;

const POLICIES = ['random', 'round_robin', 'least_latency'];
const LISTED_STATES = ['ready', 'resyncing', 'unreachable'];

/** One member as the pool sees it. */
class Member {
  constructor(ep, seed) {
    this.host = ep.host;              // what the pool dials
    this.port = ep.port;
    this.key = `${ep.host}:${ep.port}`;
    this.seed = seed;                 // configured: discovery never drops it
    this.canonical = null;            // its own SHOW ENDPOINTS name
    this.listed = 'ready';            // ready | resyncing | unreachable | unlisted
    this.ewma = null;                 // round-trip average, milliseconds
    this.firsts = [];                 // round trips before the average is seeded
    this.recent = [];                 // the last RECENT_SAMPLES round trips
    this.samples = 0;
    this.lastSample = -Infinity;
    this.errors = 0;                  // consecutive transport failures
    this.ejectedUntil = null;         // null = admitted
    this.ejections = 0;
    this.readmittedAt = -Infinity;
    this.inUse = 0;
    this.probing = false;
  }

  /** Measure the member afresh (a re-admission, back to ready). */
  resetLatency() {
    this.ewma = null;
    this.firsts = [];
    this.recent = [];
    this.samples = 0;
  }
}

/** Endpoint order: host, then port numerically. */
function byEndpoint(a, b) {
  if (a.host !== b.host) return a.host < b.host ? -1 : 1;
  return a.port - b.port;
}

/**
 * A pool of skaidb connections, balanced across the cluster's members.
 *
 * `maxsize` bounds the connections kept IDLE, not the number checked out:
 * a burst creates extras and the surplus is closed on return. Every Client
 * option passes through, so pooled connections inherit TLS, the session
 * database and the default consistency; `seeds` (or `host`/`port`) name the
 * starting members.
 *
 * Load balancing (milliseconds for every duration):
 *
 * - `loadBalancing` — `'random'` (default: each new connection dials the
 *   members in random order), `'round_robin'` (checkouts rotate over the
 *   members) or `'least_latency'` (of two members picked at random, the one
 *   with the lower latency average × checked-out connections).
 * - `idleOrder` — `'lifo'` (default: the most recently returned idle
 *   connection first) or `'fifo'` (the longest idle first, so every idle
 *   connection keeps getting used).
 * - `discover` — read the members from `SHOW ENDPOINTS` (server >= 0.327)
 *   when connections are first made to them and every `refreshInterval`
 *   after: members joining the cluster are added, members the server
 *   reports `resyncing` or `unreachable` get no new connections until they
 *   are `ready` again.
 * - Health ejection: a member with `ejectErrors` consecutive transport
 *   failures, or whose latency average is above `ejectLatencyFactor` times
 *   the other members' median and at least `ejectLatencyFloor` slower, gets
 *   no connections for `ejectBackoff` (doubling per repeat, up to
 *   `ejectBackoffMax`), then a background probe re-admits it. The last
 *   usable member is never ejected. The pool emits `'eject'`
 *   ({endpoint, reason}) and `'readmit'` ({endpoint}).
 * - `readYourWrites` — how long after a write checkouts with the same
 *   `affinity` go to the member that took it (the coordinator that
 *   acknowledged a write reads it back even at ONE). 0 (default) is off.
 *
 *     const pool = new Pool({ seeds: ['h1:7000', 'h2:7000'], database: 'app', maxsize: 8,
 *                             loadBalancing: 'least_latency', discover: true });
 *     const res = await pool.withConnection((c) => c.query('SELECT 1'));
 *     await pool.end();
 *
 * The probe/discovery timer is unref()'d, so it never keeps the process
 * alive, and stops when the pool ends.
 */
class Pool extends EventEmitter {
  constructor(opts = {}) {
    super();
    const {
      maxsize = 10,
      loadBalancing = 'random',
      idleOrder = 'lifo',
      discover = false,
      refreshInterval = 60000,
      readYourWrites = 0,
      ejectErrors = 3,
      ejectLatencyFactor = 3,
      ejectLatencyFloor = 50,
      ejectBackoff = 5000,
      ejectBackoffMax = 60000,
      ...clientOpts
    } = opts;
    if (maxsize < 1) throw new SkaidbError('maxsize must be >= 1');
    if (!POLICIES.includes(loadBalancing)) {
      throw new SkaidbError(
        `unknown loadBalancing ${loadBalancing} (use 'random', 'round_robin' or 'least_latency')`);
    }
    if (idleOrder !== 'lifo' && idleOrder !== 'fifo') {
      throw new SkaidbError(`unknown idleOrder ${idleOrder} (use 'lifo' or 'fifo')`);
    }
    if (!(ejectErrors >= 1)) throw new SkaidbError('ejectErrors must be >= 1');
    this.maxsize = maxsize;
    this._opts = clientOpts;
    this._policy = loadBalancing;
    this._fifo = idleOrder === 'fifo';
    this._discover = discover === true;
    this._refreshInterval = refreshInterval;
    this._ryw = readYourWrites;
    this._ejectErrors = ejectErrors;
    this._ejectFactor = ejectLatencyFactor;
    this._ejectFloor = ejectLatencyFloor;
    this._backoff = ejectBackoff;
    this._backoffMax = ejectBackoffMax;
    // The pool's own statements (probe, SHOW ENDPOINTS) give up after this
    // long: the driver has no read timeout, and a member that never answers
    // must not wedge a probe or a refresh for good.
    this._answerTimeout = clientOpts.connectTimeout || 10000;
    this._port = clientOpts.port || 7000;
    this._members = new Map();         // 'host:port' -> Member
    const seeds = clientOpts.seeds && clientOpts.seeds.length
      ? clientOpts.seeds : [`${clientOpts.host || 'localhost'}:${this._port}`];
    for (const s of seeds) {
      const m = new Member(parseEndpoint(s, this._port), true);
      if (!this._members.has(m.key)) this._members.set(m.key, m);
    }
    this._idle = [];
    this._rr = -1;
    this._affinity = new Map();        // affinity key -> {key, until}
    this._discoveryOff = false;
    this._nextRefresh = 0;
    this._refreshing = false;
    this._timer = null;
    this.closed = false;
  }

  // -- checkout / checkin --

  /**
   * Check out a usable connection: an idle one to the member the policy
   * picks, or a new one dialed to it (falling back to the other members).
   * `options.affinity` scopes read-your-writes — a user or session id; left
   * out, checkouts share one pool-wide key.
   */
  async acquire(options = {}) {
    const affinity = options && options.affinity !== undefined ? options.affinity : null;
    for (;;) {
      if (this.closed) throw new SkaidbError('pool is closed');
      const target = this._pick(affinity);
      const { conn: idle, stale } = this._takeIdle(target);
      for (const c of stale) c.end().catch(() => {});
      let conn = idle;
      if (conn === null) {
        conn = new Client(this._opts);
        conn._pool = this;
        await conn._connectVia(this._order(target));
        conn._poolEp = conn._connected;
        await this._discoverThrough(conn);
      }
      // A connection the server closed while it sat idle still looks fine
      // locally, so validate before handing it out; discard and try again.
      if (!conn.isUsable()) {
        await conn.end().catch(() => {});
        continue;
      }
      const m = this._members.get(conn._poolEp);
      if (m) m.inUse++;
      conn._poolAffinity = affinity;
      conn._poolOut = true;
      return conn;
    }
  }

  /**
   * Return a connection, closing it if it is broken, its member is ejected,
   * not ready or gone, or the pool is full.
   */
  async release(conn) {
    const m = this._members.get(conn._poolEp);
    if (conn._poolOut) {
      conn._poolOut = false;
      if (m && m.inUse > 0) m.inUse--;
    }
    if (!this.closed && conn.isUsable() && m && this._usable(m) && this._idle.length < this.maxsize) {
      this._idle.push(conn);
      return;
    }
    await conn.end().catch(() => {});
  }

  /**
   * Run `fn` with a checked-out connection, returning it however fn ends:
   * `withConnection(fn)` or `withConnection({ affinity }, fn)`.
   */
  async withConnection(a, b) {
    const [options, fn] = typeof a === 'function' ? [b, a] : [a, b];
    const conn = await this.acquire(options);
    try {
      return await fn(conn);
    } finally {
      await this.release(conn);
    }
  }

  /**
   * The members as the pool sees them, for monitoring: `endpoint` (what the
   * pool dials), `canonical` (the member's own name, once discovered),
   * `state` (`ready`, `ejected`, `resyncing`, `unreachable`, `unlisted`),
   * `latencyMs` (the round-trip average, null until measured), `inUse`,
   * `idle` and `seed`.
   */
  members() {
    const idle = new Map();
    for (const c of this._idle) idle.set(c._poolEp, (idle.get(c._poolEp) || 0) + 1);
    return [...this._members.values()].sort(byEndpoint).map((m) => ({
      endpoint: m.key,
      canonical: m.canonical,
      state: m.ejectedUntil !== null ? 'ejected' : m.listed,
      latencyMs: m.ewma,
      inUse: m.inUse,
      idle: idle.get(m.key) || 0,
      seed: m.seed,
    }));
  }

  /** Close the pool and every idle connection. Checked-out ones close on release. */
  async end() {
    this.closed = true;
    if (this._timer !== null) {
      clearInterval(this._timer);
      this._timer = null;
    }
    const idle = this._idle;
    this._idle = [];
    await Promise.all(idle.map((c) => c.end().catch(() => {})));
  }

  // -- choosing a member --

  _usable(m) {
    return m.ejectedUntil === null && m.listed === 'ready';
  }

  /**
   * The member this checkout should go to, or null for "any" (the random
   * policy, or no usable member at all).
   */
  _pick(affinity) {
    if (this._ryw > 0) {
      const hit = this._affinity.get(affinity);
      if (hit !== undefined) {
        const m = this._members.get(hit.key);
        if (performance.now() >= hit.until || !m) this._affinity.delete(affinity);
        else if (this._usable(m)) return m;
      }
    }
    const usable = [...this._members.values()].filter((m) => this._usable(m));
    if (usable.length === 0 || this._policy === 'random') return null;
    if (this._policy === 'round_robin') {
      usable.sort(byEndpoint);
      this._rr = (this._rr + 1) % usable.length;
      return usable[this._rr];
    }
    if (usable.length === 1) return usable[0];
    const i = Math.floor(Math.random() * usable.length);
    let j = Math.floor(Math.random() * (usable.length - 1));
    if (j >= i) j++;
    return this._score(usable[i]) <= this._score(usable[j]) ? usable[i] : usable[j];
  }

  _score(m) {
    // A member never measured, or not lately, scores 0: it is tried and
    // measured.
    if (m.ewma === null || performance.now() - m.lastSample > STALE_SAMPLE_MS) return 0;
    return m.ewma * (m.inUse + 1);
  }

  /**
   * Remove and return an idle connection (to `target`, or to any usable
   * member), plus the idle connections found to belong to members that are
   * no longer usable, for the caller to close.
   */
  _takeIdle(target) {
    const n = this._idle.length;
    const stale = [];
    let found = -1;
    for (let k = 0; k < n; k++) {
      const i = this._fifo ? k : n - 1 - k;
      const m = this._members.get(this._idle[i]._poolEp);
      if (!m || !this._usable(m)) { stale.push(i); continue; }
      if (target === null || m === target) { found = i; break; }
    }
    const conn = found >= 0 ? this._idle[found] : null;
    const drop = new Set(stale);
    if (found >= 0) drop.add(found);
    const staleConns = stale.map((i) => this._idle[i]);
    if (drop.size) this._idle = this._idle.filter((_, i) => !drop.has(i));
    return { conn, stale: staleConns };
  }

  /**
   * Endpoints to dial, best first: `first`, the other usable members in
   * random order, then the rest (resyncing/unreachable before ejected,
   * soonest re-admission first), with `exclude` (a key) moved last — a dial
   * never has nowhere to go while any member is configured.
   */
  _order(first = null, exclude = null) {
    const all = [...this._members.values()];
    const usable = shuffle(all.filter((m) => m !== first && this._usable(m)));
    const rest = all.filter((m) => m !== first && !this._usable(m)).sort((a, b) =>
      (a.ejectedUntil !== null) - (b.ejectedUntil !== null)
      || (a.ejectedUntil || 0) - (b.ejectedUntil || 0));
    const order = (first !== null ? [first] : []).concat(usable, rest);
    const i = exclude === null ? -1 : order.findIndex((m) => m.key === exclude);
    if (i >= 0 && order.length > 1) order.push(order.splice(i, 1)[0]);
    return order.map((m) => ({ host: m.host, port: m.port }));
  }

  // -- what connections report --

  /** A pooled connection re-dialing: the pool's order, the member it lost last. */
  _dialOrder(exclude) {
    return this._order(null, exclude);
  }

  _roundTrip(conn, ms) {
    const key = conn._connected;
    if (key !== conn._poolEp) {
      // The connection re-dialed to another member mid-checkout.
      if (conn._poolOut) {
        const old = this._members.get(conn._poolEp);
        if (old && old.inUse > 0) old.inUse--;
        const now = this._members.get(key);
        if (now) now.inUse++;
      }
      conn._poolEp = key;
    }
    const m = this._members.get(key);
    if (!m) return;
    m.errors = 0;
    m.recent.push(ms);
    if (m.recent.length > RECENT_SAMPLES) m.recent.shift();
    if (m.ewma === null) {
      m.firsts.push(ms);
      if (m.firsts.length >= EWMA_SEED_SAMPLES) {
        m.ewma = Math.min(...m.firsts);
        m.firsts = [];
      }
    } else {
      const cap = EWMA_CLAMP * Math.max(m.ewma, this._ejectFloor);
      m.ewma += EWMA_WEIGHT * (Math.min(ms, cap) - m.ewma);
    }
    m.samples++;
    m.lastSample = performance.now();
    // Only a member still in rotation can be ejected for latency: round trips
    // finishing on connections checked out before an ejection must not eject
    // it again (and double its backoff).
    if (this._usable(m) && m.ewma !== null && m.samples >= EWMA_MIN_SAMPLES && this._ejectFactor > 0) {
      const others = [];
      for (const o of this._members.values()) {
        if (o !== m && this._usable(o) && o.ewma !== null && o.samples >= EWMA_MIN_SAMPLES) {
          others.push(o.ewma);
        }
      }
      if (others.length) {
        others.sort((x, y) => x - y);
        const median = others[(others.length - 1) >> 1];   // the lower middle when even
        if (m.ewma > this._ejectFactor * median && m.ewma - median > this._ejectFloor) {
          this._eject(m, `latency ${m.ewma.toFixed(1)} ms vs median ${median.toFixed(1)} ms; `
            + `slowest of the last ${m.recent.length} round trips ${Math.max(...m.recent).toFixed(1)} ms`);
        }
      }
    }
  }

  _failed(key) {
    const m = this._members.get(key);
    if (!m) return;
    m.errors++;
    if (m.errors >= this._ejectErrors && m.ejectedUntil === null) {
      this._eject(m, `${m.errors} consecutive connection failures`);
    }
  }

  _closedOut(conn) {
    if (!conn._poolOut) return;
    conn._poolOut = false;
    const m = this._members.get(conn._poolEp);
    if (m && m.inUse > 0) m.inUse--;
  }

  _wrote(conn) {
    if (!(this._ryw > 0) || !conn._poolOut) return;
    const now = performance.now();
    if (this._affinity.size >= AFFINITY_SWEEP_AT) {
      for (const [k, v] of this._affinity) if (v.until <= now) this._affinity.delete(k);
    }
    this._affinity.set(conn._poolAffinity, { key: conn._connected, until: now + this._ryw });
  }

  // -- ejection and re-admission --

  /**
   * Take `m` out of rotation and close its idle connections — unless it is
   * the last usable member: a slow or flaky member beats none.
   */
  _eject(m, reason) {
    let other = false;
    for (const o of this._members.values()) if (o !== m && this._usable(o)) { other = true; break; }
    if (!other) return;
    const now = performance.now();
    if (now - m.readmittedAt > this._backoffMax) m.ejections = 0;   // healthy for a while: start over
    m.ejections++;
    m.ejectedUntil = now + this._backoffFor(m);
    const idle = this._idle.filter((c) => c._poolEp === m.key);
    if (idle.length) {
      this._idle = this._idle.filter((c) => c._poolEp !== m.key);
      for (const c of idle) c.end().catch(() => {});
    }
    // Listeners run on the next tick: one that throws must not fail the
    // statement that happened to report the ejection.
    process.nextTick(() => this.emit('eject', { endpoint: m.key, reason }));
    this._ensureMaintainer();
  }

  _backoffFor(m) {
    return Math.min(this._backoff * 2 ** Math.max(m.ejections - 1, 0), this._backoffMax);
  }

  /**
   * Start the background timer: it re-probes ejected members and refreshes
   * discovery. unref()'d, so it never holds the process open; end() stops it.
   */
  _ensureMaintainer() {
    if (this._timer === null && !this.closed) {
      this._timer = setInterval(() => this._maintain(), MAINTAIN_TICK_MS);
      this._timer.unref();
    }
  }

  _maintain() {
    if (this.closed) return;
    const now = performance.now();
    for (const m of this._members.values()) {
      if (m.ejectedUntil !== null && now >= m.ejectedUntil && !m.probing) {
        m.probing = true;
        this._probe(m).catch(() => { m.probing = false; });
      }
    }
    if (this._discover && !this._discoveryOff && !this._refreshing && now >= this._nextRefresh) {
      this._refreshing = true;
      this._refresh().catch(() => {}).then(() => { this._refreshing = false; });
    }
  }

  /**
   * Dial `m` alone and run one statement: success re-admits it (the probe
   * connection joins the idle set), failure ejects it again for twice as long.
   */
  async _probe(m) {
    let conn = null;
    let ok = false;
    try {
      conn = new Client(this._opts);
      await conn._connectVia([{ host: m.host, port: m.port }]);
      await this._bounded(conn, conn.query('SHOW DATABASES'));
      ok = conn.isUsable();
    } catch (_) {
      ok = false;
    }
    m.probing = false;
    if (this.closed) ok = false;
    if (ok) {
      m.ejectedUntil = null;
      m.errors = 0;
      m.resetLatency();
      m.readmittedAt = performance.now();
      process.nextTick(() => this.emit('readmit', { endpoint: m.key }));
      if (this._members.get(m.key) === m && this._idle.length < this.maxsize) {
        conn._pool = this;
        conn._poolEp = m.key;
        this._idle.push(conn);
        conn = null;
      }
    } else {
      m.ejections++;
      m.ejectedUntil = performance.now() + this._backoffFor(m);
    }
    if (conn !== null) await conn.end().catch(() => {});
  }

  /**
   * `promise`, a statement the pool itself runs on `conn`, failing `conn`
   * (as a transport timeout) if no answer comes within the connect timeout.
   */
  async _bounded(conn, promise) {
    const timer = setTimeout(
      () => conn._fail(new SkaidbError('no answer within the connect timeout')), this._answerTimeout);
    timer.unref();
    try {
      return await promise;
    } finally {
      clearTimeout(timer);
    }
  }

  // -- discovery --

  /**
   * Run discovery on a freshly dialed connection when the member it reached
   * has not named itself yet (once per member, so seeds given as host names
   * are matched to the endpoints the server reports).
   */
  async _discoverThrough(conn) {
    if (!this._discover || this._discoveryOff) return;
    const m = this._members.get(conn._poolEp);
    if (!m || m.canonical !== null) return;
    this._ensureMaintainer();
    await this._listThrough(conn);
  }

  async _refresh() {
    let conn;
    try {
      conn = await this.acquire();
    } catch (_) {
      this._nextRefresh = performance.now() + this._refreshInterval;
      return;
    }
    try {
      await this._listThrough(conn);
    } finally {
      await this.release(conn);
    }
  }

  async _listThrough(conn) {
    let res;
    try {
      res = await this._bounded(conn, conn.query({ text: 'SHOW ENDPOINTS', rowMode: 'array' }));
    } catch (e) {
      // A server that predates the statement answers an Error: keep the seeds.
      if (statementErrors.has(e)) this._discoveryOff = true;
      return;
    }
    if (res.rows.length === 0) {
      // A standalone server: no ring to learn, so keep the seeds and stop asking.
      this._discoveryOff = true;
      return;
    }
    this._applyListing(conn._poolEp, res.rows);
    let interval = this._refreshInterval;
    for (const m of this._members.values()) {
      if (m.listed === 'resyncing' || m.listed === 'unreachable') {
        interval = Math.min(interval, REFRESH_NOT_READY_MS);
        break;
      }
    }
    this._nextRefresh = performance.now() + interval;
  }

  /** Fold a non-empty `SHOW ENDPOINTS` answer ([endpoint, state, local] rows) in. */
  _applyListing(dialed, rows) {
    const listed = new Map();
    for (const [endpoint, state, local] of rows) {
      listed.set(endpoint, state);
      if (local) {
        const me = this._members.get(dialed);
        if (me) me.canonical = endpoint;
      }
    }
    // A member reported under the name of one already known (a seed given
    // as a host name) is that member, not a new one.
    const byName = new Map();
    for (const m of this._members.values()) {
      if (m.canonical !== null && (!byName.has(m.canonical) || m.seed)) byName.set(m.canonical, m);
    }
    for (const [key, m] of [...this._members]) {
      if (m.canonical !== null && byName.get(m.canonical) !== m) this._members.delete(key);  // a seed's duplicate
    }
    const seen = new Set();
    for (const [endpoint, reported] of listed) {
      let m = byName.get(endpoint);
      if (!m) {
        const ep = parseEndpoint(endpoint, this._port);
        m = this._members.get(`${ep.host}:${ep.port}`);
        if (!m) {
          m = new Member(ep, false);
          this._members.set(m.key, m);
        }
        m.canonical = endpoint;
      }
      const state = LISTED_STATES.includes(reported) ? reported : 'ready';
      if (state === 'ready' && m.listed !== 'ready') {
        // Back in rotation: measured afresh, like a re-admission.
        m.resetLatency();
        m.errors = 0;
      }
      m.listed = state;
      seen.add(m);
    }
    for (const [key, m] of [...this._members]) {
      if (seen.has(m) || (m.seed && m.canonical === null)) continue;   // listed, or a seed not identified yet
      if (m.seed) m.listed = 'unlisted';   // configured, but no longer a member
      else this._members.delete(key);
    }
  }
}

module.exports = { Client, Pool, SkaidbError, CONSISTENCY };

// The pure pieces (value codec, parameter binding, framing helpers), exposed
// for the driver's own unit tests. Not part of the supported API: anything
// under `_internal` may change between minor versions without notice.
module.exports._internal = {
  Reader, decodeValue, encodeValue, bindParams, toQmark, quote,
  decimalToString, formatUuid, encStr, u32le, resolveConsistency,
  DRAIN_BUDGET_BYTES, AUTH_MECHANISM, authStartFrame, parseAuthOutcome, scramProof,
  parseEndpoint, isWrite, memberFault,
};
