'use strict';
// The shared skaidb wire-protocol conformance suite.
//
// conformance/vectors.json is generated from the server's reference encoders
// (https://skaidb.org/conformance/vectors.json; contract in
// conformance/README.md). This harness runs it against the driver's PUBLIC
// API through a scripted fake server that sends the reference bytes — never
// bytes this driver encoded — verifies the client proof with its own SCRAM
// code, and checks every request byte for byte.
//
// JavaScript mapping of the tagged values (the driver's documented forms):
// Int -> number (bigint beyond 2^53), Float -> number (bits compared),
// Decimal -> string, Uuid -> string, Bytes -> Buffer, Timestamp -> Date,
// Array -> Array, Document -> plain object in wire order.

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('net');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { Client, SkaidbError, _internal: I } = require('../skaidb');

const V = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'conformance', 'vectors.json'), 'utf8'));

// call.methods this driver has no public API for (printed, and listed in the
// README). Every method of format_version 1 is covered.
const SKIPPED_METHODS = [];

// ---- tagged JSON <-> JavaScript -------------------------------------------

const floatBits = (n) => { const b = Buffer.alloc(8); b.writeDoubleBE(n, 0); return b.toString('hex'); };

// A Decimal as the string the driver surfaces, formatted independently.
function decimalString({ mantissa, scale }) {
  const neg = mantissa.startsWith('-');
  let digits = neg ? mantissa.slice(1) : mantissa;
  if (scale > 0) {
    digits = digits.padStart(scale + 1, '0');
    digits = `${digits.slice(0, digits.length - scale)}.${digits.slice(digits.length - scale)}`;
  }
  return neg ? `-${digits}` : digits;
}

/** Why `got` (a value the driver surfaced) is not `t`, or null when it is. */
function mismatch(t, got, where = 'value') {
  const bad = (what) => `${where}: expected ${what}, got ${describe(got)}`;
  if ('null' in t) return got === null ? null : bad('null');
  if ('bool' in t) return got === t.bool ? null : bad(`bool ${t.bool}`);
  if ('int' in t) {
    const want = BigInt(t.int);
    const safe = want >= BigInt(Number.MIN_SAFE_INTEGER) && want <= BigInt(Number.MAX_SAFE_INTEGER);
    // Documented form: a number, and a bigint only where a number would lose precision.
    if (safe) return typeof got === 'number' && BigInt(got) === want ? null : bad(`number ${t.int}`);
    return typeof got === 'bigint' && got === want ? null : bad(`bigint ${t.int}n`);
  }
  if ('float_bits' in t) {
    return typeof got === 'number' && floatBits(got) === t.float_bits ? null : bad(`float bits ${t.float_bits}`);
  }
  if ('decimal' in t) {
    const want = decimalString(t.decimal);
    return got === want ? null : bad(`decimal string ${JSON.stringify(want)}`);
  }
  if ('string' in t) return got === t.string ? null : bad(`string ${JSON.stringify(t.string)}`);
  if ('bytes' in t) {
    return Buffer.isBuffer(got) && got.toString('hex') === t.bytes ? null : bad(`Buffer ${t.bytes}`);
  }
  if ('uuid' in t) return got === t.uuid ? null : bad(`uuid string ${t.uuid}`);
  if ('timestamp_ms' in t) {
    return got instanceof Date && got.getTime() === Number(t.timestamp_ms) ? null : bad(`Date(${t.timestamp_ms})`);
  }
  if ('array' in t) {
    if (!Array.isArray(got) || got.length !== t.array.length) return bad(`array of ${t.array.length}`);
    for (let i = 0; i < got.length; i++) {
      const m = mismatch(t.array[i], got[i], `${where}[${i}]`);
      if (m) return m;
    }
    return null;
  }
  if ('document' in t) {
    if (got === null || typeof got !== 'object' || Array.isArray(got) || Buffer.isBuffer(got) || got instanceof Date) {
      return bad('a document object');
    }
    const keys = Object.keys(got);
    const want = t.document.map((e) => e.key);
    if (JSON.stringify(keys) !== JSON.stringify(want)) return bad(`keys ${JSON.stringify(want)} in wire order`);
    for (const e of t.document) {
      const m = mismatch(e.value, got[e.key], `${where}.${e.key}`);
      if (m) return m;
    }
    return null;
  }
  throw new Error(`unknown tagged value ${JSON.stringify(t)}`);
}

function describe(v) {
  if (typeof v === 'bigint') return `${v}n`;
  if (Buffer.isBuffer(v)) return `Buffer ${v.toString('hex')}`;
  if (v instanceof Date) return `Date(${v.getTime()})`;
  try { return JSON.stringify(v, (_, x) => (typeof x === 'bigint' ? `${x}n` : x)); } catch { return String(v); }
}

/**
 * Why a tagged value cannot be bound through this driver with the exact
 * encoding, or null when it can. A JS number that is an integer always binds
 * as Int (-0 excepted), so an integral Float (0.0, -2.25e10) has no parameter form;
 * Decimal and Uuid surface as strings, which bind as String.
 */
function encodeGap(t) {
  if ('decimal' in t) return 'a Decimal surfaces as a string, which binds as String';
  if ('uuid' in t) return 'a Uuid surfaces as a string, which binds as String';
  if ('float_bits' in t) {
    const n = Buffer.from(t.float_bits, 'hex').readDoubleBE(0);
    if (Number.isInteger(n) && !Object.is(n, -0)) return 'an integral JS number binds as Int, not Float';
  }
  if ('array' in t) for (const x of t.array) { const g = encodeGap(x); if (g) return g; }
  if ('document' in t) for (const e of t.document) { const g = encodeGap(e.value); if (g) return g; }
  return null;
}

/** A tagged value as the JS value the driver binds. */
function native(t) {
  if ('null' in t) return null;
  if ('bool' in t) return t.bool;
  if ('int' in t) {
    const b = BigInt(t.int);
    return b >= BigInt(Number.MIN_SAFE_INTEGER) && b <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(b) : b;
  }
  if ('float_bits' in t) return Buffer.from(t.float_bits, 'hex').readDoubleBE(0);
  if ('string' in t) return t.string;
  if ('bytes' in t) return Buffer.from(t.bytes, 'hex');
  if ('timestamp_ms' in t) return new Date(Number(t.timestamp_ms));
  if ('array' in t) return t.array.map(native);
  if ('document' in t) {
    const o = {};
    for (const e of t.document) o[e.key] = native(e.value);
    return o;
  }
  throw new Error(`no JS parameter form for ${JSON.stringify(t)}`);
}

// ---- the scripted fake server ----------------------------------------------

const u32le = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n, 0); return b; };
const frame = (p) => { const h = Buffer.alloc(4); h.writeUInt32BE(p.length, 0); return Buffer.concat([h, p]); };
const hmac = (k, m) => crypto.createHmac('sha256', k).update(m).digest();
const sha256 = (b) => crypto.createHash('sha256').update(b).digest();

/**
 * Accepts ONE connection: the handshake per `outcome`, then `exchanges`
 * ([{request, responses}] as Buffers). Any deviation is recorded in
 * `error`; `done` resolves when the connection ends.
 */
class FakeServer {
  constructor({ outcome = 'ok', exchanges = [] } = {}) {
    this.outcome = outcome;
    this.pending = exchanges.slice();
    this.error = null;
    this._buf = Buffer.alloc(0);
    this._want = null;
  }

  start() {
    return new Promise((resolve) => {
      this.done = new Promise((finish) => {
        this.server = net.createServer((sock) => {
          this.server.close();              // one connection per server
          this.sock = sock;
          sock.on('data', (d) => { this._buf = Buffer.concat([this._buf, d]); this._pump(); });
          sock.on('error', () => {});
          sock.on('close', () => { this._closed = true; this._pump(); });
          this._serve().catch((e) => { this.error = this.error || e.message; })
            .finally(() => { sock.end(); finish(); });
        });
        this.server.listen(0, '127.0.0.1', () => { this.port = this.server.address().port; resolve(this); });
      });
    });
  }

  _pump() {
    if (!this._want) return;
    if (this._buf.length >= 4) {
      const n = this._buf.readUInt32BE(0);
      if (this._buf.length >= 4 + n) {
        const p = Buffer.from(this._buf.subarray(4, 4 + n));
        this._buf = this._buf.subarray(4 + n);
        const w = this._want; this._want = null; w(p);
        return;
      }
    }
    if (this._closed) { const w = this._want; this._want = null; w(null); }
  }

  readFrame() {
    return new Promise((resolve) => { this._want = resolve; this._pump(); });
  }

  send(p) { this.sock.write(frame(p)); }

  async _serve() {
    const auth = V.auth;
    const start = await this.readFrame();
    if (!start || start[0] !== 10) throw new Error('expected AuthStart');
    let pos = 1;
    const ulen = start.readUInt32LE(pos); const user = start.subarray(pos + 4, pos + 4 + ulen); pos += 4 + ulen;
    const nlen = start.readUInt32LE(pos); const clientNonce = start.subarray(pos + 4, pos + 4 + nlen); pos += 4 + nlen;
    if (pos !== start.length && !(pos + 1 === start.length && start[pos] === 0)) {
      throw new Error(`AuthStart asks for a mechanism other than SCRAM: ${start.toString('hex')}`);
    }
    if (user.toString('utf8') !== auth.username) throw new Error(`AuthStart user ${user}`);
    const salt = Buffer.from(auth.challenge.salt, 'hex');
    const iterations = auth.challenge.iterations;
    const serverNonce = Buffer.concat([clientNonce, Buffer.from(auth.challenge.server_nonce_suffix, 'utf8')]);
    this.send(Buffer.concat([Buffer.from([11]), u32le(salt.length), salt, u32le(iterations),
      u32le(serverNonce.length), serverNonce]));

    const finish = await this.readFrame();
    if (!finish || finish[0] !== 12) throw new Error('expected AuthFinish');
    const am = Buffer.concat([user, Buffer.from([0]), clientNonce, Buffer.from([0]), serverNonce,
      Buffer.from(`\0${salt.toString('hex')}\0${iterations}`, 'utf8')]);
    const salted = crypto.pbkdf2Sync(Buffer.from(auth.password, 'utf8'), salt, iterations, 32, 'sha256');
    const clientKey = hmac(salted, 'Client Key');
    const clientSig = hmac(sha256(clientKey), am);
    const proof = Buffer.alloc(32);
    for (let i = 0; i < 32; i++) proof[i] = clientKey[i] ^ clientSig[i];
    if (!finish.subarray(1, 33).equals(proof) || finish.length !== 33) throw new Error('client proof did not verify');
    const serverSig = hmac(hmac(salted, 'Server Key'), am);
    if (this.outcome === 'bad_server_signature') { this.send(Buffer.concat([Buffer.from([13, 1]), Buffer.alloc(32, 0xaa)])); return; }
    if (this.outcome === 'denied') {
      this.send(Buffer.from(auth.outcomes.find((o) => o.name === 'denied').payload, 'hex'));
      return;
    }
    this.send(Buffer.concat([Buffer.from([13, 1]), serverSig]));

    const ddl = Buffer.from(V.ignorable_requests.ddl_payload, 'hex');
    const ignorable = Object.keys(V.ignorable_requests.opcodes).map(Number);
    for (;;) {
      const req = await this.readFrame();
      if (req === null) {
        if (this.pending.length) throw new Error(`never received request ${this.pending[0].request.toString('hex')}`);
        return;
      }
      if (ignorable.includes(req[0])) { this.send(ddl); continue; }
      const next = this.pending.shift();
      if (!next) throw new Error(`unexpected extra request ${req.toString('hex')}`);
      if (!req.equals(next.request)) {
        throw new Error(`request mismatch:\n  got  ${req.toString('hex')}\n  want ${next.request.toString('hex')}`);
      }
      for (const r of next.responses) this.send(r);
    }
  }
}

function newClient(server) {
  return new Client({ host: '127.0.0.1', port: server.port, user: V.auth.username,
                      password: V.auth.password, connectTimeout: 5000 });
}

// ---- a case's call through the public API ----------------------------------

// `?` placeholders as this driver's documented `$1, $2, …`; the driver
// rewrites them back to `?` on the wire, which the request bytes check.
function dollarPlaceholders(sql) {
  let i = 0; let inStr = false; let out = '';
  for (const ch of sql) {
    if (ch === "'") inStr = !inStr;
    out += !inStr && ch === '?' ? `$${++i}` : ch;
  }
  return out;
}

async function outcome(fn) {
  try { return await fn(); } catch (e) {
    if (!(e instanceof SkaidbError)) throw e;
    return { error: e.message };
  }
}

function resultOf(res) {
  if (res.command === 'DDL') return { ddl: true };
  if (res.command === 'MUTATION') return { affected: String(res.rowCount) };
  const sets = res.resultSets || [res];
  const rs = sets.map((s) => ({ columns: s.columns, rows: s.rows }));
  return rs.length > 1 ? { result_sets: rs } : { rows: rs[0] };
}

async function runCall(client, call) {
  switch (call.method) {
    case 'sequence': {
      const out = [];
      for (const c of call.calls) out.push(await runCall(client, c));
      return { sequence: out };
    }
    case 'query':
      return outcome(async () => resultOf(await client.query(
        { text: call.sql, consistency: call.consistency, rowMode: 'array' })));
    case 'query_stream':
      return outcome(async () => {
        const opts = { rowMode: 'array' };
        if (call.consistency !== undefined) opts.consistency = call.consistency;
        const rows = [];
        try {
          for await (const row of client.stream(call.sql, opts)) rows.push(row);
        } catch (e) {
          if (!(e instanceof SkaidbError) || client.stream.columns === null) throw e;
          return { rows_then_error: { rows: { columns: client.stream.columns, rows }, error: e.message } };
        }
        if (client.stream.command === 'MUTATION') return { affected: String(client.stream.affected) };
        if (client.stream.command === 'DDL') return { ddl: true };
        return { rows: { columns: client.stream.columns, rows } };
      });
    case 'execute_prepared':
      return outcome(async () => resultOf(await client.query({
        text: dollarPlaceholders(call.sql), values: call.params.map(native),
        consistency: call.consistency, rowMode: 'array' })));
    case 'execute_batch':
      return outcome(async () => {
        const n = await client.batch(dollarPlaceholders(call.sql), call.rows.map((r) => r.map(native)),
          { consistency: call.consistency });
        return { affected: String(n) };
      });
    default:
      throw new Error(`unknown call.method ${call.method}`);
  }
}

/** Why `got` does not satisfy `expect`, or null. */
function unmet(expect, got, where = 'result') {
  const kinds = ['rows', 'affected', 'ddl', 'error', 'result_sets', 'rows_then_error', 'sequence'];
  const kind = kinds.find((k) => k in expect);
  if (!(kind in got)) return `${where}: expected ${kind}, got ${describe(got)}`;
  const e = expect[kind]; const g = got[kind];
  switch (kind) {
    case 'error': return g.includes(e) ? null : `${where}: error ${JSON.stringify(g)} lacks ${JSON.stringify(e)}`;
    case 'affected': return g === e ? null : `${where}: affected ${g}, want ${e}`;
    case 'ddl': return null;
    case 'rows': return rowsUnmet(e, g, where);
    case 'result_sets':
      if (g.length !== e.length) return `${where}: ${g.length} result sets, want ${e.length}`;
      for (let i = 0; i < e.length; i++) { const m = rowsUnmet(e[i], g[i], `${where}.set[${i}]`); if (m) return m; }
      return null;
    case 'rows_then_error':
      return rowsUnmet(e.rows, g.rows, where)
        || (g.error.includes(e.error) ? null : `${where}: error ${JSON.stringify(g.error)} lacks ${JSON.stringify(e.error)}`);
    case 'sequence':
      if (g.length !== e.length) return `${where}: ${g.length} results, want ${e.length}`;
      for (let i = 0; i < e.length; i++) { const m = unmet(e[i], g[i], `${where}[${i}]`); if (m) return m; }
      return null;
    default: return null;
  }
}

function rowsUnmet(e, g, where) {
  if (JSON.stringify(g.columns) !== JSON.stringify(e.columns)) {
    return `${where}: columns ${JSON.stringify(g.columns)}, want ${JSON.stringify(e.columns)}`;
  }
  if (g.rows.length !== e.rows.length) return `${where}: ${g.rows.length} rows, want ${e.rows.length}`;
  for (let r = 0; r < e.rows.length; r++) {
    if (g.rows[r].length !== e.rows[r].length) return `${where}: row ${r} has ${g.rows[r].length} cells`;
    for (let c = 0; c < e.rows[r].length; c++) {
      const m = mismatch(e.rows[r][c], g.rows[r][c], `${where}.rows[${r}].${e.columns[c]}`);
      if (m) return m;
    }
  }
  return null;
}

// ---- the tests --------------------------------------------------------------

test('conformance: format version is one this harness knows', () => {
  assert.equal(V.format_version, 1);
});

test('conformance: every value vector decodes, and encodes where JS can bind it', () => {
  const skipped = [];
  for (const v of V.values) {
    const r = new I.Reader(Buffer.from(v.encoded, 'hex'));
    const got = I.decodeValue(r);
    assert.equal(r.pos, r.buf.length, `${v.name}: decode left bytes unread`);
    const m = mismatch(v.value, got, v.name);
    assert.equal(m, null, m);
    const gap = encodeGap(v.value);
    if (gap) { skipped.push(`${v.name} (${gap})`); continue; }
    assert.equal(I.encodeValue(native(v.value)).toString('hex'), v.encoded, `${v.name}: encoding`);
  }
  console.log(`conformance: encode skipped for ${skipped.length} value(s): ${skipped.join('; ')}`);
});

test('conformance: SCRAM computations', () => {
  for (const s of V.scram) {
    const salt = Buffer.from(s.salt, 'hex');
    const am = Buffer.from([s.username, s.client_nonce, s.server_nonce, salt.toString('hex'),
      String(s.iterations)].join('\0'), 'utf8');
    assert.equal(am.toString('hex'), s.auth_message, `${s.username}: auth message`);
    const { salted, proof, serverSignature } = I.scramProof(s.password, salt, s.iterations, am);
    assert.equal(salted.toString('hex'), s.salted_password, `${s.username}: salted password`);
    assert.equal(proof.toString('hex'), s.client_proof, `${s.username}: client proof`);
    assert.equal(serverSignature.toString('hex'), s.server_signature, `${s.username}: server signature`);
  }
});

for (const o of V.auth.outcomes) {
  test(`conformance: auth outcome ${o.name}`, async () => {
    const server = await new FakeServer({ outcome: o.name }).start();
    const client = newClient(server);
    if (o.expect === 'connected') {
      await client.connect();
      await client.end();
    } else {
      await assert.rejects(client.connect(), (e) => {
        assert.ok(e instanceof SkaidbError, `not a SkaidbError: ${e}`);
        if (o.reason) assert.match(e.message, new RegExp(o.reason));
        return true;
      });
    }
    await server.done;
    assert.equal(server.error, null, server.error);
  });
}

test('conformance: call methods skipped by this driver', () => {
  console.log(`conformance: skipped call methods: ${SKIPPED_METHODS.length ? SKIPPED_METHODS.join(', ') : 'none'}`);
});

for (const c of V.cases) {
  const methods = c.call.method === 'sequence' ? c.call.calls.map((x) => x.method) : [c.call.method];
  const skip = methods.find((m) => SKIPPED_METHODS.includes(m));
  test(`conformance case ${c.name}`, { skip: skip ? `no API for ${skip}` : false }, async () => {
    const exchanges = c.exchanges.map((e) => ({
      request: Buffer.from(e.request, 'hex'), responses: e.responses.map((r) => Buffer.from(r, 'hex')) }));
    const server = await new FakeServer({ exchanges }).start();
    const client = newClient(server);
    let got;
    try {
      await client.connect();
      got = await runCall(client, c.call);
    } finally {
      await client.end();
    }
    await server.done;
    assert.equal(server.error, null, `${c.name}: ${server.error}`);
    const m = unmet(c.expect, got);
    assert.equal(m, null, `${c.name}: ${m}`);
  });
}
