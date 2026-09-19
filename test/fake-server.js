'use strict';
// An in-process skaidb server for the driver's unit tests: speaks the frame
// layer and the SCRAM-SHA-256 handshake for real, and answers requests with
// whatever the test scripts. No skaidb binary is involved.

const net = require('net');
const crypto = require('crypto');
const { _internal: I } = require('../skaidb');

// ---- frame builders (server -> client payloads, §3.2) ---------------------

function str(s) { return I.encStr(String(s)); }

function rowsBlock(columns, rows) {
  const parts = [I.u32le(columns.length)];
  for (const c of columns) parts.push(str(c));
  parts.push(I.u32le(rows.length));
  for (const row of rows) {
    parts.push(I.u32le(row.length));
    for (const v of row) { const b = I.encodeValue(v); parts.push(I.u32le(b.length), b); }
  }
  return Buffer.concat(parts);
}

const F = {
  rows: (columns, rows) => Buffer.concat([Buffer.from([0]), rowsBlock(columns, rows)]),
  mutation: (n) => { const b = Buffer.alloc(9); b[0] = 1; b.writeBigUInt64LE(BigInt(n), 1); return b; },
  ddl: () => Buffer.from([2]),
  error: (msg) => Buffer.concat([Buffer.from([3]), str(msg)]),
  prepared: (id, nparams) => { const b = Buffer.alloc(7); b[0] = 4; b.writeUInt32LE(id, 1); b.writeUInt16LE(nparams, 5); return b; },
  header: (columns) => {
    const parts = [Buffer.from([5]), I.u32le(columns.length)];
    for (const c of columns) parts.push(str(c));
    return Buffer.concat(parts);
  },
  chunk: (rows) => {
    const parts = [Buffer.from([6]), I.u32le(rows.length)];
    for (const row of rows) {
      parts.push(I.u32le(row.length));
      for (const v of row) { const b = I.encodeValue(v); parts.push(I.u32le(b.length), b); }
    }
    return Buffer.concat(parts);
  },
  end: () => Buffer.from([7]),
  resultSets: (sets) => Buffer.concat([Buffer.from([8]), I.u32le(sets.length),
    ...sets.map(([columns, rows]) => rowsBlock(columns, rows))]),
};

function frame(payload) {
  const head = Buffer.alloc(4);
  head.writeUInt32BE(payload.length, 0);
  return Buffer.concat([head, payload]);
}

// ---- request parsers (client -> server payloads, §3) ----------------------

function parseRequest(payload) {
  const r = new I.Reader(payload);
  const op = r.u8();
  switch (op) {
    case 1: case 5: { const consistency = r.u8(); return { op, consistency, sql: r.text() }; }
    case 2: return { op, sql: r.text() };
    case 3: {
      const consistency = r.u8(); const id = r.u32(); const n = r.u16();
      const params = [];
      for (let i = 0; i < n; i++) params.push(I.decodeValue(new I.Reader(r.blob())));
      return { op, consistency, id, params };
    }
    case 4: return { op, id: r.u32() };
    case 7: {
      const consistency = r.u8(); const id = r.u32(); const nrows = r.u32();
      const rows = [];
      for (let i = 0; i < nrows; i++) {
        const n = r.u16(); const row = [];
        for (let j = 0; j < n; j++) row.push(I.decodeValue(new I.Reader(r.blob())));
        rows.push(row);
      }
      return { op, consistency, id, rows };
    }
    case 8: return { op, name: r.text(), version: r.text() };
    default: return { op, raw: payload };
  }
}

// ---- the server -----------------------------------------------------------

class FakeServer {
  /**
   * opts.password  — the one password every user has (default 'secret')
   * opts.handle    — (req, conn) => payload | payload[] | undefined
   *                  called for every post-handshake request; Hello is
   *                  answered with Ddl unless the handler returns something.
   */
  constructor(opts = {}) {
    this.password = opts.password === undefined ? 'secret' : opts.password;
    this.handle = opts.handle || (() => F.ddl());
    this.iterations = 1000;
    this.requests = [];        // every parsed post-handshake request, in order
    this.hellos = [];
    this.connections = 0;      // TCP connections accepted so far
    this.sockets = new Set();
    this.server = null;
    this.port = 0;
  }

  start() {
    return new Promise((resolve) => {
      this.server = net.createServer((sock) => this._onConnection(sock));
      this.server.listen(0, '127.0.0.1', () => { this.port = this.server.address().port; resolve(this); });
    });
  }

  /** Kill every live connection, as a node restart would. */
  closeAll() { for (const s of this.sockets) s.destroy(); this.sockets.clear(); }

  stop() {
    this.closeAll();
    return new Promise((resolve) => this.server.close(() => resolve()));
  }

  _onConnection(sock) {
    this.connections++;
    this.sockets.add(sock);
    sock.on('close', () => this.sockets.delete(sock));
    sock.on('error', () => {});
    const conn = { sock, state: 'start', salted: null, authMessage: null, send: (p) => sock.write(frame(p)) };
    let buf = Buffer.alloc(0);
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        if (buf.length < 4) return;
        const len = buf.readUInt32BE(0);
        if (buf.length < 4 + len) return;
        const payload = Buffer.from(buf.subarray(4, 4 + len));
        buf = buf.subarray(4 + len);
        try { this._onFrame(conn, payload); } catch (e) { conn.send(F.error(`fake server: ${e.message}`)); }
      }
    });
  }

  _onFrame(conn, payload) {
    const r = new I.Reader(payload);
    if (conn.state === 'start') {
      if (r.u8() !== 10) throw new Error('expected AuthStart');
      const user = r.text(); const clientNonce = r.text();
      const salt = crypto.randomBytes(16);
      const serverNonce = `s${Math.random().toString(36).slice(2)}`;
      conn.user = user;
      conn.authMessage = Buffer.from(
        [user, clientNonce, serverNonce, salt.toString('hex'), String(this.iterations)].join('\0'), 'utf8');
      conn.salted = crypto.pbkdf2Sync(this.password, salt, this.iterations, 32, 'sha256');
      conn.send(Buffer.concat([Buffer.from([11]), I.u32le(salt.length), salt, I.u32le(this.iterations), str(serverNonce)]));
      conn.state = 'finish';
      return;
    }
    if (conn.state === 'finish') {
      if (r.u8() !== 12) throw new Error('expected AuthFinish');
      const proof = r.take(32);
      const clientKey = hmac(conn.salted, Buffer.from('Client Key'));
      const storedKey = sha256(clientKey);
      const clientSig = hmac(storedKey, conn.authMessage);
      const recovered = Buffer.alloc(32);
      for (let i = 0; i < 32; i++) recovered[i] = proof[i] ^ clientSig[i];
      if (!crypto.timingSafeEqual(sha256(recovered), storedKey)) {
        conn.send(Buffer.concat([Buffer.from([13, 0]), str('bad password')]));
        conn.sock.end();
        return;
      }
      const serverKey = hmac(conn.salted, Buffer.from('Server Key'));
      let serverSig = hmac(serverKey, conn.authMessage);
      if (this.corruptServerSignature) serverSig = Buffer.alloc(32, 7);
      conn.send(Buffer.concat([Buffer.from([13, 1]), serverSig]));
      conn.state = 'ready';
      return;
    }
    const req = parseRequest(payload);
    this.requests.push(req);
    if (req.op === 8) this.hellos.push(req);
    let out = this.handle(req, conn);
    if (out === undefined) out = req.op === 8 ? F.ddl() : F.error(`fake server: unhandled op ${req.op}`);
    for (const p of Array.isArray(out) ? out : [out]) conn.send(p);
  }
}

function hmac(key, msg) { return crypto.createHmac('sha256', key).update(msg).digest(); }
function sha256(b) { return crypto.createHash('sha256').update(b).digest(); }

module.exports = { FakeServer, F, frame, parseRequest };
