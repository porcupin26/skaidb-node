'use strict';
// Certificate login (PROTOCOL.md §2.4, mechanism 2 = EXTERNAL) and client
// certificates over TLS. The frame builder and outcome parser are checked
// against bytes spelled out here; the end-to-end tests run a real TLS fake
// server that demands a client certificate, with a throwaway CA made by the
// `openssl` CLI (skipped when it is not installed).

const test = require('node:test');
const assert = require('node:assert/strict');
const tls = require('tls');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');
const { Client, Pool, SkaidbError, _internal: I } = require('../skaidb');

const u32le = (n) => { const b = Buffer.alloc(4); b.writeUInt32LE(n, 0); return b; };
const frame = (p) => { const h = Buffer.alloc(4); h.writeUInt32BE(p.length, 0); return Buffer.concat([h, p]); };

test('EXTERNAL AuthStart: tag 10, username, empty nonce, mechanism byte 2', () => {
  assert.equal(I.AUTH_MECHANISM.EXTERNAL, 2);
  assert.equal(I.authStartFrame('app', '', 2).toString('hex'),
    '0a' + '03000000' + Buffer.from('app').toString('hex') + '00000000' + '02');
  assert.equal(I.authStartFrame('', '', 2).toString('hex'), '0a' + '00000000' + '00000000' + '02');
  // SCRAM omits the byte, which every server reads as mechanism 0.
  assert.equal(I.authStartFrame('u', 'n').toString('hex'), '0a' + '01000000' + '75' + '01000000' + '6e');
});

test('AuthOutcome: ok returns the 32-byte signature; denied throws the reason', () => {
  const sig = I.parseAuthOutcome(Buffer.concat([Buffer.from([13, 1]), Buffer.alloc(32)]));
  assert.equal(sig.toString('hex'), '00'.repeat(32));
  const reason = Buffer.from('no role for certificate CN');
  assert.throws(() => I.parseAuthOutcome(Buffer.concat([Buffer.from([13, 0]), u32le(reason.length), reason])),
    /authentication denied: no role for certificate CN/);
  assert.throws(() => I.parseAuthOutcome(Buffer.from([11])), /bad handshake outcome/);
});

test('authMechanism and client-certificate options are validated up front', () => {
  const cert = { tlsClientCert: '/c.pem', tlsClientKey: '/k.pem' };
  assert.throws(() => new Client({ authMechanism: 'certificate' }), /needs a client certificate/);
  assert.throws(() => new Client({ tlsClientCert: '/c.pem' }), /go together/);
  assert.throws(() => new Client({ tlsClientKey: '/k.pem' }), /go together/);
  assert.throws(() => new Client({ authMechanism: 'kerberos' }), /unknown authMechanism/);
  const c = new Client({ authMechanism: 'certificate', ...cert });
  assert.equal(c.authMechanism, 'certificate');
  assert.equal(c.tls, true);                     // a client certificate implies TLS
  assert.equal(c.user, '');                      // no user: the certificate CN decides
  // Never the SCRAM default 'anonymous': the server denies a non-empty name
  // that differs from the certificate CN.
  assert.equal(new Client({ authMechanism: 'certificate', user: '', ...cert }).user, '');
  assert.equal(new Client({ authMechanism: 'CERTIFICATE', user: 'app', ...cert }).user, 'app');
  assert.equal(new Client({ authMechanism: 'external', ...cert }).authMechanism, 'certificate');
  const s = new Client({ ...cert });
  assert.equal(s.authMechanism, 'scram');
  assert.equal(s.tls, true);
  assert.equal(s.user, 'anonymous');
  assert.equal(new Client().authMechanism, 'scram');
  assert.equal(new Client().tls, false);
});

// ---- TLS end to end --------------------------------------------------------

function haveOpenssl() {
  try { execFileSync('openssl', ['version'], { stdio: 'ignore' }); return true; } catch { return false; }
}

let pki = null;
/** A CA, a server certificate for DNS:skaidb, and a client certificate CN=app. */
function makePki() {
  if (pki) return pki;
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'skaidb-node-pki-'));
  const f = (n) => path.join(dir, n);
  const ssl = (...args) => execFileSync('openssl', args, { stdio: 'pipe' });
  const ec = ['-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes'];
  ssl('req', '-x509', ...ec, '-keyout', f('ca.key'), '-out', f('ca.crt'), '-days', '2', '-subj', '/CN=test-ca');
  const leaf = (name, subj, ext) => {
    ssl('req', ...ec, '-keyout', f(`${name}.key`), '-out', f(`${name}.csr`), '-subj', subj);
    fs.writeFileSync(f(`${name}.ext`), ext);
    ssl('x509', '-req', '-in', f(`${name}.csr`), '-CA', f('ca.crt'), '-CAkey', f('ca.key'),
      '-CAcreateserial', '-out', f(`${name}.crt`), '-days', '2', '-extfile', f(`${name}.ext`));
  };
  leaf('server', '/CN=skaidb', 'subjectAltName=DNS:skaidb\nextendedKeyUsage=serverAuth\n');
  leaf('client', '/CN=app', 'extendedKeyUsage=clientAuth\n');
  pki = { dir, ca: f('ca.crt'), serverCert: f('server.crt'), serverKey: f('server.key'),
          clientCert: f('client.crt'), clientKey: f('client.key') };
  process.on('exit', () => fs.rmSync(dir, { recursive: true, force: true }));
  return pki;
}

/**
 * A TLS server that REQUIRES a client certificate signed by the test CA,
 * records the peer CN and the raw AuthStart, answers it with `outcome`, then
 * answers Hello with Ddl and every query with Rows [cn].
 */
async function tlsServer(p, outcome) {
  const seen = [];
  const server = tls.createServer({
    key: fs.readFileSync(p.serverKey), cert: fs.readFileSync(p.serverCert),
    ca: fs.readFileSync(p.ca), requestCert: true, rejectUnauthorized: true,
  }, (sock) => {
    const conn = { cn: sock.getPeerCertificate().subject.CN, authStart: null };
    seen.push(conn);
    let buf = Buffer.alloc(0);
    sock.on('error', () => {});
    sock.on('data', (d) => {
      buf = Buffer.concat([buf, d]);
      while (buf.length >= 4 && buf.length >= 4 + buf.readUInt32BE(0)) {
        const n = buf.readUInt32BE(0);
        const payload = buf.subarray(4, 4 + n);
        buf = buf.subarray(4 + n);
        if (conn.authStart === null) {
          conn.authStart = Buffer.from(payload);
          sock.write(frame(outcome));
          if (outcome[1] !== 1) sock.end();
        } else if (payload[0] === 8) {
          sock.write(frame(Buffer.from([2])));
        } else {
          const cn = Buffer.from(conn.cn);
          const cell = Buffer.concat([Buffer.from([5]), u32le(cn.length), cn]);
          sock.write(frame(Buffer.concat([Buffer.from([0]), u32le(1), u32le(2), Buffer.from('cn'),
            u32le(1), u32le(1), u32le(cell.length), cell])));
        }
      }
    });
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return { port: server.address().port, seen, close: () => new Promise((r) => server.close(r)) };
}

const OK = Buffer.concat([Buffer.from([13, 1]), Buffer.alloc(32)]);
const skip = haveOpenssl() ? false : 'openssl CLI not installed';

test('certificate login over TLS: the certificate is the credential, the zero signature is not verified',
  { skip }, async () => {
    const p = makePki();
    const srv = await tlsServer(p, OK);
    try {
      const opts = { host: '127.0.0.1', port: srv.port, tlsCa: p.ca, tlsClientCert: p.clientCert,
                     tlsClientKey: p.clientKey, authMechanism: 'certificate' };
      const c = new Client(opts);
      await c.connect();
      assert.equal(srv.seen[0].cn, 'app');
      assert.equal(srv.seen[0].authStart.toString('hex'), I.authStartFrame('', '', 2).toString('hex'));
      assert.deepEqual((await c.query('SELECT current_user')).rows, [{ cn: 'app' }]);
      await c.end();
      // A given user asserts the identity and travels in AuthStart.
      const d = new Client({ ...opts, user: 'app' });
      await d.connect();
      assert.equal(srv.seen[1].authStart.toString('hex'), I.authStartFrame('app', '', 2).toString('hex'));
      await d.end();
      // Pooled connections inherit the mechanism and the certificate.
      const pool = new Pool({ ...opts, maxsize: 1 });
      assert.deepEqual((await pool.withConnection((x) => x.query('SELECT 1'))).rows, [{ cn: 'app' }]);
      await pool.end();
    } finally { await srv.close(); }
  });

test('certificate login denied: the connect fails with the server reason', { skip }, async () => {
  const p = makePki();
  const reason = Buffer.from('certificate CN app maps to no role');
  const srv = await tlsServer(p, Buffer.concat([Buffer.from([13, 0]), u32le(reason.length), reason]));
  try {
    const c = new Client({ host: '127.0.0.1', port: srv.port, tlsCa: p.ca, tlsClientCert: p.clientCert,
                           tlsClientKey: p.clientKey, authMechanism: 'certificate' });
    await assert.rejects(c.connect(), (e) => e instanceof SkaidbError
      && /authentication denied: certificate CN app maps to no role/.test(e.message));
  } finally { await srv.close(); }
});

test('a client certificate with SCRAM: presented in TLS, AuthStart stays SCRAM', { skip }, async () => {
  const p = makePki();
  // The fake server only records AuthStart; it cannot continue SCRAM, so
  // the connect fails at the challenge.
  const srv = await tlsServer(p, OK);
  try {
    const c = new Client({ host: '127.0.0.1', port: srv.port, user: 'ada', password: 'pw', tlsCa: p.ca,
                           tlsClientCert: p.clientCert, tlsClientKey: p.clientKey });
    await assert.rejects(c.connect(), /bad handshake challenge/);
    assert.equal(srv.seen[0].cn, 'app');
    const start = srv.seen[0].authStart;
    assert.equal(start[0], 10);
    const ulen = start.readUInt32LE(1);
    assert.equal(start.subarray(5, 5 + ulen).toString(), 'ada');
    const nlen = start.readUInt32LE(5 + ulen);
    assert.ok(nlen > 0, 'SCRAM sends a client nonce');
    assert.equal(start.length, 5 + ulen + 4 + nlen, 'no mechanism byte: SCRAM');
  } finally { await srv.close(); }
});

test('without a client certificate, a server that demands one refuses the TLS session', { skip }, async () => {
  const p = makePki();
  const srv = await tlsServer(p, OK);
  try {
    const c = new Client({ host: '127.0.0.1', port: srv.port, tlsCa: p.ca, connectTimeout: 3000 });
    await assert.rejects(c.connect(), SkaidbError);
    assert.equal(srv.seen.filter((s) => s.authStart).length, 0);
  } finally { await srv.close(); }
});

test('an unreadable client certificate or key is reported by path', async () => {
  const c = new Client({ host: '127.0.0.1', port: 1, tlsClientCert: '/nonexistent/c.pem',
                         tlsClientKey: '/nonexistent/k.pem', authMechanism: 'certificate' });
  await assert.rejects(c.connect(), /cannot read tlsClientCert \/nonexistent\/c\.pem/);
});
