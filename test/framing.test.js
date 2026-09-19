'use strict';
// Framing (§1): a big-endian u32 length prefix, reassembled across arbitrary
// TCP read boundaries, and queued when no reader is waiting.
const test = require('node:test');
const assert = require('node:assert/strict');
const { Client, _internal: I } = require('../skaidb');
const { frame } = require('./fake-server');

function clientWithCapturedSocket() {
  const c = new Client();
  const written = [];
  c._sock = { write: (b) => written.push(Buffer.from(b)) };
  return { c, written };
}

test('_writeFrame prefixes the payload with a big-endian length', () => {
  const { c, written } = clientWithCapturedSocket();
  c._writeFrame(Buffer.from('hello'));
  assert.equal(written.length, 1);
  assert.deepEqual(written[0].subarray(0, 4), Buffer.from([0, 0, 0, 5]));
  assert.equal(written[0].subarray(4).toString(), 'hello');
  c._writeFrame(Buffer.alloc(0x01020304 % 4096));
  assert.equal(written[1].readUInt32BE(0), 0x01020304 % 4096);
});

test('encStr and u32le are little-endian inside payloads', () => {
  assert.deepEqual(I.u32le(0x01020304), Buffer.from([4, 3, 2, 1]));
  assert.deepEqual(I.encStr('ab'), Buffer.from([2, 0, 0, 0, 0x61, 0x62]));
  assert.deepEqual(I.encStr('é'), Buffer.from([2, 0, 0, 0, 0xc3, 0xa9]));
});

test('_onData reassembles a frame split byte by byte', async () => {
  const c = new Client();
  const got = c._readFrame();                      // a waiter parks first
  const f = frame(Buffer.from([2, 9, 9]));
  for (const byte of f) c._onData(Buffer.from([byte]));
  assert.deepEqual(await got, Buffer.from([2, 9, 9]));
});

test('_onData splits several frames from one read and queues the extras', async () => {
  const c = new Client();
  const a = Buffer.from([1, 1]); const b = Buffer.from([2]); const d = Buffer.from([3, 3, 3]);
  const partial = frame(d).subarray(0, 5);         // header + 1 of 3 body bytes
  c._onData(Buffer.concat([frame(a), frame(b), partial]));
  assert.equal(c._frames.length, 2);               // nobody was waiting: queued, not dropped
  assert.deepEqual(await c._readFrame(), a);
  assert.deepEqual(await c._readFrame(), b);
  const third = c._readFrame();                    // parks: the third frame is incomplete
  assert.equal(c._waiters.length, 1);
  c._onData(frame(d).subarray(5));
  assert.deepEqual(await third, d);
  assert.equal(c._buf.length, 0);
});

test('an empty payload is a legal frame', async () => {
  const c = new Client();
  c._onData(Buffer.from([0, 0, 0, 0]));
  assert.deepEqual(await c._readFrame(), Buffer.alloc(0));
});

test('a waiter is rejected when the transport fails, and later reads fail fast', async () => {
  const c = new Client();
  const pending = c._readFrame();
  c._fail(new Error('boom'));
  await assert.rejects(pending, /boom/);
  await assert.rejects(c._readFrame(), /connection lost/);
  assert.equal(c.isUsable(), false);
});

test('seeds parse host:port, bare hosts and IPv6-ish brackets fall back to the port', () => {
  const c = new Client({ seeds: ['a:7001', 'b', 'c:7002'], port: 7000 });
  assert.deepEqual(c.seeds, [
    { host: 'a', port: 7001 }, { host: 'b', port: 7000 }, { host: 'c', port: 7002 }]);
  const d = new Client({ host: 'h', port: 9 });
  assert.deepEqual(d.seeds, [{ host: 'h', port: 9 }]);
  const e = new Client({ seeds: [] });
  assert.deepEqual(e.seeds, [{ host: 'localhost', port: 7000 }]);
});

test('TLS is implied by tlsCa or tlsInsecure', () => {
  assert.equal(new Client().tls, false);
  assert.equal(new Client({ tls: true }).tls, true);
  assert.equal(new Client({ tlsCa: '/x.pem' }).tls, true);
  assert.equal(new Client({ tlsInsecure: true }).tls, true);
  assert.equal(new Client().tlsServerName, 'skaidb');
});
