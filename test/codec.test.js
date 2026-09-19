'use strict';
// The value codec (PROTOCOL.md §4): every tag decodes, encodes, and round-trips.
const test = require('node:test');
const assert = require('node:assert/strict');
const { _internal: I, SkaidbError } = require('../skaidb');

const roundtrip = (v) => I.decodeValue(new I.Reader(I.encodeValue(v)));

test('null, bool, int, float round-trip', () => {
  assert.equal(roundtrip(null), null);
  assert.equal(roundtrip(undefined), null);            // undefined binds as NULL
  assert.equal(roundtrip(true), true);
  assert.equal(roundtrip(false), false);
  assert.equal(roundtrip(0), 0);
  assert.equal(roundtrip(-42), -42);
  assert.equal(roundtrip(1.5), 1.5);
  assert.equal(roundtrip(-0.25), -0.25);
});

test('integers travel as Int (tag 2), non-integers as Float (tag 3)', () => {
  assert.equal(I.encodeValue(7)[0], 2);
  assert.equal(I.encodeValue(7.5)[0], 3);
  assert.equal(I.encodeValue(7n)[0], 2);
  assert.equal(I.encodeValue(7).length, 9);
  assert.equal(I.encodeValue(7).readBigInt64LE(1), 7n);
});

test('ints beyond 2^53 decode as bigint, safe ones as number', () => {
  const big = 2n ** 62n + 1n;
  assert.equal(roundtrip(big), big);
  assert.equal(typeof roundtrip(big), 'bigint');
  assert.equal(roundtrip(BigInt(Number.MAX_SAFE_INTEGER)), Number.MAX_SAFE_INTEGER);
  assert.equal(typeof roundtrip(BigInt(Number.MAX_SAFE_INTEGER)), 'number');
  assert.equal(roundtrip(BigInt(Number.MIN_SAFE_INTEGER) - 1n), BigInt(Number.MIN_SAFE_INTEGER) - 1n);
  assert.equal(roundtrip(-(2n ** 63n)), -(2n ** 63n));
});

test('NaN and Infinity are refused', () => {
  assert.throws(() => I.encodeValue(NaN), SkaidbError);
  assert.throws(() => I.encodeValue(Infinity), SkaidbError);
  assert.throws(() => I.encodeValue(1n << 63n), /does not fit a 64-bit integer/);
  assert.throws(() => I.encodeValue(-(1n << 63n) - 1n), SkaidbError);
});

test('string (UTF-8), bytes (Buffer), timestamp (Date, ms)', () => {
  assert.equal(roundtrip('héllo — wörld 🚀'), 'héllo — wörld 🚀');
  assert.equal(roundtrip(''), '');
  const b = Buffer.from([0, 1, 2, 255]);
  const out = roundtrip(b);
  assert.ok(Buffer.isBuffer(out));
  assert.deepEqual(out, b);
  const d = new Date(1700000000123);
  assert.ok(roundtrip(d) instanceof Date);
  assert.equal(roundtrip(d).getTime(), 1700000000123);
  assert.equal(I.encodeValue(d)[0], 8);
  assert.equal(roundtrip(new Date(-1000)).getTime(), -1000);  // pre-epoch
});

test('arrays and documents nest and keep order', () => {
  const v = [1, 'two', [3, null], { a: true, b: [Buffer.from('x')] }];
  assert.deepEqual(roundtrip(v), v);
  const doc = { z: 1, a: 2, m: { q: [1, 2, 3] } };
  assert.deepEqual(Object.keys(roundtrip(doc)), ['z', 'a', 'm']);
  assert.deepEqual(roundtrip(doc), doc);
  assert.equal(I.encodeValue([])[0], 9);
  assert.equal(I.encodeValue({})[0], 10);
});

test('decimal decodes to an exact string', () => {
  const dec = (mantissa, scale) => {
    const m = Buffer.alloc(16);
    let v = BigInt(mantissa);
    if (v < 0n) v += 1n << 128n;
    for (let i = 0; i < 16; i++) { m[i] = Number(v & 0xffn); v >>= 8n; }
    return Buffer.concat([Buffer.from([4]), m, I.u32le(scale)]);
  };
  assert.equal(I.decodeValue(new I.Reader(dec(12345, 2))), '123.45');
  assert.equal(I.decodeValue(new I.Reader(dec(-12345, 2))), '-123.45');
  assert.equal(I.decodeValue(new I.Reader(dec(5, 3))), '0.005');
  assert.equal(I.decodeValue(new I.Reader(dec(-5, 3))), '-0.005');
  assert.equal(I.decodeValue(new I.Reader(dec(42, 0))), '42');
  assert.equal(I.decodeValue(new I.Reader(dec(10n ** 30n, 10))), '100000000000000000000.0000000000');
  assert.equal(I.decimalToString(-(10n ** 20n) - 1n, 4), '-10000000000000000.0001');
});

test('uuid decodes to the canonical lowercase form', () => {
  const raw = Buffer.from('123e4567e89b12d3a456426614174000', 'hex');
  const v = I.decodeValue(new I.Reader(Buffer.concat([Buffer.from([7]), raw])));
  assert.equal(v, '123e4567-e89b-12d3-a456-426614174000');
  assert.equal(I.formatUuid(Buffer.alloc(16)), '00000000-0000-0000-0000-000000000000');
});

test('unknown tags and truncated payloads are SkaidbErrors', () => {
  assert.throws(() => I.decodeValue(new I.Reader(Buffer.from([42]))), /unknown value tag 42/);
  assert.throws(() => I.decodeValue(new I.Reader(Buffer.from([2, 1, 2]))), /truncated/);
  assert.throws(() => I.decodeValue(new I.Reader(Buffer.from([5, 10, 0, 0, 0, 65]))), /truncated/);
  assert.throws(() => I.encodeValue(Symbol('x')), /cannot bind value of type symbol/);
  assert.throws(() => I.encodeValue(() => 1), /cannot bind value of type function/);
});

test('Reader reads little-endian integers and length-prefixed blobs', () => {
  const buf = Buffer.concat([
    Buffer.from([0x01]),
    Buffer.from([0x34, 0x12]),
    Buffer.from([0x78, 0x56, 0x34, 0x12]),
    I.u32le(3), Buffer.from('abc'),
  ]);
  const r = new I.Reader(buf);
  assert.equal(r.u8(), 1);
  assert.equal(r.u16(), 0x1234);
  assert.equal(r.u32(), 0x12345678);
  assert.equal(r.text(), 'abc');
  assert.equal(r.pos, buf.length);
});
