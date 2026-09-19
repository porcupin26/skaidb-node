'use strict';
// Parameter binding: the client-side literal fallback (§5) and the $N -> ?
// rewrite used by the prepared path.
const test = require('node:test');
const assert = require('node:assert/strict');
const { _internal: I, SkaidbError, CONSISTENCY } = require('../skaidb');

test('quote renders every literal form', () => {
  assert.equal(I.quote(null), 'NULL');
  assert.equal(I.quote(undefined), 'NULL');
  assert.equal(I.quote(true), 'TRUE');
  assert.equal(I.quote(false), 'FALSE');
  assert.equal(I.quote(42), '42');
  assert.equal(I.quote(-1.5), '-1.5');
  assert.equal(I.quote(123n), '123');
  assert.equal(I.quote("O'Brien"), "'O''Brien'");
  assert.equal(I.quote("a\\b"), "'a\\b'");                 // backslashes are literal
  assert.equal(I.quote(new Date(1000)), '1000');            // timestamps as epoch ms
  assert.equal(I.quote(Buffer.from([0xde, 0xad])), "'dead'");
  assert.throws(() => I.quote(NaN), /NaN/);
  assert.throws(() => I.quote({ a: 1 }), /cannot bind value of type object/);
});

test('bindParams substitutes $N and is the injection boundary', () => {
  assert.equal(I.bindParams('SELECT 1', undefined), 'SELECT 1');
  assert.equal(I.bindParams('SELECT 1', []), 'SELECT 1');
  assert.equal(
    I.bindParams('SELECT * FROM t WHERE a = $1 AND b = $2', ["x'; DROP TABLE t; --", 2]),
    "SELECT * FROM t WHERE a = 'x''; DROP TABLE t; --' AND b = 2");
  assert.equal(I.bindParams('SELECT $2, $1, $2', ['a', 'b']), "SELECT 'b', 'a', 'b'");
  assert.equal(I.bindParams('SELECT $10', [1, 2, 3, 4, 5, 6, 7, 8, 9, 'ten']), "SELECT 'ten'");
});

test('bindParams leaves $N inside string literals alone', () => {
  assert.equal(I.bindParams("SELECT '$1' , $1", ['v']), "SELECT '$1' , 'v'");
  assert.equal(I.bindParams("SELECT 'it''s $1', $1", ['v']), "SELECT 'it''s $1', 'v'");
  assert.equal(I.bindParams('SELECT $notaparam, $1', ['v']), "SELECT $notaparam, 'v'");
});

test('bindParams rejects a placeholder with no parameter', () => {
  assert.throws(() => I.bindParams('SELECT $2', ['only one']), /no parameter for \$2/);
  assert.throws(() => I.bindParams('SELECT $0', ['x']), SkaidbError);
});

test('toQmark rewrites to positional ? in wire order, repeating reused params', () => {
  const q = I.toQmark('UPDATE t SET a = $2, b = $1 WHERE id = $1', ['id', 'val']);
  assert.equal(q.sql, 'UPDATE t SET a = ?, b = ? WHERE id = ?');
  assert.deepEqual(q.params, ['val', 'id', 'id']);
});

test('toQmark keeps literals and typed values intact', () => {
  const arr = [1, 2]; const doc = { k: 'v' };
  const q = I.toQmark("INSERT INTO t VALUES ('$1', $1, $2)", [arr, doc]);
  assert.equal(q.sql, "INSERT INTO t VALUES ('$1', ?, ?)");
  assert.equal(q.params[0], arr);                       // same object, no stringification
  assert.equal(q.params[1], doc);
  assert.throws(() => I.toQmark('SELECT $3', [1, 2]), /no parameter for \$3/);
  assert.deepEqual(I.toQmark('SELECT 1', []), { sql: 'SELECT 1', params: [] });
});

test('resolveConsistency accepts numbers and names, rejects the rest', () => {
  assert.equal(I.resolveConsistency(undefined), CONSISTENCY.QUORUM);
  assert.equal(I.resolveConsistency('one'), 0);
  assert.equal(I.resolveConsistency('QUORUM'), 1);
  assert.equal(I.resolveConsistency('All'), 2);
  assert.equal(I.resolveConsistency(2), 2);
  assert.throws(() => I.resolveConsistency(3), /bad consistency 3/);
  assert.throws(() => I.resolveConsistency('eventual'), /bad consistency eventual/);
});
