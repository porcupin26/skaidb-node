# Types and TypeScript

## Value mapping

Results decode from skaidb's typed wire values; parameters encode back to
them when a statement is prepared (which is the normal path whenever you
pass values).

| skaidb type | Result value | Accepted as a parameter |
|---|---|---|
| Null | `null` | `null`, `undefined` |
| Bool | `boolean` | `boolean` |
| Int (64-bit) | `number` when within ±2^53, otherwise `bigint` | `number` with an integer value; any `bigint` that fits 64 bits |
| Float (64-bit) | `number` | `number` with a fractional value (NaN and ±Infinity are refused) |
| Decimal | `string`, exact (`'123.45'`, `'-0.005'`) | bind a `string`; the driver has no Decimal encoder |
| String | `string` | `string` |
| Bytes | `Buffer` | `Buffer` |
| Uuid | `string`, canonical lowercase `8-4-4-4-12` | bind a `string`; the driver has no Uuid encoder |
| Timestamp | `Date` (millisecond precision, may predate 1970) | `Date` |
| Array | `Array` of mapped values | `Array` (nested values follow this table) |
| Document | plain object, insertion order kept | plain object (own enumerable keys) |

Notes:

- An `Int` beyond the safe integer range comes back as a `bigint` so no
  precision is lost; compare with `typeof v === 'bigint'` if you store ids
  near 2^63. To bind an Int that is not safely representable, pass a `bigint`.
- A `number` such as `3` binds as Int and `3.5` as Float. To force Float for
  an integral value, there is no marker; store the column as float and let
  the server convert, or bind `3.0` via a string cast in SQL.
- A `Buffer` result is a copy; mutating it does not affect the driver.
- In the client-side fallback (statements the server will not prepare, e.g.
  DDL), values are rendered as SQL literals: `Date` → epoch milliseconds,
  `Buffer` → hex string, arrays and objects are refused.

## TypeScript

The package ships `skaidb.d.ts` (via `"types"` in `package.json`); no
`@types/skaidb` is needed. It references `@types/node` for `Buffer`, which
any Node project already has.

```ts
import { Client, Pool, SkaidbError, CONSISTENCY,
         ClientOptions, PoolOptions, QueryResult, QueryConfig,
         StreamOptions, StreamEvent, SubscribeOptions, Value, Consistency } from 'skaidb';
```

### Typed rows

`query`, `stream` and the `resultSets` entries take a row type parameter;
the default is `any`.

```ts
interface User { id: number; name: string; tags: string[] }

const res = await client.query<User>('SELECT id, name, tags FROM users');
res.rows[0].name;                                       // string

for await (const u of client.stream<User>('SELECT id, name, tags FROM users')) { ... }

const arr = await client.query<Value[]>({ text: 'SELECT id FROM users', rowMode: 'array' });
arr.rows[0][0];                                         // Value
```

The row type is an assertion, not a check: the driver does not validate rows
against it. `Value` is the union of everything the codec can return, useful
for `rowMode: 'array'` and for `Document`-shaped columns.

### Options and results

`ClientOptions`/`PoolOptions` describe the constructors; `QueryConfig` the
object form of `query()`; `QueryResult<Row>` every result (with optional
`columns` and `resultSets`); `StreamOptions`, `SubscribeOptions` and
`StreamEvent` the streaming APIs. `Consistency` is
`0 | 1 | 2 | 'ONE' | 'QUORUM' | 'ALL'` (lowercase names also accepted).

`client.stream` is declared as a callable with an optional `columns:
string[]` property, matching the runtime.

### Errors

```ts
try {
  await client.query('SELECT nope');
} catch (e) {
  if (e instanceof SkaidbError) console.error(e.message);
  else throw e;
}
```

### ESM

```ts
import { Client } from 'skaidb';            // named imports work: the module is CommonJS
```

### Keeping the declarations honest

`test/types/consumer.ts` exercises every public member with its declared
types and is compiled in CI (`npm run typecheck`). When the runtime gains an
API, the declarations and that file change with it.
