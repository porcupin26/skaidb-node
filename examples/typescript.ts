// The driver from TypeScript: typed rows, typed options, typed errors.
//
//   npx tsc examples/typescript.ts --module commonjs --target es2020 --esModuleInterop
//   node examples/typescript.js
import { Client, SkaidbError, QueryResult } from 'skaidb';

interface Person { id: number; name: string; age: number }

async function main(): Promise<void> {
  const client = new Client({
    seeds: ['localhost:7000'],
    user: 'anonymous',
    password: '',
    consistency: 'QUORUM',
  });
  await client.connect();
  try {
    await client.query('CREATE TABLE IF NOT EXISTS people (PRIMARY KEY (id))');
    await client.batch('INSERT INTO people (id, name, age) VALUES ($1, $2, $3)',
      [[1, 'Ada', 36], [2, 'Linus', 54]]);

    const res: QueryResult<Person> = await client.query<Person>(
      'SELECT id, name, age FROM people WHERE age > $1', [40]);
    for (const p of res.rows) console.log(p.name, p.age);

    for await (const p of client.stream<Person>('SELECT id, name, age FROM people')) {
      if (p.id > 1) break;
    }
  } catch (e) {
    if (e instanceof SkaidbError) console.error('skaidb:', e.message);
    else throw e;
  } finally {
    await client.query('DROP TABLE IF EXISTS people');
    await client.end();
  }
}

main();
