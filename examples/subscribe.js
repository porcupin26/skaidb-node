// Follow a stream (CREATE STREAM) as events land, with a resumable cursor.
//
//   node examples/subscribe.js <stream-name> [host] [port] [user] [password]
//
// subscribe() polls the stream's log and yields { id, op, k, ts, doc }.
// `id` is an opaque string that sorts in log order (not a number). Persist
// the last `id` you handled and pass it as `after` on restart to resume
// exactly there. Ctrl-C aborts the signal, which ends the loop cleanly.
// For push delivery use any MQTT client on `$stream/<db>/<name>` instead;
// the events are identical.
const { Client } = require('../skaidb');

const [name = 'big_orders', host = 'localhost', port = '7000', user = 'anonymous', password = ''] = process.argv.slice(2);

(async () => {
  const client = new Client({ host, port: Number(port), user, password });
  await client.connect();

  const ac = new AbortController();
  process.once('SIGINT', () => ac.abort());

  let cursor = null;                 // load the saved position (an id string) here
  for await (const ev of client.subscribe(name, { after: cursor, pollMs: 500, signal: ac.signal })) {
    console.log(ev.id, ev.op, ev.k, ev.ts.toISOString(), ev.doc);
    cursor = ev.id;                  // save it wherever you persist state
  }
  await client.end();
})().catch((e) => { console.error(e); process.exit(1); });
