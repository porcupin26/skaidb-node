'use strict';
// The scripted server of idle.test.js, in a worker thread: it can close a
// connection while the thread that runs the client is busy, which a server
// sharing the client's event loop cannot. Not a test file.
//
// workerData.flag is an Int32Array over shared memory: set to 1 (and
// notified) once the close has left this side. Messages in: 'half-close'
// (end the sending side of the first connection, keep reading), 'reset',
// 'report', 'stop'. Messages out: { port } once listening, and on 'report'
// { connections, late, lateRequests } — connections accepted, and the bytes
// and requests that arrived on the closed connection after its close.
const { parentPort, workerData } = require('worker_threads');
const { FakeServer, F } = require('./fake-server');

const flag = workerData.flag;
let dead = null;
let late = 0;
let lateRequests = 0;
const fake = new FakeServer({
  allowHalfOpen: true,
  handle: (req, conn) => {
    if (conn.sock === dead) lateRequests++;
    if (req.op === 8) return undefined;                                // Hello -> Ddl
    if (req.op === 2) return F.prepared(0, (req.sql.match(/\?/g) || []).length);
    if (req.op === 5 || req.op === 9) return [F.header(['v']), F.chunk([[1]]), F.end()];
    if (req.op === 3 || req.op === 7) return F.mutation(1);
    if (/^(begin|commit|rollback)\b/i.test(req.sql)) return F.ddl();
    return /^select/i.test(req.sql) ? F.rows(['v'], [[1]]) : F.mutation(1);
  },
});
const closed = () => { Atomics.store(flag, 0, 1); Atomics.notify(flag, 0); };

fake.start().then(() => parentPort.postMessage({ port: fake.port }));
parentPort.on('message', (m) => {
  if (m === 'half-close' || m === 'reset') {
    dead = [...fake.sockets][0];
    dead.on('data', (d) => { late += d.length; });
    if (m === 'reset') { dead.resetAndDestroy(); closed(); }
    else dead.end(closed);                       // called back once its FIN is out
  } else if (m === 'report') {
    parentPort.postMessage({ connections: fake.connections, late, lateRequests });
  } else if (m === 'stop') {
    fake.stop().then(() => process.exit(0));
  }
});
