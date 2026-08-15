import WebSocket from 'ws';

const GATEWAY_WS = process.env.GATEWAY_WS ?? 'ws://localhost:8080';
const MSG_COUNT = 50000;
const TOPIC      = 'benchmark.test';

console.log(`[Benchmark] Target: ${GATEWAY_WS}`);
console.log(`[Benchmark] Messages: ${MSG_COUNT.toLocaleString()}`);

const ws = new WebSocket(GATEWAY_WS);

let startTime;
let receivedCount = 0;
const latencies = new Float64Array(MSG_COUNT);

ws.on('open', () => {
  console.log('[Benchmark] Connected. Subscribing...');
  ws.send(JSON.stringify({ type: 'subscribe', topic: TOPIC }));
});

ws.on('message', (raw) => {
  const msg = JSON.parse(raw.toString());

  if (msg.type === 'subscribed') {
    console.log('[Benchmark] Subscribed. Starting blast in 1 second...');
    setTimeout(startBlast, 1000);
  } else if (msg.type === 'event' && msg.topic === TOPIC) {
    const now = performance.now();
    const sendTime = msg.body.ts;
    const latency = now - sendTime;
    
    latencies[receivedCount] = latency;
    receivedCount++;

    // ACK to keep broker memory clean
    ws.send(JSON.stringify({ type: 'ack', msgId: msg.msgId }));

    if (receivedCount % 5000 === 0) {
      console.log(`[Benchmark] Received ${receivedCount} / ${MSG_COUNT} ...`);
    }

    if (receivedCount === MSG_COUNT) {
      finishBenchmark();
    }
  }
});

async function startBlast() {
  startTime = performance.now();
  
  // Blast messages in batches so we don't blow up the Node.js memory buffer
  const BATCH_SIZE = 1000;
  
  for (let i = 0; i < MSG_COUNT; i += BATCH_SIZE) {
    for (let j = 0; j < BATCH_SIZE && i + j < MSG_COUNT; j++) {
      ws.send(JSON.stringify({
        type: 'publish',
        topic: TOPIC,
        body: { ts: performance.now(), seq: i + j }
      }));
    }
    
    // Application-level backpressure: 
    // Don't let in-flight messages exceed 5000, otherwise the broker's 
    // 5-second ACK timeout will kick in and cause endless requeues!
    while ((i + BATCH_SIZE) - receivedCount > 5000) {
      await new Promise(r => setTimeout(r, 10));
    }
  }
  
  console.log('[Benchmark] All published. Waiting for remaining acks/events...');
}

function finishBenchmark() {
  const totalTimeSec = (performance.now() - startTime) / 1000;
  const throughput = Math.round(MSG_COUNT / totalTimeSec);

  latencies.sort();
  const p50 = latencies[Math.floor(MSG_COUNT * 0.50)];
  const p95 = latencies[Math.floor(MSG_COUNT * 0.95)];
  const p99 = latencies[Math.floor(MSG_COUNT * 0.99)];

  console.log('\n--- 📊 BENCHMARK RESULTS ---');
  console.log(`Total Time:  ${totalTimeSec.toFixed(2)} seconds`);
  console.log(`Throughput:  ${throughput.toLocaleString()} msg/sec`);
  console.log(`p50 Latency: ${p50.toFixed(2)} ms`);
  console.log(`p95 Latency: ${p95.toFixed(2)} ms`);
  console.log(`p99 Latency: ${p99.toFixed(2)} ms`);
  console.log('----------------------------\n');
  
  process.exit(0);
}
