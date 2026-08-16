import WebSocket from 'ws';

const GATEWAY_WS = process.env.GATEWAY_WS ?? 'ws://localhost:8080';
const MSG_COUNT = 50000;
const TOPIC      = 'benchmark.test';

console.log(`[Benchmark] Target: ${GATEWAY_WS}`);
console.log(`[Benchmark] Messages: ${MSG_COUNT.toLocaleString()}`);

const ws = new WebSocket(GATEWAY_WS);

let startTime;
let publishedCount = 0;
let receivedCount = 0;
const MAX_IN_FLIGHT = 5000;
const BATCH_SIZE = 2000;

const sendTimes = new BigInt64Array(MSG_COUNT);
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
    const recvTime = process.hrtime.bigint();
    const seq = msg.body.seq;
    const sendTime = sendTimes[seq];
    const latencyMs = Number(recvTime - sendTime) / 1_000_000.0;
    
    latencies[receivedCount] = latencyMs;
    receivedCount++;

    // ACK to keep broker memory clean
    ws.send(JSON.stringify({ type: 'ack', msgId: msg.msgId }));

    if (receivedCount % 5000 === 0) {
      console.log(`[Benchmark] Received ${receivedCount} / ${MSG_COUNT} ...`);
    }

    if (receivedCount === MSG_COUNT) {
      // Allow time for final TCP ACKs to flush before process exit!
      setTimeout(finishBenchmark, 250);
    } else {
      pump();
    }
  }
});

function startBlast() {
  startTime = performance.now();
  pump();
}

function pump() {
  // Only pump if we have drained enough to send a full batch
  if (publishedCount - receivedCount > MAX_IN_FLIGHT - BATCH_SIZE) {
    return;
  }

  while (publishedCount - receivedCount < MAX_IN_FLIGHT && publishedCount < MSG_COUNT) {
    sendTimes[publishedCount] = process.hrtime.bigint();
    ws.send(JSON.stringify({
      type: 'publish',
      topic: TOPIC,
      body: { seq: publishedCount }
    }));
    publishedCount++;
  }
}

function finishBenchmark() {
  const totalTimeSec = (performance.now() - startTime) / 1000;
  const throughput = Math.round(MSG_COUNT / totalTimeSec);

  latencies.sort();
  const min = latencies[0];
  const p50 = latencies[Math.floor(MSG_COUNT * 0.50)];
  const p90 = latencies[Math.floor(MSG_COUNT * 0.90)];
  const p95 = latencies[Math.floor(MSG_COUNT * 0.95)];
  const p99 = latencies[Math.floor(MSG_COUNT * 0.99)];
  const max = latencies[MSG_COUNT - 1];

  console.log('\n--- 📊 BENCHMARK RESULTS ---');
  console.log(`Total Time:  ${totalTimeSec.toFixed(2)} seconds`);
  console.log(`Throughput:  ${throughput.toLocaleString()} msg/sec`);
  console.log(`Min Latency: ${min.toFixed(3)} ms`);
  console.log(`p50 Latency: ${p50.toFixed(3)} ms`);
  console.log(`p90 Latency: ${p90.toFixed(3)} ms`);
  console.log(`p95 Latency: ${p95.toFixed(3)} ms`);
  console.log(`p99 Latency: ${p99.toFixed(3)} ms`);
  console.log(`Max Latency: ${max.toFixed(3)} ms`);
  console.log('----------------------------\n');
  
  process.exit(0);
}
