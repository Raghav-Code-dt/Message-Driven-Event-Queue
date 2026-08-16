import WebSocket from 'ws';

const GATEWAY_WS = process.env.GATEWAY_WS ?? 'ws://localhost:8080';
const MSG_COUNT = parseInt(process.env.TOTAL_MESSAGES || '50000', 10);
const TOPIC      = 'benchmark.test';

console.log(`[Benchmark] Target: ${GATEWAY_WS}`);
console.log(`[Benchmark] Messages: ${MSG_COUNT.toLocaleString()}`);

const ws = new WebSocket(GATEWAY_WS);

let startTime;
let publishedCount = 0;
let receivedCount = 0;
// 1000 in-flight messages fully saturates the pipeline to reach 20k+ throughput
const MAX_IN_FLIGHT = parseInt(process.env.MAX_IN_FLIGHT || '200', 10);

const sendTimes = new Map();
const latencies = [];

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
    const seq = msg.body.seq;
    if (sendTimes.has(seq)) {
        const start = sendTimes.get(seq);
        const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
        latencies.push(elapsedMs);
        sendTimes.delete(seq);

        receivedCount++;
        ws.send(JSON.stringify({ type: 'ack', msgId: msg.msgId }));
        
        if (receivedCount % 5000 === 0) {
            console.log(`[Benchmark] Received ${receivedCount} / ${MSG_COUNT} ...`);
        }
        
        pump(); // Keep window filled
    }

    if (receivedCount === MSG_COUNT) {
        setTimeout(finishBenchmark, 250);
    }
  }
});

function startBlast() {
  startTime = performance.now();
  pump();

  // Watchdog timer
  let lastReceived = -1;
  setInterval(() => {
    if (receivedCount === lastReceived && receivedCount < MSG_COUNT) {
      console.error(`\n[Watchdog] ⚠️ STALL DETECTED! No ACKs for 5s.`);
      console.error(`  Published: ${publishedCount}`);
      console.error(`  Received:  ${receivedCount}`);
      console.error(`  In-Flight: ${publishedCount - receivedCount}`);
      process.exit(1);
    }
    lastReceived = receivedCount;
  }, 5000).unref();
}

function pump() {
  while ((publishedCount - receivedCount) < MAX_IN_FLIGHT && publishedCount < MSG_COUNT) {
    const id = publishedCount;
    sendTimes.set(id, process.hrtime.bigint());
    ws.send(JSON.stringify({
      type: 'publish',
      topic: TOPIC,
      body: { seq: id }
    }));
    publishedCount++;
  }
}

function finishBenchmark() {
  const totalTimeSec = (performance.now() - startTime) / 1000;
  const throughput = Math.round(MSG_COUNT / totalTimeSec);

  latencies.sort((a, b) => a - b);
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
