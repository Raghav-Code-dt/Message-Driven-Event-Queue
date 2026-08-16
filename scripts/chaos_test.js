import 'dotenv/config';
import WebSocket from 'ws';

const GATEWAY_WS = process.env.GATEWAY_WS ?? 'ws://localhost:8080';
const ws = new WebSocket(GATEWAY_WS);

const TEST_TOPIC = 'chaos.test';
const TRACE_ID = `chaos-${Date.now()}`;

let state = 'INIT';
let retryCount = 0;

console.log(`[Chaos] Connecting to ${GATEWAY_WS}...`);

ws.on('open', () => {
  console.log(`[Chaos] Connected. Subscribing to ${TEST_TOPIC} and $DLQ.*`);
  ws.send(JSON.stringify({ type: 'subscribe', topic: TEST_TOPIC, group: 'chaos-consumer' }));
  ws.send(JSON.stringify({ type: 'subscribe', topic: '$DLQ.*', group: 'chaos-dlq-monitor' }));

  setTimeout(() => {
    console.log(`[Chaos] Publishing poison pill message [${TRACE_ID}]`);
    state = 'PUBLISHED';
    ws.send(JSON.stringify({
      type: 'publish',
      topic: TEST_TOPIC,
      body: { traceId: TRACE_ID, data: 'Poison Pill' }
    }));
  }, 500);
});

ws.on('message', (raw) => {
  let msg;
  try { msg = JSON.parse(raw); } catch { return; }
  if (msg.type !== 'event') return;

  if (msg.topic === TEST_TOPIC && msg.body?.traceId === TRACE_ID) {
    retryCount++;
    console.log(`[Chaos] Received delivery attempt ${retryCount} for [${TRACE_ID}]. Intentional NACK!`);
    ws.send(JSON.stringify({ type: 'nack', msgId: msg.msgId }));
  }

  if (msg.topic === `$DLQ.${TEST_TOPIC}`) {
    if (msg.body?.traceId === TRACE_ID) {
      console.log(`[Chaos] DLQ event received for [${TRACE_ID}]!`);
      console.log(`\n[SUCCESS] DLQ Routing & Fault Tolerance Verified!`);
      ws.close();
      process.exit(0);
    }
  }
});

ws.on('close', () => {
  console.log('[Chaos] Disconnected.');
});

setTimeout(() => {
  console.error('[Chaos] FAILED: Timeout waiting for DLQ event.');
  process.exit(1);
}, 20000);
