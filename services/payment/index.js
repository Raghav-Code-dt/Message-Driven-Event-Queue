/**
 * Payment Service
 * Subscribes to `order.created` (consumer group: "payment").
 * Validates the order amount, simulates 800ms processing delay,
 * sends an explicit ACK, then publishes `payment.success` or `payment.failed`.
 */

import 'dotenv/config';
import { GatewayClient } from './gatewayClient.js';

const GATEWAY_WS  = process.env.GATEWAY_WS  ?? 'ws://localhost:8080';
const MAX_AMOUNT  = parseInt(process.env.PAYMENT_MAX_AMOUNT ?? '200000', 10);
const PROCESS_MS  = parseInt(process.env.PAYMENT_PROCESS_MS ?? '800',    10);

const gw = new GatewayClient(GATEWAY_WS);

gw.on('connect', () => {
  console.log('[Payment] Connected to gateway');
  // Competing consumer group — only one payment instance processes each order
  gw.subscribe('order.created', 'payment');
});

gw.on('event', async (msg) => {
  if (msg.topic !== 'order.created') return;

  const { msgId, body } = msg;
  const { orderId, total, currency } = body ?? {};

  if (!orderId || total == null) {
    console.warn('[Payment] ⚠️  Malformed event — NACKing');
    gw.nack(msgId);
    return;
  }

  console.log(`[Payment] 🔄 Processing order #${orderId}  ₹${total.toLocaleString('en-IN')} ...`);

  // Simulate async payment gateway call
  await new Promise(r => setTimeout(r, PROCESS_MS));

  const valid  = total > 0 && total <= MAX_AMOUNT;
  const topic  = valid ? 'payment.success' : 'payment.failed';
  const result = {
    orderId,
    amount:    total,
    currency:  currency ?? 'INR',
    status:    valid ? 'success' : 'failed',
    reason:    valid ? null : (total <= 0 ? 'invalid_amount' : 'limit_exceeded'),
    timestamp: Date.now(),
  };

  // ✅ Send ACK BEFORE publishing the result (message is processed)
  gw.ack(msgId);

  gw.publish(topic, result);

  const icon = valid ? '✅' : '❌';
  console.log(`[Payment] ${icon} ${topic} — order #${orderId}  ₹${total.toLocaleString('en-IN')}`);
});

gw.connect();

process.on('SIGINT',  () => { gw.stop(); process.exit(0); });
process.on('SIGTERM', () => { gw.stop(); process.exit(0); });

console.log(`[Payment] Starting — Gateway: ${GATEWAY_WS} | Max: ₹${MAX_AMOUNT.toLocaleString('en-IN')}`);
