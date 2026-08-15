/**
 * Notification Service
 * Subscribes to `payment.*` (wildcard — catches both success and failed).
 * Consumer group: "notification" — one instance handles each event.
 * Sends explicit ACK after processing, then publishes `notification.sent`.
 */

import 'dotenv/config';
import { GatewayClient } from './gatewayClient.js';

const GATEWAY_WS = process.env.GATEWAY_WS ?? 'ws://localhost:8080';

// Simple in-memory stats
const stats = { success: 0, failed: 0, total: 0 };

function formatNotification(topic, body) {
  const { orderId, amount, reason } = body ?? {};
  const amountStr = amount != null
    ? `₹${Number(amount).toLocaleString('en-IN')}`
    : 'unknown';

  if (topic === 'payment.success') {
    return `🎉 Order #${orderId} confirmed — ${amountStr} charged successfully.`;
  }
  if (topic === 'payment.failed') {
    return `⚠️  Order #${orderId} failed — ${amountStr} (${reason ?? 'unknown reason'}).`;
  }
  return `📢 Notification for order #${orderId} — topic: ${topic}`;
}

const gw = new GatewayClient(GATEWAY_WS);

gw.on('connect', () => {
  console.log('[Notification] Connected to gateway');
  // Wildcard — catches payment.success AND payment.failed
  gw.subscribe('payment.*', 'notification');
});

gw.on('event', (msg) => {
  const { topic, msgId, body } = msg;
  if (!topic.startsWith('payment.')) return;

  const message = formatNotification(topic, body);
  console.log(`[Notification] ${message}`);

  // ✅ ACK immediately — notification is "delivered"
  gw.ack(msgId);

  // Track stats
  stats.total++;
  if (topic === 'payment.success') stats.success++;
  if (topic === 'payment.failed')  stats.failed++;

  // Publish notification.sent for dashboard observability
  gw.publish('notification.sent', {
    orderId:   body?.orderId,
    topic,
    message,
    stats:     { ...stats },
    timestamp: Date.now(),
  });
});

gw.connect();

// Periodic stats summary
setInterval(() => {
  if (stats.total > 0) {
    console.log(
      `[Notification] 📊 Stats — Total: ${stats.total} | ` +
      `Success: ${stats.success} | Failed: ${stats.failed}`
    );
  }
}, 10_000);

process.on('SIGINT',  () => { gw.stop(); process.exit(0); });
process.on('SIGTERM', () => { gw.stop(); process.exit(0); });

console.log(`[Notification] Starting — Gateway: ${GATEWAY_WS}`);
