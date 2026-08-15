/**
 * Order Service
 * Publishes `order.created` events every 3 seconds to simulate a real
 * e-commerce order flow. Also listens for payment outcomes on its orders.
 */

import 'dotenv/config';
import { GatewayClient } from './gatewayClient.js';

const GATEWAY_WS    = process.env.GATEWAY_WS    ?? 'ws://localhost:8080';
const ORDER_INTERVAL = parseInt(process.env.ORDER_INTERVAL_MS ?? '3000', 10);

const PRODUCTS = [
  { productId: 1, name: 'Laptop',      price: 75_000 },
  { productId: 2, name: 'Headphones',  price:  2_999 },
  { productId: 3, name: 'Monitor',     price: 22_000 },
  { productId: 4, name: 'Keyboard',    price:  3_499 },
  { productId: 5, name: 'Mouse',       price:  1_299 },
];

let orderCount = 0;

function makeOrder() {
  const product  = PRODUCTS[Math.floor(Math.random() * PRODUCTS.length)];
  const quantity = Math.floor(Math.random() * 3) + 1;
  const orderId  = `ORD-${Date.now()}-${++orderCount}`;

  return {
    orderId,
    customerId: `cust_${Math.floor(Math.random() * 500)}`,
    items: [{ ...product, quantity }],
    total: product.price * quantity,
    currency: 'INR',
    timestamp: Date.now(),
  };
}

const gw = new GatewayClient(GATEWAY_WS);

gw.on('connect', () => {
  console.log('[Order] Connected to gateway — starting order flow');

  // Listen for payment outcomes on our orders
  gw.subscribe('payment.success', 'order-svc');
  gw.subscribe('payment.failed',  'order-svc');

  const timer = setInterval(() => {
    const order = makeOrder();
    gw.publish('order.created', order);
    console.log(`[Order] ✅ Published order #${order.orderId}  ₹${order.total.toLocaleString('en-IN')}`);
  }, ORDER_INTERVAL);

  gw.once('disconnect', () => clearInterval(timer));
});

gw.on('event', (msg) => {
  const { topic, body } = msg;
  if (topic === 'payment.success') {
    console.log(`[Order] 💳 Payment SUCCESS for order #${body?.orderId}`);
  } else if (topic === 'payment.failed') {
    console.log(`[Order] ❌ Payment FAILED  for order #${body?.orderId} — reason: ${body?.reason}`);
  }
});

gw.connect();

process.on('SIGINT',  () => { gw.stop(); process.exit(0); });
process.on('SIGTERM', () => { gw.stop(); process.exit(0); });

console.log(`[Order] Starting — Gateway: ${GATEWAY_WS}`);
