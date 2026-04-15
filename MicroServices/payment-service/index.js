// payment-service/payment-service.js
import { io } from "socket.io-client";
import fetch from "node-fetch";

const GATEWAY_WS = "http://localhost:5000";
const GATEWAY_HTTP = "http://localhost:5000/emit";

console.log("💳 Payment Service starting…");

const socket = io(GATEWAY_WS, { transports: ["websocket"] });

socket.on("connect", () => {
  console.log("💳 Payment connected to Gateway");
});

socket.on("event", async (evt) => {
  if (!evt || !evt.topic) return;

  if (evt.topic === "order.placed") {
    const { orderId, order } = evt.payload || {};
    if (!orderId || !order) return;

    const amount = Number(order.total || 0);
    const fail = amount <= 0 || amount > 2_00_000; // fail too big / invalid

    console.log(`💳 Processing payment for order #${orderId} (₹${amount})…`);

    setTimeout(async () => {
      const topic = fail ? "payment.failed" : "payment.success";
      const payload = {
        orderId,
        amount,
        currency: "INR",
        reason: fail ? "limit_exceeded_or_invalid_amount" : "ok",
      };

      await fetch(GATEWAY_HTTP, {
        method: "POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify({
          topic,
          payload,
          service: "payment",
          subscribers: "shipping, ecomm, dashboard",
          timestamp: Date.now()
        })
      });

      console.log(`💳 ${topic} for order #${orderId}`);
    }, 1200);
  }
});
