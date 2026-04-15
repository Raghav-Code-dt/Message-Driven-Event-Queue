// shipping-service.js
import { io } from "socket.io-client";
import fetch from "node-fetch";

const GATEWAY_WS = "http://localhost:5000";
const GATEWAY_HTTP = "http://localhost:5000/emit";

console.log("📦 Shipping Service starting...");

const socket = io(GATEWAY_WS);

socket.on("connect", () => {
  console.log("📦 Shipping connected to Gateway");
});

// Listen for order.placed
socket.on("event", async evt => {
  if (!evt.topic) return;

  if (evt.topic === "payment.sucess") {
    const { orderId } = evt.payload;
    console.log(`📦 Order received, shipping soon: ${orderId}`);

    // Simulate async shipping delay
    setTimeout(async () => {
      console.log(`✅ Order shipped: ${orderId}`);

      await fetch(GATEWAY_HTTP, {
        method: "POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify({
          topic: "order.shipped",
          payload: { orderId, status: "shipped", timestamp: Date.now() },
          service: "shipping",
          subscribers: "ecomm, dashboard"
        })
      });
    }, 10000);          // runs 10s after order is placed 
  }
});
