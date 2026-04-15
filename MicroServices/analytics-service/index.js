import { io } from "socket.io-client";
import fetch from "node-fetch";

const GATEWAY_WS = "http://localhost:5000";
const GATEWAY_HTTP = "http://localhost:5000/emit";

console.log("📊 Analytics Service running...");

const socket = io(GATEWAY_WS);

let totalOrders = 0;
let totalRevenue = 0;
let productFrequency = {}; // {productId: count}

socket.on("connect", () => {
  console.log("📊 Analytics connected to Gateway");
});

// Listen to all events
socket.on("event", async (evt) => {
  if (!evt || !evt.topic) return;

  if (evt.topic === "order.placed") {
    const { order } = evt.payload;
    totalOrders++;
    totalRevenue += order.total;
    order.items.forEach(item => {
      productFrequency[item.productId] =
        (productFrequency[item.productId] || 0) + item.quantity;
    });
  }
});

// Send analytics update every 1 minute
setInterval(async () => {
  await fetch(GATEWAY_HTTP, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      topic: "analytics.update",
      payload: {
        totalOrders,
        totalRevenue,
        productFrequency
      },
      service: "analytics",
      subscribers: "dashboard"
    })
  });
}, 60000);
