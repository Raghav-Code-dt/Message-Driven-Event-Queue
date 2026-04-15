// recommendation-service/recommendation-service.js
import { io } from "socket.io-client";
import fetch from "node-fetch";

const GATEWAY_WS = "http://localhost:5000";
const GATEWAY_HTTP = "http://localhost:5000/emit";

console.log("🤝 Recommendation Service starting…");

const socket = io(GATEWAY_WS, { transports: ["websocket"] });

let coBuy = new Map(); // key "a|b" (sorted ids) -> count
let productNames = new Map(); // optional: cache names from orders

socket.on("connect", () => {
  console.log("🤝 Recommendation connected to Gateway");
});

socket.on("event", async (evt) => {
  if (!evt || !evt.topic) return;

  if (evt.topic === "order.placed") {
    const { order } = evt.payload || {};
    if (!order || !Array.isArray(order.items)) return;

    // record names (optional, for nice payloads)
    order.items.forEach(it => {
      if (it?.productId && it?.name) productNames.set(it.productId, it.name);
    });

    const ids = order.items
      .filter(i => typeof i.productId === "number")
      .map(i => i.productId);

    // all unordered pairs
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const a = Math.min(ids[i], ids[j]);
        const b = Math.max(ids[i], ids[j]);
        const key = `${a}|${b}`;
        coBuy.set(key, (coBuy.get(key) || 0) + 1);
      }
    }
  }
});

// emit recommendations every 5 seconds
setInterval(async () => {
  // build adjacency list: productId -> [{productId, score, name}]
  const graph = new Map();
  for (const [key, score] of coBuy.entries()) {
    const [a, b] = key.split("|").map(Number);
    if (!graph.has(a)) graph.set(a, []);
    if (!graph.has(b)) graph.set(b, []);
    graph.get(a).push({ productId: b, score, name: productNames.get(b) || String(b) });
    graph.get(b).push({ productId: a, score, name: productNames.get(a) || String(a) });
  }

  // top 3 per product
  const recommendations = {};
  for (const [pid, list] of graph.entries()) {
    recommendations[pid] = list
      .sort((x, y) => y.score - x.score)
      .slice(0, 3);
  }

  await fetch(GATEWAY_HTTP, {
    method: "POST",
    headers: { "Content-Type":"application/json" },
    body: JSON.stringify({
      topic: "recommendation.update",
      payload: { recommendations },
      service: "recommendation",
      subscribers: "dashboard, ecomm",
      timestamp: Date.now()
    })
  });
}, 5000);
