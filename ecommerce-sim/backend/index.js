// backend/index.js
import express from "express";
import http from "http";
import { Server } from "socket.io";
import cors from "cors";
import fetch from "node-fetch";
import { io as ioClient } from "socket.io-client";

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "http://localhost:5173" },
});

const PORT = 4000;
const GATEWAY_HTTP = "http://localhost:5000/emit";
const GATEWAY_WS = "http://localhost:5000";

// ---- Local product store ----
let products = [
  { id: 1, name: "Laptop", price: 75000, stock: 5 , img:'../include/laptop.jpeg'},
  { id: 2, name: "Headphones", price: 2999, stock: 10 , img:'../include/headphone.avif'},
];

function broadcastStockUpdate(productId) {
  const p = products.find(x => x.id === productId);
  if (!p) return;
  io.emit("stock:update", { productId, stock: p.stock });
}

// ---- Publish to Gateway (with service metadata) ----
async function publishToGateway(topic, payload) {
  try {
    await fetch(GATEWAY_HTTP, {
      method: "POST",
      headers: { "Content-Type":"application/json" },
      body: JSON.stringify({
        topic,
        payload,
        service: "ecomm",
        subscribers: "broker, dashboard"
      }),
    });
  } catch (err) {
    console.error("[GW-PUBLISH ERROR]", err.message);
  }
}

// ---- ✅ Subscribe to EventBus via Gateway ----
const gatewaySocket = ioClient(GATEWAY_WS);

gatewaySocket.on("connect", () => {
  console.log("[BACKEND ↔ GATEWAY] connected");
});

gatewaySocket.on("event", (evt) => {
  if (!evt || !evt.topic) return;
  console.log("[BACKEND RECEIVED]", evt.topic, evt.payload);

  // Shipment status forwarded to frontend
  if (evt.topic === "order.shipped") {
    io.emit("order:shipped", evt.payload);
  }

  // Inventory sync
  if (evt.topic === "inventory.update") {
    const { productId, stock } = evt.payload;
    const p = products.find(x => x.id === productId);
    if (p) {
      p.stock = stock;
      broadcastStockUpdate(productId);
    }
  }
});

// ---- Frontend WebSocket ----
io.on("connection", (socket) => {
  console.log("[FRONTEND] connected", socket.id);
  socket.emit("products", products);

  socket.on("cart:add", async ({ productId }) => {
    const p = products.find(x => x.id === productId);
    if (!p || p.stock <= 0) return;

    p.stock -= 1;
    broadcastStockUpdate(productId);

    await publishToGateway("cart.add", {
      productId,
      timestamp: Date.now()
    });
  });

  socket.on("cart:remove", async ({ productId }) => {
    const p = products.find(x => x.id === productId);
    if (!p) return;

    p.stock += 1;
    broadcastStockUpdate(productId);

    await publishToGateway("cart.remove", {
      productId,
      timestamp: Date.now()
    });
  });

  socket.on("checkout", async (order) => {
    const orderId = Math.floor(Math.random()*900000)+100000;

    await publishToGateway("order.placed", {
      orderId,
      order,
      timestamp: Date.now()
    });

    socket.emit("order:confirmed", { orderId, success: true });
  });
});

server.listen(PORT, () =>
  console.log(`✅ Backend at http://localhost:${PORT}`)
);
