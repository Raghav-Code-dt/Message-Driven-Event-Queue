import express from "express";
import fetch from "node-fetch";
import { Server } from "socket.io";
import http from "http";

const app = express();
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const EVENTBUS = "http://localhost:9001"; // C++ Broker

// ✅ Utility: Enrich event if missing metadata
function enrichEvent(evt) {
  return {
    ...evt,
    timestamp: evt.timestamp || Date.now(),
    service: evt.service || "broker",
    subscribers: evt.subscribers || "dashboard, ecomm"
  };
}

// Backend publishes to C++ broker through gateway
app.post("/emit", async (req, res) => {
  const data = enrichEvent(req.body);

  await fetch(EVENTBUS + "/publish", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data)
  });

  res.json({ ok: true });
});1

// Poll broker for events -> forward to Dashboard + Backend
setInterval(async () => {
  try {
    const r = await fetch(EVENTBUS + "/events");
    const events = await r.json();

    if (events.length > 0) {
      events.forEach(evt => {
        io.emit("event", enrichEvent(evt)); // broadcast enriched
      });
    }
  } catch (err) {
    console.log("Gateway Poll Error:", err.message);
  }
}, 500);

// WS Connections from Backend + Dashboard
io.on("connection", socket => {
  console.log("🔌 Gateway WebSocket client:", socket.id);
});

server.listen(5000, () =>
  console.log("🚪 Gateway running at http://localhost:5000")
);
