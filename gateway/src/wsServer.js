/**
 * wsServer.js
 * WebSocket server that bridges JSON ↔ Binary TCP protocol.
 *
 * Clients send JSON messages:
 *   { type: 'subscribe', topic: 'order.*', group: 'payment' }
 *   { type: 'publish',   topic: 'order.created', body: { ... } }
 *   { type: 'ack',       msgId: '123' }
 *   { type: 'nack',      msgId: '123' }
 *
 * Clients receive JSON messages:
 *   { type: 'event', topic: 'order.created', msgId: '123', body: { ... } }
 *   { type: 'error', message: '...' }
 */

import { WebSocketServer, WebSocket } from 'ws';
import {
  MsgType,
  encodeDataFrame,
  encodeSubscribe,
  encodeAck,
  decodeEventPayload,
  topicMatches,
} from './protocol.js';

export class WsServer {
  #port;
  #tcp;
  #wss = null;

  // Map: ws → Set<topic_pattern>  (what each WS client subscribed to)
  #clientTopics = new Map();

  // Map: topic_pattern → Set<ws>  (reverse index for dispatch)
  #topicClients = new Map();

  constructor(port, tcpClient) {
    this.#port = port;
    this.#tcp  = tcpClient;
  }

  start() {
    this.#wss = new WebSocketServer({ port: this.#port });

    this.#wss.on('listening', () => {
      console.log(`[WS] Gateway WebSocket server listening on port ${this.#port}`);
    });

    this.#wss.on('connection', (ws, req) => {
      const remoteAddr = req.socket.remoteAddress;
      console.log(`[WS] Client connected from ${remoteAddr}`);
      this.#clientTopics.set(ws, new Set());

      ws.on('message', (raw) => this.#handleMessage(ws, raw));

      ws.on('close', () => {
        console.log(`[WS] Client disconnected from ${remoteAddr}`);
        this.#cleanupClient(ws);
      });

      ws.on('error', (err) => {
        console.error('[WS] Client error:', err.message);
      });
    });

    // ── Route incoming TCP frames to WS clients ───────────────────────────
    this.#tcp.on('frame', ({ type, msgId, payload }) => {
      if (type === MsgType.EVENT_DATA) {
        const { topic, body } = decodeEventPayload(payload);
        this.#dispatchEvent(topic, msgId, body);
      } else if (type === MsgType.HEARTBEAT) {
        // Broker echoed our heartbeat — ignore
      } else if (type === MsgType.ERROR_RESP) {
        console.warn('[TCP] Broker sent ERROR_RESP');
      }
    });

    this.#tcp.on('connect', () => {
      this.#broadcastStatus('connected');
    });

    this.#tcp.on('disconnect', () => {
      this.#broadcastStatus('broker_disconnected');
    });
  }

  stop() {
    this.#wss?.close();
  }

  // ── Private ───────────────────────────────────────────────────────────────

  #handleMessage(ws, raw) {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      ws.send(JSON.stringify({ type: 'error', message: 'Invalid JSON' }));
      return;
    }

    switch (msg.type) {
      case 'subscribe': {
        const topic = String(msg.topic || '');
        const group = String(msg.group || '');
        if (!topic) return;
        this.#subscribe(ws, topic, group);
        break;
      }

      case 'publish': {
        const topic = String(msg.topic || '');
        const body  = msg.body ?? {};
        if (!topic) return;
        const frame = encodeDataFrame(MsgType.PUBLISH, topic, body);
        this.#tcp.send(frame);
        break;
      }

      case 'ack': {
        if (msg.msgId == null) return;
        this.#tcp.send(encodeAck(MsgType.ACK, BigInt(msg.msgId)));
        break;
      }

      case 'nack': {
        if (msg.msgId == null) return;
        this.#tcp.send(encodeAck(MsgType.NACK, BigInt(msg.msgId)));
        break;
      }

      default:
        ws.send(JSON.stringify({ type: 'error', message: `Unknown type: ${msg.type}` }));
    }
  }

  #subscribe(ws, topic, group) {
    // Track locally
    const clientSet = this.#clientTopics.get(ws);
    if (!clientSet) return;
    clientSet.add(topic);

    if (!this.#topicClients.has(topic)) {
      this.#topicClients.set(topic, new Set());
    }
    this.#topicClients.get(topic).add(ws);

    // Forward subscription to C++ broker
    this.#tcp.send(encodeSubscribe(topic, group));

    console.log(`[WS] SUBSCRIBE topic=${topic} group=${group || '(unique)'}`);

    ws.send(JSON.stringify({ type: 'subscribed', topic, group }));
  }

  #dispatchEvent(topic, msgId, body) {
    let dispatched = 0;

    // Parse body bytes to JSON if possible, otherwise keep as string
    let parsedBody;
    try {
      parsedBody = JSON.parse(body.toString('utf8'));
    } catch {
      parsedBody = body.toString('utf8');
    }

    const envelope = JSON.stringify({
      type:  'event',
      topic,
      msgId: msgId.toString(),   // BigInt → string (JSON-safe)
      body:  parsedBody,
    });

    for (const [pattern, clients] of this.#topicClients) {
      if (!topicMatches(pattern, topic)) continue;
      for (const ws of clients) {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(envelope);
          dispatched++;
        }
      }
    }

    if (dispatched > 0) {
      console.log(`[WS] Dispatched topic=${topic} msgId=${msgId} to ${dispatched} client(s)`);
    }
  }

  #broadcastStatus(status) {
    if (!this.#wss) return;
    const msg = JSON.stringify({ type: 'status', status });
    for (const ws of this.#wss.clients) {
      if (ws.readyState === WebSocket.OPEN) ws.send(msg);
    }
  }

  #cleanupClient(ws) {
    const topics = this.#clientTopics.get(ws) ?? new Set();
    for (const topic of topics) {
      const clients = this.#topicClients.get(topic);
      if (clients) {
        clients.delete(ws);
        if (clients.size === 0) this.#topicClients.delete(topic);
      }
    }
    this.#clientTopics.delete(ws);
  }
}
