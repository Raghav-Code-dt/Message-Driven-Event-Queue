/**
 * gatewayClient.js — WebSocket client for the MQ gateway.
 * Handles reconnect with exponential backoff and clean subscribe/publish/ack API.
 */

import WebSocket    from 'ws';
import { EventEmitter } from 'events';

const RECONNECT_MIN_MS = 1_000;
const RECONNECT_MAX_MS = 30_000;

export class GatewayClient extends EventEmitter {
  #url;
  #ws           = null;
  #delay        = RECONNECT_MIN_MS;
  #stopping     = false;
  #pendingQueue = [];

  constructor(url) {
    super();
    this.#url = url;
  }

  connect() {
    if (this.#stopping) return;
    this.#ws = new WebSocket(this.#url);

    this.#ws.on('open', () => {
      console.log(`[GW] Connected to ${this.#url}`);
      this.#delay = RECONNECT_MIN_MS;
      this.emit('connect');
      for (const msg of this.#pendingQueue) this.#sendRaw(msg);
      this.#pendingQueue = [];
    });

    this.#ws.on('message', (raw) => {
      let msg;
      try { msg = JSON.parse(raw.toString()); } catch { return; }
      this.emit('message', msg);
      if (msg.type === 'event')  this.emit('event', msg);
      if (msg.type === 'status') this.emit('status', msg.status);
    });

    this.#ws.on('close', () => {
      if (this.#stopping) return;
      console.log(`[GW] Disconnected. Reconnecting in ${this.#delay}ms...`);
      this.emit('disconnect');
      setTimeout(() => this.connect(), this.#delay);
      this.#delay = Math.min(this.#delay * 2, RECONNECT_MAX_MS);
    });

    this.#ws.on('error', (err) => console.error('[GW] Error:', err.message));
  }

  subscribe(topic, group = '') { this.#send({ type: 'subscribe', topic, group }); }
  publish(topic, body)         { this.#send({ type: 'publish', topic, body });    }
  ack(msgId)                   { this.#send({ type: 'ack',  msgId: String(msgId) }); }
  nack(msgId)                  { this.#send({ type: 'nack', msgId: String(msgId) }); }
  stop() { this.#stopping = true; this.#ws?.close(); }

  #send(msg) {
    if (this.#ws?.readyState === WebSocket.OPEN) this.#sendRaw(msg);
    else this.#pendingQueue.push(msg);
  }
  #sendRaw(msg) { this.#ws.send(JSON.stringify(msg)); }
}
