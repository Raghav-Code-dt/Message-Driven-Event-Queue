/**
 * tcpClient.js
 * TCP client connecting to the C++ broker.
 * - Accumulates incoming bytes and emits complete parsed frames.
 * - Reconnects with exponential backoff (100ms → 30s cap).
 * - Queues outbound frames during broker downtime and flushes on reconnect.
 */

import net          from 'net';
import { EventEmitter } from 'events';
import {
  HEADER_SIZE,
  decodeHeader,
  decodeEventPayload,
  encodeHeartbeat,
} from './protocol.js';

const RECONNECT_MIN_MS = 100;
const RECONNECT_MAX_MS = 30_000;
const HEARTBEAT_MS     = 10_000;

export class TcpClient extends EventEmitter {
  #host;
  #port;
  #socket      = null;
  #readBuf     = Buffer.allocUnsafe(0);
  #outQueue    = [];           // frames pending while disconnected
  #connected   = false;
  #delay       = RECONNECT_MIN_MS;
  #hbTimer     = null;
  #stopping    = false;

  constructor(host, port) {
    super();
    this.#host = host;
    this.#port = port;
  }

  connect() {
    if (this.#stopping) return;

    this.#socket = net.createConnection({ host: this.#host, port: this.#port });

    this.#socket.on('connect', () => {
      this.#connected = true;
      this.#delay     = RECONNECT_MIN_MS; // reset backoff
      console.log(`[TCP] Connected to broker at ${this.#host}:${this.#port}`);
      this.emit('connect');

      // Flush queued frames
      for (const frame of this.#outQueue) {
        this.#socket.write(frame);
      }
      this.#outQueue = [];

      // Heartbeat to keep connection alive
      this.#hbTimer = setInterval(() => {
        if (this.#connected) this.#socket.write(encodeHeartbeat());
      }, HEARTBEAT_MS);
    });

    this.#socket.on('data', (chunk) => {
      // Strict Buffer accumulation — never coerce to string
      this.#readBuf = Buffer.concat([this.#readBuf, chunk]);
      this.#parseFrames();
    });

    this.#socket.on('close', () => {
      this.#onDisconnect();
    });

    this.#socket.on('error', (err) => {
      // 'close' will follow, so just log here
      console.error('[TCP] Socket error:', err.message);
    });
  }

  /** Send a pre-encoded Buffer frame to the broker. */
  send(frame) {
    if (!Buffer.isBuffer(frame))
      throw new TypeError('[TCP] send() requires a Buffer');

    if (this.#connected && this.#socket) {
      this.#socket.write(frame);
    } else {
      // Queue while disconnected — hard cap to prevent unbounded growth
      if (this.#outQueue.length < 10_000) {
        this.#outQueue.push(frame);
      } else {
        console.warn('[TCP] Outbound queue full, dropping frame');
      }
    }
  }

  stop() {
    this.#stopping = true;
    clearInterval(this.#hbTimer);
    this.#socket?.destroy();
  }

  get isConnected() { return this.#connected; }

  // ── Private ────────────────────────────────────────────────────────────────

  #parseFrames() {
    while (this.#readBuf.length >= HEADER_SIZE) {
      const header = decodeHeader(this.#readBuf);

      if (!header.valid) {
        console.error('[TCP] Bad magic byte — dropping connection');
        this.#socket.destroy();
        return;
      }

      const totalSize = HEADER_SIZE + header.payloadLen;
      if (this.#readBuf.length < totalSize) break; // Wait for more bytes

      // Extract payload as a slice (no string conversion)
      const payload = this.#readBuf.slice(HEADER_SIZE, totalSize);

      // Consume processed bytes
      this.#readBuf = this.#readBuf.slice(totalSize);

      // Emit the parsed frame to wsServer
      this.emit('frame', {
        type:    header.type,
        msgId:   header.msgId,   // BigInt
        payload,                  // raw Buffer
      });
    }
  }

  #onDisconnect() {
    this.#connected = false;
    clearInterval(this.#hbTimer);
    this.emit('disconnect');

    if (this.#stopping) return;

    console.log(`[TCP] Disconnected. Reconnecting in ${this.#delay}ms...`);
    setTimeout(() => this.connect(), this.#delay);
    this.#delay = Math.min(this.#delay * 2, RECONNECT_MAX_MS);
  }
}
