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
  #readBuf     = Buffer.allocUnsafe(2 * 1024 * 1024); // 2MB pre-allocated
  #readLen     = 0;
  #outQueue    = [];           // frames pending while disconnected
  #connected   = false;
  #delay       = RECONNECT_MIN_MS;
  #hbTimer     = null;
  #stopping    = false;
  #corkTimer   = null;

  constructor(host, port) {
    super();
    this.#host = host;
    this.#port = port;
  }

  connect() {
    if (this.#stopping) return;

    this.#socket = net.createConnection({ host: this.#host, port: this.#port });

    this.#socket.on('connect', () => {
      this.#socket.setNoDelay(true); // TCP_NODELAY for predictable latency
      this.#connected = true;
      this.#delay     = RECONNECT_MIN_MS; // reset backoff
      console.log(`[TCP] Connected to broker at ${this.#host}:${this.#port}`);
      this.emit('connect');

      // Flush queued frames
      for (const frame of this.#outQueue) {
        this.send(frame);
      }
      this.#outQueue = [];

      // Heartbeat to keep connection alive
      this.#hbTimer = setInterval(() => {
        if (this.#connected) this.send(encodeHeartbeat());
      }, HEARTBEAT_MS);
    });

    this.#socket.on('data', (chunk) => {
      // Zero-copy framing accumulation
      if (this.#readLen + chunk.length > this.#readBuf.length) {
         const newBuf = Buffer.allocUnsafe(Math.max(this.#readBuf.length * 2, this.#readLen + chunk.length));
         this.#readBuf.copy(newBuf, 0, 0, this.#readLen);
         this.#readBuf = newBuf;
      }
      chunk.copy(this.#readBuf, this.#readLen);
      this.#readLen += chunk.length;
      this.#parseFrames();
    });

    this.#socket.on('close', () => {
      this.#onDisconnect();
    });

    this.#socket.on('error', (err) => {
      console.error('[TCP] Socket error:', err.message);
    });
  }

  /** Send a pre-encoded Buffer frame to the broker. */
  send(frame) {
    if (!Buffer.isBuffer(frame))
      throw new TypeError('[TCP] send() requires a Buffer');

    if (this.#connected && this.#socket) {
      this.#socket.cork(); // Buffer writes at the application level
      this.#socket.write(frame);
      
      if (!this.#corkTimer) {
        this.#corkTimer = process.nextTick(() => {
          this.#socket.uncork(); // Flush the batched frames as one TCP packet
          this.#corkTimer = null;
        });
      }
    } else {
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
    let offset = 0;
    while (this.#readLen - offset >= HEADER_SIZE) {
      const header = decodeHeader(this.#readBuf.subarray(offset));

      if (!header.valid) {
        console.error('[TCP] Bad magic byte — dropping connection');
        this.#socket.destroy();
        return;
      }

      const totalSize = HEADER_SIZE + header.payloadLen;
      if (this.#readLen - offset < totalSize) break; // Wait for more bytes

      // EventEmitter is synchronous. This subarray view is consumed instantly by wsServer.
      const payload = this.#readBuf.subarray(offset + HEADER_SIZE, offset + totalSize);
      offset += totalSize;

      this.emit('frame', {
        type:    header.type,
        msgId:   header.msgId,
        payload,
      });
    }

    if (offset > 0) {
      if (offset === this.#readLen) {
        this.#readLen = 0; // consumed everything
      } else {
        // Shift remaining bytes
        this.#readBuf.copy(this.#readBuf, 0, offset, this.#readLen);
        this.#readLen -= offset;
      }
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
