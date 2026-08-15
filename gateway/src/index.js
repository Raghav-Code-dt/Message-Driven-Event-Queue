/**
 * index.js — Gateway entry point.
 * Wires TcpClient → WsServer and starts both.
 */

import 'dotenv/config';
import { TcpClient } from './tcpClient.js';
import { WsServer }  from './wsServer.js';

const TCP_HOST = process.env.TCP_HOST ?? 'localhost';
const TCP_PORT = parseInt(process.env.TCP_PORT  ?? '9099', 10);
const WS_PORT  = parseInt(process.env.WS_PORT   ?? '8080', 10);

const tcp = new TcpClient(TCP_HOST, TCP_PORT);
const ws  = new WsServer(WS_PORT, tcp);

ws.start();
tcp.connect();

// Graceful shutdown
function shutdown() {
  console.log('\n[Gateway] Shutting down...');
  tcp.stop();
  ws.stop();
  process.exit(0);
}

process.on('SIGINT',  shutdown);
process.on('SIGTERM', shutdown);

console.log(`[Gateway] Starting — Broker: ${TCP_HOST}:${TCP_PORT} | WS: ${WS_PORT}`);
