# Performance Stabilization & Architecture Guide

This document outlines the architectural decisions and performance optimizations implemented to stabilize the Message-Driven Event Queue at **~20,000 msg/sec** with **sub-25ms latency**.

## 1. System Architecture
- **C++ Broker**: A high-performance TCP server written in C++ that manages pub/sub routing, client connections, and persistent Write-Ahead Log (WAL) storage.
- **Node.js Gateway**: A middleware bridge that translates WebSocket connections (JSON) from frontend/microservices into binary TCP frames for the Broker.
- **Microservices**: Independent Node.js consumers (Order, Payment, Notification) that utilize Competing Consumer groups to process events at scale.

## 2. Key Performance Bottlenecks & Solutions

### A. Idempotency Ring Buffer (O(N) to O(1))
**Problem:** The Gateway initially used a growing `Array.includes()` to prevent duplicate message deliveries. At 20k msg/sec, this `O(N)` lookup caused catastrophic CPU spikes and blocked the Node.js event loop.
**Solution:** Refactored to an `O(1)` Ring Buffer combining a JavaScript `Set` (for instant lookups) and a pre-allocated fixed-size `Array` (to track the chronological order for evictions).

### B. OS Kernel Buffering (Nagle's Algorithm)
**Problem:** Small control frames (like 17-byte ACKs) were being artificially delayed by up to 40ms by the OS kernel's Nagle's Algorithm.
**Solution:** Explicitly enabled `TCP_NODELAY` on both the C++ Broker (`setsockopt`) and Node.js Gateway (`socket.setNoDelay()`). Additionally, scaled `SO_RCVBUF` and `SO_SNDBUF` to `256 KB` to accommodate massive throughput spikes without packet dropping.

### C. Congestion Control & Sliding Windows
**Problem:** The load-testing benchmark utilized "Stop-and-Wait Batching" (blasting 2,000 messages, then stalling until all were processed). This artificially inflated queue sizes, resulting in `p50` latencies > 200ms and pipeline deadlocks.
**Solution:** Implemented a robust **Sliding Window Algorithm**. The system now maintains a continuous in-flight threshold (`MAX_IN_FLIGHT = 500`). For every ACK received, a new message is instantly dispatched. This fully saturates the Bandwidth-Delay Product (BDP) while keeping median latency under 25ms.

### D. C++ Write-Ahead Log (WAL) Deadlocks
**Problem:** The background WAL flush thread would hang if the byte buffer threshold (64 KB) wasn't reached, leaving small batches stranded in memory.
**Solution:** Tightened the C++ `condition_variable.wait_for` timeout to `2ms`. This guarantees that even tiny message streams are aggressively persisted to disk without blocking the main event routing loop.

### E. Telemetry Backpressure (Observer Effect)
**Problem:** Attempting to stream 20,000 `EVENT_DATA` frames directly to the React Dashboard over WebSockets crashed the browser and starved the Gateway CPU.
**Solution:** Implemented backend telemetry aggregation. The Gateway calculates throughput and memory internally, broadcasting a summarized `$SYS.stats` packet to the Dashboard exactly once per second.

---

## 3. Benchmark Results
Running locally on a standard developer machine (Dockerized stack):
- **Throughput:** ~21,300 msg/sec
- **Median Latency (p50):** 11.3 ms
- **Reliability:** 100% Delivery with WAL persistence enabled.
