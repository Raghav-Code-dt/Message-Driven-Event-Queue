# Performance & Architecture Stabilization

This document explains the core architecture of the Message-Driven Event Queue and details the aggressive optimizations that allowed us to scale from ~5k messages/second to over **21k messages/second** with sub-millisecond latencies.

## Core Architecture

The system is composed of two primary layers:
1. **The C++ Broker:** A highly optimized, multi-threaded TCP server that handles message persistence (WAL), topic routing, and reliable delivery (AckEngine).
2. **The Node.js Gateway:** A proxy that bridges JSON WebSockets to the binary TCP protocol. It handles client connections, idempotency, and protocol translation.

The communication between the Gateway and the Broker uses a custom **17-byte binary TCP protocol**, completely eliminating the overhead of parsing HTTP headers or JSON strings on the C++ side.

---

## How We Stabilized and Optimized Performance

Achieving >20k msgs/sec required identifying and eliminating several hidden bottlenecks across both Node.js and C++.

### 1. Nagle's Algorithm & TCP Batching
**The Problem:** We initially disabled Nagle's Algorithm (`TCP_NODELAY = true`) on the C++ Broker to reduce latency. However, this caused the Broker to send 50,000 tiny 60-byte packets over the Docker bridge network. The overhead of individual system calls and packet headers destroyed our throughput.
**The Fix:** We re-enabled Nagle's algorithm on the C++ Broker so the OS can intelligently batch our tiny frames into large 1,400-byte MTU packets. On the Node.js side, we kept `TCP_NODELAY` enabled but implemented strict **Application-Level TCP Batching** using `socket.cork()` and `process.nextTick() -> uncork()`. This guarantees massive pipelined throughput without relying on unpredictable OS delays.

### 2. Zero-Copy Buffer Framing in Node.js
**The Problem:** The Gateway was originally using `Buffer.concat()` or `Buffer.from()` to accumulate and slice incoming TCP streams. At 50,000 messages/sec, this triggered millions of memory allocations (`malloc`), causing the V8 Garbage Collector to constantly "stop the world" and freeze the Gateway.
**The Fix:** We replaced the buffer logic with a pre-allocated 2MB circular buffer. When parsing frames, we use `Buffer.subarray()`, which is a strictly zero-copy operation that simply points to existing memory. The V8 GC now does zero work during message streaming.

### 3. O(1) Idempotency Ring Buffer
**The Problem:** To prevent duplicate messages, the Gateway tracked the last 50,000 message IDs in an array. When it exceeded 50k, it called `Array.shift()` to remove the oldest entry. `shift()` is an `O(N)` operation because it shifts every memory pointer forward. At scale, this resulted in 2.5 billion memory shifts, dropping throughput to 6k msgs/s.
**The Fix:** We replaced the dynamic array with a fixed-size `Array(50000)` and a modulo `%` pointer (a true Ring Buffer). Inserting and evicting IDs is now strictly `O(1)`.

### 4. Asynchronous Group-Commit WAL
**The Problem:** The C++ Broker was performing a synchronous `std::ofstream::write` inside a Mutex lock every time a message was published, blocking the network threads while waiting for disk I/O.
**The Fix:** We upgraded the `WalWriter` to use a double-buffered background thread. The main thread pushes the message into a fast `std::vector` memory buffer and returns instantly. A background thread wakes up every 5ms, swaps the active buffer, and writes the entire batch to disk at once (Group Commit).

### 5. O(1) Min-Heap AckEngine Timeouts
**The Problem:** The `AckEngine` was storing unacknowledged messages in a `std::unordered_map`. A background thread locked the global Mutex every 200ms and iterated over all 50,000 items (`O(N)`) to check for expired timeouts.
**The Fix:** We added a `std::priority_queue` (Min-Heap) sorted by deadline. The background thread now checks only the very top item (`O(1)`). If it hasn't expired, the thread goes back to sleep instantly.

### 6. Eliminating Cascading Backpressure from `stdout`
**The Problem:** If a benchmark script exited abruptly without sending its final ACKs, the Broker would hold the messages in memory. When they eventually timed out, the Broker requeued them and logged `std::cout << "[AckEngine] Timeout..."` for every message. `std::cout` is synchronous, so logging 5,000 timeouts blocked the global Mutex for an entire second. This blocked the network threads, backed up the TCP buffers, and throttled new incoming connections.
**The Fix:** We removed verbose logging from the critical `redelivery_loop` and added a `setTimeout` to the Node.js benchmark client to ensure all ACKs are flushed to the OS before the process exits.
