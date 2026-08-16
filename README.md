# Message-Driven-Event-Queue

A high-performance, multithreaded, production-grade Pub-Sub Messaging Engine built on a custom binary TCP protocol.

This repository demonstrates systems-level engineering concepts including:
- Custom binary wire framing over raw TCP streams
- MPMC (Multi-Producer Multi-Consumer) thread-safe data structures in C++20
- Correct delivery semantics with Acknowledgements (ACK), Timeouts, and Dead-Letter Queues (DLQ)
- Application-level Idempotency and Backpressure controls
- Persistent Write-Ahead Logging (WAL) for durability and recovery
- Event-driven non-blocking I/O Architecture

## Architecture

```
[ Browser Dashboard ]   [ Microservices (Order / Payment / Notification) ]
         ^                               ^
         |          (WebSockets / JSON)  |
         +---------------+---------------+
                         |
              +----------v------------------+
              |   Node.js TCP Gateway       |  :8080 (WS) -> :9099 (TCP)
              |   - Stream Framer           |
              |   - Reconnect + Backoff     |
              |   - Topic Subscription      |
              |   - O(1) Idempotency Ring   |
              |   - Telemetry Aggregator    |
              +----------+------------------+
                         |  (Binary Length-Prefixed TCP)
                         v
              +-------------------------------------------+
              |        C++ TCP Broker  :9099              |
              |                                           |
              |  [MPMC Bounded Blocking Queue]            |
              |         |                                 |
              |  [TopicRouter / ConsumerGroups]           |
              |    - Wildcard Matching                    |
              |    - Round-Robin Distribution             |
              |         |                                 |
              |  [Ack Engine & DLQ]                       |
              |    - In-Flight Cache (msg_id -> deadline) |
              |    - Timeout Redelivery Thread            |
              |    - Publishes $SYS.dlq to Dashboard      |
              |         |                                 |
              |  [Write-Ahead Log (WAL)]                  |
              |    - Append-Only File / Crash Recovery    |
              +-------------------------------------------+
```

## Getting Started

Ensure you have Docker and Docker Compose installed. 

To spin up the entire cluster (C++ Broker, Node.js Gateway, Microservices, and the React Dashboard):

```bash
docker-compose down -v
docker-compose up --build
```

**Access the Dashboard:**
Navigate to [http://localhost:5173](http://localhost:5173) in your browser to watch the real-time events flowing between the microservices.

## Benchmarks

The custom binary TCP protocol and C++ core are heavily optimized to prevent lock contention, avoid Nagle's Algorithm latency, and ensure non-blocking Write-Ahead Log (WAL) disk persistence on the critical path. The system utilizes a Sliding Window congestion control model to maintain high throughput without artificially stalling.

To run the load test:
```bash
$env:MAX_IN_FLIGHT=500
$env:TOTAL_MESSAGES=100000
npm run start -w mq-benchmarks
```

**Results (Localhost / Docker Desktop / Windows):**
- **Throughput:** ~20,000 messages/sec
- **p50 Latency (End-to-End):** ~22 ms (including Node.js Gateway ↔ C++ Broker ↔ Disk WAL Persistence ↔ WebSocket ↔ Client)
- **Reliability:** 100% delivery rate with strict O(1) Idempotency Ring Buffers and zero TCP pipeline deadlocks.
- **Architectural Highlights:**
  - O(N) Array loops replaced with O(1) Ring Buffers (Set + Array eviction).
  - Explicit `TCP_NODELAY` and scaled socket buffers (256KB) eliminate kernel queueing spikes.
  - Aggregated backpressure telemetry protects downstream Dashboard WebSockets.

## Wire Protocol Specification

The C++ Broker and Node.js Gateway communicate using a custom length-prefixed binary TCP protocol. Every frame starts with a fixed **17-byte header**:

| Offset | Size (Bytes) | Type | Description |
|--------|-------------|------|-------------|
| 0 | 4 | `uint32_t` | **Total Frame Length** (excluding this 4-byte field). Used for stream framing. |
| 4 | 1 | `uint8_t` | **Message Type** (0=SUBSCRIBE, 1=PUBLISH, 2=EVENT_DATA, 3=ACK, 4=NACK, 5=HEARTBEAT) |
| 5 | 8 | `uint64_t` | **Message ID**. Auto-incrementing identifier for ACKs/NACKs and Idempotency. |
| 13 | 4 | `uint32_t` | **Topic Length**. Number of bytes in the topic string. |
| 17 | `var` | `string` | **Topic**. The UTF-8 string topic (e.g. `order.created`). |
| 17 + len | `var` | `bytes` | **Payload**. The actual JSON payload or raw bytes. |

## Microservices Simulation

The repository includes a simulated e-commerce backend built on top of the pub-sub engine to demonstrate real-world usage:

1. **Order Service:** Publishes `order.created` messages.
2. **Payment Service:** Subscribes to `order.created`, processes the payment, ACKs the message, and publishes `payment.success`.
3. **Notification Service:** Subscribes to `payment.*`, ACKs the message, and increments its success counters.

If any service crashes mid-flight, the broker's `AckEngine` will wait 5 seconds and automatically redeliver the message. If the message fails 3 times, it is routed to the Dead-Letter Queue (DLQ) and published as a `$SYS.dlq` event to the dashboard.

## Project Checklist

- [x] 1. C++20 Core MPMC Queue & Topic Router
- [x] 2. 17-Byte Binary Length-Prefixed Wire Protocol & TCP Server
- [x] 3. Asynchronous Double-Buffered Write-Ahead Log (WAL)
- [x] 4. At-Least-Once Delivery Semantics (ACK / Timeout / DLQ Engine)
- [x] 5. High-Performance Sliding-Window Benchmark (22.8k msg/sec)
- [x] 6. Native WebSocket Gateway & React Dashboard Connection
- [x] 7. Automated Chaos & Fault-Tolerance Demo Script (High Interview Impact)
- [x] 8. Docker Compose One-Command Orchestration (`docker-compose up`)
- [x] 9. Repository Cleanup (Purge legacy folders & committed node_modules)
- [x] 10. Polished GitHub README (Architecture diagrams, wire specs, benchmarks)
- [x] 11. GitHub Actions CI (Sanitizers & automated build validation)
