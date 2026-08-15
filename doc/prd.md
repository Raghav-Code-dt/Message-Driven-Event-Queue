# PRD: Message-Driven-Event-Queue — Production-Grade Pub-Sub Messaging Engine

> **Status:** Planning  
> **Authors:** Raghav + Antigravity  
> **Last Updated:** 2026-08-15  
> **Version:** 1.0

---

## 1. Background & Motivation

### What exists today

The current repository is a functional academic demo of a message-driven architecture. It spans multiple directories:

| Component | Location | Technology |
|---|---|---|
| C++ HTTP Event Broker (v1) | `eventbus/src/broker.cpp` | C++, httplib, nlohmann/json |
| C++ HTTP Event Broker (v2) | `eventbus2/src/main.cpp` | C++, httplib, ThreadSafeQueue |
| Node.js Gateway | `gateway/index.js` | Express, Socket.io, node-fetch |
| E-Commerce Backend | `ecommerce-sim/backend/index.js` | Express, Socket.io |
| E-Commerce Frontend | `ecommerce-sim/frontend/src/App.jsx` | React, Vite, Tailwind |
| Analytics Microservice | `MicroServices/analytics-service/index.js` | Node.js, Socket.io-client |
| Payment Microservice | `MicroServices/payment-service/index.js` | Node.js, Socket.io-client |
| Dashboard | `dashboard/src/App.jsx` | React, Recharts, Socket.io-client |

While the core idea—decouple producers from consumers via an event bus—is well-executed for a demo, the implementation has fundamental architectural flaws that make it unsuitable for real-world or systems-level engineering study.

### Current Shortcomings (Audit Findings)

The following issues were identified by a full code audit of all tracked source files:

#### SF-01 · Inefficient HTTP Polling Transport
- **File:** `gateway/index.js:38–51`
- **Problem:** The gateway uses `setInterval(500ms)` to HTTP GET `/events` from the C++ broker. This introduces up to **500ms of unnecessary latency** on every event and burns CPU/IO even when the queue is completely empty.
- **Root cause:** The C++ broker only exposes an HTTP REST interface; it has no push capability.

#### SF-02 · Destructive Reads / Single-Consumer Architecture
- **Files:** `eventbus/src/broker.cpp:23–32`, `eventbus2/src/EventBus.cpp:7–12`, `eventbus2/include/ThreadSafeQueue.hpp:17–26`
- **Problem:** `getEvents()` / `popAll()` drain the queue destructively. A second consumer (e.g., a second gateway for load balancing) would steal events from the first. This violates the core pub-sub fan-out contract where every subscriber receives every event.
- **Root cause:** Single global `std::queue` with no per-subscriber offset tracking.

#### SF-03 · Zero Durability (In-Memory Only)
- **Files:** `eventbus/src/broker.cpp:14`, `eventbus2/include/EventBus.hpp:9`
- **Problem:** All in-flight events live in `std::queue<json>` in process memory. A broker restart or crash permanently destroys all unprocessed messages. There is no Write-Ahead Log, no WAL, no persistence layer whatsoever.

#### SF-04 · No Acknowledgement / No Retry / No Dead-Letter Queue
- **File:** `gateway/index.js:44–46`
- **Problem:** The gateway broadcasts events to microservices via `io.emit(...)` — fire-and-forget. If the Payment or Analytics service crashes after receiving an event but before processing it, the event is permanently lost with no indication of failure and no retry mechanism.

#### SF-05 · Unfiltered Fan-out (Broadcast Anti-pattern)
- **Files:** `MicroServices/payment-service/index.js:19`, `MicroServices/analytics-service/index.js:23`
- **Problem:** The gateway broadcasts all events to all WebSocket clients simultaneously. Each microservice receives irrelevant events and discards them with an `if (evt.topic !== "...") return;` guard. This wastes network bandwidth and scales quadratically with the number of topics and subscribers.

#### SF-06 · Hardcoded Ports & URLs
- **Files:** All service `index.js` files
- **Problem:** `http://localhost:5000`, `http://localhost:9001`, `http://localhost:4000` etc. are hardcoded throughout. Makes Docker and cloud deployment impossible without source changes.

#### SF-07 · Fragmented Project Structure & No Monorepo
- **Problem:** Each service has its own isolated `package.json` and `node_modules`. Even critically, **`node_modules` is committed to Git** because the `.gitignore` was added after the fact and the files were never removed from the index. This bloats the repo with 10,000+ tracked files.
- **Impact:** No central way to start all services. Dependency updates must happen in 5+ places.

#### SF-08 · No Build System for C++ Components
- **Problem:** There is no `CMakeLists.txt` or `Makefile`. Build instructions are stored in `.vscode/run.txt` which only works with the exact VS Code setup. The project cannot be built on CI or in Docker without manual toolchain configuration.

#### SF-09 · Single-Threaded C++ Broker
- **Files:** `eventbus/src/broker.cpp:55`, `eventbus2/src/main.cpp:32`
- **Problem:** `httplib::Server` runs in single-threaded mode by default. All publish/consume calls are serialized, creating a hard throughput ceiling.

#### SF-10 · Typo / Syntax Error in Production Code
- **File:** `gateway/index.js:35`
- **Problem:** Line `res.json({ ok: true });1` has a dangling `1` after the semicolon. The code has never been linted and no automated tests exist to catch regressions.

---

## 2. Mission & Objective

Upgrade this repository from an academic HTTP-polling demo into a **high-performance, multithreaded, production-grade Pub-Sub Messaging Engine** built on native TCP, demonstrating real systems-level engineering skills including:

- Custom binary wire protocols over raw TCP
- MPMC (Multi-Producer Multi-Consumer) thread-safe data structures in C++20
- Correct delivery semantics with ACK/NACK/Timeout/DLQ
- Event-driven non-blocking I/O
- Monorepo project hygiene with Docker Compose orchestration
- Benchmarked throughput and latency characterization

---

## 3. Target Architecture

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
              |  [ACK Engine & DLQ]                       |
              |    - In-Flight Cache (msg_id -> deadline) |
              |    - Timeout Redelivery Thread            |
              |    - Dead Letter Queue                    |
              |         |                                 |
              |  [Write-Ahead Log (WAL)]                  |
              |    - Append-Only File / Replay            |
              +-------------------------------------------+
```

**Transport:** Replaced HTTP polling with a persistent, full-duplex **binary TCP stream** using a custom length-prefixed framing protocol.

---

## 4. Target Repository Layout

```
Message-Driven-Event-Queue/
├── broker/                         # C++ TCP Pub-Sub Broker
│   ├── include/
│   │   ├── SafeQueue.hpp           # MPMC Bounded Blocking Queue
│   │   ├── Message.hpp             # Message struct + MessageID
│   │   ├── TopicRouter.hpp         # Topic matching + consumer groups
│   │   ├── AckEngine.hpp           # In-flight cache + DLQ
│   │   ├── WalWriter.hpp           # Write-Ahead Log
│   │   └── TcpServer.hpp           # Non-blocking TCP server
│   ├── src/
│   │   ├── SafeQueue.cpp
│   │   ├── TopicRouter.cpp
│   │   ├── AckEngine.cpp
│   │   ├── WalWriter.cpp
│   │   ├── TcpServer.cpp
│   │   └── main.cpp
│   └── CMakeLists.txt
├── gateway/                        # Node.js TCP->WebSocket Gateway
│   ├── src/
│   │   ├── tcpClient.js            # Binary TCP framer + reconnect logic
│   │   ├── wsServer.js             # WebSocket server + topic routing
│   │   └── protocol.js             # Frame encode/decode helpers
│   └── package.json
├── services/                       # Microservices simulation
│   ├── order/index.js              # Publishes order.created
│   ├── payment/index.js            # SUB order.created -> PUB payment.*
│   └── notification/index.js       # SUB payment.success -> emit notification
├── dashboard/                      # React observability dashboard
│   └── src/App.jsx
├── benchmarks/
│   ├── load_test.js
│   └── results/
├── docker-compose.yml
├── .env.example
├── prd.md                          # This document
└── README.md
```

**Directories to archive/remove:** `eventbus/`, `eventbus2/`, `MicroServices/`, `ecommerce-sim/`

---

## 5. Detailed Implementation Specification

### 5.1 — Custom Binary Wire Protocol

Every frame over the TCP connection uses this fixed 17-byte header:

```
+-----------------------------------+
| magic        (4 bytes) 0xDEADBEEF |
+-----------------------------------+
| msg_type     (1 byte)             |
+-----------------------------------+
| payload_len  (4 bytes, big-endian)|
+-----------------------------------+
| msg_id       (8 bytes, big-endian)|
+-----------------------------------+
| payload      (payload_len bytes)  |
|  [topic_len(2)] [topic] [body]    |
+-----------------------------------+
```

| Value | Name | Direction | Description |
|---|---|---|---|
| `0x01` | `PUBLISH` | Client -> Broker | Publish a message to a topic |
| `0x02` | `SUBSCRIBE` | Client -> Broker | Subscribe to a topic / consumer group |
| `0x03` | `ACK` | Client -> Broker | Acknowledge successful message processing |
| `0x04` | `NACK` | Client -> Broker | Signal processing failure (trigger redelivery) |
| `0x05` | `HEARTBEAT` | Client <-> Broker | Keep-alive ping/pong |
| `0x06` | `EVENT_DATA` | Broker -> Client | Delivered event frame |
| `0x07` | `ERROR` | Broker -> Client | Error response frame |

---

### 5.2 — C++ Core: `SafeQueue<T>` (MPMC Bounded Blocking Queue)

Replaces the current unbounded `ThreadSafeQueue` that has no backpressure.

```cpp
template<typename T>
class SafeQueue {
public:
    explicit SafeQueue(std::size_t capacity);
    void push(T item);                                    // Blocks if full (backpressure)
    T pop();                                              // Blocks until item available
    std::optional<T> try_pop_for(std::chrono::milliseconds timeout);
    std::size_t size() const;
    std::size_t capacity() const;
    bool empty() const;
private:
    std::queue<T>           queue_;
    mutable std::mutex      mu_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::size_t             capacity_;
};
```

**Fixes SF-02 (single-consumer), SF-09 (serialized throughput).**

---

### 5.3 — C++ Core: `TopicRouter` & Consumer Groups

```cpp
class TopicRouter {
public:
    void subscribe(const std::string& topic, const std::string& group, int client_fd);
    void unsubscribe(int client_fd);
    // Independent groups all receive a copy.
    // Competing consumers (same group) get round-robin'd.
    std::vector<std::pair<int,uint64_t>> route(const Message& msg);
private:
    // topic -> group_name -> [subscriber fds]
    std::unordered_map<std::string,
        std::unordered_map<std::string, std::vector<int>>> subscriptions_;
    std::mutex mu_;
    bool matches(const std::string& pattern, const std::string& topic) const;
};
```

**Fixes SF-02, SF-05.**

---

### 5.4 — C++ Core: `AckEngine` (In-Flight Cache + DLQ)

```cpp
class AckEngine {
public:
    AckEngine(SafeQueue<Message>& main_queue, SafeQueue<Message>& dlq,
              std::chrono::milliseconds ack_timeout, int max_retries);
    uint64_t track(Message msg, int subscriber_fd);
    void ack(uint64_t msg_id);
    void nack(uint64_t msg_id);
    void redelivery_loop();   // Dedicated thread: scans for expired in-flight
    void stop();
private:
    std::unordered_map<uint64_t, InFlight> in_flight_;
    std::mutex                             mu_;
    std::atomic<uint64_t>                  next_id_{1};
    // ...
};
```

**Fixes SF-04.**

---

### 5.5 — Node.js Gateway Refactor

**`gateway/src/protocol.js`:** Binary frame encode/decode helpers.

**`gateway/src/tcpClient.js`:**
- Connects to broker via `net.Socket` on `TCP_HOST:TCP_PORT` (env)
- `readBuffer` accumulates bytes; emits complete `frame` events
- **Exponential backoff reconnect:** 100ms -> 200ms -> ... -> 30s cap
- Queues outbound frames during downtime; flushes on reconnect

**`gateway/src/wsServer.js`:**
- WebSocket server on `WS_PORT` (env, default `8080`)
- `subscribe` WS msg -> `SUBSCRIBE` TCP frame to broker
- `publish` WS msg -> `PUBLISH` TCP frame to broker
- `EVENT_DATA` TCP frame -> routes to subscribed WS clients by topic only

**Fixes SF-01, SF-05, SF-06.**

---

### 5.6 — Microservices Simulation (`/services`)

Three services demonstrating a complete distributed event-driven flow:

- **Order Service:** Publishes `order.created` with `{ orderId, amount, items }` every few seconds
- **Payment Service:** SUBs to `order.created`, validates, **sends ACK**, PUBs `payment.success` or `payment.failed`
- **Notification Service:** SUBs to `payment.*`, **sends ACK**, logs/emits user notifications

**Fixes SF-04, SF-05.**

---

### 5.7 — CMake Build System

```cmake
cmake_minimum_required(VERSION 3.20)
project(MQBroker CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -Wpedantic)

option(ENABLE_ASAN "AddressSanitizer"  OFF)
option(ENABLE_TSAN "ThreadSanitizer"   OFF)

add_executable(broker src/main.cpp src/TopicRouter.cpp
               src/AckEngine.cpp src/WalWriter.cpp src/TcpServer.cpp)
target_include_directories(broker PRIVATE include)
target_link_libraries(broker PRIVATE pthread)
```

**Fixes SF-08.**

---

### 5.8 — Monorepo & Environment Config

**Root `package.json`** (npm workspaces) with a single `npm run dev` command starts all Node.js services concurrently.

**`.env.example`:**
```dotenv
TCP_HOST=localhost
TCP_PORT=9099
WS_PORT=8080
ACK_TIMEOUT_MS=5000
MAX_RETRIES=3
QUEUE_CAPACITY=10000
WAL_PATH=./data/wal.log
```

**Fixes SF-06, SF-07.**

---

### 5.9 — Docker Compose

Single `docker-compose.yml` starts `broker`, `gateway`, `order-service`, `payment-service`, `notification-service`, and `dashboard` with proper `depends_on` ordering and environment variable injection.

---

### 5.10 — Benchmarking

`benchmarks/load_test.js` measures:
- **Throughput:** Messages published per second (target: >10k msg/s)
- **Latency percentiles:** p50, p95, p99 end-to-end (publish -> delivery -> ACK)
- **Backpressure behavior:** Queue fill rate and producer blocking time

---

## 6. Code Quality Standards

| Area | Standard |
|---|---|
| C++ standard | C++20 strict |
| Warnings | `-Wall -Wextra -Wpedantic`, zero tolerance |
| Memory safety | RAII, `unique_ptr`/`shared_ptr`, no raw `new`/`delete`, ASAN clean |
| Thread safety | All shared state guarded, TSAN clean |
| Node.js | ESM, `async/await`, try/catch on all async paths |
| Binary data | `Buffer` only, no string coercion of binary frames |
| Config | All values via `process.env`, no hardcoded URLs or ports |
| Docs | JSDoc on all public JS modules, Doxygen on all public C++ classes |

---

## 7. Phased Implementation Plan

| Phase | Scope | Deliverable |
|---|---|---|
| **P1** | C++ `SafeQueue`, `Message`, `TopicRouter` | Core data structures |
| **P2** | C++ `TcpServer`, wire protocol framer | Broker accepts TCP connections |
| **P3** | C++ `AckEngine`, `WalWriter` | Full delivery semantics + durability |
| **P4** | Node.js Gateway (`tcpClient`, `wsServer`, `protocol`) | TCP <-> WebSocket bridge |
| **P5** | Microservices (`order`, `payment`, `notification`) | End-to-end ACK flow |
| **P6** | CMake, Docker Compose, `.env` config | One-command build and run |
| **P7** | Benchmarks, Dashboard enhancements, README | Performance data + final polish |

---

## 8. What Is Preserved & Enhanced

| Component | Action | Reason |
|---|---|---|
| `dashboard/src/App.jsx` | Enhance | Good bones — will add ACK status, DLQ events, latency metrics |
| Order -> Payment -> Notification domain | Re-implement in `/services` | Well-chosen domain for demonstrating pub-sub value |
| `eventbus/`, `eventbus2/`, `MicroServices/`, `ecommerce-sim/` | Archive / remove | Superseded by new structure |

---

## 9. Success Criteria

| Criterion | Target |
|---|---|
| Concurrent clients | >= 10 simultaneous without deadlock |
| End-to-end throughput | >= 10,000 msg/sec on local loopback |
| p99 end-to-end latency | < 10ms on local loopback |
| Unacknowledged message redelivery | Verified by killing payment service mid-flight |
| DLQ routing | Verified after `max_retries` exhausted |
| WAL recovery | Broker restart replays pending messages |
| ThreadSanitizer | Zero data races reported |
| AddressSanitizer | Zero memory errors reported |
| `docker-compose up` | All services communicate end-to-end |

---

*This document supersedes the previous shortcomings analysis and serves as the single source of truth for this refactor.*