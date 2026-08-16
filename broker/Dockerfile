# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM gcc:13 AS builder

WORKDIR /build
COPY include/ ./include/
COPY src/     ./src/

RUN g++ -std=c++20 -I include -O3 -DNDEBUG -Wall \
    src/TopicRouter.cpp \
    src/TcpServer.cpp   \
    src/AckEngine.cpp   \
    src/WalWriter.cpp   \
    src/main.cpp        \
    -o broker           \
    -pthread -static-libgcc -static-libstdc++

# ── Stage 2: Minimal runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim

WORKDIR /app
RUN mkdir -p data

COPY --from=builder /build/broker ./broker

EXPOSE 9099

# Use a simpler healthcheck or just let the gateway reconnect logic handle it
HEALTHCHECK NONE

ENV TCP_PORT=9099 \
    QUEUE_CAPACITY=10000 \
    ACK_TIMEOUT_MS=5000  \
    MAX_RETRIES=3        \
    WAL_PATH=/app/data/broker.wal

CMD ["./broker"]
