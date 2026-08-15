# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM gcc:13 AS builder

WORKDIR /build
COPY include/ ./include/
COPY src/     ./src/

RUN g++ -std=c++20 -I include -O2 -Wall \
    src/TopicRouter.cpp \
    src/TcpServer.cpp   \
    src/AckEngine.cpp   \
    src/WalWriter.cpp   \
    src/main.cpp        \
    -o broker           \
    -pthread -static-libgcc -static-libstdc++

# ── Stage 2: Minimal runtime ──────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN useradd -r -u 1001 broker
WORKDIR /app
RUN mkdir -p data && chown broker:broker data

COPY --from=builder /build/broker ./broker

USER broker

EXPOSE 9099

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD bash -c 'echo > /dev/tcp/localhost/9099' 2>/dev/null && echo OK || exit 1

ENV TCP_PORT=9099 \
    QUEUE_CAPACITY=10000 \
    ACK_TIMEOUT_MS=5000  \
    MAX_RETRIES=3        \
    WAL_PATH=/app/data/broker.wal

CMD ["./broker"]
