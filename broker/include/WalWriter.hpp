#pragma once

#include <string>
#include <mutex>
#include <fstream>
#include "Message.hpp"
#include "SafeQueue.hpp"

namespace broker {

// ── Write-Ahead Log ───────────────────────────────────────────────────────────
//
// Binary format (append-only):
//
//  PUBLISH record:
//   [1 byte]  record_type = 0x01
//   [8 bytes] msg_id      (BE)
//   [2 bytes] topic_len   (BE)
//   [N bytes] topic
//   [4 bytes] body_len    (BE)
//   [N bytes] body
//
//  ACK record:
//   [1 byte]  record_type = 0x02
//   [8 bytes] msg_id      (BE)
//
// Replay:  read all PUBLISH records into a map, erase any that have a
//          matching ACK record, push survivors back into the SafeQueue.
// ─────────────────────────────────────────────────────────────────────────────

class WalWriter {
public:
    explicit WalWriter(const std::string& path);
    ~WalWriter();

    // Append a PUBLISH record to the WAL. Thread-safe.
    void append(const Message& msg);

    // Append an ACK record. Thread-safe.
    void acknowledge(uint64_t msg_id);

    // On broker startup: replay unacknowledged messages into the queue.
    void replay(SafeQueue<Message>& queue);

private:
    void write_be64(std::ostream& out, uint64_t v);
    void write_be32(std::ostream& out, uint32_t v);
    void write_be16(std::ostream& out, uint16_t v);

    uint64_t read_be64(std::istream& in);
    uint32_t read_be32(std::istream& in);
    uint16_t read_be16(std::istream& in);

    std::string   path_;
    std::ofstream file_;
    std::mutex    mu_;
};

} // namespace broker
