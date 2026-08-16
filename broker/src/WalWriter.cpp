#include "WalWriter.hpp"
#include "Protocol.hpp"   // for to_be* helpers
#include <iostream>
#include <fstream>
#include <unordered_map>
#include <unordered_set>
#include <stdexcept>
#include <filesystem>

namespace broker {

static constexpr uint8_t WAL_PUBLISH = 0x01;
static constexpr uint8_t WAL_ACK     = 0x02;

// ── Big-Endian stream helpers ─────────────────────────────────────────────────

void WalWriter::write_be64(std::vector<uint8_t>& out, uint64_t v) {
    uint64_t be = to_be64(v);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&be);
    out.insert(out.end(), p, p + 8);
}
void WalWriter::write_be32(std::vector<uint8_t>& out, uint32_t v) {
    uint32_t be = to_be32(v);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&be);
    out.insert(out.end(), p, p + 4);
}
void WalWriter::write_be16(std::vector<uint8_t>& out, uint16_t v) {
    uint16_t be = static_cast<uint16_t>((v >> 8) | (v << 8));
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&be);
    out.insert(out.end(), p, p + 2);
}

uint64_t WalWriter::read_be64(std::istream& in) {
    uint64_t raw = 0;
    in.read(reinterpret_cast<char*>(&raw), 8);
    return to_be64(raw);
}
uint32_t WalWriter::read_be32(std::istream& in) {
    uint32_t raw = 0;
    in.read(reinterpret_cast<char*>(&raw), 4);
    return from_be32(raw);
}
uint16_t WalWriter::read_be16(std::istream& in) {
    uint16_t raw = 0;
    in.read(reinterpret_cast<char*>(&raw), 2);
    return static_cast<uint16_t>((raw >> 8) | (raw << 8));
}

// ── Constructor / Destructor ──────────────────────────────────────────────────

WalWriter::WalWriter(const std::string& path) : path_(path) {
    std::filesystem::path p(path);
    if (p.has_parent_path()) {
        std::filesystem::create_directories(p.parent_path());
    }
    file_.open(path, std::ios::binary | std::ios::app);
    if (!file_.is_open())
        throw std::runtime_error("[WAL] Cannot open WAL file: " + path);

    active_buf_.reserve(128 * 1024);
    flush_buf_.reserve(128 * 1024);
    flush_thread_ = std::thread(&WalWriter::flush_thread_loop, this);

    std::cout << "[WAL] Opened: " << path << "\n";
}

WalWriter::~WalWriter() {
    running_ = false;
    cv_.notify_all();
    if (flush_thread_.joinable()) flush_thread_.join();
    if (file_.is_open()) file_.close();
}

// ── Background Flush Thread ───────────────────────────────────────────────────

void WalWriter::flush_thread_loop() {
    while (running_) {
        {
            std::unique_lock<std::mutex> lock(mu_);
            cv_.wait_for(lock, std::chrono::milliseconds(2), [this] {
                return !running_ || active_buf_.size() > 64 * 1024;
            });

            if (active_buf_.empty()) continue;
            std::swap(active_buf_, flush_buf_);
        }

        file_.write(reinterpret_cast<const char*>(flush_buf_.data()), flush_buf_.size());
        file_.flush();
        flush_buf_.clear();
    }

    // Final flush on shutdown
    std::unique_lock<std::mutex> lock(mu_);
    if (!active_buf_.empty()) {
        file_.write(reinterpret_cast<const char*>(active_buf_.data()), active_buf_.size());
        file_.flush();
    }
}

// ── Append a PUBLISH record ───────────────────────────────────────────────────

void WalWriter::append(const Message& msg) {
    std::lock_guard<std::mutex> lock(mu_);

    auto topic_len = static_cast<uint16_t>(msg.topic.size());
    auto body_len  = static_cast<uint32_t>(msg.body.size());

    active_buf_.push_back(WAL_PUBLISH);
    write_be64(active_buf_, msg.msg_id);
    write_be16(active_buf_, topic_len);
    active_buf_.insert(active_buf_.end(), msg.topic.begin(), msg.topic.end());
    write_be32(active_buf_, body_len);
    active_buf_.insert(active_buf_.end(), msg.body.begin(), msg.body.end());

    if (active_buf_.size() > 64 * 1024) cv_.notify_one();
}

// ── Append an ACK record ──────────────────────────────────────────────────────

void WalWriter::acknowledge(uint64_t msg_id) {
    std::lock_guard<std::mutex> lock(mu_);
    active_buf_.push_back(WAL_ACK);
    write_be64(active_buf_, msg_id);
    
    if (active_buf_.size() > 64 * 1024) cv_.notify_one();
}

// ── Replay: push unacknowledged messages back into SafeQueue ──────────────────

void WalWriter::replay(SafeQueue<Message>& queue) {
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) {
        std::cout << "[WAL] No existing WAL file to replay.\n";
        return;
    }

    std::unordered_map<uint64_t, Message> published;
    std::unordered_set<uint64_t>          acked;

    while (in.peek() != EOF) {
        uint8_t record_type = 0;
        in.read(reinterpret_cast<char*>(&record_type), 1);
        if (in.fail()) break;

        if (record_type == WAL_PUBLISH) {
            Message msg;
            msg.msg_id = read_be64(in);
            uint16_t topic_len = read_be16(in);
            msg.topic.resize(topic_len);
            in.read(msg.topic.data(), topic_len);
            uint32_t body_len = read_be32(in);
            msg.body.resize(body_len);
            in.read(reinterpret_cast<char*>(msg.body.data()), body_len);
            if (in.fail()) break;
            published[msg.msg_id] = std::move(msg);

        } else if (record_type == WAL_ACK) {
            uint64_t msg_id = read_be64(in);
            if (in.fail()) break;
            acked.insert(msg_id);

        } else {
            std::cerr << "[WAL] Unknown record type 0x"
                      << std::hex << static_cast<int>(record_type) << std::dec
                      << " — stopping replay\n";
            break;
        }
    }

    int replayed = 0;
    for (auto& [id, msg] : published) {
        if (acked.find(id) == acked.end()) {
            std::cout << "[WAL] Replaying msg_id=" << id
                      << " topic=" << msg.topic << "\n";
            queue.push(std::move(msg));
            ++replayed;
        }
    }

    std::cout << "[WAL] Replay complete: " << replayed << " message(s) recovered.\n";
}

} // namespace broker
