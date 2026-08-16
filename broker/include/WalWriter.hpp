#pragma once

#include <string>
#include <mutex>
#include <fstream>
#include <vector>
#include <thread>
#include <condition_variable>
#include <atomic>
#include "Message.hpp"
#include "SafeQueue.hpp"

namespace broker {

class WalWriter {
public:
    explicit WalWriter(const std::string& path);
    ~WalWriter();

    void append(const Message& msg);
    void acknowledge(uint64_t msg_id);
    void replay(SafeQueue<Message>& queue);

private:
    void write_be64(std::vector<uint8_t>& out, uint64_t v);
    void write_be32(std::vector<uint8_t>& out, uint32_t v);
    void write_be16(std::vector<uint8_t>& out, uint16_t v);

    uint64_t read_be64(std::istream& in);
    uint32_t read_be32(std::istream& in);
    uint16_t read_be16(std::istream& in);

    void flush_thread_loop();

    std::string   path_;
    std::ofstream file_;
    
    std::mutex              mu_;
    std::condition_variable cv_;
    std::vector<uint8_t>    active_buf_;
    std::vector<uint8_t>    flush_buf_;
    std::atomic<bool>       running_{true};
    std::thread             flush_thread_;
};

} // namespace broker
