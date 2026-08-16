#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace broker {

// Portable client identifier
// SOCKET on Windows is UINT_PTR (64-bit), so we use intptr_t to hold it safely
using ClientId = intptr_t;

enum class MessageType : uint8_t {
    PUBLISH    = 0x01,
    SUBSCRIBE  = 0x02,
    ACK        = 0x03,
    NACK       = 0x04,
    HEARTBEAT  = 0x05,
    EVENT_DATA = 0x06,
    ERROR_RESP = 0x07
};

// Application-level message (what flows through SafeQueue and TopicRouter)
struct Message {
    uint64_t             msg_id{0};
    std::string          topic;
    std::vector<uint8_t> body; // raw JSON bytes
    int                  retry_count{0};
};

} // namespace broker
