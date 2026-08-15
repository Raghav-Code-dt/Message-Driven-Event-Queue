#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "Message.hpp"

namespace broker {

// ─── Wire Protocol Constants ─────────────────────────────────────────────────
constexpr uint32_t FRAME_MAGIC   = 0xDEADBEEF;
constexpr size_t   HEADER_SIZE   = 17; // 4 (magic) + 1 (type) + 4 (len) + 8 (id)

// ─── Parsed payload structs ───────────────────────────────────────────────────
struct ParsedPublish {
    std::string          topic;
    std::vector<uint8_t> body;
};

struct ParsedSubscribe {
    std::string topic;
    std::string group;
};

struct ParsedHeader {
    uint32_t    magic;
    MessageType type;
    uint32_t    payload_len;
    uint64_t    msg_id;
    bool        valid; // false if magic mismatch
};

// ─── Big-Endian helpers ───────────────────────────────────────────────────────
inline uint32_t to_be32(uint32_t v) {
    return ((v & 0xFF000000u) >> 24) |
           ((v & 0x00FF0000u) >>  8) |
           ((v & 0x0000FF00u) <<  8) |
           ((v & 0x000000FFu) << 24);
}

inline uint64_t to_be64(uint64_t v) {
    return ((uint64_t)to_be32((uint32_t)(v & 0xFFFFFFFFu)) << 32) |
           (uint64_t)to_be32((uint32_t)(v >> 32));
}

inline uint32_t from_be32(uint32_t v) { return to_be32(v); }
inline uint64_t from_be64(uint64_t v) { return to_be64(v); }

// ─── Frame encoding ───────────────────────────────────────────────────────────

// Encode a complete frame: 17-byte header + payload
// Payload layout: [topic_len 2B BE][topic bytes][body bytes]
inline std::vector<uint8_t> encode_frame(MessageType type,
                                          uint64_t    msg_id,
                                          const std::string& topic,
                                          const std::vector<uint8_t>& body)
{
    uint16_t topic_len = static_cast<uint16_t>(topic.size());
    uint32_t payload_len = 2u + topic_len + static_cast<uint32_t>(body.size());

    std::vector<uint8_t> frame;
    frame.reserve(HEADER_SIZE + payload_len);

    // Magic (4 bytes BE)
    uint32_t magic_be = to_be32(FRAME_MAGIC);
    auto* pm = reinterpret_cast<uint8_t*>(&magic_be);
    frame.insert(frame.end(), pm, pm + 4);

    // Type (1 byte)
    frame.push_back(static_cast<uint8_t>(type));

    // Payload length (4 bytes BE)
    uint32_t plen_be = to_be32(payload_len);
    auto* pp = reinterpret_cast<uint8_t*>(&plen_be);
    frame.insert(frame.end(), pp, pp + 4);

    // Message ID (8 bytes BE)
    uint64_t id_be = to_be64(msg_id);
    auto* pi = reinterpret_cast<uint8_t*>(&id_be);
    frame.insert(frame.end(), pi, pi + 8);

    // Topic length (2 bytes BE)
    uint16_t tlen_be = (topic_len >> 8) | (topic_len << 8);
    frame.push_back(static_cast<uint8_t>(tlen_be >> 8));
    frame.push_back(static_cast<uint8_t>(tlen_be & 0xFF));

    // Topic bytes
    frame.insert(frame.end(), topic.begin(), topic.end());

    // Body bytes
    frame.insert(frame.end(), body.begin(), body.end());

    return frame;
}

// Encode a subscribe frame:
// Payload: [topic_len 2B BE][topic][group_len 2B BE][group]
inline std::vector<uint8_t> encode_subscribe_frame(const std::string& topic,
                                                    const std::string& group,
                                                    uint64_t msg_id = 0)
{
    uint16_t topic_len = static_cast<uint16_t>(topic.size());
    uint16_t group_len = static_cast<uint16_t>(group.size());
    uint32_t payload_len = 2u + topic_len + 2u + group_len;

    std::vector<uint8_t> frame;
    frame.reserve(HEADER_SIZE + payload_len);

    uint32_t magic_be = to_be32(FRAME_MAGIC);
    auto* pm = reinterpret_cast<uint8_t*>(&magic_be);
    frame.insert(frame.end(), pm, pm + 4);

    frame.push_back(static_cast<uint8_t>(MessageType::SUBSCRIBE));

    uint32_t plen_be = to_be32(payload_len);
    auto* pp = reinterpret_cast<uint8_t*>(&plen_be);
    frame.insert(frame.end(), pp, pp + 4);

    uint64_t id_be = to_be64(msg_id);
    auto* pi = reinterpret_cast<uint8_t*>(&id_be);
    frame.insert(frame.end(), pi, pi + 8);

    // topic
    frame.push_back(static_cast<uint8_t>(topic_len >> 8));
    frame.push_back(static_cast<uint8_t>(topic_len & 0xFF));
    frame.insert(frame.end(), topic.begin(), topic.end());

    // group
    frame.push_back(static_cast<uint8_t>(group_len >> 8));
    frame.push_back(static_cast<uint8_t>(group_len & 0xFF));
    frame.insert(frame.end(), group.begin(), group.end());

    return frame;
}

// Encode an ACK or NACK frame (no payload needed, msg_id is in header)
inline std::vector<uint8_t> encode_ack_frame(MessageType type, uint64_t msg_id) {
    std::vector<uint8_t> frame;
    frame.reserve(HEADER_SIZE);

    uint32_t magic_be = to_be32(FRAME_MAGIC);
    auto* pm = reinterpret_cast<uint8_t*>(&magic_be);
    frame.insert(frame.end(), pm, pm + 4);

    frame.push_back(static_cast<uint8_t>(type));

    uint32_t plen_be = to_be32(0u);
    auto* pp = reinterpret_cast<uint8_t*>(&plen_be);
    frame.insert(frame.end(), pp, pp + 4);

    uint64_t id_be = to_be64(msg_id);
    auto* pi = reinterpret_cast<uint8_t*>(&id_be);
    frame.insert(frame.end(), pi, pi + 8);

    return frame;
}

// ─── Frame decoding ───────────────────────────────────────────────────────────

inline ParsedHeader decode_header(const uint8_t* data) {
    ParsedHeader h{};

    uint32_t raw_magic;
    __builtin_memcpy(&raw_magic, data, 4);
    h.magic = from_be32(raw_magic);
    h.valid = (h.magic == FRAME_MAGIC);

    h.type = static_cast<MessageType>(data[4]);

    uint32_t raw_len;
    __builtin_memcpy(&raw_len, data + 5, 4);
    h.payload_len = from_be32(raw_len);

    uint64_t raw_id;
    __builtin_memcpy(&raw_id, data + 9, 8);
    h.msg_id = from_be64(raw_id);

    return h;
}

inline ParsedPublish decode_publish(const std::vector<uint8_t>& payload) {
    ParsedPublish p;
    if (payload.size() < 2) return p;

    uint16_t topic_len = (static_cast<uint16_t>(payload[0]) << 8) | payload[1];
    if (payload.size() < 2u + topic_len) return p;

    p.topic.assign(reinterpret_cast<const char*>(payload.data() + 2), topic_len);
    size_t body_offset = 2 + topic_len;
    p.body.assign(payload.begin() + static_cast<ptrdiff_t>(body_offset), payload.end());
    return p;
}

inline ParsedSubscribe decode_subscribe(const std::vector<uint8_t>& payload) {
    ParsedSubscribe s;
    if (payload.size() < 2) return s;

    uint16_t topic_len = (static_cast<uint16_t>(payload[0]) << 8) | payload[1];
    if (payload.size() < 2u + topic_len + 2u) return s;

    s.topic.assign(reinterpret_cast<const char*>(payload.data() + 2), topic_len);

    size_t g_offset = 2 + topic_len;
    uint16_t group_len = (static_cast<uint16_t>(payload[g_offset]) << 8) | payload[g_offset + 1];
    g_offset += 2;

    if (payload.size() >= g_offset + group_len)
        s.group.assign(reinterpret_cast<const char*>(payload.data() + g_offset), group_len);

    return s;
}

} // namespace broker
