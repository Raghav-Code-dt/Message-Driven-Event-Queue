#include "AckEngine.hpp"
#include <iostream>
#include <algorithm>

namespace broker {

AckEngine::AckEngine(SafeQueue<Message>& main_queue,
                     SafeQueue<Message>& dlq,
                     std::chrono::milliseconds ack_timeout,
                     int max_retries)
    : main_queue_(main_queue)
    , dlq_(dlq)
    , ack_timeout_(ack_timeout)
    , max_retries_(max_retries)
{
    scanner_thread_ = std::thread(&AckEngine::redelivery_loop, this);
}

AckEngine::~AckEngine() {
    stop();
}

void AckEngine::stop() {
    running_ = false;
    if (scanner_thread_.joinable())
        scanner_thread_.join();
}

void AckEngine::track(const Message& msg, ClientId subscriber_id) {
    std::lock_guard<std::mutex> lock(mu_);
    in_flight_[msg.msg_id] = InFlight{
        msg,
        subscriber_id,
        std::chrono::steady_clock::now() + ack_timeout_,
        0
    };
}

void AckEngine::ack(uint64_t msg_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = in_flight_.find(msg_id);
    if (it != in_flight_.end()) {
        std::cout << "[AckEngine] ACK msg_id=" << msg_id << "\n";
        in_flight_.erase(it);
    }
}

void AckEngine::nack(uint64_t msg_id) {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = in_flight_.find(msg_id);
    if (it == in_flight_.end()) return;

    std::cout << "[AckEngine] NACK msg_id=" << msg_id
              << " retry=" << it->second.retry_count << "\n";

    requeue_or_dlq(it->second);
    in_flight_.erase(it);
}

void AckEngine::client_disconnected(ClientId id) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = in_flight_.begin(); it != in_flight_.end(); ) {
        if (it->second.subscriber_id == id) {
            std::cout << "[AckEngine] Client " << id
                      << " disconnected, requeueing msg_id=" << it->first << "\n";
            requeue_or_dlq(it->second);
            it = in_flight_.erase(it);
        } else {
            ++it;
        }
    }
}

void AckEngine::requeue_or_dlq(InFlight& entry) {
    if (entry.retry_count >= max_retries_) {
        // Route to Dead-Letter Queue topic
        Message dlq_msg;
        dlq_msg.msg_id = entry.msg.msg_id;
        dlq_msg.topic  = "$DLQ." + entry.msg.topic;
        dlq_msg.body   = entry.msg.body;
        std::cout << "[AckEngine] DLQ msg_id=" << dlq_msg.msg_id
                  << " topic=" << dlq_msg.topic << "\n";
        dlq_.push(std::move(dlq_msg));
    } else {
        // Requeue with incremented retry count
        entry.retry_count++;
        entry.deadline = std::chrono::steady_clock::now() + ack_timeout_;
        Message requeue_msg = entry.msg;
        std::cout << "[AckEngine] Requeue msg_id=" << requeue_msg.msg_id
                  << " attempt=" << entry.retry_count << "\n";
        main_queue_.push(std::move(requeue_msg));
    }
}

// ── Dedicated scanner thread ─────────────────────────────────────────────────
// Wakes every 200ms and evicts all expired in-flight messages.
void AckEngine::redelivery_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(mu_);

        for (auto it = in_flight_.begin(); it != in_flight_.end(); ) {
            if (now >= it->second.deadline) {
                std::cout << "[AckEngine] Timeout msg_id=" << it->first
                          << " retry=" << it->second.retry_count << "\n";
                requeue_or_dlq(it->second);
                it = in_flight_.erase(it);
            } else {
                ++it;
            }
        }
    }
}

} // namespace broker
