#pragma once

#include <unordered_map>
#include <chrono>
#include <mutex>
#include <atomic>
#include <thread>
#include "Message.hpp"
#include "SafeQueue.hpp"

namespace broker {

struct InFlight {
    Message                                    msg;
    ClientId                                   subscriber_id;
    std::chrono::steady_clock::time_point      deadline;
    int                                        retry_count{0};
};

class AckEngine {
public:
    AckEngine(SafeQueue<Message>& main_queue,
              SafeQueue<Message>& dlq,
              std::chrono::milliseconds ack_timeout,
              int max_retries);

    ~AckEngine();

    // Called by dispatcher after delivering a message to a subscriber.
    // Starts tracking the message for acknowledgement.
    void track(const Message& msg, ClientId subscriber_id);

    // Called when a client sends an ACK frame.
    void ack(uint64_t msg_id);

    // Called when a client sends a NACK frame — immediately requeues.
    void nack(uint64_t msg_id);

    // Called when a client disconnects — fail all its in-flight messages.
    void client_disconnected(ClientId id);

    void stop();

private:
    void redelivery_loop();
    void requeue_or_dlq(InFlight& entry);

    SafeQueue<Message>&                     main_queue_;
    SafeQueue<Message>&                     dlq_;
    std::chrono::milliseconds               ack_timeout_;
    int                                     max_retries_;
    std::unordered_map<uint64_t, InFlight>  in_flight_;
    std::mutex                              mu_;
    std::atomic<bool>                       running_{true};
    std::thread                             scanner_thread_;
};

} // namespace broker
