#include <iostream>
#include <csignal>
#include <atomic>
#include <thread>
#include <cstdlib>
#include <string>
#include <memory>

#include "SafeQueue.hpp"
#include "TopicRouter.hpp"
#include "TcpServer.hpp"
#include "Protocol.hpp"
#include "Message.hpp"
#include "AckEngine.hpp"
#include "WalWriter.hpp"

using namespace broker;

static std::atomic<bool> g_running{true};

static void signal_handler(int) {
    g_running = false;
    std::cout << "\n[Broker] Shutting down...\n";
}

int main() {
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // ── Config from environment (with defaults) ──────────────────────────
    auto env = [](const char* key, const char* def) -> std::string {
        const char* v = std::getenv(key);
        return v ? v : def;
    };

    const uint16_t TCP_PORT     = static_cast<uint16_t>(std::stoi(env("TCP_PORT",      "9099")));
    const size_t   QUEUE_CAP    = static_cast<size_t>  (std::stoi(env("QUEUE_CAPACITY","10000")));
    const int      MAX_RETRIES  = std::stoi(env("MAX_RETRIES",    "3"));
    const int      ACK_TIMEOUT  = std::stoi(env("ACK_TIMEOUT_MS", "5000"));
    const std::string WAL_PATH  = env("WAL_PATH", "./data/broker.wal");

    // ── Core components ──────────────────────────────────────────────────
    SafeQueue<Message> main_queue(QUEUE_CAP);
    SafeQueue<Message> dlq(QUEUE_CAP);

    // ── Write-Ahead Log ──────────────────────────────────────────────────
    WalWriter wal(WAL_PATH);
    wal.replay(main_queue);   // Recover unacknowledged messages on startup

    // ── Topic Router ─────────────────────────────────────────────────────
    TopicRouter router;

    // ── ACK Engine ───────────────────────────────────────────────────────
    AckEngine ack_engine(
        main_queue,
        dlq,
        std::chrono::milliseconds(ACK_TIMEOUT),
        MAX_RETRIES
    );

    // ── Monotonic message ID counter ─────────────────────────────────────
    static std::atomic<uint64_t> next_id{1};

    // ── DLQ logger thread ─────────────────────────────────────────────────
    std::thread dlq_logger([&]() {
        while (g_running) {
            auto opt = dlq.try_pop_for(std::chrono::milliseconds(200));
            if (!opt) continue;
            std::cout << "[DLQ] Dead-lettered msg_id=" << opt->msg_id
                      << " topic=" << opt->topic << "\n";
                      
            // Re-publish the DLQ event so monitoring dashboards can see it
            Message sys_msg;
            sys_msg.msg_id = next_id.fetch_add(1);
            sys_msg.topic = "$SYS.dlq";
            
            std::string body_str = "{\"original_id\": \"" + std::to_string(opt->msg_id) + 
                                   "\", \"original_topic\": \"" + opt->topic + "\"}";
            sys_msg.body.assign(body_str.begin(), body_str.end());
            
            wal.append(sys_msg);
            main_queue.push(std::move(sys_msg));
        }
    });

    // ── Forward-declare server pointer so on_frame lambda can capture it ─
    std::shared_ptr<TcpServer> server_ptr;

    // ── Frame handler: called by TcpServer for every complete parsed frame ─
    auto on_frame = [&](ClientId id,
                        MessageType type,
                        uint64_t    msg_id,
                        const std::vector<uint8_t>& payload)
    {
        switch (type) {
        case MessageType::SUBSCRIBE: {
            auto sub = decode_subscribe(payload);
            if (sub.topic.empty()) return;
            router.subscribe(sub.topic, sub.group, id);
            std::cout << "[Broker] SUBSCRIBE id=" << id
                      << " topic=" << sub.topic
                      << " group=" << (sub.group.empty() ? "(unique)" : sub.group) << "\n";
            break;
        }

        case MessageType::PUBLISH: {
            auto pub = decode_publish(payload);
            if (pub.topic.empty()) return;

            Message msg;
            msg.msg_id = (msg_id != 0) ? msg_id : next_id.fetch_add(1);
            msg.topic  = pub.topic;
            msg.body   = pub.body;

            // std::cout << "[Broker] PUBLISH id=" << id
            //           << " topic=" << msg.topic
            //           << " msg_id=" << msg.msg_id << "\n";

            wal.append(msg);           // Persist before queuing
            main_queue.push(std::move(msg));
            break;
        }

        case MessageType::ACK: {
            // std::cout << "[Broker] ACK id=" << id << " msg_id=" << msg_id << "\n";
            ack_engine.ack(msg_id);
            wal.acknowledge(msg_id);   // Mark as processed in WAL
            break;
        }

        case MessageType::NACK: {
            std::cout << "[Broker] NACK id=" << id << " msg_id=" << msg_id << "\n";
            ack_engine.nack(msg_id);
            break;
        }

        case MessageType::HEARTBEAT: {
            if (server_ptr)
                server_ptr->send_to(id, encode_ack_frame(MessageType::HEARTBEAT, 0));
            break;
        }

        case MessageType::ERROR_RESP: {
            // Sentinel: client disconnected
            router.unsubscribe(id);
            ack_engine.client_disconnected(id);
            break;
        }

        default:
            std::cerr << "[Broker] Unknown frame type from id=" << id << "\n";
        }
    };

    // ── TcpServer ────────────────────────────────────────────────────────
    server_ptr = std::make_shared<TcpServer>(TCP_PORT, on_frame);
    server_ptr->start();

    // ── Dispatcher thread: main_queue → TopicRouter → send EVENT_DATA ────
    std::thread dispatcher([&]() {
        while (g_running) {
            auto opt = main_queue.try_pop_for(std::chrono::milliseconds(100));
            if (!opt) continue;

            Message& msg = *opt;
            auto targets = router.route(msg);

            if (targets.empty()) {
                std::cout << "[Broker] No subscribers for topic=" << msg.topic << "\n";
                continue;
            }

            for (auto& [client_id, routed_msg_id] : targets) {
                // Track for ACK before sending
                ack_engine.track(msg, client_id);

                auto frame = encode_frame(MessageType::EVENT_DATA,
                                         msg.msg_id,
                                         msg.topic,
                                         msg.body);
                server_ptr->send_to(client_id, frame);
            }
        }
    });

    // ── Wait for shutdown signal ──────────────────────────────────────────
    std::cout << "[Broker] Running. Press Ctrl+C to stop.\n";
    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // ── Graceful shutdown ─────────────────────────────────────────────────
    ack_engine.stop();
    server_ptr->stop();
    dispatcher.join();
    dlq_logger.join();

    std::cout << "[Broker] Stopped cleanly.\n";
    return 0;
}
