#pragma once

#ifdef _WIN32
#  ifndef WIN32_LEAN_AND_MEAN
#    define WIN32_LEAN_AND_MEAN
#  endif
#  include <winsock2.h>
#  include <ws2tcpip.h>
   using socket_t = SOCKET;
   constexpr socket_t INVALID_SOCK = INVALID_SOCKET;
#else
#  include <sys/socket.h>
#  include <netinet/in.h>
#  include <unistd.h>
   using socket_t = int;
   constexpr socket_t INVALID_SOCK = -1;
#endif

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>
#include <memory>
#include <functional>
#include "Message.hpp"
#include "SafeQueue.hpp"
#include "TopicRouter.hpp"

namespace broker {

struct ClientConn {
    socket_t sock{INVALID_SOCK};
    std::mutex send_mu;
};

// Called whenever a complete, parsed frame arrives from a client.
// Arguments: client_id, msg_type, msg_id, payload bytes
using FrameHandler = std::function<void(ClientId,
                                        MessageType,
                                        uint64_t,
                                        const std::vector<uint8_t>&)>;

class TcpServer {
public:
    TcpServer(uint16_t port, FrameHandler on_frame);
    ~TcpServer();

    void start();   // Non-blocking: launches accept thread
    void stop();

    // Thread-safe: send pre-encoded frame bytes to a specific client
    void send_to(ClientId id, const std::vector<uint8_t>& frame);

private:
    void accept_loop();
    void handle_client(socket_t sock, ClientId id);

    uint16_t       port_;
    FrameHandler   on_frame_;
    socket_t       listen_sock_{INVALID_SOCK};
    std::atomic<bool>           running_{false};
    std::thread                  accept_thread_;

    std::mutex                                        clients_mu_;
    std::unordered_map<ClientId, std::shared_ptr<ClientConn>> clients_;
};

} // namespace broker
