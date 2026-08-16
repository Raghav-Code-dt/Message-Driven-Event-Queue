#include "TcpServer.hpp"
#include "Protocol.hpp"
#include <iostream>
#include <cstring>
#include <stdexcept>

#ifdef _WIN32
#  define CLOSE_SOCK(s) ::closesocket(s)
#  define SOCK_ERR      WSAGetLastError()
#else
#  include <netinet/tcp.h>
#  include <sys/socket.h>
#  define CLOSE_SOCK(s) ::close(s)
#  define SOCK_ERR      errno
#endif

namespace broker {

TcpServer::TcpServer(uint16_t port, FrameHandler on_frame)
    : port_(port), on_frame_(std::move(on_frame))
{
#ifdef _WIN32
    WSADATA wsa{};
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0)
        throw std::runtime_error("WSAStartup failed");
#endif
}

TcpServer::~TcpServer() {
    stop();
#ifdef _WIN32
    WSACleanup();
#endif
}

void TcpServer::start() {
    listen_sock_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock_ == INVALID_SOCK)
        throw std::runtime_error("socket() failed");

    // Allow port reuse
    int opt = 1;
    ::setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR,
                 reinterpret_cast<const char*>(&opt), sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (::bind(listen_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        throw std::runtime_error("bind() failed on port " + std::to_string(port_));

    if (::listen(listen_sock_, SOMAXCONN) < 0)
        throw std::runtime_error("listen() failed");

    running_ = true;
    accept_thread_ = std::thread(&TcpServer::accept_loop, this);
    std::cout << "[Broker] TCP server listening on port " << port_ << "\n";
}

void TcpServer::stop() {
    running_ = false;
    if (listen_sock_ != INVALID_SOCK) {
        CLOSE_SOCK(listen_sock_);
        listen_sock_ = INVALID_SOCK;
    }
    if (accept_thread_.joinable())
        accept_thread_.join();
}

void TcpServer::accept_loop() {
    while (running_) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);

        socket_t client_sock = ::accept(listen_sock_,
                                        reinterpret_cast<sockaddr*>(&client_addr),
                                        &client_len);
        if (client_sock == INVALID_SOCK) {
            if (running_)
                std::cerr << "[Broker] accept() error: " << SOCK_ERR << "\n";
            break;
        }

        int flag = 1;
        ::setsockopt(client_sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char*>(&flag), sizeof(int));
        int buf_size = 256 * 1024;
        ::setsockopt(client_sock, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char*>(&buf_size), sizeof(int));
        ::setsockopt(client_sock, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char*>(&buf_size), sizeof(int));

        ClientId id = static_cast<ClientId>(client_sock);
        auto conn   = std::make_shared<ClientConn>();
        conn->sock  = client_sock;

        {
            std::lock_guard<std::mutex> lock(clients_mu_);
            clients_[id] = conn;
        }

        std::cout << "[Broker] Client connected id=" << id << "\n";

        // Each client gets its own handler thread
        std::thread([this, client_sock, id]() {
            handle_client(client_sock, id);
        }).detach();
    }
}

// ─── Per-connection reader: handles TCP stream fragmentation ─────────────────
void TcpServer::handle_client(socket_t sock, ClientId id) {
    std::vector<uint8_t> read_buf;
    read_buf.reserve(4096);

    char tmp[4096];

    while (running_) {
        int n = ::recv(sock, tmp, sizeof(tmp), 0);
        if (n <= 0) {
            // Client disconnected or error
            break;
        }

        // Append received bytes to accumulation buffer
        read_buf.insert(read_buf.end(),
                        reinterpret_cast<uint8_t*>(tmp),
                        reinterpret_cast<uint8_t*>(tmp) + n);

        // Parse as many complete frames as possible
        while (read_buf.size() >= HEADER_SIZE) {
            ParsedHeader hdr = decode_header(read_buf.data());

            if (!hdr.valid) {
                std::cerr << "[Broker] Bad magic from client " << id << " — dropping connection\n";
                goto disconnect;
            }

            size_t total_frame = HEADER_SIZE + hdr.payload_len;
            if (read_buf.size() < total_frame) break; // Wait for more data

            // Extract payload
            std::vector<uint8_t> payload(
                read_buf.begin() + HEADER_SIZE,
                read_buf.begin() + static_cast<ptrdiff_t>(total_frame)
            );

            // Remove consumed bytes
            read_buf.erase(read_buf.begin(),
                           read_buf.begin() + static_cast<ptrdiff_t>(total_frame));

            // Dispatch to handler
            on_frame_(id, hdr.type, hdr.msg_id, payload);
        }
    }

disconnect:
    std::cout << "[Broker] Client disconnected id=" << id << "\n";
    CLOSE_SOCK(sock);
    {
        std::lock_guard<std::mutex> lock(clients_mu_);
        clients_.erase(id);
    }
    // Notify upper layer that this client is gone (Phase 3: unsubscribe)
    // 0x05 represents ERROR_RESP / Disconnect sentinel
    on_frame_(id, MessageType::ERROR_RESP, 0, {});
}

void TcpServer::send_to(ClientId id, const std::vector<uint8_t>& frame) {
    std::shared_ptr<ClientConn> conn;
    {
        std::lock_guard<std::mutex> lock(clients_mu_);
        auto it = clients_.find(id);
        if (it == clients_.end()) return;
        conn = it->second;
    }

    std::lock_guard<std::mutex> send_lock(conn->send_mu);
    size_t total_sent = 0;
    while (total_sent < frame.size()) {
        int sent = ::send(conn->sock,
                          reinterpret_cast<const char*>(frame.data() + total_sent),
                          static_cast<int>(frame.size() - total_sent),
                          0);
        if (sent <= 0) break;
        total_sent += static_cast<size_t>(sent);
    }
}

} // namespace broker
