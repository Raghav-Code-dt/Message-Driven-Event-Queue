#pragma once

#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <utility>
#include "Message.hpp"

namespace broker {

class TopicRouter {
public:
    void subscribe(const std::string& topic, const std::string& group, ClientId client_id);
    void unsubscribe(ClientId client_id);

    // Returns a list of (ClientId, msg_id) targets for a given message.
    // Independent groups each get a copy. Competing consumers in the same
    // group are served via round-robin.
    std::vector<std::pair<ClientId, uint64_t>> route(const Message& msg);

private:
    struct GroupState {
        std::vector<ClientId> members;
        size_t                rr_idx{0};
    };

    // topic_pattern -> group_name -> GroupState
    std::unordered_map<std::string,
        std::unordered_map<std::string, GroupState>> subscriptions_;
    std::mutex mu_;

    bool matches(const std::string& pattern, const std::string& topic) const;
};

} // namespace broker
