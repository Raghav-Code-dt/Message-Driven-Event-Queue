#include "TopicRouter.hpp"
#include <algorithm>

namespace broker {

void TopicRouter::subscribe(const std::string& topic, const std::string& group, ClientId client_id) {
    std::lock_guard<std::mutex> lock(mu_);
    // Empty group = each subscriber gets its own independent copy
    std::string g = group.empty() ? ("__unique_" + std::to_string(client_id)) : group;

    auto& state = subscriptions_[topic][g];
    if (std::find(state.members.begin(), state.members.end(), client_id) == state.members.end()) {
        state.members.push_back(client_id);
    }
}

void TopicRouter::unsubscribe(ClientId client_id) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto topic_it = subscriptions_.begin(); topic_it != subscriptions_.end(); ) {
        auto& groups = topic_it->second;
        for (auto g_it = groups.begin(); g_it != groups.end(); ) {
            auto& state = g_it->second;
            state.members.erase(
                std::remove(state.members.begin(), state.members.end(), client_id),
                state.members.end()
            );
            g_it = state.members.empty() ? groups.erase(g_it) : std::next(g_it);
        }
        topic_it = groups.empty() ? subscriptions_.erase(topic_it) : std::next(topic_it);
    }
}

bool TopicRouter::matches(const std::string& pattern, const std::string& topic) const {
    if (pattern == topic) return true;
    size_t star = pattern.find('*');
    if (star != std::string::npos) {
        std::string prefix = pattern.substr(0, star);
        if (!prefix.empty() && topic.compare(0, prefix.size(), prefix) == 0) return true;
        if (prefix.empty()) return true; // bare "*" matches everything
    }
    return false;
}

std::vector<std::pair<ClientId, uint64_t>> TopicRouter::route(const Message& msg) {
    std::vector<std::pair<ClientId, uint64_t>> targets;
    std::lock_guard<std::mutex> lock(mu_);

    for (auto& [pattern, groups] : subscriptions_) {
        if (!matches(pattern, msg.topic)) continue;

        for (auto& [group_name, state] : groups) {
            if (state.members.empty()) continue;

            if (group_name.rfind("__unique_", 0) == 0) {
                // Independent subscriber — always gets a copy
                for (ClientId id : state.members) {
                    targets.emplace_back(id, msg.msg_id);
                }
            } else {
                // Competing consumer group — round-robin
                size_t idx = state.rr_idx % state.members.size();
                targets.emplace_back(state.members[idx], msg.msg_id);
                state.rr_idx++;
            }
        }
    }

    return targets;
}

} // namespace broker
