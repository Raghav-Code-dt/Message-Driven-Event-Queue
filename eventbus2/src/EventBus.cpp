#include "EventBus.hpp"

void EventBus::publish(const json& event) {
    events.push(event);
}

json EventBus::getEvents() {
    auto all = events.popAll();
    json arr = json::array();
    for (auto& e : all) arr.push_back(e);
    return arr;
}
