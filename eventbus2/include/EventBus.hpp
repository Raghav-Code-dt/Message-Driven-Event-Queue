#pragma once

#include "Message.hpp"
#include "ThreadSafeQueue.hpp"
#include "json.hpp"

class EventBus {
private:
    ThreadSafeQueue<json> events;

public:
    void publish(const json& event);
    json getEvents();
};
