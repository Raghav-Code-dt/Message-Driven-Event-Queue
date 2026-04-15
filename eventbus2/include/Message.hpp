#pragma once
#include <string>
#include "json.hpp"
using json = nlohmann::json;

class Message {
public:
    std::string topic;
    json payload;

    Message(const std::string& t, const json& p): topic(t), payload(p) {}
};
