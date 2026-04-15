#define CPPHTTPLIB_NO_MMAP
#define _WIN32_WINNT 0x0A00 

#include <iostream>
#include <queue>
#include <mutex>
#include "httplib.h"
#include "json.hpp"

using json = nlohmann::json;

class EventBus {
private:
    std::queue<json> events;
    std::mutex mu;

public:
    void publish(const json& event) {
        std::lock_guard<std::mutex> lock(mu);
        events.push(event);
    }

    json getEvents() {
        std::lock_guard<std::mutex> lock(mu);

        json arr = json::array();
        while(!events.empty()) {
            arr.push_back(events.front());
            events.pop();
        }
        return arr;
    }
};

EventBus bus;

int main() {
    std::cout<<"Server starting" ;
    httplib::Server server;

    std::cout << "✅ C++ Event Bus running on http://localhost:9001\n";

    server.Post("/publish", [](const httplib::Request& req, httplib::Response& res){
        auto data = json::parse(req.body);
        bus.publish(data);

        res.set_content(R"({"ok":true})", "application/json");
    });

    server.Get("/events", [](const httplib::Request&, httplib::Response& res){
        json ev = bus.getEvents();
        res.set_content(ev.dump(), "application/json");
    });

    server.listen("0.0.0.0", 9001);
    std::cout << "Server started\n";
    while (true) { std::this_thread::sleep_for(std::chrono::hours(1)); }
}
