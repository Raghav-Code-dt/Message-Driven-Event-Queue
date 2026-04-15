#define CPPHTTPLIB_NO_MMAP
#define _WIN32_WINNT 0x0A00

#include <iostream>
#include "httplib.h"
#include "json.hpp"
#include "EventBus.hpp"

using json = nlohmann::json;
EventBus bus;

int main() {
    std::cout << "Server starting for C++ Event Bus  is running at http://localhost:9001\n";

    httplib::Server server;

    server.Get("/", [](const httplib::Request&, httplib::Response& res){
        res.set_content("<h1>C++ Event Bus Server is Running!</h1><p>For Dashboard for visit localhost:5174</p>", "text/html");
    });

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
