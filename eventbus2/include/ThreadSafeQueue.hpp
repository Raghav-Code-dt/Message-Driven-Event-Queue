#pragma once
#include <queue>
#include <mutex>

template<typename T>
class ThreadSafeQueue {
private:
    std::queue<T> q;
    std::mutex mu;

public:
    void push(const T& value) {
        std::lock_guard<std::mutex> lock(mu);
        q.push(value);
    }

    std::vector<T> popAll() {
        std::lock_guard<std::mutex> lock(mu);
        std::vector<T> items;

        while (!q.empty()) {
            items.push_back(q.front());
            q.pop();
        }
        return items;
    }
};
