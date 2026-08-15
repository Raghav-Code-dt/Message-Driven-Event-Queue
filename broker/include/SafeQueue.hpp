#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <optional>
#include <cstddef>

namespace broker {

template<typename T>
class SafeQueue {
public:
    explicit SafeQueue(std::size_t capacity) : capacity_(capacity) {}

    // Blocks if full (backpressure)
    void push(T item) {
        std::unique_lock<std::mutex> lock(mu_);
        not_full_.wait(lock, [this]() { return queue_.size() < capacity_; });
        queue_.push(std::move(item));
        lock.unlock();
        not_empty_.notify_one();
    }

    // Blocks until an item is available
    T pop() {
        std::unique_lock<std::mutex> lock(mu_);
        not_empty_.wait(lock, [this]() { return !queue_.empty(); });
        T item = std::move(queue_.front());
        queue_.pop();
        lock.unlock();
        not_full_.notify_one();
        return item;
    }

    // Returns std::nullopt after timeout if empty
    std::optional<T> try_pop_for(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!not_empty_.wait_for(lock, timeout, [this]() { return !queue_.empty(); })) {
            return std::nullopt;
        }
        T item = std::move(queue_.front());
        queue_.pop();
        lock.unlock();
        not_full_.notify_one();
        return item;
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.size();
    }

    std::size_t capacity() const {
        return capacity_;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.empty();
    }

private:
    std::queue<T>           queue_;
    mutable std::mutex      mu_;
    std::condition_variable not_full_;
    std::condition_variable not_empty_;
    std::size_t             capacity_;
};

} // namespace broker
