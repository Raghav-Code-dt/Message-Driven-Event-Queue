#include <iostream>
#include <thread>
#include <vector>
#include <cassert>
#include <atomic>
#include "SafeQueue.hpp"
#include "TopicRouter.hpp"

using namespace broker;

void test_safe_queue() {
    std::cout << "Running SafeQueue tests...\n";
    SafeQueue<int> q(5);

    assert(q.empty());
    assert(q.capacity() == 5);

    for (int i = 0; i < 5; ++i) q.push(i);
    assert(q.size() == 5);

    // MPMC: 4 producers x 100 items, 4 consumers x 100 items
    SafeQueue<int> mpmc_q(1000);
    std::atomic<int> sum_produced{0};
    std::atomic<int> sum_consumed{0};

    auto producer = [&]() {
        for (int i = 1; i <= 100; ++i) {
            mpmc_q.push(i);
            sum_produced += i;
        }
    };
    auto consumer = [&]() {
        for (int i = 0; i < 100; ++i) {
            int val = mpmc_q.pop();
            sum_consumed += val;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) threads.emplace_back(producer);
    for (int i = 0; i < 4; ++i) threads.emplace_back(consumer);
    for (auto& t : threads) t.join();

    assert(mpmc_q.empty());
    assert(sum_produced == sum_consumed);
    std::cout << "SafeQueue tests passed.\n";
}

void test_topic_router() {
    std::cout << "Running TopicRouter tests...\n";
    TopicRouter router;

    // groupA has two members — competing consumers (round-robin)
    router.subscribe("order.created", "groupA", 1);
    router.subscribe("order.created", "groupA", 2);
    // groupB has one member — independent copy
    router.subscribe("order.created", "groupB", 3);
    // Wildcard subscriber — independent copy
    router.subscribe("order.*", "", 4);

    Message msg;
    msg.msg_id = 101;
    msg.topic  = "order.created";

    auto r1 = router.route(msg);
    auto r2 = router.route(msg);

    // Exactly 3 targets per route call:
    // groupA (1 member via rr), groupB (fd 3), wildcard (fd 4)
    assert(r1.size() == 3);
    assert(r2.size() == 3);

    // After two calls, groupA round-robin should have hit BOTH fd 1 and fd 2
    bool saw_fd1 = false, saw_fd2 = false;
    for (auto& [fd, _] : r1) { if (fd == 1) saw_fd1 = true; if (fd == 2) saw_fd2 = true; }
    for (auto& [fd, _] : r2) { if (fd == 1) saw_fd1 = true; if (fd == 2) saw_fd2 = true; }
    assert(saw_fd1 && saw_fd2);

    // Unsubscribe fd 3 and verify it no longer appears
    router.unsubscribe(3);
    auto r3 = router.route(msg);
    for (auto& [fd, _] : r3) assert(fd != 3);

    std::cout << "TopicRouter tests passed.\n";
}

int main() {
    test_safe_queue();
    test_topic_router();
    std::cout << "All Phase 1 tests passed successfully.\n";
    return 0;
}
