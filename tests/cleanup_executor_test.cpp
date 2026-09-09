#include "../cleanup_executor.h"

#include <atomic>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

using mod_audio_stream::CleanupExecutor;

static void expect(bool condition, const char *message) {
    if (!condition) {
        std::cerr << "FAILED: " << message << '\n';
        std::exit(1);
    }
}

struct Session {
    std::atomic<bool> alive{false};
    std::promise<void> resume;
    std::promise<void> joining;
    std::thread writer;
    std::thread::id owner;

    static bool retain(void *p) {
        auto &s = *static_cast<Session *>(p);
        s.owner = std::this_thread::get_id();
        s.alive = true;
        return true;
    }

    static void cleanup(void *p) {
        auto &s = *static_cast<Session *>(p);
        s.joining.set_value();
        if (s.writer.joinable()) s.writer.join();
        expect(s.owner == std::this_thread::get_id(), "session lock must be released by its acquiring thread");
        s.alive = false;
    }
};

int main() {
    {
        CleanupExecutor executor;
        Session session;
        auto *job = executor.reserve(Session::cleanup, &session, Session::retain);
        expect(job && session.alive, "retain must finish before a writer is published");
        auto resume = session.resume.get_future();
        session.writer = std::thread([&] {
            resume.wait(); // Simulate a writer first scheduled after hangup.
            expect(session.alive, "pool must survive until the delayed writer exits");
        });
        executor.submit(job); // A CLOSE callback must return without joining.
        session.joining.get_future().wait();
        expect(session.alive, "cleanup must not release a session before join completes");
        expect(!executor.shutdown(), "module cannot unload while a writer still uses it");
        session.resume.set_value();
        executor.waitIdle();
        expect(!session.alive && !session.writer.joinable(), "cleanup must join and release the session");
        expect(executor.shutdown(), "idle executor must join its own worker");
    }
    {
        CleanupExecutor executor;
        Session session;
        auto *job = executor.reserve(Session::cleanup, &session, Session::retain);
        executor.submit(job); // Writer creation failed: still release the lease.
        executor.waitIdle();
        expect(!session.alive, "startup failure must release the session");
        auto reject = [](void *) -> bool { return false; };
        expect(!executor.reserve(Session::cleanup, &session, reject), "failed retain must reject a session");
        expect(executor.shutdown(), "failed reserve must not leak an outstanding job");
        expect(!executor.reserve(Session::cleanup, &session), "shutdown must reject new sessions");
    }
    {
        CleanupExecutor executor;
        std::atomic<int> released{0};
        std::vector<std::thread> producers;
        for (int i = 0; i < 4; ++i) {
            producers.emplace_back([&] {
                for (int j = 0; j < 250; ++j) {
                    auto *job = executor.reserve([](void *p) { ++*static_cast<std::atomic<int> *>(p); }, &released);
                    expect(job != nullptr, "reserve must succeed before shutdown");
                    executor.submit(job);
                }
            });
        }
        for (auto &producer : producers) producer.join();
        executor.waitIdle();
        expect(released == 1000, "every concurrent cleanup job must run exactly once");
    }
    std::cout << "cleanup_executor_test: PASS\n";
}
