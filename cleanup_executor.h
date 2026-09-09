#ifndef MOD_AUDIO_STREAM_CLEANUP_EXECUTOR_H
#define MOD_AUDIO_STREAM_CLEANUP_EXECUTOR_H

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>

namespace mod_audio_stream {

// Reserve a job before publishing a session. Submitting from a CLOSE callback
// allocates nothing and never joins a thread that might need the callback's locks.
class CleanupExecutor {
public:
    struct Job {
        void (*run)(void *);
        void *data;
        Job *next;
        bool (*prepare)(void *);
        bool prepared;
        bool accepted;
    };

    CleanupExecutor() : worker_([this] { work(); }) {}
    ~CleanupExecutor() { waitIdle(); shutdown(); }
    CleanupExecutor(const CleanupExecutor &) = delete;
    CleanupExecutor &operator=(const CleanupExecutor &) = delete;

    Job *reserve(void (*run)(void *), void *data, bool (*prepare)(void *) = nullptr) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (closing_) return nullptr;
        Job *job = new Job{run, data, nullptr, prepare, false, false};
        ++outstanding_;
        enqueue(job);
        // Acquire/release thread-affine resources on the SAME worker. The caller
        // must keep data alive until reserve returns, before publishing it.
        prepared_.wait(lock, [job] { return job->prepared; });
        if (!job->accepted) {
            delete job;
            return nullptr;
        }
        return job;
    }

    void submit(Job *job) {
        std::lock_guard<std::mutex> lock(mutex_);
        enqueue(job);
    }

    // Refuse unloading while any reserved session can still call module code.
    // Once shutdown is requested, no new sessions may reserve jobs.
    bool shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closing_ = true;
            if (outstanding_) return false;
            stopping_ = true;
            ready_.notify_one();
        }
        if (worker_.joinable()) worker_.join();
        return true;
    }

    void waitIdle() {
        std::unique_lock<std::mutex> lock(mutex_);
        idle_.wait(lock, [this] { return outstanding_ == 0; });
    }

private:
    void enqueue(Job *job) {
        job->next = nullptr;
        if (tail_) tail_->next = job;
        else head_ = job;
        tail_ = job;
        ready_.notify_one();
    }

    void work() {
        for (;;) {
            Job *job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                ready_.wait(lock, [this] { return head_ || stopping_; });
                if (!head_) return;
                job = head_;
                head_ = job->next;
                if (!head_) tail_ = nullptr;
            }
            if (!job->prepared) {
                const bool accepted = !job->prepare || job->prepare(job->data);
                std::lock_guard<std::mutex> lock(mutex_);
                job->accepted = accepted;
                job->prepared = true;
                if (!accepted && !--outstanding_) idle_.notify_all();
                prepared_.notify_all();
                continue;
            }
            job->run(job->data);
            delete job;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                --outstanding_;
                if (!outstanding_) idle_.notify_all();
            }
        }
    }

    std::mutex mutex_;
    std::condition_variable ready_, idle_, prepared_;
    Job *head_ = nullptr;
    Job *tail_ = nullptr;
    std::size_t outstanding_ = 0;
    bool closing_ = false;
    bool stopping_ = false;
    std::thread worker_;
};

}
#endif
