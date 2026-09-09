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
        bool (*drain)(void *);
        void (*release)();
        bool prepared;
        bool accepted;
    };

    CleanupExecutor() : worker_([this] { work(); }) {
        try { drainer_ = std::thread([this] { drainWork(); }); }
        catch (...) { shutdown(); throw; }
    }
    ~CleanupExecutor() { waitIdle(); shutdown(); }
    CleanupExecutor(const CleanupExecutor &) = delete;
    CleanupExecutor &operator=(const CleanupExecutor &) = delete;

    Job *reserve(void (*run)(void *), void *data, bool (*prepare)(void *) = nullptr,
                 bool (*drain)(void *) = nullptr, void (*release)() = nullptr) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (closing_) return nullptr;
        Job *job = new Job{run, data, nullptr, prepare, drain, release, false, false};
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
        if (job->drain) {
            job->next = nullptr;
            if (drainTail_) drainTail_->next = job;
            else drainHead_ = job;
            drainTail_ = job;
            drainReady_.notify_one();
        } else enqueue(job);
    }

    // Refuse unloading while any reserved session can still call module code.
    // A refused shutdown leaves the executor open for new sessions.
    bool shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (outstanding_) return false;
            closing_ = true;
            stopping_ = true;
            ready_.notify_one();
            drainReady_.notify_one();
        }
        if (drainer_.joinable()) drainer_.join();
        if (worker_.joinable()) worker_.join();
        return true;
    }

    void waitIdle() {
        std::unique_lock<std::mutex> lock(mutex_);
        idle_.wait(lock, [this] { return outstanding_ == 0; });
    }

private:
    // Blocking joins/socket shutdown run separately from thread-affine prepare
    // and final release, so a slow old writer cannot stall a new reserve.
    void drainWork() {
        for (;;) {
            Job *job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                drainReady_.wait(lock, [this] { return drainHead_ || stopping_; });
                if (!drainHead_) return;
                job = drainHead_;
                drainHead_ = job->next;
                if (!drainHead_) drainTail_ = nullptr;
            }
            const bool drained = job->drain(job->data);
            std::lock_guard<std::mutex> lock(mutex_);
            if (drained) enqueue(job);
            else {
                // The owner deliberately retains resources on a failed join.
                delete job;
                if (!--outstanding_) idle_.notify_all();
            }
        }
    }

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
            {
                std::lock_guard<std::mutex> lock(mutex_);
                --outstanding_;
                if (job->release) job->release();
                delete job;
                if (!outstanding_) idle_.notify_all();
            }
        }
    }

    std::mutex mutex_;
    std::condition_variable ready_, idle_, prepared_, drainReady_;
    Job *drainHead_ = nullptr, *drainTail_ = nullptr;
    Job *head_ = nullptr;
    Job *tail_ = nullptr;
    std::size_t outstanding_ = 0;
    bool closing_ = false;
    bool stopping_ = false;
    std::thread worker_;
    std::thread drainer_;
};

}
#endif
