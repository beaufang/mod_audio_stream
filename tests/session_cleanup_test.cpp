// Exercise the actual module cleanup functions with a small FreeSWITCH host
// double. Real FreeSWITCH/Speex headers provide the API declarations; this is
// not an RTP integration test. Link with --gc-sections to discard unused paths.
#include "../audio_streamer_glue.cpp"
#include <future>
#include <iostream>

struct switch_channel { void *bug = nullptr; };
struct switch_media_bug { void *data = nullptr; };
struct switch_core_session {
    std::atomic<bool> retained{false};
    std::thread::id owner;
    switch_channel_t channel;
};

static std::promise<void> joining;
static void require(bool value, const char *message) {
    if (!value) { std::cerr << "FAILED: " << message << '\n'; std::abort(); }
}

extern "C" {
switch_channel_t *switch_core_session_get_channel(switch_core_session_t *s) { return &s->channel; }
void *switch_channel_get_private(switch_channel_t *c, const char *) { return c->bug; }
switch_status_t switch_channel_set_private(switch_channel_t *c, const char *, const void *p) {
    c->bug = const_cast<void *>(p);
    return SWITCH_STATUS_SUCCESS;
}
void *switch_core_media_bug_get_user_data(switch_media_bug_t *bug) { return bug->data; }
switch_status_t switch_core_media_bug_remove(switch_core_session_t *, switch_media_bug_t **) {
    require(false, "hangup callback must not recursively remove its own bug");
    return SWITCH_STATUS_SUCCESS;
}
switch_status_t switch_core_session_read_lock(switch_core_session_t *s) {
    s->owner = std::this_thread::get_id();
    s->retained = true;
    return SWITCH_STATUS_SUCCESS;
}
void switch_core_session_rwunlock(switch_core_session_t *s) {
    require(s->owner == std::this_thread::get_id(), "session rwlock changed owners");
    s->retained = false;
}
char *switch_core_session_get_uuid(switch_core_session_t *) {
    static char uuid[] = "cleanup-test";
    return uuid;
}
switch_status_t switch_mutex_lock(switch_mutex_t *m) {
    reinterpret_cast<std::recursive_mutex *>(m)->lock();
    return SWITCH_STATUS_SUCCESS;
}
switch_status_t switch_mutex_unlock(switch_mutex_t *m) {
    reinterpret_cast<std::recursive_mutex *>(m)->unlock();
    return SWITCH_STATUS_SUCCESS;
}
uint32_t switch_atomic_read(volatile switch_atomic_t *p) { return __atomic_load_n(p, __ATOMIC_SEQ_CST); }
void switch_atomic_set(volatile switch_atomic_t *p, uint32_t v) { __atomic_store_n(p, v, __ATOMIC_SEQ_CST); }
switch_status_t switch_thread_join(switch_status_t *status, switch_thread_t *thread) {
    joining.set_value();
    reinterpret_cast<std::thread *>(thread)->join();
    *status = SWITCH_STATUS_SUCCESS;
    return SWITCH_STATUS_SUCCESS;
}
void switch_buffer_destroy(switch_buffer_t **) {}
void switch_log_printf(switch_text_channel_t, const char *, const char *, int,
                      const char *, switch_log_level_t, const char *, ...) {}
void speex_resampler_destroy(SpeexResamplerState *) {}
}

WebSocketClient::~WebSocketClient() = default;
void WebSocketClient::disconnect() {}
bool WebSocketClient::isConnected() { return false; }
bool WebSocketClient::sendMessage(const char *, size_t) { return false; }

int main() {
    require(stream_module_init() == SWITCH_STATUS_SUCCESS, "cleanup worker did not start");
    switch_core_session_t session;
    private_t data{};
    std::recursive_mutex mutex, resamplerMutex;
    data.session = &session;
    data.mutex = reinterpret_cast<switch_mutex_t *>(&mutex);
    data.resampler_mutex = reinterpret_cast<switch_mutex_t *>(&resamplerMutex);
    switch_media_bug_t bug;
    bug.data = &data;
    session.channel.bug = &bug;
    data.pPlaybackQueue = new std::shared_ptr<PlaybackQueue>(std::make_shared<PlaybackQueue>(64));
    std::weak_ptr<PlaybackQueue> queue = *static_cast<std::shared_ptr<PlaybackQueue> *>(data.pPlaybackQueue);
    data.cleanup_job = cleanupExecutor->reserve(finish_session_cleanup, &data, retain_session);
    require(data.cleanup_job && session.retained, "session was published without a lease");
    std::promise<void> resume;
    auto resumed = resume.get_future();
    std::thread writer([&] {
        resumed.wait();
        require(session.retained && !queue.expired(), "cleanup freed a delayed writer's state");
        require(switch_atomic_read(&data.close_requested) != 0, "writer did not receive stop");
    });
    data.write_thread = reinterpret_cast<switch_thread_t *>(&writer);
    require(stream_session_cleanup(&session, nullptr, 1) == SWITCH_STATUS_SUCCESS, "hangup cleanup failed");
    require(!session.channel.bug, "hangup did not detach the published stream");
    joining.get_future().wait();
    require(stream_session_abort(&data) == SWITCH_STATUS_SUCCESS, "duplicate abort failed");
    require(session.retained && !queue.expired(), "abort released resources before join");
    resume.set_value();
    cleanupExecutor->waitIdle();
    require(!writer.joinable() && !session.retained && queue.expired(), "cleanup leaked worker, lease or queue");
    // Startup failure with no writer must release the same resources exactly once.
    data.cleanup_started = 0;
    data.write_thread = nullptr;
    data.cleanup_job = cleanupExecutor->reserve(finish_session_cleanup, &data, retain_session);
    require(stream_session_abort(&data) == SWITCH_STATUS_SUCCESS, "startup abort failed");
    cleanupExecutor->waitIdle();
    require(!session.retained, "startup failure leaked its session lease");
    require(stream_module_shutdown() == SWITCH_STATUS_SUCCESS, "module cleanup worker leaked");
    std::cout << "session_cleanup_test: PASS\n";
}
