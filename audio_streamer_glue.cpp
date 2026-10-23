#include <string>
#include <cstring>
#include <cstdio>
#include <algorithm>
#include "audio_streamer_glue.h"
#include "playback_queue.h"
#include "cleanup_executor.h"
#include "WebSocketClient.h"
#include <switch_json.h>
#include <fstream>
#include <switch_buffer.h>
#include <unordered_set>
#include <atomic>
#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include "base64.h"

#define FRAME_SIZE_8000  320 /* 1000x0.02 (20ms)= 160 x(16bit= 2 bytes) 320 frame size*/
#define DEFAULT_PLAYBACK_BUFFER_MS 5000
#define MAX_PLAYBACK_BUFFER_MS 30000
#define PLAYBACK_CHANNELS 1
#define MAX_RAW_AUDIO_SAMPLE_RATE 64000

static const size_t MAX_RAW_AUDIO_BYTES =
    static_cast<size_t>(MAX_RAW_AUDIO_SAMPLE_RATE)
    * static_cast<size_t>(PLAYBACK_CHANNELS)
    * sizeof(spx_int16_t)
    * static_cast<size_t>(MAX_PLAYBACK_BUFFER_MS)
    / 1000;
static const size_t MAX_RAW_AUDIO_BASE64_BYTES =
    ((MAX_RAW_AUDIO_BYTES + 2) / 3) * 4;

using mod_audio_stream::EnqueueAudioResult;
using mod_audio_stream::EnqueueMarkResult;
using mod_audio_stream::FlushResult;
using mod_audio_stream::PlaybackQueue;
static mod_audio_stream::CleanupExecutor *cleanupExecutor = nullptr;
static std::atomic<bool> cleanupFailed{false};
static switch_thread_rwlock_t *moduleLock = nullptr;

using JsonPtr = std::unique_ptr<cJSON, decltype(&cJSON_Delete)>;

// Deferred, exception-safe ownership of FreeSWITCH's pool-allocated mutexes.
class SwitchLock {
public:
    explicit SwitchLock(switch_mutex_t *mutex) : mutex_(mutex), owned_(false) {}
    ~SwitchLock() { unlock(); }
    SwitchLock(const SwitchLock &) = delete;
    SwitchLock &operator=(const SwitchLock &) = delete;
    switch_status_t lock() {
        const auto status = switch_mutex_lock(mutex_);
        owned_ = status == SWITCH_STATUS_SUCCESS;
        return status;
    }
    switch_status_t tryLock() {
        const auto status = switch_mutex_trylock(mutex_);
        owned_ = status == SWITCH_STATUS_SUCCESS;
        return status;
    }
    void unlock() {
        if (owned_) { owned_ = false; switch_mutex_unlock(mutex_); }
    }
private:
    switch_mutex_t *mutex_;
    bool owned_;
};

struct SessionUnlock {
    switch_core_session_t *session;
    ~SessionUnlock() { switch_core_session_rwunlock(session); }
};

static std::shared_ptr<PlaybackQueue> getPlaybackQueue(private_t *techPvt)
{
    std::shared_ptr<PlaybackQueue> queue;
    if (!techPvt || !techPvt->mutex) return queue;

    if (switch_mutex_lock(techPvt->mutex) == SWITCH_STATUS_SUCCESS) {
        if (!switch_atomic_read(&techPvt->cleanup_started) && techPvt->pPlaybackQueue) {
            auto *queueWrap = static_cast<std::shared_ptr<PlaybackQueue> *>(
                techPvt->pPlaybackQueue);
            if (queueWrap && *queueWrap) {
                queue = *queueWrap;
            }
        }
        switch_mutex_unlock(techPvt->mutex);
    }
    return queue;
}

static bool isCloseRequested(private_t *techPvt)
{
    return !techPvt || switch_atomic_read(&techPvt->close_requested) != 0;
}

class AudioStreamer {
public:
    // Factory
    static std::shared_ptr<AudioStreamer> create(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers,
        const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation) {

        std::shared_ptr<AudioStreamer> sp(new AudioStreamer(
            uuid, wsUri, callback, deflate, heart_beat,
            suppressLog, extra_headers,
            tls_cafile, tls_keyfile,
            tls_certfile, tls_disable_hostname_validation
        ));

        sp->bindCallbacks(std::weak_ptr<AudioStreamer>(sp));

        sp->client->connect();

        return sp;
    }

    ~AudioStreamer()= default;

    void disconnect() {
        // Move ownership before joining the event thread. A callback may be in
        // writeText(), so never hold this mutex while waiting for it to finish.
        std::unique_ptr<WebSocketClient> closing;
        {
            std::lock_guard<std::mutex> lock(m_clientMutex);
            closing = std::move(client);
        }
        if (closing) closing->disconnect();
    }

    bool isConnected() {
        std::lock_guard<std::mutex> lock(m_clientMutex);
        return client && client->isConnected();
    }

    void writeBinary(uint8_t* buffer, size_t len) {
        std::lock_guard<std::mutex> lock(m_clientMutex);
        if (client && !isCleanedUp() && client->isConnected()) client->sendBinary(buffer, len);
    }

    void writeText(const char* text) {
        std::lock_guard<std::mutex> lock(m_clientMutex);
        if (client && !isCleanedUp() && client->isConnected()) client->sendMessage(text, strlen(text));
    }

    void sendPlaybackMarkDone(const std::string &markId) {
        JsonPtr root(cJSON_CreateObject(), &cJSON_Delete);
        JsonPtr data(cJSON_CreateObject(), &cJSON_Delete);
        if (!root || !data) {
            return;
        }

        cJSON_AddStringToObject(root.get(), "type", "playbackMarkDone");
        cJSON_AddStringToObject(data.get(), "markId", markId.c_str());
        cJSON_AddItemToObject(root.get(), "data", data.release());
        char *json = cJSON_PrintUnformatted(root.get());
        if (json) {
            writeText(json);
            switch_safe_free(json);
        }
    }

    void sendDtmfDigit(char digit) {
        char payload[64];
        std::snprintf(payload, sizeof(payload),
                      "{\"type\":\"dtmf\",\"data\":{\"digit\":\"%c\"}}", digit);
        writeText(payload);
    }

    void deleteFiles() {
        std::unordered_set<std::string> files;

        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            if (m_Files.empty())
                return;

            files.swap(m_Files);
            m_playFile = 0;
        }

        for (const auto& fn : files) {
            ::remove(fn.c_str());
        }
    }

    void markCleanedUp() {
        m_cleanedUp.store(true, std::memory_order_release);
        // Already-running callbacks retain self. disconnect() drains them on
        // the cleanup worker; clearing callbacks alone cannot cancel them.
    }

    bool isCleanedUp() const {
        return m_cleanedUp.load(std::memory_order_acquire);
    }

private:
    // Ctor
    AudioStreamer(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers,
        const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation
    ) : m_sessionId(uuid), m_notify(callback), m_suppress_log(suppressLog),
        m_extra_headers(extra_headers), m_playFile(0) {
        WebSocketHeaders hdrs;
        WebSocketTLSOptions tls;

        if (m_extra_headers) {
            cJSON *headers_json = cJSON_Parse(m_extra_headers);
            if (headers_json) {
                cJSON *iterator = headers_json->child;
                while (iterator) {
                    if (iterator->type == cJSON_String && iterator->valuestring != nullptr) {
                        hdrs.set(iterator->string, iterator->valuestring);
                    }
                    iterator = iterator->next;
                }
                cJSON_Delete(headers_json);
            }
        }

        client->setUrl(wsUri);

        // Setup TLS options
        // NONE - disables validation
        // SYSTEM - uses the system CAs bundle
        if (tls_cafile) {
            tls.caFile = tls_cafile;
        }

        if (tls_keyfile) {
            tls.keyFile = tls_keyfile;
        }

        if (tls_certfile) {
            tls.certFile = tls_certfile;
        }

        tls.disableHostnameValidation = tls_disable_hostname_validation;
        client->setTLSOptions(tls);

        // Optional heart beat, sent every xx seconds when there is not any traffic
        // to make sure that load balancers do not kill an idle connection.
        if(heart_beat)
            client->setPingInterval(heart_beat);

        // Per message deflate connection is enabled by default. You can tweak its parameters or disable it
        if(deflate)
            client->enableCompression(false);

        // Set extra headers if any
        if(!hdrs.empty())
            client->setHeaders(hdrs);
    }

    enum class MessageKind {
        JSON,
        PLAY_FILE,
        RAW_AUDIO,
        REJECTED_RAW_AUDIO,
        PLAYBACK_MARK
    };

    struct ProcessResult {
        MessageKind kind = MessageKind::JSON;
        std::string rewrittenJsonData;
        std::vector<std::string> errors;
        int sampleRate = 0;
        std::string rawAudio;
        std::string playbackMarkId;
    };

    static inline void push_err(ProcessResult& out, const std::string& sid, const std::string& s) {
        out.errors.push_back("(" + sid + ") " + s);
    }

    void bindCallbacks(std::weak_ptr<AudioStreamer> wp) {
        client->setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            try {
                self->eventCallback(MESSAGE, message.c_str());
            } catch (const std::exception &e) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                                  "WebSocket audio processing failed: %s\n", e.what());
                if (auto *session = switch_core_session_locate(self->m_sessionId.c_str())) {
                    SessionUnlock unlock{session};
                    self->media_bug_close(session);
                }
            }
        });

        client->setOpenCallback([wp]() {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "connected");
            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_SUCCESS, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client->setErrorCallback([wp](int code, const std::string& msg) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "error");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "error", msg.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_ERROR, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client->setCloseCallback([wp](int code, const std::string& reason) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "disconnected");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "reason", reason.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECTION_DROPPED, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });
    }

    switch_media_bug_t *get_media_bug(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if(!channel) {
            return nullptr;
        }
        auto *bug = (switch_media_bug_t *) switch_channel_get_private(channel, MY_BUG_NAME);
        if (isCleanedUp()) return nullptr;
        if (bug) {
            auto *tech = static_cast<private_t *>(switch_core_media_bug_get_user_data(bug));
            SwitchLock lock(tech->mutex);
            if (lock.lock() != SWITCH_STATUS_SUCCESS) return nullptr;
            auto *owner = static_cast<std::shared_ptr<AudioStreamer> *>(tech->pAudioStreamer);
            if (!owner || owner->get() != this || switch_atomic_read(&tech->cleanup_started)) return nullptr;
        }
        return bug;
    }

    void rejectPlaybackResponse(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if (!bug) return;

        auto *tech_pvt = static_cast<private_t *>(switch_core_media_bug_get_user_data(bug));
        if (!tech_pvt) return;
        const std::shared_ptr<PlaybackQueue> queue = getPlaybackQueue(tech_pvt);
        if (!queue) return;

        SwitchLock writeLock(tech_pvt->write_mutex);

        if (writeLock.lock() == SWITCH_STATUS_SUCCESS) {
            queue->rejectCurrentResponse();
            writeLock.unlock();
        }
    }

    inline void media_bug_close(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            switch_atomic_set(&tech_pvt->close_requested, 1);
            switch_core_media_bug_close(&bug, SWITCH_FALSE);
        }
    }

    inline void send_initial_metadata(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if(tech_pvt && strlen(tech_pvt->initialMetadata) > 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                          "sending initial metadata %s\n", tech_pvt->initialMetadata);
                writeText(tech_pvt->initialMetadata);
            }
        }
    }

    void enqueueRawAudio(switch_core_session_t *session,
                         std::string rawAudio,
                         int sampleRate) {
        auto *bug = get_media_bug(session);
        if (!bug) return;

        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || isCloseRequested(tech_pvt)) return;
        const std::shared_ptr<PlaybackQueue> queue = getPlaybackQueue(tech_pvt);
        if (!queue) return;

        const int outRate = tech_pvt->sampling;
        const int channels = tech_pvt->writeChannels;
        const int inRate = sampleRate ? sampleRate : tech_pvt->wsSampling;
        const size_t inputBytes = rawAudio.size();

        if (rawAudio.empty() || channels <= 0 || inRate <= 0 || outRate <= 0) return;
        if (rawAudio.size() % (sizeof(spx_int16_t) * static_cast<size_t>(channels)) != 0) {
            rejectPlaybackResponse(session);
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "%s enqueueRawAudio: invalid PCM byte count %zu for %d channel(s)\n",
                              tech_pvt->sessionId, rawAudio.size(), channels);
            return;
        }

        SwitchLock writeLock(tech_pvt->write_mutex);
        SwitchLock resamplerLock(tech_pvt->resampler_mutex);

        if (writeLock.lock() != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "%s enqueueRawAudio: write mutex lock failed, dropping %zu bytes\n",
                              tech_pvt->sessionId, rawAudio.size());
            return;
        }

        EnqueueAudioResult enqueueResult = EnqueueAudioResult::OVERFLOW;
        if (inRate == outRate) {
            // The queue barrier rejects network-late PCM; generation handles in-flight work.
            const std::uint64_t queueGeneration = queue->generation();
            enqueueResult =
                queue->enqueueAudioIfGeneration(std::move(rawAudio), queueGeneration);
            writeLock.unlock();
        } else {
            writeLock.unlock();

            std::string output;
            int resampleStatus = RESAMPLER_ERR_SUCCESS;
            spx_uint32_t inFrames = 0;
            spx_uint32_t inLength = 0;
            spx_uint32_t outLength = 0;
            bool resamplerReady = false;

            if (resamplerLock.lock() != SWITCH_STATUS_SUCCESS) {
                rejectPlaybackResponse(session);
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: resampler mutex lock failed, dropping %zu bytes\n",
                    tech_pvt->sessionId,
                    rawAudio.size());
                return;
            }
            if (isCloseRequested(tech_pvt)) {
                resamplerLock.unlock();
                return;
            }

            if (writeLock.lock() != SWITCH_STATUS_SUCCESS) {
                resamplerLock.unlock();
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: playback mutex lock failed before resampling\n",
                    tech_pvt->sessionId);
                return;
            }
            if (queue->isDiscarding()) {
                writeLock.unlock();
                resamplerLock.unlock();
                return;
            }
            inFrames = static_cast<spx_uint32_t>(
                rawAudio.size() / (sizeof(spx_int16_t) * static_cast<size_t>(channels)));
            inLength = inFrames;
            outLength = static_cast<spx_uint32_t>(
                (static_cast<uint64_t>(inFrames) * static_cast<uint64_t>(outRate)
                 + static_cast<uint64_t>(inRate) - 1)
                / static_cast<uint64_t>(inRate) + 8);
            const size_t outputCapacityBytes =
                static_cast<size_t>(outLength)
                * static_cast<size_t>(channels)
                * sizeof(spx_int16_t);
            if (outputCapacityBytes > queue->availableBytes()) {
                queue->rejectCurrentResponse();
                writeLock.unlock();
                resamplerLock.unlock();
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: resampled chunk exceeds available playback capacity, "
                    "dropping %zu input bytes\n",
                    tech_pvt->sessionId,
                    inputBytes);
                return;
            }
            const std::uint64_t queueGeneration = queue->generation();
            writeLock.unlock();

            int initStatus = RESAMPLER_ERR_SUCCESS;
            if (!tech_pvt->write_resampler || tech_pvt->writeResamplerInputRate != inRate) {
                if (tech_pvt->write_resampler) {
                    speex_resampler_destroy(tech_pvt->write_resampler);
                    tech_pvt->write_resampler = nullptr;
                }
                tech_pvt->write_resampler = speex_resampler_init(
                    channels, inRate, outRate, SWITCH_RESAMPLE_QUALITY, &initStatus);
                tech_pvt->writeResamplerInputRate =
                    initStatus == RESAMPLER_ERR_SUCCESS ? inRate : 0;
            }

            if (initStatus == RESAMPLER_ERR_SUCCESS && tech_pvt->write_resampler) {
                output.resize(outputCapacityBytes);
                auto *outputSamples = reinterpret_cast<spx_int16_t *>(&output[0]);
                resampleStatus = channels == 1
                    ? speex_resampler_process_int(
                        tech_pvt->write_resampler,
                        0,
                        reinterpret_cast<const spx_int16_t *>(rawAudio.data()),
                        &inLength,
                        outputSamples,
                        &outLength)
                    : speex_resampler_process_interleaved_int(
                        tech_pvt->write_resampler,
                        reinterpret_cast<const spx_int16_t *>(rawAudio.data()),
                        &inLength,
                        outputSamples,
                        &outLength);
                output.resize(
                    static_cast<size_t>(outLength)
                    * static_cast<size_t>(channels)
                    * sizeof(spx_int16_t));
                resamplerReady = true;
            }

            if (!resamplerReady) {
                resamplerLock.unlock();
                rejectPlaybackResponse(session);
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: failed to initialize %d Hz -> %d Hz resampler (%s)\n",
                    tech_pvt->sessionId,
                    inRate,
                    outRate,
                    speex_resampler_strerror(initStatus));
                return;
            }
            if (resampleStatus != RESAMPLER_ERR_SUCCESS || inLength != inFrames) {
                resamplerLock.unlock();
                rejectPlaybackResponse(session);
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: resampling failed (%s), consumed %u/%u frames\n",
                    tech_pvt->sessionId,
                    speex_resampler_strerror(resampleStatus),
                    inLength,
                    inFrames);
                return;
            }

            if (writeLock.lock() != SWITCH_STATUS_SUCCESS) {
                resamplerLock.unlock();
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: playback mutex lock failed after resampling\n",
                    tech_pvt->sessionId);
                return;
            }
            enqueueResult = queue->enqueueAudioIfGeneration(
                std::move(output), queueGeneration);
            writeLock.unlock();
            resamplerLock.unlock();
        }

        if (enqueueResult == EnqueueAudioResult::OVERFLOW) {
            switch_log_printf(
                SWITCH_CHANNEL_SESSION_LOG(session),
                SWITCH_LOG_WARNING,
                "%s enqueueRawAudio: playback buffer full, dropping %zu input bytes\n",
                tech_pvt->sessionId,
                inputBytes);
        } else if (enqueueResult == EnqueueAudioResult::DISCARDED) {
            switch_log_printf(
                SWITCH_CHANNEL_SESSION_LOG(session),
                SWITCH_LOG_WARNING,
                "%s enqueueRawAudio: DISCARDED (generation/discarding), %zu input bytes\n",
                tech_pvt->sessionId,
                inputBytes);

        }
    }

    void enqueuePlaybackMark(switch_core_session_t *session, const std::string &markId) {
        auto *bug = get_media_bug(session);
        if (!bug) return;

        auto *tech_pvt = static_cast<private_t *>(switch_core_media_bug_get_user_data(bug));
        if (!tech_pvt || isCloseRequested(tech_pvt)) return;
        const std::shared_ptr<PlaybackQueue> queue = getPlaybackQueue(tech_pvt);
        if (!queue) return;

        SwitchLock writeLock(tech_pvt->write_mutex);

        if (writeLock.lock() != SWITCH_STATUS_SUCCESS) {
            return;
        }
        const EnqueueMarkResult result = queue->enqueueMark(markId);
        writeLock.unlock();

        if (result == EnqueueMarkResult::INVALIDATED) {
            switch_log_printf(
                SWITCH_CHANNEL_SESSION_LOG(session),
                SWITCH_LOG_DEBUG,
                "%s enqueuePlaybackMark: invalidated flushed mark %s\n",
                tech_pvt->sessionId,
                markId.c_str());
        } else if (result == EnqueueMarkResult::INVALID) {
            switch_log_printf(
                SWITCH_CHANNEL_SESSION_LOG(session),
                SWITCH_LOG_WARNING,
                "%s enqueuePlaybackMark: invalid markId rejected\n",
                tech_pvt->sessionId);
        } else if (result == EnqueueMarkResult::OVERFLOW) {
            switch_log_printf(
                SWITCH_CHANNEL_SESSION_LOG(session),
                SWITCH_LOG_WARNING,
                "%s enqueuePlaybackMark: pending marker limit reached\n",
                tech_pvt->sessionId);
        }
    }

    void eventCallback(notifyEvent_t event, const char* message) {
        std::string msg = message ? message : "";

        // processing without holding a session
        ProcessResult pr;
        if (event == MESSAGE) {
            pr = processMessage(msg);
            if (pr.kind == MessageKind::PLAY_FILE) {
                msg = pr.rewrittenJsonData;
            }
        }

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) {
            return;
        }

        SessionUnlock sessionUnlock{psession};
        if (isCleanedUp()) return;

        for (const auto& e : pr.errors) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_ERROR, "%s\n", e.c_str());
        }

        switch (event) {
            case CONNECT_SUCCESS:
                send_initial_metadata(psession);
                m_notify(psession, EVENT_CONNECT, msg.c_str());
                break;

            case CONNECTION_DROPPED:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection closed\n");
                m_notify(psession, EVENT_DISCONNECT, msg.c_str());
                break;

            case CONNECT_ERROR:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection error\n");
                m_notify(psession, EVENT_ERROR, msg.c_str());
                media_bug_close(psession);
                break;

            case MESSAGE:
                switch (pr.kind) {
                    case MessageKind::RAW_AUDIO: {
                        switch_channel_t *ch = switch_core_session_get_channel(psession);
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                          "STREAMDBG streamAudio received bytes=%zu sampleRate=%d\n",
                                          pr.rawAudio.size(), pr.sampleRate);
                        if (ch && switch_channel_ready(ch)) {
                            enqueueRawAudio(psession, std::move(pr.rawAudio), pr.sampleRate);
                        } else {
                            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_WARNING,
                                              "STREAMDBG channel not ready, dropping %zu audio bytes\n",
                                              pr.rawAudio.size());
                        }
                        break;
                    }
                    case MessageKind::REJECTED_RAW_AUDIO:
                        rejectPlaybackResponse(psession);
                        break;
                    case MessageKind::PLAYBACK_MARK:
                        enqueuePlaybackMark(psession, pr.playbackMarkId);
                        break;
                    case MessageKind::PLAY_FILE:
                        m_notify(psession, EVENT_PLAY, msg.c_str());
                        break;
                    case MessageKind::JSON:
                        m_notify(psession, EVENT_JSON, msg.c_str());
                        break;
                }

                if (!m_suppress_log
                    && pr.kind != MessageKind::RAW_AUDIO
                    && pr.kind != MessageKind::REJECTED_RAW_AUDIO) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                    "response: %s\n", msg.c_str());
                }
                break;
        }

    }


    ProcessResult processMessage(const std::string& message) {
        ProcessResult out;

        if (isCleanedUp()) return out;

        JsonPtr root(cJSON_Parse(message.c_str()), &cJSON_Delete);
        if (!root) return out;

        const char* jsonType = cJSON_GetObjectCstr(root.get(), "type");
        if (!jsonType) {
            return out; // not ours
        }

        if (std::strcmp(jsonType, "playbackMark") == 0) {
            cJSON *jsonData = cJSON_GetObjectItem(root.get(), "data");
            const char *markId = jsonData ? cJSON_GetObjectCstr(jsonData, "markId") : nullptr;
            if (!markId) {
                markId = cJSON_GetObjectCstr(root.get(), "markId");
            }
            if (!markId || markId[0] == '\0') {
                push_err(out, m_sessionId, "processMessage - playbackMark missing non-empty markId");
                return out;
            }
            out.kind = MessageKind::PLAYBACK_MARK;
            out.playbackMarkId = markId;
            return out;
        }

        if (std::strcmp(jsonType, "streamAudio") != 0) {
            return out;
        }

        cJSON* jsonData = cJSON_GetObjectItem(root.get(), "data");
        if (!jsonData) {
            push_err(out, m_sessionId, "processMessage - no data in streamAudio");
            return out;
        }

        const char* jsAudioDataType = cJSON_GetObjectCstr(jsonData, "audioDataType");
        if (!jsAudioDataType) jsAudioDataType = "";
        const bool isRaw = std::strcmp(jsAudioDataType, "raw") == 0;
        if (isRaw) {
            out.kind = MessageKind::REJECTED_RAW_AUDIO;
        }

        JsonPtr jsonAudio(cJSON_DetachItemFromObject(jsonData, "audioData"), &cJSON_Delete);

        if (!jsonAudio) {
            push_err(out, m_sessionId, "processMessage - streamAudio missing 'audioData' field");
            return out;
        }

        if (!cJSON_IsString(jsonAudio.get()) || !jsonAudio->valuestring) {
            push_err(out, m_sessionId, "processMessage - 'audioData' is not a string (expected base64 string)");
            return out;
        }

        // sampleRate (only meaningful for raw)
        int sampleRate = 0;
        if (cJSON* jsonSampleRate = cJSON_GetObjectItem(jsonData, "sampleRate")) {
            sampleRate = jsonSampleRate->valueint;
        }

        // map file type (raw is handled out-of-band via write buffer; see eventCallback)
        std::string fileType;
        if (isRaw) {
            switch (sampleRate) {
                case 8000:
                case 16000:
                case 24000:
                case 32000:
                case 48000:
                case 64000:
                    break;
                default:
                    push_err(out, m_sessionId, "processMessage - unsupported sample rate: " + std::to_string(sampleRate));
                    return out;
            }
            const size_t encodedAudioBytes = std::strlen(jsonAudio->valuestring);
            if (encodedAudioBytes > MAX_RAW_AUDIO_BASE64_BYTES) {
                push_err(
                    out,
                    m_sessionId,
                    "processMessage - raw audioData exceeds the configured playback limit");
                return out;
            }
        } else if (std::strcmp(jsAudioDataType, "wav") == 0)  fileType = ".wav";
        else if (std::strcmp(jsAudioDataType, "mp3") == 0)   fileType = ".mp3";
        else if (std::strcmp(jsAudioDataType, "ogg") == 0)   fileType = ".ogg";
        else if (std::strcmp(jsAudioDataType, "pcmu") == 0)  fileType = ".pcmu";
        else if (std::strcmp(jsAudioDataType, "pcma") == 0)  fileType = ".pcma";
        else {
            push_err(out, m_sessionId, "processMessage - unsupported audio type: " + std::string(jsAudioDataType));
            return out;
        }

        // base64 decode
        std::string decoded;
        try {
            decoded = base64_decode(jsonAudio->valuestring);
        } catch (const std::exception& e) {
            push_err(out, m_sessionId, "processMessage - base64 decode error: " + std::string(e.what()));
            return out;
        }

        if (isRaw) {
            out.kind = MessageKind::RAW_AUDIO;
            out.sampleRate = sampleRate;
            out.rawAudio = std::move(decoded);
            return out;
        }

        // reserve file index
        int idx = 0;
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            idx = m_playFile++;
        }

        char filePath[256];
        switch_snprintf(filePath, sizeof(filePath), "%s%s%s_%d.tmp%s",
                        SWITCH_GLOBAL_dirs.temp_dir, SWITCH_PATH_SEPARATOR,
                        m_sessionId.c_str(), idx, fileType.c_str());

        // write file
        {
            std::ofstream f(filePath, std::ios::binary);
            if (!f.is_open()) {
                push_err(out, m_sessionId, std::string("processMessage - failed to open file for write: ") + filePath);
                return out;
            }
            f.write(decoded.data(), static_cast<std::streamsize>(decoded.size()));
            if (!f.good()) {
                push_err(out, m_sessionId, std::string("processMessage - failed writing file: ") + filePath);
                return out;
            }
        }

        // track file for cleanup
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            m_Files.insert(filePath);
        }

        cJSON_AddItemToObject(jsonData, "file", cJSON_CreateString(filePath));

        // return rewritten jsonData as string
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        if (!jsonString) {
            push_err(out, m_sessionId, "processMessage - cJSON_PrintUnformatted failed");
            return out;
        }

        out.rewrittenJsonData.assign(jsonString);
        std::free(jsonString);
        out.kind = MessageKind::PLAY_FILE;
        return out;
    }

private:
    std::string m_sessionId;
    responseHandler_t m_notify;
    std::mutex m_clientMutex;
    std::unique_ptr<WebSocketClient> client{new WebSocketClient};
    bool m_suppress_log;
    const char* m_extra_headers;
    int m_playFile;
    std::unordered_set<std::string> m_Files;
    std::atomic<bool> m_cleanedUp{false};
    std::mutex m_stateMutex;
};


namespace {

    struct WriterMedia {
        switch_codec_t codec{};
        switch_timer_t timer{};
        bool codecReady = false;
        bool timerReady = false;
        ~WriterMedia() {
            if (timerReady) switch_core_timer_destroy(&timer);
            if (codecReady) switch_core_codec_destroy(&codec);
        }
    };

    void write_frames(private_t *tech_pvt) {
        switch_core_session_t *session = tech_pvt->session;
        switch_channel_t *channel = switch_core_session_get_channel(session);
        const std::shared_ptr<PlaybackQueue> playbackQueue = getPlaybackQueue(tech_pvt);
        if (!channel || !playbackQueue || isCloseRequested(tech_pvt)) return;

        WriterMedia media;
        switch_timer_t &timer = media.timer;
        switch_frame_t write_frame = {};
        switch_codec_t &write_codec = media.codec;
        switch_codec_t *read_codec;

        uint32_t sample_rate = tech_pvt->sampling;
        uint32_t channels = tech_pvt->writeChannels;

        read_codec = switch_core_session_get_read_codec(session);
        if (!read_codec || !read_codec->implementation) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "write_frame_thread: no read codec available, shutting down\n");
            return;
        }

        uint32_t interval = read_codec->implementation->microseconds_per_packet / 1000;
        uint32_t samples = switch_samples_per_packet(sample_rate, interval);
        uint32_t tsamples = samples;
        uint32_t bytes = samples * 2 * channels;
        if (!interval || !bytes || bytes > SWITCH_RECOMMENDED_BUFFER_SIZE) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "Invalid playback frame size or packet interval\n");
            return;
        }

        if (switch_core_codec_init(&write_codec, "L16", NULL, NULL, sample_rate, interval, channels,
                                   SWITCH_CODEC_FLAG_ENCODE | SWITCH_CODEC_FLAG_DECODE, NULL,
                                   switch_core_session_get_pool(session)) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "write_frame_thread: Codec Init Failed. Cannot Start Write Thread\n");
            return;
        }
        media.codecReady = true;
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                          "Codec Activated L16@%uhz %u channels %dms\n", sample_rate, channels, interval);
        write_frame.codec = &write_codec;
        write_frame.data = switch_core_session_alloc(session, SWITCH_RECOMMENDED_BUFFER_SIZE);
        write_frame.channels = channels;
        write_frame.rate = sample_rate;
        write_frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                          "started write frame thread with sample rate [%u] interval [%u] samples [%u] tsamples [%u] bytes [%u]\n",
                          sample_rate, interval, samples, tsamples, bytes);

        if (switch_core_timer_init(&timer, "soft", interval, tsamples,
                                   switch_core_session_get_pool(session)) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Timer Setup Failed. Cannot Start Write Thread\n");
            return;
        }

        media.timerReady = true;
        SwitchLock writeLock(tech_pvt->write_mutex);

        std::vector<std::string> completedMarks;
        // 20ms 节拍统计,每 500 个成功 tick(约 10s)输出一次:区分下行静音的三种
        // 来源——队列空转(idleTicks 占满,上游未送 PCM)、上游送零(audioTicks 高
        // 但 nonZeroBytes 为 0)、帧写入后被静默丢弃(audioTicks 与 nonZeroBytes
        // 均正常但通话无声)。
        std::uint64_t statIdleTicks = 0, statAudioTicks = 0;
        std::uint64_t statAudioBytes = 0, statNonZeroBytes = 0;
        while (!isCloseRequested(tech_pvt) && switch_core_session_running(session)) {
            if (switch_channel_test_flag(channel, CF_HOLD)
                || switch_channel_test_flag(channel, CF_AUDIO_PAUSE_WRITE)) {
                switch_core_timer_next(&timer);
                continue;
            }
            bool playbackSucceeded = false;
            std::uint64_t batchGeneration = 0;
            if (writeLock.tryLock() == SWITCH_STATUS_SUCCESS) {
                batchGeneration = playbackQueue->generation();
                const size_t audioBytes = playbackQueue->takeInto(
                    static_cast<uint8_t *>(write_frame.data),
                    bytes,
                    completedMarks);

                if (audioBytes == 0) {
                    ++statIdleTicks;
                } else {
                    ++statAudioTicks;
                    statAudioBytes += audioBytes;
                    const uint8_t *pcm = static_cast<const uint8_t *>(write_frame.data);
                    for (size_t i = 0; i < audioBytes; ++i) {
                        if (pcm[i] != 0) ++statNonZeroBytes;
                    }
                }
                if (statIdleTicks + statAudioTicks >= 500) {
                    switch_log_printf(
                        SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                        "%s playback stats: idleTicks=%llu audioTicks=%llu "
                        "audioBytes=%llu nonZeroBytes=%llu\n",
                        tech_pvt->sessionId,
                        static_cast<unsigned long long>(statIdleTicks),
                        static_cast<unsigned long long>(statAudioTicks),
                        static_cast<unsigned long long>(statAudioBytes),
                        static_cast<unsigned long long>(statNonZeroBytes));
                    statIdleTicks = statAudioTicks = 0;
                    statAudioBytes = statNonZeroBytes = 0;
                }

                playbackSucceeded = audioBytes == 0;
                if (audioBytes > 0 && switch_channel_ready(channel)) {
                    // A short final chunk must not remain buffered in the core
                    // until the next response. Submit a complete packet of PCM.
                    std::memset(static_cast<uint8_t *>(write_frame.data) + audioBytes, 0, bytes - audioBytes);
                    // 帧对象跨 tick 复用,每帧显式清空标记,不继承任何下层设置
                    write_frame.flags = 0;
                    write_frame.datalen = bytes;
                    write_frame.samples = samples;
                    playbackSucceeded = !isCloseRequested(tech_pvt)
                        && switch_core_session_write_frame(
                            session, &write_frame, SWITCH_IO_FLAG_NONE, 0) == SWITCH_STATUS_SUCCESS;
                    if (!playbackSucceeded) {
                        // Do not acknowledge this or later marks after a failed write.
                        switch_atomic_set(&tech_pvt->close_requested, 1);
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                                          "Channel playback write failed; stopping stream\n");
                    }
                } else if (!switch_channel_ready(channel)) {
                    playbackSucceeded = false;
                }
                writeLock.unlock();
            } else {
                completedMarks.clear();
            }

            if (playbackSucceeded && !completedMarks.empty()) {
                std::shared_ptr<AudioStreamer> streamer;
                if (switch_mutex_lock(tech_pvt->mutex) == SWITCH_STATUS_SUCCESS) {
                    if (tech_pvt->pAudioStreamer) {
                        auto *spWrap = static_cast<std::shared_ptr<AudioStreamer> *>(
                            tech_pvt->pAudioStreamer);
                        if (spWrap && *spWrap) {
                            streamer = *spWrap;
                        }
                    }
                    switch_mutex_unlock(tech_pvt->mutex);
                }
                if (streamer && writeLock.lock() == SWITCH_STATUS_SUCCESS) {
                    if (!isCloseRequested(tech_pvt) && batchGeneration == playbackQueue->generation()) {
                        for (const std::string &markId : completedMarks) streamer->sendPlaybackMarkDone(markId);
                    }
                    writeLock.unlock();
                }
            }
            switch_core_timer_next(&timer);
        }

        return;
    }

    void *SWITCH_THREAD_FUNC write_frame_thread(switch_thread_t *, void *obj) {
        auto *tech_pvt = static_cast<private_t *>(obj);
        try {
            write_frames(tech_pvt);
        } catch (const std::exception &e) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(tech_pvt->session), SWITCH_LOG_ERROR,
                              "Playback worker failed: %s\n", e.what());
        }
        // Startup and transport failures must also detach the bug and schedule
        // cleanup. The executor can join this thread after this callback returns.
        if (!switch_atomic_read(&tech_pvt->cleanup_started)) {
            stream_session_cleanup(tech_pvt->session, nullptr, 0);
        }
        std::shared_ptr<AudioStreamer> streamer;
        switch_mutex_lock(tech_pvt->mutex);
        if (tech_pvt->pAudioStreamer) {
            streamer = *static_cast<std::shared_ptr<AudioStreamer> *>(tech_pvt->pAudioStreamer);
        }
        switch_mutex_unlock(tech_pvt->mutex);
        if (streamer) streamer->disconnect();
        return nullptr;
    }

    switch_status_t stream_data_init(private_t *tech_pvt, switch_core_session_t *session, char *wsUri,
                                     uint32_t sampling, int desiredSampling, int channels, char *metadata, responseHandler_t responseHandler,
                                     int deflate, int heart_beat, bool suppressLog, int rtp_packets,
                                     int playbackBufferMs, const char* extra_headers,
                                     const char *tls_cafile, const char *tls_keyfile, const char *tls_certfile,
                                     bool tls_disable_hostname_validation)
    {
        int err; //speex

        switch_memory_pool_t *pool = switch_core_session_get_pool(session);


        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
        strncpy(tech_pvt->ws_uri, wsUri, MAX_WS_URI);
        tech_pvt->sampling = sampling;
        tech_pvt->wsSampling = desiredSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->writeChannels = PLAYBACK_CHANNELS;
        switch_atomic_set(&tech_pvt->audio_paused, 0);

        if (metadata) {
            strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);
            JsonPtr meta(cJSON_Parse(metadata), &cJSON_Delete);
            const cJSON *relay = meta ? cJSON_GetObjectItem(meta.get(), "dtmfRelay") : nullptr;
            if (cJSON_IsTrue(relay)
                || (cJSON_IsNumber(relay) && relay->valuedouble != 0)) {
                tech_pvt->dtmf_relay = 1;
            }
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                              "(%s) dtmf relay %s\n",
                              tech_pvt->sessionId, tech_pvt->dtmf_relay ? "enabled" : "disabled");
        }

        //size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * 1000 / RTP_PERIOD * BUFFERED_SEC);
        const size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * rtp_packets);
        
        auto sp = AudioStreamer::create(tech_pvt->sessionId, wsUri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers,
                                        tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        const size_t playbackCapacity =
            static_cast<size_t>(sampling)
            * static_cast<size_t>(tech_pvt->writeChannels)
            * sizeof(spx_int16_t)
            * static_cast<size_t>(playbackBufferMs)
            / 1000;
        tech_pvt->pPlaybackQueue = new std::shared_ptr<PlaybackQueue>(
            std::make_shared<PlaybackQueue>(playbackCapacity));

        if (switch_buffer_create(pool, &tech_pvt->read_sbuffer, buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating read switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        if (desiredSampling != sampling) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) resampling from %u to %u\n", tech_pvt->sessionId, sampling, desiredSampling);
            tech_pvt->read_resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing read resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        }
        else {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) no resampling needed for this call\n", tech_pvt->sessionId);
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_data_init\n", tech_pvt->sessionId);

        return SWITCH_STATUS_SUCCESS;
    }

    void destroy_tech_pvt(private_t* tech_pvt) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s destroy_tech_pvt\n", tech_pvt->sessionId);
        if (tech_pvt->read_resampler) {
            speex_resampler_destroy(tech_pvt->read_resampler);
            tech_pvt->read_resampler = nullptr;
        }
        if (tech_pvt->write_resampler) {
            speex_resampler_destroy(tech_pvt->write_resampler);
            tech_pvt->write_resampler = nullptr;
        }
        // Pool mutexes are reclaimed with the session, after all session readers
        // (including concurrent API callers) have released their references.
        if (tech_pvt->read_sbuffer) {
            switch_buffer_destroy(&tech_pvt->read_sbuffer);
            tech_pvt->read_sbuffer = nullptr;
        }
        if (tech_pvt->pPlaybackQueue) {
            delete static_cast<std::shared_ptr<PlaybackQueue> *>(tech_pvt->pPlaybackQueue);
            tech_pvt->pPlaybackQueue = nullptr;
        }
    }

    bool retain_session(void *data) {
        auto *tech_pvt = static_cast<private_t *>(data);
        if (moduleLock && switch_thread_rwlock_rdlock(moduleLock) != SWITCH_STATUS_SUCCESS) return false;
        if (switch_core_session_read_lock(tech_pvt->session) == SWITCH_STATUS_SUCCESS) return true;
        if (moduleLock) switch_thread_rwlock_unlock(moduleLock);
        return false;
    }

    void release_module() {
        if (moduleLock) switch_thread_rwlock_unlock(moduleLock);
    }

    bool drain_session(void *data) {
        auto *tech_pvt = static_cast<private_t *>(data);
        switch_core_session_t *session = tech_pvt->session;
        if (tech_pvt->write_thread) {
            switch_status_t status = SWITCH_STATUS_SUCCESS;
            // This runs outside all media callbacks/session I/O locks. Retain
            // the session read lock until both the thread and its APR handle
            // have been joined; neither is allowed to outlive the session pool.
            if (switch_thread_join(&status, tech_pvt->write_thread) != SWITCH_STATUS_SUCCESS) {
                // With exactly one owner, failure means our ownership invariant
                // is broken. Never free memory a possibly live thread still uses.
                cleanupFailed.store(true);
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_CRIT,
                                  "Cannot join playback worker; retaining session to prevent use-after-free\n");
                return false;
            }
        }
        auto *streamer = static_cast<std::shared_ptr<AudioStreamer> *>(tech_pvt->pAudioStreamer);
        if (streamer && *streamer) {
            (*streamer)->markCleanedUp();
            (*streamer)->disconnect();
            (*streamer)->deleteFiles();
        }
        return true;
    }

    void finish_session_cleanup(void *data) {
        auto *tech_pvt = static_cast<private_t *>(data);
        switch_core_session_t *session = tech_pvt->session;
        auto *streamer = static_cast<std::shared_ptr<AudioStreamer> *>(tech_pvt->pAudioStreamer);
        switch_mutex_lock(tech_pvt->resampler_mutex);
        switch_mutex_lock(tech_pvt->mutex);
        tech_pvt->pAudioStreamer = nullptr;
        delete streamer;
        destroy_tech_pvt(tech_pvt);
        switch_mutex_unlock(tech_pvt->mutex);
        switch_mutex_unlock(tech_pvt->resampler_mutex);
        switch_core_session_rwunlock(session);
    }

}

extern "C" {
    switch_status_t stream_module_init(switch_thread_rwlock_t *module_lock) {
        moduleLock = module_lock;
        try {
            cleanupExecutor = new mod_audio_stream::CleanupExecutor;
            return SWITCH_STATUS_SUCCESS;
        } catch (const std::exception &e) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                              "Cannot start stream cleanup worker: %s\n", e.what());
            return SWITCH_STATUS_FALSE;
        }
    }

    switch_status_t stream_module_shutdown(void) {
        if (cleanupFailed.load() || (cleanupExecutor && !cleanupExecutor->shutdown())) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
                              "Active audio streams are still cleaning up; retaining module code\n");
            return SWITCH_STATUS_NOUNLOAD;
        }
        delete cleanupExecutor;
        cleanupExecutor = nullptr;
        return SWITCH_STATUS_SUCCESS;
    }

    int validate_ws_uri(const char* url, char* wsUri) {
        const char* scheme = nullptr;
        const char* hostStart = nullptr;
        const char* hostEnd = nullptr;
        const char* portStart = nullptr;

        // Check scheme
        if (strncmp(url, "ws://", 5) == 0) {
            scheme = "ws";
            hostStart = url + 5;
        } else if (strncmp(url, "wss://", 6) == 0) {
            scheme = "wss";
            hostStart = url + 6;
        } else {
            return 0;
        }

        // Find host end or port start
        hostEnd = hostStart;
        while (*hostEnd && *hostEnd != ':' && *hostEnd != '/') {
            if (!std::isalnum(*hostEnd) && *hostEnd != '-' && *hostEnd != '.') {
                return 0;
            }
            ++hostEnd;
        }

        // Check if host is empty
        if (hostStart == hostEnd) {
            return 0;
        }

        // Check for port
        if (*hostEnd == ':') {
            portStart = hostEnd + 1;
            while (*portStart && *portStart != '/') {
                if (!std::isdigit(*portStart)) {
                    return 0;
                }
                ++portStart;
            }
        }

        // Copy valid URI to wsUri
        std::strncpy(wsUri, url, MAX_WS_URI);
        return 1;
    }

    switch_status_t is_valid_utf8(const char *str) {
        switch_status_t status = SWITCH_STATUS_FALSE;
        while (*str) {
            if ((*str & 0x80) == 0x00) {
                // 1-byte character
                str++;
            } else if ((*str & 0xE0) == 0xC0) {
                // 2-byte character
                if ((str[1] & 0xC0) != 0x80) {
                    return status;
                }
                str += 2;
            } else if ((*str & 0xF0) == 0xE0) {
                // 3-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80) {
                    return status;
                }
                str += 3;
            } else if ((*str & 0xF8) == 0xF0) {
                // 4-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80 || (str[3] & 0xC0) != 0x80) {
                    return status;
                }
                str += 4;
            } else {
                // invalid character
                return status;
            }
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_send_text(switch_core_session_t *session, char* text) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_send_text failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        std::shared_ptr<AudioStreamer> streamer;

        switch_mutex_lock(tech_pvt->mutex);

        if (tech_pvt->pAudioStreamer) {
            auto sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap; // copy shared_ptr
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (streamer) {
            streamer->writeText(text);
            return SWITCH_STATUS_SUCCESS;
        }

        return SWITCH_STATUS_FALSE;
    }

    switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_pauseresume failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        switch_core_media_bug_flush(bug);
        switch_atomic_set(&tech_pvt->audio_paused, pause);
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_flush(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_flush failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_STATUS_FALSE;
        const std::shared_ptr<PlaybackQueue> queue = getPlaybackQueue(tech_pvt);
        if (!queue) return SWITCH_STATUS_FALSE;

        SwitchLock writeLock(tech_pvt->write_mutex);
        SwitchLock resamplerLock(tech_pvt->resampler_mutex);

        if (resamplerLock.lock() != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "stream_session_flush failed to lock playback resampler\n");
            return SWITCH_STATUS_FALSE;
        }
        if (isCloseRequested(tech_pvt)) {
            resamplerLock.unlock();
            return SWITCH_STATUS_FALSE;
        }
        if (writeLock.lock() != SWITCH_STATUS_SUCCESS) {
            resamplerLock.unlock();
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "stream_session_flush failed to lock playback queue\n");
            return SWITCH_STATUS_FALSE;
        }
        const bool waitForTerminalMark = switch_true(
            switch_channel_get_variable(channel, "STREAM_FLUSH_WAIT_FOR_MARK"));
        const FlushResult flushResult = queue->flush(waitForTerminalMark);
        writeLock.unlock();
        if (tech_pvt->write_resampler) {
            speex_resampler_reset_mem(tech_pvt->write_resampler);
        }
        resamplerLock.unlock();

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                          "stream_session_flush: playback queue cleared, %zu mark(s) invalidated, "
                          "discarding late audio: %s\n",
                          flushResult.invalidatedMarkCount,
                          flushResult.discardingLateAudio ? "true" : "false");
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_init(switch_core_session_t *session,
                                        responseHandler_t responseHandler,
                                        uint32_t samples_per_second,
                                        char *wsUri,
                                        int sampling,
                                        int channels,
                                        char* metadata,
                                        void **ppUserData)
    {
        int deflate = 0;
        int heart_beat = 0;
        bool suppressLog = false;
        const char* buffer_size;
        const char* extra_headers;
        int rtp_packets = 1; //20ms burst
        int playbackBufferMs = DEFAULT_PLAYBACK_BUFFER_MS;
        const char* tls_cafile = NULL;
        const char* tls_keyfile = NULL;
        const char* tls_certfile = NULL;
        bool tls_disable_hostname_validation = false;

        switch_channel_t *channel = switch_core_session_get_channel(session);

        if (switch_channel_var_true(channel, "STREAM_MESSAGE_DEFLATE")) {
            deflate = 1;
        }

        if (switch_channel_var_true(channel, "STREAM_SUPPRESS_LOG")) {
            suppressLog = true;
        }

        tls_cafile = switch_channel_get_variable(channel, "STREAM_TLS_CA_FILE");
        tls_keyfile = switch_channel_get_variable(channel, "STREAM_TLS_KEY_FILE");
        tls_certfile = switch_channel_get_variable(channel, "STREAM_TLS_CERT_FILE");

        if (switch_channel_var_true(channel, "STREAM_TLS_DISABLE_HOSTNAME_VALIDATION")) {
            tls_disable_hostname_validation = true;
        }

        const char* heartBeat = switch_channel_get_variable(channel, "STREAM_HEART_BEAT");
        if (heartBeat) {
            char *endptr;
            long value = strtol(heartBeat, &endptr, 10);
            if (*endptr == '\0' && value <= INT_MAX && value >= INT_MIN) {
                heart_beat = (int) value;
            }
        }

        if ((buffer_size = switch_channel_get_variable(channel, "STREAM_BUFFER_SIZE"))) {
            int bSize = atoi(buffer_size);
            if(bSize % 20 != 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "%s: Buffer size of %s is not a multiple of 20ms. Using default 20ms.\n",
                                  switch_channel_get_name(channel), buffer_size);
            } else if(bSize >= 20){
                rtp_packets = bSize/20;
            }
        }

        const char *playbackBufferSize =
            switch_channel_get_variable(channel, "STREAM_PLAYBACK_BUFFER_SIZE");
        if (playbackBufferSize) {
            char *endptr = nullptr;
            const long value = strtol(playbackBufferSize, &endptr, 10);
            if (*endptr == '\0' && value >= 20 && value <= MAX_PLAYBACK_BUFFER_MS) {
                playbackBufferMs = static_cast<int>(value);
            } else {
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s: STREAM_PLAYBACK_BUFFER_SIZE must be between 20 and %d ms; "
                    "using default %d ms\n",
                    switch_channel_get_name(channel),
                    MAX_PLAYBACK_BUFFER_MS,
                    DEFAULT_PLAYBACK_BUFFER_MS);
            }
        }

        extra_headers = switch_channel_get_variable(channel, "STREAM_EXTRA_HEADERS");

        // allocate per-session tech_pvt
        auto* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));

        if (!tech_pvt) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
            return SWITCH_STATUS_FALSE;
        }
        memset(tech_pvt, 0, sizeof(*tech_pvt));
        tech_pvt->session = session;
        switch_memory_pool_t *pool = switch_core_session_get_pool(session);
        if (switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool) != SWITCH_STATUS_SUCCESS
            || switch_mutex_init(&tech_pvt->write_mutex, SWITCH_MUTEX_NESTED, pool) != SWITCH_STATUS_SUCCESS
            || switch_mutex_init(&tech_pvt->resampler_mutex, SWITCH_MUTEX_NESTED, pool) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_STATUS_FALSE;
        }
        try {
            tech_pvt->cleanup_job = cleanupExecutor->reserve(finish_session_cleanup, tech_pvt, retain_session, drain_session, release_module);
        } catch (const std::exception &) {
            return SWITCH_STATUS_FALSE;
        }
        if (!tech_pvt->cleanup_job) return SWITCH_STATUS_FALSE;
        try {
            if (stream_data_init(tech_pvt, session, wsUri, samples_per_second, sampling, channels,
                                 metadata, responseHandler, deflate, heart_beat, suppressLog, rtp_packets,
                                 playbackBufferMs, extra_headers, tls_cafile, tls_keyfile, tls_certfile,
                                 tls_disable_hostname_validation) != SWITCH_STATUS_SUCCESS) {
                stream_session_abort(tech_pvt);
                return SWITCH_STATUS_FALSE;
            }
        } catch (const std::exception &e) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "Stream initialization failed: %s\n", e.what());
            stream_session_abort(tech_pvt);
            return SWITCH_STATUS_FALSE;
        }

        *ppUserData = tech_pvt;

        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_write_thread_init(switch_core_session_t *session, void *pUserData) {
        private_t *tech_pvt = (private_t *)pUserData;
        switch_threadattr_t *thd_attr = nullptr;
        switch_mutex_lock(tech_pvt->mutex);
        switch_status_t status = SWITCH_STATUS_FALSE;
        if (!isCloseRequested(tech_pvt)
            && switch_threadattr_create(&thd_attr, switch_core_session_get_pool(session)) == SWITCH_STATUS_SUCCESS
            && switch_threadattr_detach_set(thd_attr, 0) == SWITCH_STATUS_SUCCESS
            && switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE) == SWITCH_STATUS_SUCCESS) {
            status = switch_thread_create(&tech_pvt->write_thread, thd_attr, write_frame_thread,
                                          tech_pvt, switch_core_session_get_pool(session));
            if (status != SWITCH_STATUS_SUCCESS) tech_pvt->write_thread = nullptr;
        }
        // The worker's first mutex acquisition cannot precede publishing its handle.
        switch_mutex_unlock(tech_pvt->mutex);
        return status;
    }

    static switch_bool_t stream_frame_impl(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (switch_atomic_read(&tech_pvt->audio_paused) || switch_atomic_read(&tech_pvt->cleanup_started)) return SWITCH_TRUE;
        
        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        SwitchLock mainLock(tech_pvt->mutex);

        if (mainLock.tryLock() != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            mainLock.unlock();
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            mainLock.unlock();
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

        // Park-based Lua entries no longer consume DTMF themselves; when the
        // gateway asked for relay (asr.start "dtmfRelay": true) forward the
        // channel DTMF queue over the same WS before streaming read audio.
        if (tech_pvt->dtmf_relay) {
            switch_channel_t *dtmf_channel = switch_core_session_get_channel(tech_pvt->session);
            if (dtmf_channel) {
                switch_dtmf_t dtmf = {};
                while (switch_channel_dequeue_dtmf(dtmf_channel, &dtmf) == SWITCH_STATUS_SUCCESS) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(tech_pvt->session), SWITCH_LOG_INFO,
                                      "(%s) forwarding DTMF %c to gateway\n",
                                      tech_pvt->sessionId, (char)dtmf.digit);
                    streamer->sendDtmfDigit(static_cast<char>(dtmf.digit));
                }
            }
        }

        auto *resampler = tech_pvt->read_resampler;
        const int channels = tech_pvt->channels;
        const int rtp_packets = tech_pvt->rtp_packets;

        if (nullptr == resampler) {
            
            uint8_t data_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data_buf;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                if (rtp_packets == 1) {
                    pending_send.emplace_back((uint8_t*)frame.data, (uint8_t*)frame.data + frame.datalen);
                    continue;
                }

                size_t freespace = switch_buffer_freespace(tech_pvt->read_sbuffer);
                
                if (freespace >= frame.datalen) {
                    switch_buffer_write(tech_pvt->read_sbuffer, static_cast<uint8_t *>(frame.data), frame.datalen);
                }

                if (switch_buffer_freespace(tech_pvt->read_sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->read_sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
            
        } else {

            uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if(!frame.datalen) {
                    continue;
                }

                const size_t freespace = switch_buffer_freespace(tech_pvt->read_sbuffer);
                spx_uint32_t in_len = frame.samples;
                spx_uint32_t out_len = (freespace / (tech_pvt->channels * sizeof(spx_int16_t)));
                
                if(out_len == 0) {
                    if(freespace == 0) {
                        switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                        if (inuse > 0) {
                            std::vector<uint8_t> tmp(inuse);
                            switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                            switch_buffer_zero(tech_pvt->read_sbuffer);
                            pending_send.emplace_back(std::move(tmp));
                        }
                    }
                    continue;
                }

                std::vector<spx_int16_t> out;
                out.resize((size_t)out_len * (size_t)channels);

                if(channels == 1) {
                    speex_resampler_process_int(resampler,
                                    0,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                } else {
                    speex_resampler_process_interleaved_int(resampler,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                }

                if(out_len > 0) {
                    const size_t bytes_written = (size_t)out_len * (size_t)channels * sizeof(spx_int16_t);

                    if (rtp_packets == 1) { //20ms packet
                        const uint8_t* p = (const uint8_t*)out.data();
                        pending_send.emplace_back(p, p + bytes_written);
                        continue;
                    }

                    if (bytes_written <= switch_buffer_freespace(tech_pvt->read_sbuffer)) {
                        switch_buffer_write(tech_pvt->read_sbuffer, (const uint8_t *)out.data(), bytes_written);
                    }
                }

                if (switch_buffer_freespace(tech_pvt->read_sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->read_sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
        }
        
        mainLock.unlock();
    
        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    switch_bool_t stream_frame(switch_media_bug_t *bug) {
        try {
            return stream_frame_impl(bug);
        } catch (const std::exception &e) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
                              "Audio capture failed: %s\n", e.what());
            return SWITCH_FALSE;
        }
    }

    switch_status_t stream_session_abort(void *pUserData) {
        auto *tech_pvt = static_cast<private_t *>(pUserData);
        if (!tech_pvt) return SWITCH_STATUS_FALSE;
        switch_mutex_lock(tech_pvt->mutex);
        if (switch_atomic_read(&tech_pvt->cleanup_started)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_STATUS_SUCCESS;
        }
        switch_atomic_set(&tech_pvt->cleanup_started, 1);
        switch_atomic_set(&tech_pvt->close_requested, 1);
        if (tech_pvt->pAudioStreamer) {
            auto *streamer = static_cast<std::shared_ptr<AudioStreamer> *>(tech_pvt->pAudioStreamer);
            if (*streamer) (*streamer)->markCleanedUp();
        }
        auto *job = static_cast<mod_audio_stream::CleanupExecutor::Job *>(tech_pvt->cleanup_job);
        tech_pvt->cleanup_job = nullptr;
        switch_mutex_unlock(tech_pvt->mutex);
        cleanupExecutor->submit(job);
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = static_cast<switch_media_bug_t *>(switch_channel_get_private(channel, MY_BUG_NAME));
        if (!bug) return SWITCH_STATUS_FALSE;
        auto *tech_pvt = static_cast<private_t *>(switch_core_media_bug_get_user_data(bug));
        switch_mutex_lock(tech_pvt->mutex);
        if (switch_atomic_read(&tech_pvt->cleanup_started)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_STATUS_SUCCESS;
        }
        switch_atomic_set(&tech_pvt->cleanup_started, 1);
        switch_atomic_set(&tech_pvt->close_requested, 1);
        switch_channel_set_private(channel, MY_BUG_NAME, nullptr);
        if (tech_pvt->pAudioStreamer) {
            auto *streamer = static_cast<std::shared_ptr<AudioStreamer> *>(tech_pvt->pAudioStreamer);
            if (*streamer) {
                if (text) {
                    try { (*streamer)->writeText(text); }
                    catch (const std::exception &e) {
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                                          "Cannot send final stream text: %s\n", e.what());
                    }
                }
                (*streamer)->markCleanedUp();
            }
        }
        auto *job = static_cast<mod_audio_stream::CleanupExecutor::Job *>(tech_pvt->cleanup_job);
        tech_pvt->cleanup_job = nullptr;
        switch_mutex_unlock(tech_pvt->mutex);
        // The CLOSE callback must not recursively remove its own bug.
        if (!channelIsClosing) switch_core_media_bug_remove(session, &bug);
        // Both explicit stop and hangup use the same nonblocking cleanup path.
        // The reserved job owns the session reference until after join+disconnect.
        cleanupExecutor->submit(job);
        return SWITCH_STATUS_SUCCESS;
    }
}
