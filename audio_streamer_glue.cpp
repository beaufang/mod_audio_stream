#include <string>
#include <cstring>
#include <algorithm>
#include "audio_streamer_glue.h"
#include "playback_queue.h"
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
using JsonPtr = std::unique_ptr<cJSON, decltype(&cJSON_Delete)>;

static std::shared_ptr<PlaybackQueue> getPlaybackQueue(private_t *techPvt)
{
    std::shared_ptr<PlaybackQueue> queue;
    if (!techPvt || !techPvt->mutex) return queue;

    if (switch_mutex_lock(techPvt->mutex) == SWITCH_STATUS_SUCCESS) {
        if (techPvt->pPlaybackQueue) {
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
    if (!techPvt || !techPvt->mutex) return true;

    bool closeRequested = true;
    if (switch_mutex_lock(techPvt->mutex) == SWITCH_STATUS_SUCCESS) {
        closeRequested = techPvt->close_requested != 0;
        switch_mutex_unlock(techPvt->mutex);
    }
    return closeRequested;
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

        sp->client.connect();

        return sp;
    }

    ~AudioStreamer()= default;

    void disconnect() {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "disconnecting...\n");
        client.disconnect();
    }

    bool isConnected() {
        return client.isConnected();
    }

    void writeBinary(uint8_t* buffer, size_t len) {
        if(!this->isConnected()) return;
        client.sendBinary(buffer, len);
    }

    void writeText(const char* text) {
        if(!this->isConnected()) return;
        client.sendMessage(text, strlen(text));
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

    void deleteFiles() {
        std::vector<std::string> files;

        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            if (m_Files.empty())
                return;

            files.assign(m_Files.begin(), m_Files.end());
            m_Files.clear();
            m_playFile = 0;
        }

        for (const auto& fn : files) {
            ::remove(fn.c_str());
        }
    }

    void markCleanedUp() {
        m_cleanedUp.store(true, std::memory_order_release);
        client.setMessageCallback({});
        client.setOpenCallback({});
        client.setErrorCallback({});
        client.setCloseCallback({});
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

        client.setUrl(wsUri);

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
        client.setTLSOptions(tls);

        // Optional heart beat, sent every xx seconds when there is not any traffic
        // to make sure that load balancers do not kill an idle connection.
        if(heart_beat)
            client.setPingInterval(heart_beat);

        // Per message deflate connection is enabled by default. You can tweak its parameters or disable it
        if(deflate)
            client.enableCompression(false);

        // Set extra headers if any
        if(!hdrs.empty())
            client.setHeaders(hdrs);
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
        client.setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            self->eventCallback(MESSAGE, message.c_str());
        });

        client.setOpenCallback([wp]() {
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

        client.setErrorCallback([wp](int code, const std::string& msg) {
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

        client.setCloseCallback([wp](int code, const std::string& reason) {
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
        return bug;
    }

    void rejectPlaybackResponse(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if (!bug) return;

        auto *tech_pvt = static_cast<private_t *>(switch_core_media_bug_get_user_data(bug));
        if (!tech_pvt) return;
        const std::shared_ptr<PlaybackQueue> queue = getPlaybackQueue(tech_pvt);
        if (!queue) return;

        if (switch_mutex_lock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS) {
            queue->rejectCurrentResponse();
            switch_mutex_unlock(tech_pvt->write_mutex);
        }
    }

    inline void media_bug_close(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            tech_pvt->close_requested = 1;
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

        if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "%s enqueueRawAudio: write mutex lock failed, dropping %zu bytes\n",
                              tech_pvt->sessionId, rawAudio.size());
            return;
        }

        EnqueueAudioResult enqueueResult = EnqueueAudioResult::OVERFLOW;
        if (inRate == outRate) {
            // 等速率路径：同样按代际校验，flush 后旧流迟到音频不会混入下一句
            const std::uint64_t queueGeneration = queue->generation();
            enqueueResult =
                queue->enqueueAudioIfGeneration(std::move(rawAudio), queueGeneration);
            switch_mutex_unlock(tech_pvt->write_mutex);
        } else {
            switch_mutex_unlock(tech_pvt->write_mutex);

            std::string output;
            int resampleStatus = RESAMPLER_ERR_SUCCESS;
            spx_uint32_t inFrames = 0;
            spx_uint32_t inLength = 0;
            spx_uint32_t outLength = 0;
            bool resamplerReady = false;

            if (switch_mutex_lock(tech_pvt->resampler_mutex) != SWITCH_STATUS_SUCCESS) {
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
                switch_mutex_unlock(tech_pvt->resampler_mutex);
                return;
            }

            if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) {
                switch_mutex_unlock(tech_pvt->resampler_mutex);
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: playback mutex lock failed before resampling\n",
                    tech_pvt->sessionId);
                return;
            }
            if (queue->isDiscarding()) {
                switch_mutex_unlock(tech_pvt->write_mutex);
                switch_mutex_unlock(tech_pvt->resampler_mutex);
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
                switch_mutex_unlock(tech_pvt->write_mutex);
                switch_mutex_unlock(tech_pvt->resampler_mutex);
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
            switch_mutex_unlock(tech_pvt->write_mutex);

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
                switch_mutex_unlock(tech_pvt->resampler_mutex);
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
                switch_mutex_unlock(tech_pvt->resampler_mutex);
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

            if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) {
                switch_mutex_unlock(tech_pvt->resampler_mutex);
                switch_log_printf(
                    SWITCH_CHANNEL_SESSION_LOG(session),
                    SWITCH_LOG_WARNING,
                    "%s enqueueRawAudio: playback mutex lock failed after resampling\n",
                    tech_pvt->sessionId);
                return;
            }
            enqueueResult = queue->enqueueAudioIfGeneration(
                std::move(output), queueGeneration);
            switch_mutex_unlock(tech_pvt->write_mutex);
            switch_mutex_unlock(tech_pvt->resampler_mutex);
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
        } else if (enqueueResult == EnqueueAudioResult::QUEUED) {
            switch_log_printf(
                SWITCH_CHANNEL_SESSION_LOG(session),
                SWITCH_LOG_INFO,
                "%s enqueueRawAudio: QUEUED %zu bytes, buffered=%zu\n",
                tech_pvt->sessionId,
                inputBytes,
                queue->bufferedBytes());
        }
    }

    void enqueuePlaybackMark(switch_core_session_t *session, const std::string &markId) {
        auto *bug = get_media_bug(session);
        if (!bug) return;

        auto *tech_pvt = static_cast<private_t *>(switch_core_media_bug_get_user_data(bug));
        if (!tech_pvt || isCloseRequested(tech_pvt)) return;
        const std::shared_ptr<PlaybackQueue> queue = getPlaybackQueue(tech_pvt);
        if (!queue) return;

        if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) {
            return;
        }
        const EnqueueMarkResult result = queue->enqueueMark(markId);
        switch_mutex_unlock(tech_pvt->write_mutex);

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
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO,
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

        switch_core_session_rwunlock(psession);
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
    WebSocketClient client;
    bool m_suppress_log;
    const char* m_extra_headers;
    int m_playFile;
    std::unordered_set<std::string> m_Files;
    std::atomic<bool> m_cleanedUp{false};
    std::mutex m_stateMutex;
};


namespace {

    // G.711 A-law 编码（PCMA）：L16 线性样本 → 8-bit A-law 字节
    static uint8_t linear2alaw(int16_t pcm) {
        int sign = (pcm >> 8) & 0x80;
        int p = pcm;
        if (sign) p = -p;
        if (p > 32635) p = 32635;
        p += 132;
        int exponent = 7;
        int exp_mask = 0x4000;
        while (!(p & exp_mask) && exponent > 0) { exponent--; exp_mask >>= 1; }
        int mantissa = (p >> (exponent + 3)) & 0x0F;
        return (uint8_t)((sign | (exponent << 4) | mantissa) ^ 0x55);
    }

    void *SWITCH_THREAD_FUNC write_frame_thread(switch_thread_t *thread, void *obj) {
        switch_core_session_t *session = (switch_core_session_t *)obj;
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if (!channel) return NULL;

        auto *bug = (switch_media_bug_t *)switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "no media bug in write frame thread\n");
            return NULL;
        }

        private_t *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "write_frame_thread: missing tech_pvt\n");
            return NULL;
        }
        const std::shared_ptr<PlaybackQueue> playbackQueue = getPlaybackQueue(tech_pvt);
        if (!playbackQueue) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "write_frame_thread: missing playback queue\n");
            return NULL;
        }

        switch_timer_t timer = {0};
        switch_frame_t write_frame = {0};
        switch_codec_t write_codec = {0};
        switch_codec_t *read_codec;

        uint32_t sample_rate = tech_pvt->sampling;
        uint32_t channels = tech_pvt->writeChannels;

        read_codec = switch_core_session_get_read_codec(session);
        if (!read_codec || !read_codec->implementation) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "write_frame_thread: no read codec available, shutting down\n");
            return NULL;
        }

        uint32_t interval = read_codec->implementation->microseconds_per_packet / 1000;
        uint32_t samples = switch_samples_per_packet(sample_rate, interval);
        uint32_t tsamples = read_codec->implementation->actual_samples_per_second;
        uint32_t bytes = samples * 2 * channels;

        if (switch_core_codec_init(&write_codec, "L16", NULL, NULL, sample_rate, interval, channels,
                                   SWITCH_CODEC_FLAG_ENCODE | SWITCH_CODEC_FLAG_DECODE, NULL,
                                   switch_core_session_get_pool(session)) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "write_frame_thread: Codec Init Failed. Cannot Start Write Thread\n");
            return NULL;
        }
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

        // RTP 直发（绕过 switch_core_media_write_frame 的媒体方向检查：
        // 本 fork 的 SDP 协商从不设置 smode（sendrecv 分支缺失），smode 恒为 NONE，
        // 导致所有 write thread 的下行帧被静默丢弃）
        switch_rtp_t *rtp_session = switch_core_media_get_rtp_session(session, SWITCH_MEDIA_TYPE_AUDIO);

        // 用 soft timer（hard timer 在本 fork 中不可用，初始化失败）。
        // soft timer 在会话线程读帧（Lua sleep）时按 20ms 节流，pop 间隙的 busy 空转影响很小。
        if (switch_core_timer_init(&timer, "soft", interval, tsamples,
                                   switch_core_session_get_pool(session)) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Timer Setup Failed. Cannot Start Write Thread\n");
            switch_core_codec_destroy(&write_codec);
            return NULL;
        }

        // switch_rtp_t 不透明，无法读取协商 payload；通道写 codec 为 PCMA（pt=8）时用 8。
        // 若写 codec 非 PCMA，直发将不兼容（会话协商基本固定 PCMA/PCMU，可接受）。
        switch_payload_t rtp_payload = 8; /* PCMA */
        const char *write_codec_name =
            switch_core_session_get_write_codec(session)
            && switch_core_session_get_write_codec(session)->implementation
            ? switch_core_session_get_write_codec(session)->implementation->iananame : "unknown";
        uint32_t rtp_ts = (uint32_t)(switch_micro_time_now() / 1000);
        uint8_t pcma_buf[1024];
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                          "write_frame_thread: RTP direct mode rtp_session=%p write_codec=%s payload=%d\n",
                          (void *) rtp_session, write_codec_name, (int) rtp_payload);

        std::vector<std::string> completedMarks;
        int64_t dbg_iter = 0;
        int64_t dbg_zero_iters = 0;
        int64_t dbg_voice_samples = 0;
        int64_t dbg_total_audio_bytes = 0;
        while (!tech_pvt->close_requested && switch_core_session_running(session)) {
            bool playbackSucceeded = false;
            if (switch_mutex_trylock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS) {
                const size_t audioBytes = playbackQueue->takeInto(
                    static_cast<uint8_t *>(write_frame.data),
                    bytes,
                    completedMarks);

                dbg_iter++;
                dbg_total_audio_bytes += static_cast<int64_t>(audioBytes);
                if (audioBytes == 0) {
                    dbg_zero_iters++;
                } else {
                    const int16_t *s = reinterpret_cast<const int16_t *>(write_frame.data);
                    const size_t n = audioBytes / 2;
                    int64_t sum = 0;
                    for (size_t i = 0; i < n; i += 4) sum += (s[i] < 0 ? -s[i] : s[i]);
                    if (n >= 4 && sum / static_cast<int64_t>(n / 4) > 300) dbg_voice_samples++;
                }
                if (dbg_iter % 1000 == 0) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                                      "STREAMDBG write iter=%lld audioBytesTotal=%lld zeroIters=%lld voiceFrames=%lld buffered=%zu mediaFlow=%d\n",
                                      static_cast<long long>(dbg_iter),
                                      static_cast<long long>(dbg_total_audio_bytes),
                                      static_cast<long long>(dbg_zero_iters),
                                      static_cast<long long>(dbg_voice_samples),
                                      playbackQueue->bufferedBytes(),
                                      switch_core_session_media_flow(session, SWITCH_MEDIA_TYPE_AUDIO));
                }

                playbackSucceeded = audioBytes == 0;
                if (audioBytes > 0 && switch_channel_ready(channel)) {
                    write_frame.datalen = static_cast<uint32_t>(audioBytes);
                    write_frame.samples = write_frame.datalen / sizeof(spx_int16_t) / channels;
                    const size_t nSamples = write_frame.samples * channels;
                    if (rtp_session && nSamples <= sizeof(pcma_buf)) {
                        // RTP 直发：L16 → PCMA 编码 → switch_rtp_write_manual
                        const int16_t *in = reinterpret_cast<const int16_t *>(write_frame.data);
                        for (size_t i = 0; i < nSamples; i++) {
                            pcma_buf[i] = linear2alaw(in[i]);
                        }
                        switch_frame_flag_t fflags = 0;
                        const int ret = switch_rtp_write_manual(
                            rtp_session, pcma_buf, (uint32_t) nSamples, 0, rtp_payload, rtp_ts, &fflags);
                        rtp_ts += (uint32_t) nSamples;
                        playbackSucceeded = ret >= 0;
                        if (!playbackSucceeded) {
                            switch_log_printf(
                                SWITCH_CHANNEL_SESSION_LOG(session),
                                SWITCH_LOG_WARNING,
                                "write_frame_thread: RTP direct write failed ret=%d\n", ret);
                        }
                    } else {
                        // fallback：走通道写帧（本 fork 会被媒体方向检查丢弃，仅兜底）
                        playbackSucceeded =
                            switch_core_session_write_frame(
                                session, &write_frame, SWITCH_IO_FLAG_NONE, 0)
                            == SWITCH_STATUS_SUCCESS;
                        if (!playbackSucceeded) {
                            switch_log_printf(
                                SWITCH_CHANNEL_SESSION_LOG(session),
                                SWITCH_LOG_WARNING,
                                "write_frame_thread: Channel write failed; playback marks will not be acknowledged\n");
                            tech_pvt->close_requested = 1;
                        }
                    }
                } else if (!switch_channel_ready(channel)) {
                    playbackSucceeded = false;
                }
                switch_mutex_unlock(tech_pvt->write_mutex);
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
                if (streamer) {
                    for (const std::string &markId : completedMarks) {
                        streamer->sendPlaybackMarkDone(markId);
                    }
                }
            }
            switch_core_timer_next(&timer);
        }

        switch_core_timer_destroy(&timer);
        switch_core_codec_destroy(&write_codec);
        return NULL;
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

        memset(tech_pvt, 0, sizeof(private_t));

        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
        strncpy(tech_pvt->ws_uri, wsUri, MAX_WS_URI);
        tech_pvt->sampling = sampling;
        tech_pvt->wsSampling = desiredSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->writeChannels = PLAYBACK_CHANNELS;
        tech_pvt->audio_paused = 0;

        if (metadata) strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);

        //size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * 1000 / RTP_PERIOD * BUFFERED_SEC);
        const size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * rtp_packets);
        
        auto sp = AudioStreamer::create(tech_pvt->sessionId, wsUri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers,
                                        tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool);
        switch_mutex_init(&tech_pvt->write_mutex, SWITCH_MUTEX_NESTED, pool);
        switch_mutex_init(&tech_pvt->resampler_mutex, SWITCH_MUTEX_NESTED, pool);
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
        if (tech_pvt->mutex) {
            switch_mutex_destroy(tech_pvt->mutex);
            tech_pvt->mutex = nullptr;
        }
        if (tech_pvt->write_mutex) {
            switch_mutex_destroy(tech_pvt->write_mutex);
            tech_pvt->write_mutex = nullptr;
        }
        if (tech_pvt->resampler_mutex) {
            switch_mutex_destroy(tech_pvt->resampler_mutex);
            tech_pvt->resampler_mutex = nullptr;
        }
        if (tech_pvt->read_sbuffer) {
            switch_buffer_destroy(&tech_pvt->read_sbuffer);
            tech_pvt->read_sbuffer = nullptr;
        }
        if (tech_pvt->pPlaybackQueue) {
            delete static_cast<std::shared_ptr<PlaybackQueue> *>(tech_pvt->pPlaybackQueue);
            tech_pvt->pPlaybackQueue = nullptr;
        }
    }

}

extern "C" {
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
        tech_pvt->audio_paused = pause;
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

        if (switch_mutex_lock(tech_pvt->resampler_mutex) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "stream_session_flush failed to lock playback resampler\n");
            return SWITCH_STATUS_FALSE;
        }
        if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) {
            switch_mutex_unlock(tech_pvt->resampler_mutex);
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "stream_session_flush failed to lock playback queue\n");
            return SWITCH_STATUS_FALSE;
        }
        const FlushResult flushResult = queue->flush();
        switch_mutex_unlock(tech_pvt->write_mutex);
        if (tech_pvt->write_resampler) {
            speex_resampler_reset_mem(tech_pvt->write_resampler);
        }
        switch_mutex_unlock(tech_pvt->resampler_mutex);

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
        if (SWITCH_STATUS_SUCCESS != stream_data_init(tech_pvt, session, wsUri, samples_per_second, sampling, channels,
                                                        metadata, responseHandler, deflate, heart_beat, suppressLog, rtp_packets,
                                                        playbackBufferMs,
                                                        extra_headers, tls_cafile, tls_keyfile, tls_certfile, tls_disable_hostname_validation)) {
            stream_session_abort(tech_pvt);
            return SWITCH_STATUS_FALSE;
        }

        *ppUserData = tech_pvt;

        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_write_thread_init(switch_core_session_t *session, void *pUserData) {
        private_t *tech_pvt = (private_t *)pUserData;
        switch_threadattr_t *thd_attr = NULL;
        switch_threadattr_create(&thd_attr, switch_core_session_get_pool(session));
        switch_threadattr_detach_set(thd_attr, 0);
        switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
        return switch_thread_create(
            &tech_pvt->write_thread,
            thd_attr,
            write_frame_thread,
            session,
            switch_core_session_get_pool(session));
    }

    switch_bool_t stream_frame(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (tech_pvt->audio_paused || tech_pvt->cleanup_started) return SWITCH_TRUE;
        
        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        if (switch_mutex_trylock(tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

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
        
        switch_mutex_unlock(tech_pvt->mutex);
    
        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    switch_status_t stream_session_abort(void *pUserData) {
        auto *tech_pvt = (private_t *)pUserData;
        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        if (tech_pvt->pAudioStreamer) {
            auto *sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap) {
                if (*sp_wrap) {
                    (*sp_wrap)->markCleanedUp();
                    (*sp_wrap)->disconnect();
                }
                delete sp_wrap;
            }
            tech_pvt->pAudioStreamer = nullptr;
        }
        destroy_tech_pvt(tech_pvt);
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if(bug)
        {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            char sessionId[MAX_SESSION_ID];
            strcpy(sessionId, tech_pvt->sessionId);

            std::shared_ptr<AudioStreamer>* sp_wrap = nullptr;
            std::shared_ptr<PlaybackQueue>* queue_wrap = nullptr;
            std::shared_ptr<AudioStreamer> streamer;
            switch_thread_t *write_thread = nullptr;

            switch_mutex_lock(tech_pvt->mutex);

            if (tech_pvt->cleanup_started) {
                switch_mutex_unlock(tech_pvt->mutex);
                return SWITCH_STATUS_SUCCESS;
            }
            tech_pvt->cleanup_started = 1;
            tech_pvt->close_requested = 1;

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_session_cleanup\n", sessionId);

            switch_channel_set_private(channel, MY_BUG_NAME, nullptr);

            sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            tech_pvt->pAudioStreamer = nullptr;
            queue_wrap = static_cast<std::shared_ptr<PlaybackQueue>*>(
                tech_pvt->pPlaybackQueue);
            tech_pvt->pPlaybackQueue = nullptr;

            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }

            write_thread = tech_pvt->write_thread;
            tech_pvt->write_thread = nullptr;

            switch_mutex_unlock(tech_pvt->mutex);

            if (!channelIsClosing) {
                switch_core_media_bug_remove(session, &bug);
            }

            if (sp_wrap) {
                delete sp_wrap;
                sp_wrap = nullptr;
            }
            if (queue_wrap) {
                delete queue_wrap;
                queue_wrap = nullptr;
            }

            if(streamer) {
                streamer->deleteFiles();
                if (text) streamer->writeText(text);

                /* markCleanedUp() nulls all callbacks first so no websocket
                   event can fire into session context after this point. */
                streamer->markCleanedUp();

                if (!channelIsClosing) {
                    /* Explicit stop runs outside session teardown. */
                    streamer->disconnect();
                } else {
                    /* WebSocket close may wait for its bounded handshake timer;
                       retain ownership off the session teardown path. */
                    std::shared_ptr<AudioStreamer> closingStreamer = std::move(streamer);
                    std::thread([closingStreamer]() {
                        closingStreamer->disconnect();
                    }).detach();
                }
            }

            if (write_thread) {
                if (!channelIsClosing) {
                    /* Explicit stop (uuid_audio_stream stop, etc.): safe to join because
                       we are NOT in the session teardown path, so write_frame_thread can
                       still call switch_core_session_write_frame() without deadlocking. */
                    switch_status_t thread_status = SWITCH_STATUS_SUCCESS;
                    switch_status_t join_result = switch_thread_join(&thread_status, write_thread);
                    if (join_result != SWITCH_STATUS_SUCCESS) {
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                                          "(%s) stream_session_cleanup: failed to join write thread (%d)\n",
                                          sessionId, join_result);
                    }
                } else {
                    /* Far-end hangup path (channelIsClosing=1): called from SWITCH_ABC_TYPE_CLOSE
                       while the FreeSWITCH session teardown holds the session I/O lock.
                       Joining here would deadlock: write_frame_thread blocks inside
                       switch_core_session_write_frame() waiting for that same lock.
                       close_requested=1 (set above) causes the write thread to exit its loop
                       within one timer tick (~20ms); switch_channel_ready() guards the actual
                       write call so it never blocks on a dead channel. The thread exits safely
                       before the session pool is freed. */
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                      "(%s) stream_session_cleanup: skipping write thread join on channel close "
                                      "(close_requested set, thread will self-exit)\n", sessionId);
                }
            }

            if (!channelIsClosing) {
                /* Full cleanup when we own the teardown. */
                destroy_tech_pvt(tech_pvt);
            } else {
                /* Channel-closing path: only free heap-allocated resources.
                   Pool-allocated objects (write_mutex, thread state, etc.) must remain
                   valid until the write thread exits; they will be reclaimed with the
                   session pool, which is freed after all callbacks complete. The write
                   thread keeps its own shared_ptr to the playback queue. */
                if (tech_pvt->read_resampler) {
                    speex_resampler_destroy(tech_pvt->read_resampler);
                    tech_pvt->read_resampler = nullptr;
                }
                if (tech_pvt->resampler_mutex
                    && switch_mutex_lock(tech_pvt->resampler_mutex) == SWITCH_STATUS_SUCCESS) {
                    if (tech_pvt->write_resampler) {
                        speex_resampler_destroy(tech_pvt->write_resampler);
                        tech_pvt->write_resampler = nullptr;
                    }
                    switch_mutex_unlock(tech_pvt->resampler_mutex);
                } else if (tech_pvt->write_resampler) {
                    switch_log_printf(
                        SWITCH_CHANNEL_SESSION_LOG(session),
                        SWITCH_LOG_WARNING,
                        "(%s) stream_session_cleanup: preserving write resampler because "
                        "its mutex could not be acquired\n",
                        sessionId);
                }
            }

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%s) stream_session_cleanup: connection closed\n", sessionId);
            return SWITCH_STATUS_SUCCESS;
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "stream_session_cleanup: no bug - websocket connection already closed\n");
        return SWITCH_STATUS_FALSE;
    }
}
