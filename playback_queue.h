#ifndef PLAYBACK_QUEUE_H
#define PLAYBACK_QUEUE_H

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string>
#include <vector>

namespace mod_audio_stream {

enum class EnqueueAudioResult {
    QUEUED,
    DISCARDED,
    OVERFLOW
};

enum class EnqueueMarkResult {
    QUEUED,
    INVALIDATED,
    INVALID,
    OVERFLOW
};

struct PlaybackBatch {
    std::vector<uint8_t> audio;
    std::vector<std::string> completedMarks;
};

struct FlushResult {
    std::size_t invalidatedMarkCount;
    bool discardingLateAudio;
};

class PlaybackQueue {
public:
    explicit PlaybackQueue(std::size_t capacityBytes,
                           std::size_t maxPendingMarks = 1024,
                           std::size_t maxMarkIdBytes = 256);

    EnqueueAudioResult enqueueAudio(const uint8_t *data, std::size_t length);
    EnqueueAudioResult enqueueAudio(std::string &&audio);
    EnqueueAudioResult enqueueAudioIfGeneration(std::string &&audio,
                                                std::uint64_t expectedGeneration);
    EnqueueMarkResult enqueueMark(const std::string &markId);
    void rejectCurrentResponse();
    PlaybackBatch take(std::size_t maxBytes);
    std::size_t takeInto(uint8_t *destination,
                         std::size_t capacity,
                         std::vector<std::string> &completedMarks);
    // Legacy default does not require cancelled responses to emit a mark.
    FlushResult flush(bool waitForTerminalMark = false);

    std::size_t bufferedBytes() const;
    std::size_t availableBytes() const;
    std::size_t pendingMarks() const;
    bool isDiscarding() const;
    std::uint64_t generation() const;

private:
    enum class ItemType {
        AUDIO,
        MARK
    };

    struct Item {
        ItemType type;
        std::string audio;
        std::size_t offset;
        std::string markId;
    };

    void takeLeadingMarks(std::vector<std::string> &completedMarks);

    const std::size_t capacityBytes_;
    const std::size_t maxPendingMarks_;
    const std::size_t maxMarkIdBytes_;
    std::size_t bufferedBytes_;
    std::size_t pendingMarks_;
    bool discarding_;
    bool barrierSeenSinceLastAudio_;
    std::uint64_t generation_;
    std::deque<Item> items_;
};

}

#endif
