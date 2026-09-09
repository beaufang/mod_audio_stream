#include "playback_queue.h"

#include <algorithm>
#include <cstring>
#include <utility>

namespace mod_audio_stream {

PlaybackQueue::PlaybackQueue(std::size_t capacityBytes,
                             std::size_t maxPendingMarks,
                             std::size_t maxMarkIdBytes)
    : capacityBytes_(capacityBytes),
      maxPendingMarks_(maxPendingMarks),
      maxMarkIdBytes_(maxMarkIdBytes),
      bufferedBytes_(0),
      pendingMarks_(0),
      discarding_(false),
      barrierSeenSinceLastAudio_(false),
      generation_(0)
{
}

EnqueueAudioResult PlaybackQueue::enqueueAudio(const uint8_t *data, std::size_t length)
{
    if (discarding_) {
        return EnqueueAudioResult::DISCARDED;
    }
    if (length == 0) {
        return EnqueueAudioResult::QUEUED;
    }
    if (data == nullptr || length > capacityBytes_ - bufferedBytes_) {
        rejectCurrentResponse();
        return EnqueueAudioResult::OVERFLOW;
    }

    return enqueueAudio(std::string(
        reinterpret_cast<const char *>(data),
        length));
}

EnqueueAudioResult PlaybackQueue::enqueueAudio(std::string &&audio)
{
    if (discarding_) {
        return EnqueueAudioResult::DISCARDED;
    }
    if (audio.empty()) {
        return EnqueueAudioResult::QUEUED;
    }
    if (audio.size() > capacityBytes_ - bufferedBytes_) {
        rejectCurrentResponse();
        return EnqueueAudioResult::OVERFLOW;
    }

    Item item;
    item.type = ItemType::AUDIO;
    item.audio = std::move(audio);
    item.offset = 0;
    bufferedBytes_ += item.audio.size();
    items_.push_back(std::move(item));
    barrierSeenSinceLastAudio_ = false;
    return EnqueueAudioResult::QUEUED;
}

EnqueueAudioResult PlaybackQueue::enqueueAudioIfGeneration(
    std::string &&audio,
    std::uint64_t expectedGeneration)
{
    if (expectedGeneration != generation_) {
        return EnqueueAudioResult::DISCARDED;
    }
    return enqueueAudio(std::move(audio));
}

EnqueueMarkResult PlaybackQueue::enqueueMark(const std::string &markId)
{
    if (markId.empty()) {
        return EnqueueMarkResult::INVALID;
    }
    if (discarding_) {
        discarding_ = false;
        barrierSeenSinceLastAudio_ = true;
        return EnqueueMarkResult::INVALIDATED;
    }
    if (markId.size() > maxMarkIdBytes_) {
        barrierSeenSinceLastAudio_ = true;
        return EnqueueMarkResult::INVALID;
    }
    if (pendingMarks_ >= maxPendingMarks_) {
        barrierSeenSinceLastAudio_ = true;
        return EnqueueMarkResult::OVERFLOW;
    }

    Item item;
    item.type = ItemType::MARK;
    item.offset = 0;
    item.markId = markId;
    items_.push_back(std::move(item));
    ++pendingMarks_;
    barrierSeenSinceLastAudio_ = true;
    return EnqueueMarkResult::QUEUED;
}

void PlaybackQueue::rejectCurrentResponse()
{
    discarding_ = true;
    barrierSeenSinceLastAudio_ = false;
}

PlaybackBatch PlaybackQueue::take(std::size_t maxBytes)
{
    PlaybackBatch batch;
    batch.audio.resize(maxBytes);
    const std::size_t written =
        takeInto(batch.audio.data(), maxBytes, batch.completedMarks);
    batch.audio.resize(written);
    return batch;
}

std::size_t PlaybackQueue::takeInto(
    uint8_t *destination,
    std::size_t capacity,
    std::vector<std::string> &completedMarks)
{
    completedMarks.clear();
    takeLeadingMarks(completedMarks);
    if (!completedMarks.empty() || capacity == 0 || destination == nullptr) {
        return 0;
    }

    std::size_t written = 0;
    while (!items_.empty() && written < capacity) {
        Item &item = items_.front();
        if (item.type == ItemType::MARK) {
            takeLeadingMarks(completedMarks);
            break;
        }

        const std::size_t remaining = item.audio.size() - item.offset;
        const std::size_t available = capacity - written;
        const std::size_t count = std::min(remaining, available);
        std::memcpy(destination + written, item.audio.data() + item.offset, count);
        written += count;
        item.offset += count;
        bufferedBytes_ -= count;

        if (item.offset == item.audio.size()) {
            items_.pop_front();
            if (!items_.empty() && items_.front().type == ItemType::MARK) {
                takeLeadingMarks(completedMarks);
                break;
            }
        }
    }
    return written;
}

FlushResult PlaybackQueue::flush(bool waitForTerminalMark)
{
    const std::size_t invalidatedMarkCount = pendingMarks_;
    items_.clear();
    bufferedBytes_ = 0;
    pendingMarks_ = 0;
    // A generation only identifies work already in flight locally. Network-late
    // PCM has no response ID, so the old mark must remain the ordering barrier.
    discarding_ = waitForTerminalMark && !barrierSeenSinceLastAudio_;
    ++generation_;
    return FlushResult{invalidatedMarkCount, discarding_};
}

std::size_t PlaybackQueue::bufferedBytes() const
{
    return bufferedBytes_;
}

std::size_t PlaybackQueue::availableBytes() const
{
    return capacityBytes_ - bufferedBytes_;
}

std::size_t PlaybackQueue::pendingMarks() const
{
    return pendingMarks_;
}

bool PlaybackQueue::isDiscarding() const
{
    return discarding_;
}

std::uint64_t PlaybackQueue::generation() const
{
    return generation_;
}

void PlaybackQueue::takeLeadingMarks(std::vector<std::string> &completedMarks)
{
    while (!items_.empty() && items_.front().type == ItemType::MARK) {
        completedMarks.push_back(std::move(items_.front().markId));
        items_.pop_front();
        --pendingMarks_;
    }
}

}
