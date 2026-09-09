#include "../playback_queue.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

namespace {

using mod_audio_stream::EnqueueAudioResult;
using mod_audio_stream::EnqueueMarkResult;
using mod_audio_stream::PlaybackBatch;
using mod_audio_stream::PlaybackQueue;

void fail(const std::string &message)
{
    std::cerr << "FAILED: " << message << std::endl;
    std::exit(EXIT_FAILURE);
}

void expect(bool condition, const std::string &message)
{
    if (!condition) {
        fail(message);
    }
}

void expectAudio(const PlaybackBatch &batch,
                 const std::vector<uint8_t> &expected,
                 const std::string &message)
{
    expect(batch.audio == expected, message);
}

void expectMarks(const PlaybackBatch &batch,
                 const std::vector<std::string> &expected,
                 const std::string &message)
{
    expect(batch.completedMarks == expected, message);
}

void testMarkerCompletesAtAudioBoundary()
{
    PlaybackQueue queue(64);
    const std::vector<uint8_t> first{1, 2, 3, 4, 5, 6};
    const std::vector<uint8_t> second{7, 8, 9, 10};

    expect(queue.enqueueAudio(first.data(), first.size()) == EnqueueAudioResult::QUEUED,
           "first audio chunk should be queued");
    expect(queue.enqueueMark("answer-1") == EnqueueMarkResult::QUEUED,
           "playback mark should be queued");
    expect(queue.enqueueAudio(second.data(), second.size()) == EnqueueAudioResult::QUEUED,
           "second audio chunk should be queued");

    PlaybackBatch batch = queue.take(4);
    expectAudio(batch, {1, 2, 3, 4}, "first frame should contain only the first four bytes");
    expectMarks(batch, {}, "marker must not complete before preceding audio is drained");

    batch = queue.take(4);
    expectAudio(batch, {5, 6}, "frame should stop exactly at the marker boundary");
    expectMarks(batch, {"answer-1"}, "marker should complete with the final preceding audio frame");

    batch = queue.take(4);
    expectAudio(batch, second, "audio after the marker must remain ordered behind the marker");
    expectMarks(batch, {}, "no extra marker should be emitted");
}

void testFlushInvalidatesAlreadyQueuedMarker()
{
    PlaybackQueue queue(64);
    const std::vector<uint8_t> oldAudio{1, 2, 3, 4};
    const std::vector<uint8_t> newAudio{7, 8};

    expect(queue.enqueueAudio(oldAudio.data(), oldAudio.size()) == EnqueueAudioResult::QUEUED,
           "old response audio should be queued");
    expect(queue.enqueueMark("old-answer") == EnqueueMarkResult::QUEUED,
           "old response marker should be queued");

    const mod_audio_stream::FlushResult flushResult = queue.flush();
    expect(flushResult.invalidatedMarkCount == 1,
           "flush should report the number of invalidated queued markers");
    expect(!flushResult.discardingLateAudio,
           "a queued marker proves the old response already crossed the WebSocket boundary");
    expect(queue.bufferedBytes() == 0, "flush should clear queued PCM atomically");
    expect(queue.pendingMarks() == 0, "flush should clear queued markers atomically");
    expect(!queue.isDiscarding(),
           "an already queued marker proves all old audio has crossed the WebSocket boundary");

    expect(queue.enqueueAudio(newAudio.data(), newAudio.size()) == EnqueueAudioResult::QUEUED,
           "new response audio should be accepted after an already queued marker is invalidated");
    PlaybackBatch batch = queue.take(8);
    expectAudio(batch, newAudio, "only new response audio should survive a flush");
    expectMarks(batch, {}, "invalidated markers must never complete successfully");
}

void testFlushDiscardsLateAudioUntilOldMarkerArrives()
{
    PlaybackQueue queue(64);
    const std::vector<uint8_t> oldAudio{1, 2, 3, 4};
    const std::vector<uint8_t> lateAudio{5, 6};
    const std::vector<uint8_t> newAudio{7, 8};

    expect(queue.enqueueAudio(oldAudio.data(), oldAudio.size()) == EnqueueAudioResult::QUEUED,
           "old response audio should be queued");
    const mod_audio_stream::FlushResult flushResult = queue.flush();
    expect(flushResult.invalidatedMarkCount == 0,
           "flush before the marker should not report an invalidated marker");
    expect(flushResult.discardingLateAudio,
           "flush before the marker should open the late-audio discard window");
    expect(queue.isDiscarding(), "flush should reject late audio until the old marker arrives");

    expect(queue.enqueueAudio(lateAudio.data(), lateAudio.size()) == EnqueueAudioResult::DISCARDED,
           "late audio from the interrupted response must be discarded");
    expect(queue.enqueueMark("old-answer") == EnqueueMarkResult::INVALIDATED,
           "the interrupted response marker is a barrier and must not be acknowledged");
    expect(!queue.isDiscarding(), "the invalidated marker should end the old-response discard window");

    expect(queue.enqueueAudio(newAudio.data(), newAudio.size()) == EnqueueAudioResult::QUEUED,
           "audio after the old-response barrier should be accepted");
    const PlaybackBatch batch = queue.take(8);
    expectAudio(batch, newAudio, "only new response audio should survive a flush");
    expectMarks(batch, {}, "invalidated markers must never complete successfully");
}

void testMarkerWithoutAudioCompletesImmediately()
{
    PlaybackQueue queue(64);
    expect(queue.enqueueMark("silence") == EnqueueMarkResult::QUEUED,
           "marker without audio should be queued");

    const PlaybackBatch batch = queue.take(16);
    expectAudio(batch, {}, "marker-only batch should not synthesize audio");
    expectMarks(batch, {"silence"}, "marker without preceding audio should complete immediately");
}

void testQueueIsBounded()
{
    PlaybackQueue queue(4);
    const std::vector<uint8_t> full{1, 2, 3, 4};
    const std::vector<uint8_t> overflow{5};

    expect(queue.enqueueAudio(full.data(), full.size()) == EnqueueAudioResult::QUEUED,
           "audio up to capacity should be queued");
    expect(queue.availableBytes() == 0, "available capacity should reflect queued PCM");
    expect(queue.enqueueAudio(overflow.data(), overflow.size()) == EnqueueAudioResult::OVERFLOW,
           "audio above capacity should be rejected");
    expect(queue.bufferedBytes() == full.size(), "overflow must preserve already queued audio");
    expect(queue.isDiscarding(), "overflow must invalidate the rest of the current response");
    expect(queue.enqueueMark("overflowed-answer") == EnqueueMarkResult::INVALIDATED,
           "a response with dropped PCM must never complete its playback marker");
    expect(!queue.isDiscarding(), "the invalidated marker should end overflow discard mode");
}

void testEmptyMarkerIsRejected()
{
    PlaybackQueue queue(64);
    expect(queue.enqueueMark("") == EnqueueMarkResult::INVALID,
           "empty mark identifiers must be rejected");
    expect(queue.pendingMarks() == 0, "invalid marker must not enter the queue");
}

void testMarkerTrafficIsBounded()
{
    PlaybackQueue queue(64, 1, 8);
    expect(queue.enqueueMark("12345678") == EnqueueMarkResult::QUEUED,
           "a marker at the identifier limit should be accepted");
    expect(queue.enqueueMark("second") == EnqueueMarkResult::OVERFLOW,
           "marker count above the configured limit should be rejected");

    std::vector<std::string> completedMarks;
    uint8_t frame[4] = {};
    expect(queue.takeInto(frame, sizeof(frame), completedMarks) == 0,
           "marker-only take should not write audio");
    expect(completedMarks == std::vector<std::string>({"12345678"}),
           "takeInto should preserve marker completion order");
    expect(queue.enqueueMark("123456789") == EnqueueMarkResult::INVALID,
           "marker identifiers above the configured limit should be rejected");
}

void testFlushGenerationRejectsInflightOldAudio()
{
    PlaybackQueue queue(64);
    const std::uint64_t oldGeneration = queue.generation();
    queue.flush();

    std::string lateAudio("\x01\x02", 2);
    expect(queue.enqueueAudioIfGeneration(std::move(lateAudio), oldGeneration)
               == EnqueueAudioResult::DISCARDED,
           "audio resampled across a flush boundary must be discarded");
    expect(queue.bufferedBytes() == 0,
           "generation-rejected audio must not mutate the playback queue");
}

void testFlushAfterCompletedBarrierAcceptsNextResponse()
{
    PlaybackQueue queue(64);
    const std::vector<uint8_t> oldAudio{1, 2, 3, 4};
    const std::vector<uint8_t> newAudio{5, 6};

    expect(queue.enqueueAudio(oldAudio.data(), oldAudio.size()) == EnqueueAudioResult::QUEUED,
           "old response audio should be queued");
    expect(queue.enqueueMark("old-answer") == EnqueueMarkResult::QUEUED,
           "old response marker should be queued");
    const PlaybackBatch completed = queue.take(8);
    expectMarks(completed, {"old-answer"}, "old response marker should complete");

    const mod_audio_stream::FlushResult flushResult = queue.flush();
    expect(!flushResult.discardingLateAudio,
           "a completed response barrier must remain visible to a racing flush");
    expect(queue.enqueueAudio(newAudio.data(), newAudio.size()) == EnqueueAudioResult::QUEUED,
           "the response after a completed barrier must not be stranded in discard mode");
}

void testFlushRequiresBarrierAfterLatestAudio()
{
    PlaybackQueue queue(64);
    const std::vector<uint8_t> firstResponse{1, 2};
    const std::vector<uint8_t> secondResponse{3, 4};

    expect(queue.enqueueAudio(firstResponse.data(), firstResponse.size())
               == EnqueueAudioResult::QUEUED,
           "first response audio should be queued");
    expect(queue.enqueueMark("first-answer") == EnqueueMarkResult::QUEUED,
           "first response marker should be queued");
    expect(queue.enqueueAudio(secondResponse.data(), secondResponse.size())
               == EnqueueAudioResult::QUEUED,
           "second response audio should be queued behind the first marker");

    const mod_audio_stream::FlushResult flushResult = queue.flush();
    expect(flushResult.discardingLateAudio,
           "an older queued marker must not stand in for the latest response barrier");
    expect(queue.enqueueAudio(secondResponse.data(), secondResponse.size())
               == EnqueueAudioResult::DISCARDED,
           "late audio from the latest interrupted response must be discarded");
}

void testNetworkLateAudioUsesCurrentGenerationButIsStillDiscarded()
{
    PlaybackQueue queue(64);
    queue.enqueueAudio(std::string("old PCM"));
    queue.flush();
    queue.flush(); // Repeated interrupts must not reopen the old response.
    const auto currentGeneration = queue.generation();
    expect(queue.enqueueAudioIfGeneration(std::string("late PCM"), currentGeneration)
               == EnqueueAudioResult::DISCARDED,
           "current generation cannot identify a network-late old response");
    expect(queue.enqueueMark("interrupted") == EnqueueMarkResult::INVALIDATED,
           "old response mark must never be acknowledged");
    expect(queue.enqueueAudio(std::string("new PCM")) == EnqueueAudioResult::QUEUED,
           "next response must be audible immediately after the old mark");
}

}

int main()
{
    testNetworkLateAudioUsesCurrentGenerationButIsStillDiscarded();
    testMarkerCompletesAtAudioBoundary();
    testFlushInvalidatesAlreadyQueuedMarker();
    testFlushDiscardsLateAudioUntilOldMarkerArrives();
    testMarkerWithoutAudioCompletesImmediately();
    testQueueIsBounded();
    testEmptyMarkerIsRejected();
    testMarkerTrafficIsBounded();
    testFlushGenerationRejectsInflightOldAudio();
    testFlushAfterCompletedBarrierAcceptsNextResponse();
    testFlushRequiresBarrierAfterLatestAudio();
    std::cout << "playback_queue_test: PASS" << std::endl;
    return EXIT_SUCCESS;
}
