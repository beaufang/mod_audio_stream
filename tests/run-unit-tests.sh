#!/usr/bin/env bash
set -euo pipefail
root_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/mod-audio-stream-tests.XXXXXX")
trap 'rm -rf -- "$build_dir"' EXIT
flags=(-std=c++11 -Wall -Wextra -Werror -pthread -g)
if [[ -n "${SANITIZERS:-}" ]]; then
    flags+=("-fsanitize=${SANITIZERS}" -fno-omit-frame-pointer)
fi
"${CXX:-g++}" "${flags[@]}" "$root_dir/tests/playback_queue_test.cpp" "$root_dir/playback_queue.cpp" -o "$build_dir/queue"
"${CXX:-g++}" "${flags[@]}" "$root_dir/tests/cleanup_executor_test.cpp" -o "$build_dir/cleanup"
"$build_dir/queue"
"$build_dir/cleanup"
if [[ -n "${FREESWITCH_CFLAGS:-}" ]]; then
    # Compiler flags only; do not evaluate this value as shell code.
    read -r -a fs_flags <<< "$FREESWITCH_CFLAGS"
    "${CXX:-g++}" "${flags[@]}" -Wno-sign-compare -Wno-unused-but-set-variable \
        -ffunction-sections -fdata-sections -Wl,--gc-sections \
        "${fs_flags[@]}" -I"$root_dir/libs/libwsc/src" \
        "$root_dir/tests/session_cleanup_test.cpp" "$root_dir/playback_queue.cpp" \
        -o "$build_dir/session"
    "$build_dir/session"
else
    echo 'session_cleanup_test: SKIP (set FREESWITCH_CFLAGS to FreeSWITCH and SpeexDSP header flags)'
fi
