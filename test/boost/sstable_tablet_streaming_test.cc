/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include "dht/token.hh"
#include "sstable_test.hh"
#include "sstables_loader.hh"
#include "test/lib/sstable_test_env.hh"

BOOST_AUTO_TEST_SUITE(sstable_tablet_streaming_test)

// Reproduces the assertion failure in stream_progress::advance() caused by
// floating-point rounding when per-tablet fractional contributions accumulate
// to slightly more than 1.0 per tablet.
SEASTAR_TEST_CASE(test_stream_progress_float_rounding) {
    // Simulate tablet_sstable_streamer::stream() with tablets whose sstable
    // counts cause inexact float divisions when batched (16 + 16 + 16 + 3 = 51
    // sstables per tablet; each batch contributes batch/51 which rounds up in
    // float32, so per-tablet total > 1.0).
    const size_t tablet_count = 32;
    const size_t sstables_per_tablet = 51;
    const size_t batch_size = 16;

    auto progress = make_shared<stream_progress>();
    progress->start(tablet_count);

    for (size_t tablet = 0; tablet < tablet_count; ++tablet) {
        // Mimics per_tablet_stream_progress with _num_sstables_mapped = sstables_per_tablet
        size_t remaining = sstables_per_tablet;
        while (remaining > 0) {
            size_t batch = std::min(batch_size, remaining);
            remaining -= batch;
            // This is what per_tablet_stream_progress::advance() does:
            float contribution = static_cast<float>(batch) / static_cast<float>(sstables_per_tablet);
            progress->advance(contribution);
        }
    }

    // Without the fix (clamping), the above triggers:
    //   assert(completed <= total) in stream_progress::advance()
    // because float32 rounding makes each tablet's contributions sum to
    // slightly more than 1.0 (≈1.0000001), and over 32 tablets the
    // accumulated error pushes completed past total.
    BOOST_REQUIRE_LE(progress->completed, progress->total);
    return make_ready_future<>();
}

BOOST_AUTO_TEST_SUITE_END()
