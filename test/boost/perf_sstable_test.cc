/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include "test/lib/tmpdir.hh"
#include "test/perf/perf_sstable.hh"

BOOST_AUTO_TEST_SUITE(perf_sstable_test)

// Smoke test for perf_sstable_test_env::compaction(): it used to abort the
// process (table dtor asserting on an unstopped compaction group) instead of
// throwing, so nothing short of running it would catch a regression here.
SEASTAR_THREAD_TEST_CASE(perf_sstable_compaction_smoke_test) {
    tmpdir dir;
    auto scf = make_sstable_compressor_factory_for_tests_in_thread();
    perf_sstable_test_env::conf cfg = {
            .partitions = 4,
            .key_size = 32,
            .num_columns = 1,
            .column_size = 8,
            .sstables = 2,
            .buffer_size = 4 << 10,
            .dir = dir.path().native(),
            .compaction_strategy = compaction::compaction_strategy_type::size_tiered,
            .timestamp_range = 0,
    };
    perf_sstable_test_env env(cfg, *scf);
    auto stop_env = defer([&env]() noexcept {
        env.stop().get();
    });
    env.fill_memtable().get();
    BOOST_REQUIRE_GT(env.compaction(0).get(), 0);
}

BOOST_AUTO_TEST_SUITE_END()
