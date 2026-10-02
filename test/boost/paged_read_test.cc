/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of paging and reconciliation. They read from simulated replicas
// through the production read path, and compare the pages with the complete
// answer of the read model. See test/lib/paged_read.hh.

#undef SEASTAR_TESTING_MAIN

#include <cstdlib>
#include <map>

#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>

#include "db/config.hh"
#include "db/extensions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"
#include "test/lib/paged_read.hh"
#include "test/lib/random_utils.hh"
#include "tombstone_gc_extension.hh"

using namespace tests::read_model;
using namespace tests::paged_read;

BOOST_AUTO_TEST_SUITE(paged_read_test)

namespace {

const std::string_view keyspace = "paged_read";

// Registers the tombstone_gc extension, so that a table can disable
// tombstone GC.
cql_test_config config_with_tombstone_gc_extension() {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
    return cql_test_config(seastar::make_shared<db::config>(ext));
}

// Creates a keyspace and a table of the read model, and runs `f` with a
// harness of the table.
void with_harness(std::function<void(harness&)> f) {
    do_with_cql_env_thread([f = std::move(f)] (cql_test_env& env) {
        env.execute_cql(fmt::format("CREATE KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
                " AND tablets = {{'enabled': 'false'}}", keyspace)).get();
        harness h(env, keyspace, "cf");
        f(h);
    }, config_with_tombstone_gc_extension()).get();
}

// Runs `c`, and reports a test error for the properties which the run
// violates. Returns the outcome of the run.
outcome run_and_check(harness& hs, const read_case& c) {
    const auto expected = evaluate(*hs.schema(), complete_history(c.history), c.query);
    testlog.debug("Running {}", describe(c));
    auto o = hs.run(c);
    const auto violations = check(o, expected);
    if (!violations.empty()) {
        BOOST_ERROR(report(c, o, expected, violations));
    }
    return o;
}

// The number of lines of the traces of `o` which contain `text`.
size_t count_trace_lines(const outcome& o, std::string_view text) {
    size_t count = 0;
    for (const auto& p : o.pages) {
        count += std::ranges::count_if(p.trace, [&] (const std::string& line) { return line.find(text) != std::string::npos; });
    }
    return count;
}

// Partition 1 has a static cell and rows 1 to 4, and row 3 is deleted.
// Partition 2 has only a static cell. Partition 3 has rows 1 to 3, and a
// range deletion of [2, 3) covers row 2. Partition 4 has only a deleted row.
history mixed_partitions() {
    return {
        static_cell_write{1, 5, 1},
        regular_cell_write{1, 1, regular_column::v1, 11, 2},
        regular_cell_write{1, 2, regular_column::v1, 12, 3},
        regular_cell_write{1, 3, regular_column::v1, 13, 4},
        regular_cell_write{1, 4, regular_column::v2, 14, 5},
        row_deletion{1, 3, 6},

        static_cell_write{2, 6, 7},

        row_marker_write{3, 1, 8},
        regular_cell_write{3, 2, regular_column::v1, 32, 9},
        regular_cell_write{3, 3, regular_column::v2, 33, 10},
        range_deletion{3, bound{2, true}, bound{3, false}, 11},

        regular_cell_write{4, 1, regular_column::v1, 41, 12},
        row_deletion{4, 1, 13},
    };
}

std::vector<select_query> paging_queries() {
    return {
        select_query{},
        select_query{.select_s = false},
        select_query{.partitions = std::vector<int32_t>{1, 3}},
        select_query{.partitions = std::vector<int32_t>{1}, .reversed = true},
        select_query{.ck_start = bound{2, true}},
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false},
        select_query{.filter = {{column::v1, comparison::gt, 11}}},
        select_query{.per_partition_limit = 1},
        select_query{.limit = 3},
    };
}

// Runs every paging query with every page size in `page_sizes`.
void run_paging_queries(harness& hs, const placed_history& h, read_options opts, std::initializer_list<int32_t> page_sizes) {
    for (const auto& q : paging_queries()) {
        for (auto page_size : page_sizes) {
            opts.page_size = page_size;
            run_and_check(hs, read_case{h, q, opts});
        }
    }
}

} // anonymous namespace

// A single replica has nothing to reconcile.
SEASTAR_THREAD_TEST_CASE(test_single_replica) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b1), read_options{.replica_count = 1}, {0, 1, 2, 3, 100});
    });
}

// Identical replicas have matching digests.
SEASTAR_THREAD_TEST_CASE(test_identical_replicas) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b11), read_options{}, {0, 1, 2, 3, 100});
    });
}

// Identical replicas give the complete answer with any schedule of the
// coordinator. The test also requires that the schedules split some scans,
// and that some rounds merge their contiguous ranges and some do not.
SEASTAR_THREAD_TEST_CASE(test_identical_replicas_with_schedules) {
    with_harness([] (harness& hs) {
        size_t merged = 0;
        size_t apart = 0;
        for (uint32_t seed = 1; seed <= 10; ++seed) {
            for (const auto& q : paging_queries()) {
                for (auto page_size : {1, 3, 100}) {
                    const read_options opts{.replica_count = 3, .extra_replicas = 1, .page_size = page_size, .schedule_seed = seed};
                    const auto o = run_and_check(hs, read_case{on_replicas(mixed_partitions(), 0b111), q, opts});
                    // The trace of a split scan says whether it merges ranges.
                    merged += count_trace_lines(o, "merging contiguous ranges");
                    apart += count_trace_lines(o, "without merging ranges");
                }
            }
        }
        BOOST_REQUIRE_GT(merged, 0);
        BOOST_REQUIRE_GT(apart, 0);
    });
}

// Without native_reverse_queries, the replicas other than the coordinator's
// own receive reversed reads in the legacy reversed format.
SEASTAR_THREAD_TEST_CASE(test_identical_replicas_with_legacy_reversed_format) {
    with_harness([] (harness& hs) {
        size_t legacy_reads = 0;
        for (uint32_t seed = 1; seed <= 5; ++seed) {
            for (auto page_size : {1, 3, 100}) {
                const read_options opts{.page_size = page_size, .native_reverse_queries = false, .read_frontiers = false, .schedule_seed = seed};
                const auto q = select_query{.partitions = std::vector<int32_t>{1}, .reversed = true};
                const auto o = run_and_check(hs, read_case{on_replicas(mixed_partitions(), 0b11), q, opts});
                legacy_reads += count_trace_lines(o, "legacy reversed format");
            }
        }
        BOOST_REQUIRE_GT(legacy_reads, 0);
    });
}

// The replica continues each page with the querier which it cached on the
// previous page. Without a schedule, no replica evicts queriers.
SEASTAR_THREAD_TEST_CASE(test_single_replica_with_querier_cache) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b1), read_options{.replica_count = 1, .querier_cache = true}, {1, 2, 3, 100});
    });
}

SEASTAR_THREAD_TEST_CASE(test_identical_replicas_with_querier_cache) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b11), read_options{.querier_cache = true}, {1, 2, 3, 100});
    });
}

// A page size in bytes of 1 stops a replica's page after its first live row.
SEASTAR_THREAD_TEST_CASE(test_single_replica_with_a_small_page_size_in_bytes) {
    with_harness([] (harness& hs) {
        run_paging_queries(hs, on_replicas(mixed_partitions(), 0b1), read_options{.replica_count = 1, .page_size_in_bytes = 1}, {1, 3, 100});
    });
}

// Cases which catch defects of a page which continues a partition. With each
// defect, its case returns a wrong answer or fails. Each comment describes the
// defect.
SEASTAR_THREAD_TEST_CASE(test_witnesses_of_page_resume) {
    const std::vector<read_case> witnesses{
        // A page which continues a partition drops the partition tombstone, so a
        // deleted row is returned.
        read_case{
            placed_history{
                {partition_deletion{1, 9}, 0b10},
                {regular_cell_write{1, 4, regular_column::v2, 8, 11, lifetime::permanent}, 0b10},
                {regular_cell_write{1, 5, regular_column::v2, 2, 2, lifetime::expiring}, 0b1},
            },
            select_query{.partitions = std::vector<int32_t>{1}, .select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 5, .page_size_in_bytes = 1, .querier_cache = true},
        },
        // A page which continues a partition drops the static row, so a row is
        // returned without its static cell.
        read_case{
            placed_history{
                {regular_cell_write{4, 2, regular_column::v2, 9, 16, lifetime::expiring}, 0b10},
                {regular_cell_write{4, 4, regular_column::v2, 1, 22, lifetime::permanent}, 0b1},
                {static_cell_write{4, 5, 2, lifetime::permanent}, 0b10},
            },
            select_query{.partitions = std::vector<int32_t>{4}, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 1, .querier_cache = true},
        },
        // A page which continues a partition reopens its range tombstone after
        // the static row, so a deleted row is returned.
        read_case{
            placed_history{
                {range_deletion{4, bound{2, true}, bound{2, true}, 18}, 0b1},
                {range_deletion{4, bound{0, true}, std::nullopt, 12}, 0b1},
                {regular_cell_write{4, 3, regular_column::v1, 6, 10, lifetime::expiring}, 0b10},
                {static_cell_write{4, 3, 11, lifetime::permanent}, 0b1},
            },
            select_query{.partitions = std::vector<int32_t>{4}, .select_s = false, .select_v1 = false},
            read_options{.replica_count = 2, .page_size = 2, .page_size_in_bytes = 279, .querier_cache = true},
        },
        // A page which continues a partition decides its static-only row with the
        // querier's slice. It fills the row limit with a row which it does not
        // return, so the query ends, and the next partition's row is lost.
        read_case{
            placed_history{
                {static_cell_write{1, 4, 3, lifetime::permanent}, 0b1},
                {row_deletion{1, 4, 4}, 0b1},
                {row_marker_write{1, 2, 6, lifetime::permanent}, 0b1},
                {row_marker_write{2, 1, 7, lifetime::permanent}, 0b1},
            },
            select_query{.select_s = false},
            read_options{.replica_count = 1, .page_size = 1, .querier_cache = true},
        },
        // A cached querier which finished a partition is reused for a page which
        // continues it, and skips the partition. The page has neither a partition
        // nor a cursor, and a later row is lost.
        read_case{
            placed_history{
                {static_cell_write{3, 2, 10, lifetime::permanent}, 0b1},
                {regular_cell_write{3, 2, regular_column::v1, std::nullopt, 6, lifetime::permanent}, 0b10},
                {range_deletion{2, bound{0, true}, bound{5, true}, 7}, 0b1},
                {static_cell_write{2, 5, 3, lifetime::permanent}, 0b1},
            },
            select_query{.select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 1, .querier_cache = true},
        },
        // A cached querier is reused for a page which asks for no clustering row
        // of its partition. It returns a row which another replica deleted, and a
        // later row is lost.
        read_case{
            placed_history{
                {regular_cell_write{1, 2, regular_column::v2, 6, 7, lifetime::permanent}, 0b1},
                {static_cell_write{2, 1, 17, lifetime::permanent}, 0b1},
                {range_deletion{1, std::nullopt, std::nullopt, 20}, 0b10},
                {row_marker_write{1, 4, 11, lifetime::permanent}, 0b1},
            },
            select_query{.select_v1 = false, .select_v2 = false},
            read_options{.replica_count = 2, .page_size = 100, .page_size_in_bytes = 6, .querier_cache = true},
        },
        // A cached querier which stopped before a row, at the end of a range
        // tombstone, is reused for a page which starts after the row, and returns
        // the row again.
        read_case{
            placed_history{
                {row_marker_write{2, 5, 8, lifetime::permanent}, 0b1},
                {regular_cell_write{2, 1, regular_column::v1, 6, 7, lifetime::permanent}, 0b1},
                {range_deletion{2, bound{0, false}, bound{5, false}, 6}, 0b1},
            },
            select_query{.select_v2 = false},
            read_options{.replica_count = 2, .extra_replicas = 1, .page_size = 1, .tombstone_limit = 2, .querier_cache = true, .schedule_seed = 3},
        },
        // A cached querier which reads nothing reports the previous page's
        // position, so the next page returns a row again.
        read_case{
            placed_history{
                {regular_cell_write{2, 1, regular_column::v2, 3, 9, lifetime::expiring}, 0b11},
                {row_marker_write{2, 3, 15, lifetime::expiring}, 0b1},
            },
            select_query{.select_s = false, .select_v1 = false},
            read_options{.replica_count = 2, .extra_replicas = 1, .page_size = 1, .querier_cache = true},
        },
        // The same defect, with another effect. Replica 2's cached querier
        // reads nothing new on page 1, and reports the position where page 0
        // ended. The digests match, and that position is the smallest one, so
        // page 1 ends where page 0 did, and the paging state repeats.
        read_case{
            placed_history{
                {row_deletion{2, 3, 4}, 0b101},
                {row_marker_write{2, 5, 3, lifetime::expired}, 0b1},
                {regular_cell_write{4, 2, regular_column::v1, 4, 12, lifetime::permanent}, 0b1},
                {regular_cell_write{1, 5, regular_column::v1, 4, 11, lifetime::permanent}, 0b1},
            },
            select_query{},
            read_options{.replica_count = 3, .extra_replicas = 2, .page_size = 2, .tombstone_limit = 1, .querier_cache = true, .schedule_seed = 2389981646},
        },
    };
    with_harness([&] (harness& hs) {
        for (const auto& c : witnesses) {
            run_and_check(hs, c);
        }
    });
}

// A case which catches a digest which covers the key of a partition which
// the result omits. Replica 0 holds a live static cell of partition 2.
// Replica 1 holds a later null static cell, which deletes it. A DISTINCT
// query which selects no static column returns the partition from replica 0
// and nothing from replica 1. If both digests cover the partition's key, they
// match, and the page returns the partition, which has no live data.
SEASTAR_THREAD_TEST_CASE(test_witness_of_digest_of_omitted_partition) {
    const read_case witness{
        placed_history{
            {static_cell_write{2, 3, 1, lifetime::permanent}, 0b1},
            {static_cell_write{2, std::nullopt, 4, lifetime::permanent}, 0b10},
        },
        select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false},
        read_options{.replica_count = 2, .page_size = 3},
    };
    with_harness([&] (harness& hs) {
        run_and_check(hs, witness);
    });
}

// A case which catches a defect of a DISTINCT query with filtering. The
// filtering pager sets the per-partition limit of each read command to the
// page size. If the per-partition limit of a DISTINCT read overrides its
// limit of one row, the replica returns two rows of the partition, and the
// query returns the partition twice.
SEASTAR_THREAD_TEST_CASE(test_witness_of_distinct_with_filtering) {
    const read_case witness{
        placed_history{
            {row_marker_write{1, 1, 1, lifetime::permanent}, 0b1},
            {row_marker_write{1, 2, 2, lifetime::permanent}, 0b1},
        },
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .filter = {{column::pk, comparison::gt, 0}}},
        read_options{.replica_count = 1, .page_size = 100},
    };
    with_harness([&] (harness& hs) {
        run_and_check(hs, witness);
    });
}

namespace {

// 1 to 4 replicas, of which all but one may be extra replicas.
read_options random_replicas() {
    read_options opts;
    opts.replica_count = tests::random::get_int<size_t>(1, 4);
    opts.extra_replicas = tests::random::get_int<size_t>(0, opts.replica_count - 1);
    return opts;
}

// Places each write of `h` on a random set of the replicas of `replicas`.
// The set includes a replica which counts toward the consistency level.
placed_history random_placement(const history& h, const read_options& replicas) {
    const size_t block_for = replicas.replica_count - replicas.extra_replicas;
    const replica_set cl_replicas((uint64_t(1) << block_for) - 1);
    placed_history placed;
    for (const auto& op : h) {
        replica_set holders(tests::random::get_int<uint64_t>(1, (uint64_t(1) << replicas.replica_count) - 1));
        if ((holders & cl_replicas).none()) {
            holders.set(tests::random::get_int<size_t>(0, block_for - 1));
        }
        placed.push_back(placed_operation{op, holders});
    }
    return placed;
}

// `opts` with its replica counts kept, and everything else random: the page
// size, the byte and tombstone limits, the cluster features, the querier
// cache, repairs and the schedule.
read_options random_options(read_options opts) {
    // A page size of 0 makes the query unpaged.
    opts.page_size = std::array{0, 1, 2, 3, 5, 100}[tests::random::get_int(0, 5)];
    // Page sizes in bytes range from 1, which stops a page after its first
    // row, to a few kilobytes, which a small history rarely reaches. Each
    // range [2^k, 2^(k+1)) is equally likely, so small sizes are more likely.
    if (tests::random::get_bool()) {
        const auto magnitude = tests::random::get_int(0, 12);
        opts.page_size_in_bytes = tests::random::get_int<uint64_t>(uint64_t(1) << magnitude, (uint64_t(2) << magnitude) - 1);
    }
    if (tests::random::get_int(0, 3) == 0) {
        opts.tombstone_limit = tests::random::get_int<uint64_t>(1, 8);
    }
    // Each feature requires the older features before it.
    opts.empty_replica_pages = tests::random::get_int(0, 3) != 0;
    opts.empty_replica_mutation_pages = opts.empty_replica_pages && tests::random::get_int(0, 3) != 0;
    opts.native_reverse_queries = opts.empty_replica_mutation_pages && tests::random::get_int(0, 3) != 0;
    opts.read_frontiers = opts.native_reverse_queries && tests::random::get_int(0, 3) != 0;
    opts.querier_cache = tests::random::get_bool();
    opts.apply_repairs = !opts.querier_cache && tests::random::get_bool();
    opts.schedule_seed = tests::random::get_int<uint32_t>();
    return opts;
}

} // anonymous namespace

// The general test. It draws random histories on random replicas. It reads
// each history with ten random queries and random options: page sizes, byte
// and tombstone limits, cluster features, a querier cache, repairs and a
// schedule of the coordinator. It uses every feature of the harness, and it
// treats no case specially.
//
// The baseline code has known defects and fails many cases. So the test runs
// only when the environment variable SCYLLA_PAGED_READ_CAMPAIGN is set. Its
// value is the number of histories. The test reports each failure with a
// shrunk case, and logs the number of failures of each kind. When the
// environment variable SCYLLA_PAGED_READ_STOP_AT_FAILURE is set, the test
// stops after its first failure. When the environment variable
// SCYLLA_PAGED_READ_VIOLATION is set, a run fails only if one of its
// violations contains the variable's value. This keeps a campaign on one
// kind of defect while the baseline still has others.
SEASTAR_THREAD_TEST_CASE(test_general) {
    const char* histories = std::getenv("SCYLLA_PAGED_READ_CAMPAIGN");
    if (!histories) {
        testlog.info("The general test runs only when SCYLLA_PAGED_READ_CAMPAIGN sets the number of histories");
        return;
    }
    const int history_count = std::stoi(histories);
    const bool stop_at_failure = std::getenv("SCYLLA_PAGED_READ_STOP_AT_FAILURE");
    const char* violation_filter = std::getenv("SCYLLA_PAGED_READ_VIOLATION");
    with_harness([history_count, stop_at_failure, violation_filter] (harness& hs) {
        size_t runs = 0;
        std::map<std::string, size_t> failures;
        auto stopped = [&] { return stop_at_failure && !failures.empty(); };
        for (int i = 0; i < history_count && !stopped(); ++i) {
            const auto replicas = random_replicas();
            // Longer histories give denser partitions and longer runs of
            // tombstones.
            const auto h = random_placement(random_history(std::array{6, 12, 24}[tests::random::get_int(0, 2)]), replicas);
            for (int j = 0; j < 10 && !stopped(); ++j) {
                const read_case c{h, random_query(), random_options(replicas)};
                testlog.debug("Running {}", describe(c));
                ++runs;
                const auto violations = hs.violations(c);
                if (violations.empty() || (violation_filter && std::ranges::none_of(violations, [&] (const std::string& v) {
                        return v.contains(violation_filter);
                    }))) {
                    continue;
                }
                ++failures[violation_kind(violations)];
                const auto shrunk = hs.shrink(c);
                const auto expected = evaluate(*hs.schema(), complete_history(shrunk.history), shrunk.query);
                const auto o = hs.run(shrunk);
                BOOST_ERROR(fmt::format("Shrunk from:\n{}\n{}", describe(c), report(shrunk, o, expected, check(o, expected))));
            }
        }
        for (const auto& [kind, count] : failures) {
            testlog.info("{} failures: {}", count, kind);
        }
        testlog.info("{} of {} runs failed", std::ranges::fold_left(failures | std::views::values, size_t(0), std::plus<>()), runs);
    });
}

BOOST_AUTO_TEST_SUITE_END()
