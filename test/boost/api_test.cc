/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/memory.hh>

#include "api/storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "test/lib/log.hh"
#include "test/lib/scylla_test_case.hh"
#include "utils/memory_data_sink.hh"
#include "utils/rjson.hh"

namespace ss = httpd::storage_service_json;

// Reproduces SCYLLADB-5320: listing the snapshots (GET /storage_service/snapshots)
// must not build a contiguous array with one entry per table of the snapshot.
SEASTAR_TEST_CASE(test_get_snapshot_details_no_large_allocation) {
    constexpr size_t large_allocation_threshold = 128 * 1024;
    // The lowest number of tables for which a contiguous array of
    // response entries exceeds the threshold.
    const size_t nr_tables = large_allocation_threshold / sizeof(ss::snapshot) + 1;
    testlog.info("sizeof(ss::snapshot)={} nr_tables={}", sizeof(ss::snapshot), nr_tables);

    std::unordered_map<sstring, db::snapshot_ctl::db_snapshot_details> details;
    auto& tables = details["snap"];
    for (size_t i = 0; i < nr_tables; ++i) {
        tables.push_back({.ks = "ks", .cf = format("cf{}", i), .details = {.total = 2, .live = 1}});
    }

    memory_data_sink_buffers bufs;
    [[maybe_unused]] const auto stats_before = memory::stats();
    {
        const memory::scoped_large_allocation_warning_threshold _{large_allocation_threshold + 1};
        // Use a small stream buffer so that it doesn't count as a large allocation.
        co_await api::get_snapshot_details(std::move(details), output_stream<char>(data_sink(std::make_unique<memory_data_sink>(bufs)), 4096));
    }
    [[maybe_unused]] const auto stats_after = memory::stats();

    sstring response;
    for (const auto& buf : bufs.buffers()) {
        response.append(buf.get(), buf.size());
    }
    auto json = rjson::parse(response);
    BOOST_REQUIRE(json.IsArray());
    BOOST_REQUIRE_EQUAL(json.Size(), 1);
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(json[0]["key"]), "snap");
    const auto& value = json[0]["value"];
    BOOST_REQUIRE(value.IsArray());
    BOOST_REQUIRE_EQUAL(value.Size(), nr_tables);
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(value[nr_tables - 1]["cf"]), format("cf{}", nr_tables - 1));

#ifndef SEASTAR_DEFAULT_ALLOCATOR // large allocations are counted only by the seastar allocator
    BOOST_CHECK_EQUAL(stats_after.large_allocations(), stats_before.large_allocations());
#endif
}
