/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/test_case.hh>
#include <seastar/core/coroutine.hh>

#include "test/lib/cql_test_env.hh"
#include "test/lib/log.hh"

#include "utils/UUID_gen.hh"
#include "transport/messages/result_message.hh"
#include "service/migration_manager.hh"

SEASTAR_TEST_CASE(test_group0_history_clearing_old_entries) {
    return do_with_cql_env([] (cql_test_env& e) -> future<> {
        using namespace std::chrono;

        auto get_history_size = [&] () -> future<size_t> {
            auto msg = co_await e.execute_cql("select * from system.group0_history");
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);
            co_return rows->rs().result_set().rows().size();
        };

        auto perform_schema_change = [&, has_ks = false] () mutable -> future<> {
            if (has_ks) {
                co_await e.execute_cql("drop keyspace new_ks");
            } else {
                co_await e.execute_cql("create keyspace new_ks with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            }
            has_ks = !has_ks;
        };

        auto size = co_await get_history_size();
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), size + 1);

        auto& mm = e.migration_manager().local();
        mm.set_group0_history_gc_duration(gc_clock::duration{0});

        // When group0_history_gc_duration is 0, any change should clear all previous history entries.
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 1);
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 1);

        mm.set_group0_history_gc_duration(duration_cast<gc_clock::duration>(weeks{1}));
        co_await perform_schema_change();
        BOOST_REQUIRE_EQUAL(co_await get_history_size(), 2);

        for (int i = 0; i < 10; ++i) {
            co_await perform_schema_change();
        }

        // Would use a shorter sleep, but gc_clock's resolution is one second.
        auto sleep_dur = seconds{1};
        co_await sleep(sleep_dur);

        for (int i = 0; i < 10; ++i) {
            co_await perform_schema_change();
        }

        auto get_history_timestamps = [&] () -> future<std::vector<milliseconds>> {
            auto msg = co_await e.execute_cql("select state_id from system.group0_history");
            auto rows = dynamic_pointer_cast<cql_transport::messages::result_message::rows>(msg);
            BOOST_REQUIRE(rows);

            std::vector<milliseconds> result;
            for (auto& row: rows->rs().result_set().rows()) {
                auto state_id = value_cast<utils::UUID>(timeuuid_type->deserialize(*row[0]));
                result.push_back(utils::UUID_gen::unix_timestamp(state_id));
            }
            co_return result;
        };

        auto timestamps1 = co_await get_history_timestamps();
        mm.set_group0_history_gc_duration(duration_cast<gc_clock::duration>(sleep_dur));
        co_await perform_schema_change();
        auto timestamps2 = co_await get_history_timestamps();
        // State IDs are sorted in descending order in the history table.
        // The first entry corresponds to the last schema change.
        auto last_ts = timestamps2.front();

        // All entries in `timestamps2` except `last_ts` should be present in `timestamps1`.
        BOOST_REQUIRE(std::includes(timestamps1.begin(), timestamps1.end(), timestamps2.begin()+1, timestamps2.end(), std::greater{}));

        // Count the number of timestamps in `timestamps1` that are older than the last entry by `sleep_dur` or more.
        // There should be about 12 because we slept for `sleep_dur` between the two loops above
        // and performing these schema changes should be much faster than `sleep_dur`.
        auto older_by_sleep_dur = std::count_if(timestamps1.begin(), timestamps1.end(), [last_ts, sleep_dur] (milliseconds ts) {
            return last_ts - ts > sleep_dur;
        });

        testlog.info("older by sleep_dur: {}", older_by_sleep_dur);

        // That last change should have cleared exactly those older than `sleep_dur` entries.
        // Therefore `timestamps2` should contain all in `timestamps1` minus those changes plus one (`last_ts`).
        BOOST_REQUIRE_EQUAL(timestamps2.size(), timestamps1.size() - older_by_sleep_dur + 1);

    }, raft_cql_test_config());
}
