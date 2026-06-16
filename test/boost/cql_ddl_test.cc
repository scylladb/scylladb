/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */


#include <boost/test/unit_test.hpp>

#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>

#include "test/lib/cql_test_env.hh"

BOOST_AUTO_TEST_SUITE(cql_ddl_test)

/// Check that writes interact with caching = {'enabled': X} as expected:
/// * enable: true -> data is merged into cache on memtable flush [1]
/// * enable: false -> data is not merged into cache on memtable flush
///
/// [1] Important: only partitions which are either already in the cache,
///     or are not present in underlying (disk) are merged.
future<> writes_with_caching_toggle(bool enabled) {
    return do_with_cql_env_thread([enabled] (cql_test_env& e) {
        e.execute_cql(format("CREATE TABLE ks.tbl (pk int PRIMARY KEY, v text) WITH CACHING = {{'enabled': '{}'}}", enabled)).get();

        const auto& table = e.local_db().find_column_family("ks", "tbl");
        const auto table_id = table.schema()->id();

        auto write_rows = [&, first_pk = 0] () mutable {
            sstring value(128, 'v');
            const auto cql3_value = cql3::raw_value::make_value(serialized(value));

            auto id = e.prepare("INSERT INTO ks.tbl (pk, v) VALUES (?, ?);").get();
            for (int flushes = 0; flushes < 5; flushes++) {
                for (int32_t pk = first_pk; pk < first_pk + 10; ++pk) {
                    const auto cql3_pk = cql3::raw_value::make_value(serialized(pk));
                    e.execute_prepared(id, {cql3_pk, cql3_value}).get();
                }
                replica::database::flush_table_on_all_shards(e.db(), table_id).get();
            }
            first_pk += 10;
        };

        auto check_expected_cache_content = [&] (bool cache_enabled) {
            const auto get_cache_shards_with_content = e.db().map_reduce0([] (const replica::database& db) {
                auto& t = db.find_column_family("ks", "tbl");
                return uint64_t(!t.get_row_cache().empty());
            }, uint64_t(0), std::plus<uint64_t>()).get();

            if (cache_enabled) {
                BOOST_REQUIRE_GT(get_cache_shards_with_content, 0);
            } else {
                BOOST_REQUIRE_EQUAL(get_cache_shards_with_content, 0);
            }
        };

        write_rows();
        check_expected_cache_content(enabled);

        replica::database::drop_cache_for_table_on_all_shards(e.db(), table_id).get();
        e.execute_cql(format("ALTER TABLE ks.tbl WITH CACHING = {{'enabled': '{}'}}", !enabled)).get();

        write_rows();
        check_expected_cache_content(!enabled);
    });
}

SEASTAR_TEST_CASE(test_writes_with_caching_disabled) {
    return writes_with_caching_toggle(false);
}

SEASTAR_TEST_CASE(test_writes_with_caching_enabled) {
    return writes_with_caching_toggle(true);
}

BOOST_AUTO_TEST_SUITE_END()
