/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests of the query model in test/lib/read_model.hh.
//
// They compare the model's answers with answers derived by hand, and with
// unpaged CQL reads on a single node. Such a read has one replica, so it
// involves neither paging nor reconciliation (the merge of the responses of
// several replicas).

#undef SEASTAR_TESTING_MAIN


#include <boost/test/unit_test.hpp>
#include <fmt/ranges.h>
#include <seastar/testing/thread_test_case.hh>

#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "mutation/frozen_mutation.hh"
#include "replica/database.hh"
#include "schema/schema_registry.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/read_model.hh"
#include "test/lib/test_utils.hh"
#include "tombstone_gc_extension.hh"
#include "transport/messages/result_message.hh"

using namespace tests::read_model;

BOOST_AUTO_TEST_SUITE(read_model_test)

namespace {

using rows = std::vector<answer_row>;

// In row 1, the newest write of v1 has the larger value, and the newest write
// of v2 has the smaller value. So the answer depends on the timestamps, not on
// the values.
history newest_write_wins() {
    return {
        regular_cell_write{1, 1, regular_column::v1, 10, 1},
        regular_cell_write{1, 1, regular_column::v1, 11, 2},
        regular_cell_write{1, 1, regular_column::v2, 21, 3},
        regular_cell_write{1, 1, regular_column::v2, 20, 4},
    };
}

// Neither row has a row marker. In row 1, newer cell tombstones delete both
// cells, so the row is dead. In row 2, a newer tombstone deletes v1. The
// tombstone of v2 is older than its value, so v2 stays live, and so does the
// row.
history cell_tombstones() {
    return {
        regular_cell_write{1, 1, regular_column::v1, 10, 1},
        regular_cell_write{1, 1, regular_column::v2, 20, 2},
        regular_cell_write{1, 1, regular_column::v1, std::nullopt, 3},
        regular_cell_write{1, 1, regular_column::v2, std::nullopt, 4},
        regular_cell_write{1, 2, regular_column::v1, 30, 5},
        regular_cell_write{1, 2, regular_column::v2, std::nullopt, 6},
        regular_cell_write{1, 2, regular_column::v2, 40, 7},
        regular_cell_write{1, 2, regular_column::v1, std::nullopt, 8},
    };
}

// Row, range and partition deletions, and writes before and after them.
//
// Partition 1: a row deletion at 3 covers row 1's marker and v1, but not its
// newer v2.
// Partition 2: a range deletion of [2, 4) at 15 covers rows 2 and 3, but row 3
// has a newer v1.
// Partition 3: a partition deletion at 23 covers the static cell and row 1,
// but not row 2.
history deletions() {
    return {
        row_marker_write{1, 1, 1},
        regular_cell_write{1, 1, regular_column::v1, 10, 2},
        row_deletion{1, 1, 3},
        regular_cell_write{1, 1, regular_column::v2, 20, 4},

        regular_cell_write{2, 1, regular_column::v1, 1, 11},
        regular_cell_write{2, 2, regular_column::v1, 2, 12},
        regular_cell_write{2, 3, regular_column::v1, 3, 13},
        regular_cell_write{2, 4, regular_column::v1, 4, 14},
        range_deletion{2, bound{2, true}, bound{4, false}, 15},
        regular_cell_write{2, 3, regular_column::v1, 33, 16},

        static_cell_write{3, 5, 21},
        regular_cell_write{3, 1, regular_column::v1, 1, 22},
        partition_deletion{3, 23},
        regular_cell_write{3, 2, regular_column::v1, 2, 24},
    };
}

// Values with a TTL.
//
// Row 1 has only an expired v1, so it is dead. Row 2 has an expired v1 and an
// expiring row marker, so it is live with null values. Row 3 has only an
// expired row marker, so it is dead. In row 4, the newest write of v2 expired,
// so v2 is null although an older write of v2 has not expired. Row 4 is live
// because of v1. The static cell expired.
history expiry() {
    return {
        regular_cell_write{1, 1, regular_column::v1, 10, 1, lifetime::expired},
        regular_cell_write{1, 2, regular_column::v1, 20, 2, lifetime::expired},
        row_marker_write{1, 2, 3, lifetime::expiring},
        row_marker_write{1, 3, 4, lifetime::expired},
        regular_cell_write{1, 4, regular_column::v2, 40, 5, lifetime::expiring},
        regular_cell_write{1, 4, regular_column::v2, 41, 6, lifetime::expired},
        regular_cell_write{1, 4, regular_column::v1, 42, 7},
        static_cell_write{1, 5, 8, lifetime::expired},
    };
}

// Partition 1 has a live static cell and a deleted row. Partition 2 has a
// live static cell and a live row with v1 = 1. Partition 3 has a live row and
// a static cell tombstone. Partition 4 has only a deleted row.
history static_rows() {
    return {
        static_cell_write{1, 5, 1},
        regular_cell_write{1, 1, regular_column::v1, 10, 2},
        row_deletion{1, 1, 3},

        static_cell_write{2, 6, 4},
        regular_cell_write{2, 1, regular_column::v1, 1, 5},

        static_cell_write{3, std::nullopt, 6},
        regular_cell_write{3, 2, regular_column::v2, 7, 7},

        regular_cell_write{4, 1, regular_column::v1, 8, 8},
        row_deletion{4, 1, 9},
    };
}

// Partition 1 has rows 1, 2 and 3, and partition 2 has rows 1 and 2.
history two_partitions() {
    return {
        regular_cell_write{1, 1, regular_column::v1, 10, 1},
        regular_cell_write{1, 2, regular_column::v1, 20, 2},
        regular_cell_write{1, 3, regular_column::v1, 30, 3},
        regular_cell_write{2, 1, regular_column::v1, 40, 4},
        regular_cell_write{2, 2, regular_column::v1, 50, 5},
    };
}

// Partitions 3 and 4, whose ring order differs from the order of their keys.
history key_and_ring_order() {
    return {
        regular_cell_write{3, 1, regular_column::v1, 30, 1},
        regular_cell_write{4, 1, regular_column::v1, 40, 2},
    };
}

// The rows of `partitions`, concatenated in the ring order of their keys.
rows in_ring_order(const schema& s, std::vector<std::pair<int32_t, rows>> partitions) {
    std::vector<int32_t> pks;
    for (auto& [pk, r] : partitions) {
        pks.push_back(pk);
    }
    rows result;
    for (auto pk : ring_order(s, pks)) {
        auto it = std::ranges::find(partitions, pk, &std::pair<int32_t, rows>::first);
        result.insert(result.end(), it->second.begin(), it->second.end());
    }
    return result;
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_newest_write_wins) {
    auto s = make_schema();
    BOOST_REQUIRE_EQUAL(evaluate(*s, newest_write_wins(), select_query{}), (rows{{.pk = 1, .ck = 1, .v1 = 11, .v2 = 20}}));
}

SEASTAR_THREAD_TEST_CASE(test_cell_tombstones) {
    auto s = make_schema();
    BOOST_REQUIRE_EQUAL(evaluate(*s, cell_tombstones(), select_query{}), (rows{{.pk = 1, .ck = 2, .v2 = 40}}));

    // A row marker keeps row 1 alive, with null values.
    auto h = cell_tombstones();
    h.push_back(row_marker_write{1, 1, 9});
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{}), (rows{{.pk = 1, .ck = 1}, {.pk = 1, .ck = 2, .v2 = 40}}));
}

SEASTAR_THREAD_TEST_CASE(test_deletions) {
    auto s = make_schema();
    auto h = deletions();
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{1}}), (rows{{.pk = 1, .ck = 1, .v2 = 20}}));
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{2}}),
            (rows{{.pk = 2, .ck = 1, .v1 = 1}, {.pk = 2, .ck = 3, .v1 = 33}, {.pk = 2, .ck = 4, .v1 = 4}}));
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{3}}), (rows{{.pk = 3, .ck = 2, .v1 = 2}}));
}

SEASTAR_THREAD_TEST_CASE(test_expiry) {
    auto s = make_schema();
    BOOST_REQUIRE_EQUAL(evaluate(*s, expiry(), select_query{}), (rows{{.pk = 1, .ck = 2}, {.pk = 1, .ck = 4, .v1 = 42}}));
}

SEASTAR_THREAD_TEST_CASE(test_static_only_rows) {
    auto s = make_schema();
    auto h = static_rows();
    const rows static_only_1{{.pk = 1, .s = 5}};
    const rows rows_of_2{{.pk = 2, .ck = 1, .s = 6, .v1 = 1}};
    const rows rows_of_3{{.pk = 3, .ck = 2, .v2 = 7}};

    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{}), in_ring_order(*s, {{1, static_only_1}, {2, rows_of_2}, {3, rows_of_3}}));

    // An unselected static cell still gives a static-only row.
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{1}, .select_s = false}), (rows{{.pk = 1}}));

    // A clustering restriction suppresses the static-only row.
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{1, 2}, .ck_start = bound{0, true}}), rows_of_2);

    // The filter rejects the only live row of partition 2. That does not give
    // a static-only row. The static-only row of partition 1 has a null v1, so
    // the filter rejects it too.
    const auto v1_above_5 = std::vector<predicate>{{column::v1, comparison::gt, 5}};
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{1, 2}, .filter = v1_above_5}), rows{});

    // A filter on the static column accepts the static-only row.
    const auto s_is_5 = std::vector<predicate>{{column::s, comparison::eq, 5}};
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.filter = s_is_5}), static_only_1);
}

SEASTAR_THREAD_TEST_CASE(test_distinct) {
    auto s = make_schema();
    auto h = static_rows();
    const select_query distinct{.distinct = true, .select_v1 = false, .select_v2 = false};

    // Partition 3 has a live row but no live static cell, so its row has a
    // null s. Partition 4 has neither, so it gives no row.
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, distinct),
            in_ring_order(*s, {{1, {{.pk = 1, .s = 5}}}, {2, {{.pk = 2, .s = 6}}}, {3, {{.pk = 3}}}}));

    auto without_s = distinct;
    without_s.select_s = false;
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, without_s), in_ring_order(*s, {{1, {{.pk = 1}}}, {2, {{.pk = 2}}}, {3, {{.pk = 3}}}}));

    auto with_limit = distinct;
    with_limit.limit = 1;
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, with_limit).size(), 1);
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, with_limit).front(), evaluate(*s, h, distinct).front());

    // A filter on the partition key keeps the rows of the partitions which
    // it accepts.
    auto pk_above_1 = distinct;
    pk_above_1.filter = {{column::pk, comparison::gt, 1}};
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, pk_above_1), in_ring_order(*s, {{2, {{.pk = 2, .s = 6}}}, {3, {{.pk = 3}}}}));
}

SEASTAR_THREAD_TEST_CASE(test_order_and_limits) {
    auto s = make_schema();
    auto h = two_partitions();
    const rows p1{{.pk = 1, .ck = 1, .v1 = 10}, {.pk = 1, .ck = 2, .v1 = 20}, {.pk = 1, .ck = 3, .v1 = 30}};
    const rows p2{{.pk = 2, .ck = 1, .v1 = 40}, {.pk = 2, .ck = 2, .v1 = 50}};
    const auto pks = ring_order(*s, {1, 2});
    const auto& first = pks[0] == 1 ? p1 : p2;

    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{}), in_ring_order(*s, {{1, p1}, {2, p2}}));
    // The listed partitions come in the order of their keys.
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{2, 1}}), (rows{p1[0], p1[1], p1[2], p2[0], p2[1]}));

    // A reversed query reverses the rows of each partition, but not the
    // order of partitions.
    auto reversed = [] (rows r) {
        std::ranges::reverse(r);
        return r;
    };
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.reversed = true}), in_ring_order(*s, {{1, reversed(p1)}, {2, reversed(p2)}}));

    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.ck_start = bound{2, true}, .ck_end = bound{3, false}}),
            in_ring_order(*s, {{1, {p1[1]}}, {2, {p2[1]}}}));
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.per_partition_limit = 1}), in_ring_order(*s, {{1, {p1[0]}}, {2, {p2[0]}}}));
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partition_limit = 1}), first);

    auto limited = evaluate(*s, h, select_query{});
    limited.resize(3);
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.limit = 3}), limited);

    // The per-partition limit counts the rows which the filter accepts.
    const auto v1_above_10 = std::vector<predicate>{{column::v1, comparison::gt, 10}};
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.filter = v1_above_10, .per_partition_limit = 1}), in_ring_order(*s, {{1, {p1[1]}}, {2, {p2[0]}}}));

    // A filter on the partition key accepts every row of its partitions.
    const auto pk_below_2 = std::vector<predicate>{{column::pk, comparison::lt, 2}};
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.filter = pk_below_2}), p1);
}

// A scan of the whole ring returns partitions in ring order, but CQL sorts the
// keys of listed partitions.
SEASTAR_THREAD_TEST_CASE(test_listed_partitions_come_in_key_order) {
    auto s = make_schema();
    auto h = key_and_ring_order();
    const answer_row p3{.pk = 3, .ck = 1, .v1 = 30};
    const answer_row p4{.pk = 4, .ck = 1, .v1 = 40};
    BOOST_REQUIRE_EQUAL(ring_order(*s, {3, 4}), (std::vector<int32_t>{4, 3}));
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{}), (rows{p4, p3}));
    BOOST_REQUIRE_EQUAL(evaluate(*s, h, select_query{.partitions = std::vector<int32_t>{4, 3}}), (rows{p3, p4}));
}

SEASTAR_THREAD_TEST_CASE(test_invalid_inputs) {
    auto s = make_schema();
    BOOST_REQUIRE_THROW(validate(history{row_marker_write{1, 1, 1}, row_deletion{1, 2, 1}}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(history{static_cell_write{1, std::nullopt, 1, lifetime::expiring}}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(history{range_deletion{1, bound{2, true}, bound{2, false}, 1}}), std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(validate(history{range_deletion{1, bound{2, true}, bound{2, true}, 1}}));

    BOOST_REQUIRE_THROW(validate(select_query{.distinct = true}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .filter = {{column::s, comparison::eq, 1}}}),
            std::invalid_argument);
    BOOST_REQUIRE_NO_THROW(validate(select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .filter = {{column::pk, comparison::gt, 1}}}));
    BOOST_REQUIRE_THROW(validate(select_query{.partitions = std::vector<int32_t>{1}, .filter = {{column::pk, comparison::gt, 1}}}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(select_query{.filter = {{column::pk, comparison::gt, 1}, {column::pk, comparison::lt, 3}}}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(select_query{.partitions = std::vector<int32_t>{1, 1}}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(select_query{.filter = {{column::v1, comparison::eq, 1}}, .partition_limit = 1}), std::invalid_argument);
    BOOST_REQUIRE_THROW(validate(select_query{.limit = 0}), std::invalid_argument);

    BOOST_REQUIRE_THROW(to_cql(select_query{.partition_limit = 1}, "ks", "cf"), std::invalid_argument);
    BOOST_REQUIRE_THROW(to_cql(select_query{.reversed = true}, "ks", "cf"), std::invalid_argument);
}

SEASTAR_THREAD_TEST_CASE(test_to_cql) {
    BOOST_REQUIRE_EQUAL(to_cql(select_query{}, "ks", "cf"), "SELECT pk, ck, s, v1, v2 FROM ks.cf");
    BOOST_REQUIRE_EQUAL(to_cql(select_query{.partitions = std::vector<int32_t>{1, 2}, .ck_start = bound{1, true}, .ck_end = bound{3, false},
            .select_v2 = false, .filter = {{column::v1, comparison::gt, 2}}, .limit = 2, .per_partition_limit = 1}, "ks", "cf"),
            "SELECT pk, ck, s, v1 FROM ks.cf WHERE pk IN (1, 2) AND ck >= 1 AND ck < 3 AND v1 > 2 PER PARTITION LIMIT 1 LIMIT 2 ALLOW FILTERING");
    BOOST_REQUIRE_EQUAL(to_cql(select_query{.partitions = std::vector<int32_t>{1}, .reversed = true, .select_s = false}, "ks", "cf"),
            "SELECT pk, ck, v1, v2 FROM ks.cf WHERE pk = 1 ORDER BY ck DESC");
    BOOST_REQUIRE_EQUAL(to_cql(select_query{.ck_end = bound{3, true}}, "ks", "cf"), "SELECT pk, ck, s, v1, v2 FROM ks.cf WHERE ck <= 3 ALLOW FILTERING");
    BOOST_REQUIRE_EQUAL(to_cql(select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .limit = 1}, "ks", "cf"),
            "SELECT DISTINCT pk, s FROM ks.cf LIMIT 1");
    BOOST_REQUIRE_EQUAL(to_cql(select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .filter = {{column::pk, comparison::gt, 1}}}, "ks", "cf"),
            "SELECT DISTINCT pk, s FROM ks.cf WHERE pk > 1 ALLOW FILTERING");
}

namespace {

const std::string_view keyspace = "read_model";

// Registers the tombstone_gc extension, so that a table can disable
// tombstone GC.
cql_test_config config_with_tombstone_gc_extension() {
    auto ext = std::make_shared<db::extensions>();
    ext->add_schema_extension<tombstone_gc_extension>(tombstone_gc_extension::NAME);
    return cql_test_config(seastar::make_shared<db::config>(ext));
}

void create_keyspace(cql_test_env& env) {
    env.execute_cql(fmt::format("CREATE KEYSPACE {} WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}"
            " AND tablets = {{'enabled': 'false'}}", keyspace)).get();
}

rows rows_of(::shared_ptr<cql_transport::messages::result_message> msg) {
    cql3::untyped_result_set rs(msg);
    rows result;
    for (const auto& r : rs) {
        result.push_back(answer_row{
            .pk = r.get_as<int32_t>("pk"),
            .ck = r.get_opt<int32_t>("ck"),
            .s = r.get_opt<int32_t>("s"),
            .v1 = r.get_opt<int32_t>("v1"),
            .v2 = r.get_opt<int32_t>("v2"),
        });
    }
    return result;
}

// Writes `h` to a new table `cf`, and compares the model's answer of each
// query in `queries` with the answer of an unpaged CQL read. Reports every
// difference. Returns the number of differences.
size_t compare_with_cql(cql_test_env& env, std::string_view cf, const history& h, const std::vector<select_query>& queries) {
    env.execute_cql(create_table_statement(keyspace, cf)).get();
    auto s = env.local_db().find_schema(sstring(keyspace), sstring(cf));
    for (const auto& m : to_mutations(s, h, gc_clock::now())) {
        smp::submit_to(dht::static_shard_of(*s, m.decorated_key().token()), [&env, gs = global_schema_ptr(s), fm = freeze(m)] () mutable {
            return env.local_db().apply(gs.get(), std::move(fm), {}, db::commitlog_force_sync::no, db::no_timeout);
        }).get();
    }

    size_t differences = 0;
    for (const auto& q : queries) {
        const auto cql = to_cql(q, keyspace, cf);
        const auto expected = evaluate(*s, h, q);
        const auto actual = rows_of(env.execute_cql(cql).get());
        if (actual != expected) {
            ++differences;
            BOOST_ERROR(fmt::format("The model and CQL differ.\n{}\n{}\n{}\nModel: {}\nCQL:   {}", describe(h), q, cql, expected, actual));
        }
    }
    return differences;
}

// Queries which cover the model's features on partitions `pks`.
std::vector<select_query> standard_queries(const std::vector<int32_t>& pks) {
    std::vector<select_query> queries{
        select_query{},
        select_query{.select_s = false},
        select_query{.select_s = false, .select_v1 = false, .select_v2 = false},
        select_query{.partitions = pks},
        select_query{.partitions = pks, .ck_start = bound{2, true}, .ck_end = bound{4, false}},
        select_query{.ck_start = bound{2, false}},
        select_query{.ck_end = bound{2, true}},
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false},
        select_query{.distinct = true, .select_s = false, .select_v1 = false, .select_v2 = false},
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .limit = 1},
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .filter = {{column::pk, comparison::gt, 1}}},
        select_query{.distinct = true, .select_v1 = false, .select_v2 = false, .filter = {{column::pk, comparison::lt, 3}}, .limit = 1},
        select_query{.filter = {{column::v1, comparison::gt, 5}}},
        select_query{.filter = {{column::pk, comparison::gt, 1}, {column::v1, comparison::gt, 5}}},
        select_query{.filter = {{column::pk, comparison::eq, 2}}, .per_partition_limit = 1},
        select_query{.filter = {{column::s, comparison::eq, 5}}},
        select_query{.filter = {{column::v2, comparison::lt, 30}}, .per_partition_limit = 1},
        // In two_partitions(), the filter rejects the first row of partition 1.
        // A per-partition limit which counted rows before the filter would give
        // no row of that partition.
        select_query{.filter = {{column::v1, comparison::gt, 10}}, .per_partition_limit = 1},
        select_query{.limit = 1},
        select_query{.limit = 2},
        select_query{.per_partition_limit = 1},
        select_query{.limit = 3, .per_partition_limit = 2},
    };
    for (auto pk : pks) {
        queries.push_back(select_query{.partitions = std::vector<int32_t>{pk}, .reversed = true});
        queries.push_back(select_query{.partitions = std::vector<int32_t>{pk}, .ck_end = bound{3, false}, .reversed = true, .limit = 1});
    }
    return queries;
}

} // anonymous namespace

// Compares the model with unpaged CQL reads of the histories of the tests
// above.
SEASTAR_THREAD_TEST_CASE(test_model_matches_unpaged_cql) {
    do_with_cql_env_thread([] (cql_test_env& env) {
        create_keyspace(env);
        const std::vector<std::pair<std::string_view, history>> histories{
            {"newest_write_wins", newest_write_wins()},
            {"cell_tombstones", cell_tombstones()},
            {"deletions", deletions()},
            {"expiry", expiry()},
            {"static_rows", static_rows()},
            {"two_partitions", two_partitions()},
            {"key_and_ring_order", key_and_ring_order()},
        };
        for (const auto& [name, h] : histories) {
            std::set<int32_t> pks;
            for (const auto& op : h) {
                pks.insert(std::visit([] (const auto& o) { return o.pk; }, op));
            }
            compare_with_cql(env, name, h, standard_queries(pks | std::ranges::to<std::vector>()));
        }
    }, config_with_tombstone_gc_extension()).get();
}

// Compares the model with unpaged CQL reads of random histories and queries.
SEASTAR_THREAD_TEST_CASE(test_model_matches_unpaged_cql_on_random_histories) {
    do_with_cql_env_thread([] (cql_test_env& env) {
        create_keyspace(env);
        for (int i = 0; i < 30; ++i) {
            std::vector<select_query> queries;
            for (int j = 0; j < 20; ++j) {
                queries.push_back(random_query());
            }
            compare_with_cql(env, fmt::format("t{}", i), random_history(), queries);
        }
    }, config_with_tombstone_gc_extension()).get();
}

BOOST_AUTO_TEST_SUITE_END()
