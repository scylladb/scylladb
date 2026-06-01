/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <deque>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/thread_test_case.hh>
#include "db/large_data_handler.hh"
#include "mutation/mutation.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/collection_mutation.hh"
#include "schema/schema_builder.hh"
#include "exceptions/exceptions.hh"
#include "types/set.hh"

using sstables::large_data_record;
using sstables::large_data_type;
using db::lookup_key;
using db::record_type;
using db::record_compare;
using db::record_set;

namespace {

large_data_record& make_record(std::deque<large_data_record>& store,
        large_data_type type, bytes pk, bytes ck = {}, bytes col = {},
        uint64_t value = 0, uint64_t elements_count = 0) {
    store.emplace_back();
    auto& rec = store.back();
    rec.type = type;
    rec.partition_key.value = std::move(pk);
    rec.clustering_key.value = std::move(ck);
    rec.column_name.value = std::move(col);
    rec.value = value;
    rec.elements_count = elements_count;
    rec.range_tombstones = 0;
    rec.dead_rows = 0;
    return rec;
}

} // anonymous namespace

BOOST_AUTO_TEST_SUITE(large_data_guardrail_test)

BOOST_AUTO_TEST_CASE(test_partition_comparator_orders_by_pk_only) {
    record_compare<record_type::partition> cmp;
    std::deque<large_data_record> store;

    auto& a = make_record(store, large_data_type::partition_size, to_bytes("aaa"));
    auto& b = make_record(store, large_data_type::partition_size, to_bytes("bbb"));

    BOOST_REQUIRE(cmp(a, b));
    BOOST_REQUIRE(!cmp(b, a));
    BOOST_REQUIRE(!cmp(a, a));

    // Clustering key differences are ignored at partition level
    auto& c = make_record(store, large_data_type::partition_size,
            to_bytes("aaa"), to_bytes("zzz"));
    BOOST_REQUIRE(!cmp(a, c));
    BOOST_REQUIRE(!cmp(c, a));
}

BOOST_AUTO_TEST_CASE(test_row_comparator_orders_by_pk_then_ck) {
    record_compare<record_type::row> cmp;
    std::deque<large_data_record> store;

    auto& a = make_record(store, large_data_type::row_size,
            to_bytes("pk1"), to_bytes("ck_a"));
    auto& b = make_record(store, large_data_type::row_size,
            to_bytes("pk1"), to_bytes("ck_b"));
    auto& c = make_record(store, large_data_type::row_size,
            to_bytes("pk2"), to_bytes("ck_a"));

    BOOST_REQUIRE(cmp(a, b));   // same pk, ck_a < ck_b
    BOOST_REQUIRE(!cmp(b, a));
    BOOST_REQUIRE(cmp(a, c));   // pk1 < pk2
    BOOST_REQUIRE(cmp(b, c));   // pk1 < pk2 regardless of ck

    // Column name differences are ignored at row level
    auto& d = make_record(store, large_data_type::row_size,
            to_bytes("pk1"), to_bytes("ck_a"), to_bytes("col_z"));
    BOOST_REQUIRE(!cmp(a, d));
    BOOST_REQUIRE(!cmp(d, a));
}

BOOST_AUTO_TEST_CASE(test_collection_comparator_orders_by_pk_ck_col) {
    record_compare<record_type::collection> cmp;
    std::deque<large_data_record> store;

    auto& a = make_record(store, large_data_type::elements_in_collection,
            to_bytes("pk1"), to_bytes("ck1"), to_bytes("col_a"));
    auto& b = make_record(store, large_data_type::elements_in_collection,
            to_bytes("pk1"), to_bytes("ck1"), to_bytes("col_b"));

    BOOST_REQUIRE(cmp(a, b));
    BOOST_REQUIRE(!cmp(b, a));
    BOOST_REQUIRE(!cmp(a, a));

    // Same pk+ck+col → equivalent
    auto& c = make_record(store, large_data_type::elements_in_collection,
            to_bytes("pk1"), to_bytes("ck1"), to_bytes("col_a"));
    BOOST_REQUIRE(!cmp(a, c));
    BOOST_REQUIRE(!cmp(c, a));
}

BOOST_AUTO_TEST_CASE(test_heterogeneous_lookup_with_lookup_key) {
    record_compare<record_type::partition> cmp;
    std::deque<large_data_record> store;

    auto& rec = make_record(store, large_data_type::partition_size, to_bytes("pk1"));
    auto pk = to_bytes("pk1");
    lookup_key lk{bytes_view(pk), {}, {}};

    // (record, key) and (key, record) must be consistent
    BOOST_REQUIRE(!cmp(rec, lk));
    BOOST_REQUIRE(!cmp(lk, rec));

    auto pk2 = to_bytes("pk2");
    lookup_key lk2{bytes_view(pk2), {}, {}};
    BOOST_REQUIRE(cmp(rec, lk2));   // pk1 < pk2
    BOOST_REQUIRE(!cmp(lk2, rec));  // pk2 > pk1
}

BOOST_AUTO_TEST_CASE(test_equal_range_groups_partition_records) {
    record_set<record_type::partition> set;
    std::deque<large_data_record> store;

    auto& r1 = make_record(store, large_data_type::partition_size,
            to_bytes("pk_a"), {}, {}, 100);
    auto& r2 = make_record(store, large_data_type::partition_size,
            to_bytes("pk_b"), {}, {}, 200);
    auto& r3 = make_record(store, large_data_type::rows_in_partition,
            to_bytes("pk_a"), {}, {}, 0, 50);
    set.insert(r1);
    set.insert(r2);
    set.insert(r3);

    // pk_a should match r1 (partition_size) and r3 (rows_in_partition)
    auto pk_a = to_bytes("pk_a");
    lookup_key lk{bytes_view(pk_a), {}, {}};
    auto [begin, end] = set.equal_range(lk, set.key_comp());

    uint64_t max_size = 0, max_rows = 0;
    int count = 0;
    for (auto it = begin; it != end; ++it) {
        max_size = std::max(max_size, it->value);
        max_rows = std::max(max_rows, it->elements_count);
        ++count;
    }
    BOOST_REQUIRE_EQUAL(count, 2);
    BOOST_REQUIRE_EQUAL(max_size, 100);
    BOOST_REQUIRE_EQUAL(max_rows, 50);

    // pk_c: no match
    auto pk_c = to_bytes("pk_c");
    lookup_key lk_miss{bytes_view(pk_c), {}, {}};
    auto [b2, e2] = set.equal_range(lk_miss, set.key_comp());
    BOOST_REQUIRE(b2 == e2);
}

BOOST_AUTO_TEST_CASE(test_equal_range_groups_row_records) {
    record_set<record_type::row> set;
    std::deque<large_data_record> store;

    auto& r1 = make_record(store, large_data_type::row_size,
            to_bytes("pk"), to_bytes("ck_a"), {}, 100);
    auto& r2 = make_record(store, large_data_type::row_size,
            to_bytes("pk"), to_bytes("ck_a"), {}, 300);
    auto& r3 = make_record(store, large_data_type::row_size,
            to_bytes("pk"), to_bytes("ck_b"), {}, 200);
    set.insert(r1);
    set.insert(r2);
    set.insert(r3);

    auto pk = to_bytes("pk");
    auto ck_a = to_bytes("ck_a");
    lookup_key lk{bytes_view(pk), bytes_view(ck_a), {}};
    auto [begin, end] = set.equal_range(lk, set.key_comp());

    uint64_t max_size = 0;
    int count = 0;
    for (auto it = begin; it != end; ++it) {
        max_size = std::max(max_size, it->value);
        ++count;
    }
    BOOST_REQUIRE_EQUAL(count, 2);  // r1 and r2
    BOOST_REQUIRE_EQUAL(max_size, 300);
}

BOOST_AUTO_TEST_CASE(test_equal_range_groups_collection_records) {
    record_set<record_type::collection> set;
    std::deque<large_data_record> store;

    auto& r1 = make_record(store, large_data_type::elements_in_collection,
            to_bytes("pk"), to_bytes("ck"), to_bytes("col"), 0, 100);
    auto& r2 = make_record(store, large_data_type::elements_in_collection,
            to_bytes("pk"), to_bytes("ck"), to_bytes("col"), 0, 500);
    auto& r3 = make_record(store, large_data_type::elements_in_collection,
            to_bytes("pk"), to_bytes("ck"), to_bytes("other"), 0, 200);
    set.insert(r1);
    set.insert(r2);
    set.insert(r3);

    auto pk = to_bytes("pk");
    auto ck = to_bytes("ck");
    auto col = to_bytes("col");
    lookup_key lk{bytes_view(pk), bytes_view(ck), bytes_view(col)};
    auto [begin, end] = set.equal_range(lk, set.key_comp());

    uint64_t max_count = 0;
    int n = 0;
    for (auto it = begin; it != end; ++it) {
        max_count = std::max(max_count, it->elements_count);
        ++n;
    }
    BOOST_REQUIRE_EQUAL(n, 2);  // r1 and r2
    BOOST_REQUIRE_EQUAL(max_count, 500);
}

BOOST_AUTO_TEST_CASE(test_auto_unlink_on_record_destruction) {
    record_set<record_type::partition> set;
    {
        std::deque<large_data_record> store;
        auto& r1 = make_record(store, large_data_type::partition_size,
                to_bytes("pk_a"), {}, {}, 100);
        auto& r2 = make_record(store, large_data_type::partition_size,
                to_bytes("pk_b"), {}, {}, 200);
        set.insert(r1);
        set.insert(r2);
        BOOST_REQUIRE(!set.empty());
    }
    // store destroyed → records destroyed → auto_unlink fires
    BOOST_REQUIRE(set.empty());
}

// ---------------------------------------------------------------------------
// Coordinator guardrail tests
// ---------------------------------------------------------------------------

namespace {

constexpr uint64_t MB = 1024 * 1024;

schema_ptr make_simple_schema() {
    return schema_builder(1, "ks", "tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("v", bytes_type)
        .build();
}

schema_ptr make_static_schema() {
    return schema_builder(1, "ks", "tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("s", bytes_type, column_kind::static_column)
        .with_column("v", bytes_type)
        .build();
}

mutation make_mutation_with_cell(schema_ptr s, size_t value_size) {
    auto key = partition_key::from_exploded(*s, {to_bytes("pk1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("ck1")});
    mutation m(s, key);
    auto cell = atomic_cell::make_live(*bytes_type, 0, bytes(value_size, 'x'));
    const column_definition& v_col = *s->get_column_definition("v");
    m.set_clustered_cell(c_key, v_col, std::move(cell));
    return m;
}

mutation make_mutation_with_static_cell(schema_ptr s, size_t value_size) {
    auto key = partition_key::from_exploded(*s, {to_bytes("pk1")});
    mutation m(s, key);
    auto cell = atomic_cell::make_live(*bytes_type, 0, bytes(value_size, 'x'));
    const column_definition& s_col = *s->get_column_definition("s");
    m.set_static_cell(s_col, std::move(cell));
    return m;
}

mutation make_mutation_with_tombstone(schema_ptr s) {
    auto key = partition_key::from_exploded(*s, {to_bytes("pk1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("ck1")});
    mutation m(s, key);
    const column_definition& v_col = *s->get_column_definition("v");
    auto cell = atomic_cell::make_dead(0, gc_clock::now());
    m.set_clustered_cell(c_key, v_col, std::move(cell));
    return m;
}

db::guardrail_config make_coordinator_config(
        uint32_t cell_fail_mb = 0, uint32_t cell_warn_mb = 0,
        uint32_t row_fail_mb = 0, uint32_t row_warn_mb = 0,
        uint32_t collection_fail = 0, uint32_t collection_warn = 0) {
    return db::guardrail_config{
        .partition_size_fail_threshold_mb = utils::updateable_value<uint32_t>(0),
        .partition_size_warn_threshold_mb = utils::updateable_value<uint32_t>(0),
        .rows_count_fail_threshold = utils::updateable_value<uint32_t>(0),
        .rows_count_warn_threshold = utils::updateable_value<uint32_t>(0),
        .row_size_fail_threshold_mb = utils::updateable_value<uint32_t>(row_fail_mb),
        .row_size_warn_threshold_mb = utils::updateable_value<uint32_t>(row_warn_mb),
        .collection_elements_fail_threshold = utils::updateable_value<uint32_t>(collection_fail),
        .collection_elements_warn_threshold = utils::updateable_value<uint32_t>(collection_warn),
        .cell_size_fail_threshold_mb = utils::updateable_value<uint32_t>(cell_fail_mb),
        .cell_size_warn_threshold_mb = utils::updateable_value<uint32_t>(cell_warn_mb),
    };
}

void coordinator_check(const db::large_data_guardrail& g, const mutation& m) {
    g.check_coordinator(*m.schema(), m.partition(), m.key());
}

} // anonymous namespace

// Cell guardrail tests

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_hard_limit_rejects_large_cell) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_hard_limit_allows_small_cell) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, 100);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_hard_limit_allows_at_threshold) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_hard_limit_disabled_when_zero) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config();
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, 2 * MB);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_hard_limit_allows_tombstone) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_tombstone(s);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_hard_limit_rejects_large_static_cell) {
    auto s = make_static_schema();
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_static_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

// Row guardrail tests

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_hard_limit_rejects_large_row) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(0, 0, 1 /* row_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_hard_limit_allows_small_row) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(0, 0, 1 /* row_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, 100);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_hard_limit_disabled_when_zero) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config();
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, 2 * MB);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_hard_limit_rejects_large_static_row) {
    auto s = make_static_schema();
    auto cfg = make_coordinator_config(0, 0, 1 /* row_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_static_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_hard_limit_allows_tombstone) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(0, 0, 1 /* row_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_tombstone(s);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

BOOST_AUTO_TEST_SUITE_END()
