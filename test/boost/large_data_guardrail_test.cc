/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <deque>
#include <seastar/util/defer.hh>
#undef SEASTAR_TESTING_MAIN
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "dht/i_partitioner.hh"
#include "locator/abstract_replication_strategy.hh"
#include "replica/database.hh"
#include "mutation/mutation.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/collection_mutation.hh"
#include "schema/schema_builder.hh"
#include "exceptions/exceptions.hh"
#include "types/set.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/eventually.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/sstable_utils.hh"
#include "compaction/compaction_group_view.hh"
#include "tasks/types.hh"
#include "utils/error_injection.hh"

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
        uint32_t collection_fail = 0, uint32_t collection_warn = 0,
        bool cql_warnings_enabled = false) {
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
        .cql_warnings_enabled = utils::updateable_value<bool>(cql_warnings_enabled),
    };
}

void coordinator_check(const db::large_data_guardrail& g, const mutation& m) {
    g.check_coordinator(*m.schema(), m.partition(), m.key(), nullptr);
}

// Runs the coordinator check collecting soft limit violations.
db::large_data_violation_type coordinator_soft_violations(const db::large_data_guardrail& g, const mutation& m) {
    db::large_data_violation_type violations = db::large_data_violation_type::none;
    g.check_coordinator(*m.schema(), m.partition(), m.key(), &violations);
    return violations;
}

schema_ptr make_set_schema() {
    auto set_type = set_type_impl::get_instance(int32_type, true);
    return schema_builder(1, "ks", "tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("s", set_type)
        .build();
}

schema_ptr make_static_set_schema() {
    auto set_type = set_type_impl::get_instance(int32_type, true);
    return schema_builder(1, "ks", "tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("s", set_type, column_kind::static_column)
        .build();
}

schema_ptr make_frozen_set_schema() {
    auto set_type = set_type_impl::get_instance(int32_type, false);
    return schema_builder(1, "ks", "tbl")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("v", set_type)
        .build();
}

mutation make_mutation_with_set(schema_ptr s, size_t element_count, bool is_static = false) {
    auto key = partition_key::from_exploded(*s, {to_bytes("pk1")});
    auto c_key = clustering_key::from_exploded(*s, {to_bytes("ck1")});
    mutation m(s, key);
    collection_mutation_writer w({});
    for (size_t i = 0; i < element_count; ++i) {
        auto key = int32_type->decompose(int(i));
        w.push_back(
            managed_bytes_view(bytes_view(key)),
            atomic_cell::make_live(*bytes_type, 0, bytes_view(), atomic_cell::collection_member::yes));
    }
    const column_definition& col = *s->get_column_definition("s");
    auto serialized = std::move(w).finish();
    if (is_static) {
        m.set_static_cell(col, std::move(serialized));
    } else {
        m.set_clustered_cell(c_key, col, std::move(serialized));
    }
    return m;
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

// Collection guardrail tests

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_hard_limit_rejects_large_set) {
    auto s = make_set_schema();
    auto cfg = make_coordinator_config(0, 0, 0, 0, 100 /* collection_fail */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 101);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_hard_limit_allows_small_set) {
    auto s = make_set_schema();
    auto cfg = make_coordinator_config(0, 0, 0, 0, 100 /* collection_fail */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 10);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_hard_limit_allows_at_threshold) {
    auto s = make_set_schema();
    auto cfg = make_coordinator_config(0, 0, 0, 0, 100 /* collection_fail */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 100);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_hard_limit_disabled_when_zero) {
    auto s = make_set_schema();
    auto cfg = make_coordinator_config();
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 10000);
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_hard_limit_rejects_large_static_set) {
    auto s = make_static_set_schema();
    auto cfg = make_coordinator_config(0, 0, 0, 0, 100 /* collection_fail */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 101, true /* is_static */);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_frozen_not_checked_by_element_count) {
    auto s = make_frozen_set_schema();
    auto set_type = set_type_impl::get_instance(int32_type, false);
    auto key = partition_key::from_exploded(*s, {to_bytes("pk1")});
    mutation m(s, key);

    set_type_impl::native_type elements;
    for (int i = 0; i < 200; ++i) {
        elements.push_back(data_value(i));
    }
    auto frozen_value = make_set_value(set_type, std::move(elements));
    const column_definition& v_col = *s->get_column_definition("v");
    auto cell = atomic_cell::make_live(*set_type, 0, frozen_value.serialize_nonnull());
    m.set_clustered_cell(clustering_key::make_empty(), v_col, std::move(cell));

    auto cfg = make_coordinator_config(0, 0, 0, 0, 100 /* collection_fail */);
    db::large_data_guardrail g(std::move(cfg));
    BOOST_REQUIRE_NO_THROW(coordinator_check(g, m));
}

// Mixed guardrail tests

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_limit_independent_of_row_limit) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_limit_independent_of_cell_limit) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(0, 0, 1 /* row_fail_mb */);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(coordinator_check(g, m), exceptions::invalid_request_exception);
}

// Per-table toggle and noop tests

SEASTAR_THREAD_TEST_CASE(test_coordinator_noop_allows_everything) {
    auto s = make_simple_schema();
    auto m = make_mutation_with_cell(s, 2 * MB);
    BOOST_REQUIRE_NO_THROW(
        db::noop_large_data_guardrail::instance()->check(*m.schema(), m.partition(), m.key(), nullptr));
}

BOOST_AUTO_TEST_CASE(test_per_table_guardrails_enabled_by_extension) {
    auto s = schema_builder(1, "ks", "tbl_guardrails")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("v", bytes_type)
        .set_large_data_guardrails_enabled(true)
        .build();
    BOOST_REQUIRE(s->large_data_guardrails_enabled());
}

BOOST_AUTO_TEST_CASE(test_per_table_guardrails_disabled_by_default) {
    auto s = schema_builder(1, "ks", "tbl_no_guardrails")
        .with_column("pk", utf8_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("v", bytes_type)
        .build();
    BOOST_REQUIRE(!s->large_data_guardrails_enabled());
}

// ---------------------------------------------------------------------------
// CQL warning (soft limit) tests
// ---------------------------------------------------------------------------

// Warning string builder

BOOST_AUTO_TEST_CASE(test_soft_violation_warning_empty_when_no_bits) {
    BOOST_REQUIRE(db::large_data_soft_violation_warning(db::large_data_violation_type::none).empty());
}

BOOST_AUTO_TEST_CASE(test_soft_violation_warning_single_category) {
    BOOST_REQUIRE_EQUAL(
        db::large_data_soft_violation_warning(db::large_data_violation_type::row),
        "Large data guardrail: Soft limit violation for row");
    BOOST_REQUIRE_EQUAL(
        db::large_data_soft_violation_warning(db::large_data_violation_type::cell),
        "Large data guardrail: Soft limit violation for cell");
    BOOST_REQUIRE_EQUAL(
        db::large_data_soft_violation_warning(db::large_data_violation_type::collection),
        "Large data guardrail: Soft limit violation for collection");
}

BOOST_AUTO_TEST_CASE(test_soft_violation_warning_multiple_categories_in_order) {
    // Regardless of how the categories were combined, the listed order is
    // row, cell, collection.
    auto cell_and_collection = db::large_data_violation_type::collection | db::large_data_violation_type::cell;
    BOOST_REQUIRE_EQUAL(
        db::large_data_soft_violation_warning(cell_and_collection),
        "Large data guardrail: Soft limit violation for cell, collection");

    auto all = db::large_data_violation_type::row | db::large_data_violation_type::cell
        | db::large_data_violation_type::collection;
    BOOST_REQUIRE_EQUAL(
        db::large_data_soft_violation_warning(all),
        "Large data guardrail: Soft limit violation for row, cell, collection");
}

// Soft limit bitmask tracking

SEASTAR_THREAD_TEST_CASE(test_coordinator_cell_soft_limit_sets_bit) {
    auto s = make_simple_schema();
    // cell warn at 1MB, no hard limit; warnings enabled.
    auto cfg = make_coordinator_config(0, 1 /* cell_warn_mb */, 0, 0, 0, 0, true);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE(coordinator_soft_violations(g, m) == db::large_data_violation_type::cell);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_row_soft_limit_sets_bit) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(0, 0, 0, 1 /* row_warn_mb */, 0, 0, true);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    auto violations = coordinator_soft_violations(g, m);
    BOOST_REQUIRE((violations & db::large_data_violation_type::row) == db::large_data_violation_type::row);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_collection_soft_limit_sets_bit) {
    auto s = make_set_schema();
    auto cfg = make_coordinator_config(0, 0, 0, 0, 0, 100 /* collection_warn */, true);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 101);
    BOOST_REQUIRE(coordinator_soft_violations(g, m) == db::large_data_violation_type::collection);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_soft_limit_below_threshold_sets_no_bit) {
    auto s = make_simple_schema();
    auto cfg = make_coordinator_config(0, 1 /* cell_warn_mb */, 0, 0, 0, 0, true);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, 100);
    BOOST_REQUIRE(coordinator_soft_violations(g, m) == db::large_data_violation_type::none);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_soft_limit_suppressed_when_warnings_disabled) {
    auto s = make_simple_schema();
    // Soft cell limit is crossed but CQL warnings are disabled -> nothing set.
    auto cfg = make_coordinator_config(0, 1 /* cell_warn_mb */, 0, 0, 0, 0, false);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE(coordinator_soft_violations(g, m) == db::large_data_violation_type::none);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_soft_limit_accumulates_multiple_categories) {
    auto s = make_set_schema();
    // Both the row (the set's serialized size) and the collection element count
    // cross their soft limits in a single mutation.
    auto cfg = make_coordinator_config(0, 0, 0, 1 /* row_warn_mb */, 0, 100 /* collection_warn */, true);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_set(s, 101);
    auto violations = coordinator_soft_violations(g, m);
    BOOST_REQUIRE((violations & db::large_data_violation_type::collection) == db::large_data_violation_type::collection);
}

SEASTAR_THREAD_TEST_CASE(test_coordinator_soft_limit_hard_limit_still_throws) {
    auto s = make_simple_schema();
    // Hard cell limit set; warnings enabled. Hard limit must still throw.
    auto cfg = make_coordinator_config(1 /* cell_fail_mb */, 0, 0, 0, 0, 0, true);
    db::large_data_guardrail g(std::move(cfg));
    auto m = make_mutation_with_cell(s, MB + 1);
    BOOST_REQUIRE_THROW(g.check_coordinator(*m.schema(), m.partition(), m.key(), nullptr),
            exceptions::invalid_request_exception);
}

// ---------------------------------------------------------------------------
// Linking large data records into the guardrail's index
// ---------------------------------------------------------------------------
//
// A large_data_record has a single intrusive hook, so it can be linked into one
// index, once. A schema change used to create a new guardrail and link every
// record into its index while records could still be linked: into the old
// guardrail, which in-flight writes keep alive (path a), or into the new index
// itself, by a flush that had already published its sstable but registered it
// only after the cache update (path b). And a compaction that handed back its
// input sstables unchanged registered them again (path c). Boost's safe-hook
// assert() then aborted the node; in builds with NDEBUG the indexes were
// silently corrupted.

namespace {

// Partitions with more than 5 rows get a record, and writes into partitions
// with more than 10 recorded rows are rejected.
cql_test_config row_count_guardrail_config() {
    auto cfg = make_shared<db::config>();
    cfg->compaction_rows_count_warning_threshold.set(5);
    cfg->rows_count_fail_threshold.set(10);
    return cql_test_config(std::move(cfg));
}

constexpr int large_partition_rows = 20;

replica::table& test_table(cql_test_env& e) {
    return e.local_db().find_column_family("ks", "t");
}

void create_test_table(cql_test_env& e, bool guardrails_enabled) {
    e.execute_cql(fmt::format("CREATE TABLE ks.t (pk int, ck int, v int, PRIMARY KEY (pk, ck)) WITH large_data_guardrails_enabled = {}",
            guardrails_enabled)).get();
}

// A partition key of ks.t owned by this shard, so that the partition's records,
// the held guardrail and the paused flush are all on the shard running the test.
int32_t local_partition_key(cql_test_env& e) {
    auto& t = test_table(e);
    auto s = t.schema();
    auto erm = t.get_effective_replication_map();
    for (int32_t pk = 0;; ++pk) {
        auto key = partition_key::from_single_value(*s, int32_type->decompose(pk));
        if (erm->shard_for_reads(*s, dht::get_token(*s, key)) == this_shard_id()) {
            return pk;
        }
    }
}

void insert_large_partition(cql_test_env& e, int32_t pk) {
    for (int ck = 0; ck < large_partition_rows; ++ck) {
        e.execute_cql(fmt::format("INSERT INTO ks.t (pk, ck, v) VALUES ({}, {}, 0)", pk, ck)).get();
    }
}

// Fails unless the guardrail rejects a write into the partition, i.e. unless the
// index holds the partition's record.
void require_write_rejected(cql_test_env& e, int32_t pk) {
    BOOST_REQUIRE_EXCEPTION(
            e.execute_cql(fmt::format("INSERT INTO ks.t (pk, ck, v) VALUES ({}, {}, 0)", pk, large_partition_rows)).get(),
            std::exception, exception_predicate::message_contains("exceeds hard limit"));
}

// Creates ks.t with the guardrail enabled and one flushed partition over the
// hard limit, and returns that partition's key.
int32_t create_table_with_large_partition(cql_test_env& e) {
    create_test_table(e, true);
    const auto pk = local_partition_key(e);
    insert_large_partition(e, pk);
    test_table(e).flush().get();
    require_write_rejected(e, pk);
    return pk;
}

} // anonymous namespace

// Path (a): a schema change while a write holds the table's guardrail, as
// database::do_apply() does across the commitlog append and storage_proxy::cas()
// across the Paxos round.
SEASTAR_TEST_CASE(test_alter_table_while_guardrail_is_held) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const auto pk = create_table_with_large_partition(e);

        auto held = test_table(e).get_large_data_guardrail();
        e.execute_cql("ALTER TABLE ks.t WITH comment = 'altered'").get();
        // The records stay indexed. Skipping the records that are still linked
        // into the held guardrail would avoid the abort but admit this write.
        require_write_rejected(e, pk);
    }, row_count_guardrail_config());
}

// Path (a) across a disable and a re-enable: the guardrail that is still held
// must not keep records linked once it is no longer the table's guardrail.
SEASTAR_TEST_CASE(test_toggle_guardrail_while_guardrail_is_held) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        const auto pk = create_table_with_large_partition(e);

        auto held = test_table(e).get_large_data_guardrail();
        e.execute_cql("ALTER TABLE ks.t WITH large_data_guardrails_enabled = false").get();
        e.execute_cql("ALTER TABLE ks.t WITH large_data_guardrails_enabled = true").get();
        require_write_rejected(e, pk);

        held = nullptr;
        require_write_rejected(e, pk);
    }, row_count_guardrail_config());
}

namespace {

// Path (b): a schema change while a flush has published its sstable in the
// table's sstable set (in the cache update) but has not finished. The sstable
// must end up registered in the guardrail's index exactly once.
future<> do_test_alter_table_during_flush(bool guardrails_enabled_before) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    testlog.info("Skipping test: requires error injection (dev or debug build)");
    return make_ready_future<>();
#else
    return do_with_cql_env_thread([guardrails_enabled_before] (cql_test_env& e) {
        create_test_table(e, guardrails_enabled_before);
        const auto pk = local_partition_key(e);
        insert_large_partition(e, pk);

        // Pauses the flush right after the cache update, which has added the
        // new sstable to the table's sstable set.
        constexpr auto injection = "replica_post_flush_after_update_cache";
        auto& injector = utils::get_local_injector();
        injector.enable(injection, false, {{"table_name", "ks.t"}});
        auto flushed = test_table(e).flush();
        {
            // Disabling the injection releases the flush, also when a check fails.
            auto release_flush = defer([&] () noexcept {
                injector.disable(injection);
                flushed.wait();
            });
            BOOST_REQUIRE(eventually_true([&] { return injector.waiters(injection) > 0; }));

            e.execute_cql("ALTER TABLE ks.t WITH large_data_guardrails_enabled = true AND comment = 'altered'").get();
        }
        flushed.get();
        require_write_rejected(e, pk);
    }, row_count_guardrail_config());
#endif
}

} // anonymous namespace

SEASTAR_TEST_CASE(test_enable_guardrail_during_flush) {
    return do_test_alter_table_during_flush(false);
}

SEASTAR_TEST_CASE(test_alter_table_during_flush) {
    return do_test_alter_table_during_flush(true);
}

namespace {

size_t maintenance_sstable_count(replica::table& t) {
    size_t count = 0;
    t.parallel_foreach_compaction_group_view([&count] (compaction::compaction_group_view& view) -> future<> {
        count += (co_await view.maintenance_sstable_set())->size();
    }).get();
    return count;
}

} // anonymous namespace

// Path (c): an off-strategy compaction moves sstables that need no reshape from
// the maintenance set to the main set as they are, handing them back as its
// outputs (a tablet split does the same with sstables that need no split).
// They are registered with the guardrail already, and must not be registered
// again.
SEASTAR_TEST_CASE(test_offstrategy_integration_keeps_guardrail_records) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_test_table(e, true);
        const auto pk = local_partition_key(e);
        auto& t = test_table(e);
        auto s = t.schema();
        mutation m(s, partition_key::from_single_value(*s, int32_type->decompose(pk)));
        for (int32_t ck = 0; ck < large_partition_rows; ++ck) {
            m.set_clustered_cell(clustering_key::from_single_value(*s, int32_type->decompose(ck)), to_bytes("v"), data_value(int32_t(0)), api::new_timestamp());
        }
        auto sst = make_sstable_containing(t.make_sstable(), {m}).get();
        t.add_sstable_and_update_cache(sst, sstables::offstrategy::yes).get();
        BOOST_REQUIRE_EQUAL(maintenance_sstable_count(t), 1);
        require_write_rejected(e, pk);

        t.perform_offstrategy_compaction(tasks::make_empty_task_info()).get();
        // Integrated into the main set as is, not rewritten by a reshape.
        BOOST_REQUIRE_EQUAL(maintenance_sstable_count(t), 0);
        BOOST_REQUIRE(t.get_sstables()->contains(sst));
        require_write_rejected(e, pk);
    }, row_count_guardrail_config());
}

// The outputs of a regular compaction are registered: two sstables with half of
// the partition each have no record over the limit, their compaction output has.
SEASTAR_TEST_CASE(test_compaction_output_is_registered_with_guardrail) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        create_test_table(e, true);
        const auto pk = local_partition_key(e);
        auto& t = test_table(e);
        for (int ck = 0; ck < large_partition_rows; ++ck) {
            e.execute_cql(fmt::format("INSERT INTO ks.t (pk, ck, v) VALUES ({}, {}, 0)", pk, ck)).get();
            if (ck == large_partition_rows / 2 - 1) {
                t.flush().get();
            }
        }
        t.flush().get();
        // Admitted: no single sstable has more rows in the partition than the limit.
        e.execute_cql(fmt::format("INSERT INTO ks.t (pk, ck, v) VALUES ({}, {}, 0)", pk, large_partition_rows)).get();

        t.compact_all_sstables(tasks::make_empty_task_info()).get();
        require_write_rejected(e, pk);
    }, row_count_guardrail_config());
}

BOOST_AUTO_TEST_SUITE_END()
