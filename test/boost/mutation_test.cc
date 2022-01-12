/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <random>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/combine.hpp>
#include "mutation_query.hh"
#include "hashers.hh"
#include "xx_hasher.hh"

#include <seastar/core/sstring.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/alloc_failure_injector.hh>
#include <seastar/util/closeable.hh>

#include "replica/database.hh"
#include "utils/UUID_gen.hh"
#include "mutation_reader.hh"
#include "clustering_interval_set.hh"
#include "schema_builder.hh"
#include "query-result-set.hh"
#include "query-result-reader.hh"
#include "partition_slice_builder.hh"
#include "test/lib/tmpdir.hh"
#include "compaction/compaction_manager.hh"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include "test/lib/mutation_assertions.hh"
#include "test/lib/result_set_assertions.hh"
#include "test/lib/failure_injecting_allocation_strategy.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/mutation_source_test.hh"
#include "cell_locking.hh"
#include "test/lib/flat_mutation_reader_assertions.hh"
#include "service/storage_proxy.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/log.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "types/set.hh"
#include "types/user.hh"
#include "concrete_types.hh"
#include "mutation_rebuilder.hh"

using namespace std::chrono_literals;

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(*bytes_type, 0, std::move(value));
}

static atomic_cell make_atomic_cell() {
    return atomic_cell::make_live(*bytes_type, 0, bytes_view());
}

template<typename T>
static atomic_cell make_atomic_cell(data_type dt, T value) {
    return atomic_cell::make_live(*dt, 0, dt->decompose(std::move(value)));
};

template<typename T>
static atomic_cell make_collection_member(data_type dt, T value) {
    return atomic_cell::make_live(*dt, 0, dt->decompose(std::move(value)), atomic_cell::collection_member::yes);
};

static mutation_partition get_partition(reader_permit permit, memtable& mt, const partition_key& key) {
    auto dk = dht::decorate_key(*mt.schema(), key);
    auto range = dht::partition_range::make_singular(dk);
    auto reader = mt.make_flat_reader(mt.schema(), std::move(permit), range);
    auto close_reader = deferred_close(reader);
    auto mo = read_mutation_from_flat_mutation_reader(reader).get0();
    BOOST_REQUIRE(bool(mo));
    return std::move(mo->partition());
}

future<>
with_column_family(schema_ptr s, replica::column_family::config cfg, noncopyable_function<future<> (replica::column_family&)> func) {
    auto tracker = make_lw_shared<cache_tracker>();
    auto dir = tmpdir();
    cfg.datadir = dir.path().string();
    auto cm = make_lw_shared<compaction_manager>();
    auto cl_stats = make_lw_shared<cell_locker_stats>();
    auto cf = make_lw_shared<replica::column_family>(s, cfg, replica::column_family::no_commitlog(), *cm, *cl_stats, *tracker);
    cf->mark_ready_for_writes();
    return func(*cf).then([cf, cm] {
        return cf->stop();
    }).finally([cf, cm, dir = std::move(dir), cl_stats, tracker] () mutable { cf = { }; });
}

SEASTAR_TEST_CASE(test_mutation_is_applied) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);

        auto mt = make_lw_shared<memtable>(s);

        const column_definition& r1_col = *s->get_column_definition("r1");
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

        mutation m(s, key);
        auto c = make_atomic_cell(int32_type, 3);
        m.set_clustered_cell(c_key, r1_col, std::move(c));
        mt->apply(std::move(m));

        auto p = get_partition(semaphore.make_permit(), *mt, key);
        row& r = p.clustered_row(*s, c_key).cells();
        auto i = r.find_cell(r1_col.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_atomic_cell(r1_col);
        BOOST_REQUIRE(cell.is_live());
        BOOST_REQUIRE(int32_type->equal(cell.value().linearize(), int32_type->decompose(3)));
    });
}

SEASTAR_TEST_CASE(test_multi_level_row_tombstones) {
    auto s = make_shared_schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}},
        {{"c1", int32_type}, {"c2", int32_type}, {"c3", int32_type}},
        {{"r1", int32_type}}, {}, utf8_type);

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(s, partition_key::from_exploded(*s, {to_bytes("key1")}));

    auto make_prefix = [s] (const std::vector<data_value>& v) {
        return clustering_key_prefix::from_deeply_exploded(*s, v);
    };
    auto make_key = [s] (const std::vector<data_value>& v) {
        return clustering_key::from_deeply_exploded(*s, v);
    };

    m.partition().apply_row_tombstone(*s, make_prefix({1, 2}), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 3})), row_tombstone(tombstone(9, ttl)));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 3}), tombstone(8, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), row_tombstone(tombstone(9, ttl)));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), row_tombstone(tombstone(8, ttl)));

    m.partition().apply_row_tombstone(*s, make_prefix({1}), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), row_tombstone(tombstone(11, ttl)));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), row_tombstone(tombstone(11, ttl)));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 4}), tombstone(6, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), row_tombstone(tombstone(11, ttl)));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), row_tombstone(tombstone(11, ttl)));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 4, 0})), row_tombstone(tombstone(11, ttl)));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_row_tombstone_updates) {
    auto s = make_shared_schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}, {"c2", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key1 = clustering_key::from_deeply_exploded(*s, {1, 0});
    auto c_key1_prefix = clustering_key_prefix::from_deeply_exploded(*s, {1});
    auto c_key2 = clustering_key::from_deeply_exploded(*s, {2, 0});
    auto c_key2_prefix = clustering_key_prefix::from_deeply_exploded(*s, {2});

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(s, key);
    m.partition().apply_row_tombstone(*s, c_key1_prefix, tombstone(1, ttl));
    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(0, ttl));

    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key1), row_tombstone(tombstone(1, ttl)));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), row_tombstone(tombstone(0, ttl)));

    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), row_tombstone(tombstone(1, ttl)));
    return make_ready_future<>();
}

collection_mutation_description make_collection_mutation(tombstone t, bytes key, atomic_cell cell)
{
    collection_mutation_description m;
    m.tomb = t;
    m.cells.emplace_back(std::move(key), std::move(cell));
    return m;
}

collection_mutation_description make_collection_mutation(tombstone t, bytes key1, atomic_cell cell1,  bytes key2, atomic_cell cell2)
{
    collection_mutation_description m;
    m.tomb = t;
    m.cells.emplace_back(std::move(key1), std::move(cell1));
    m.cells.emplace_back(std::move(key2), std::move(cell2));
    return m;
}

SEASTAR_TEST_CASE(test_map_mutations) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto my_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_map_type}}, utf8_type);
        auto mt = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto& column = *s->get_column_definition("s1");
        auto mmut1 = make_collection_mutation({}, int32_type->decompose(101), make_collection_member(utf8_type, sstring("101")));
        mutation m1(s, key);
        m1.set_static_cell(column, mmut1.serialize(*my_map_type));
        mt->apply(m1);
        auto mmut2 = make_collection_mutation({}, int32_type->decompose(102), make_collection_member(utf8_type, sstring("102")));
        mutation m2(s, key);
        m2.set_static_cell(column, mmut2.serialize(*my_map_type));
        mt->apply(m2);
        auto mmut3 = make_collection_mutation({}, int32_type->decompose(103), make_collection_member(utf8_type, sstring("103")));
        mutation m3(s, key);
        m3.set_static_cell(column, mmut3.serialize(*my_map_type));
        mt->apply(m3);
        auto mmut2o = make_collection_mutation({}, int32_type->decompose(102), make_collection_member(utf8_type, sstring("102 override")));
        mutation m2o(s, key);
        m2o.set_static_cell(column, mmut2o.serialize(*my_map_type));
        mt->apply(m2o);

        auto p = get_partition(semaphore.make_permit(), *mt, key);
        lazy_row& r = p.static_row();
        auto i = r.find_cell(column.id);
        BOOST_REQUIRE(i);
        i->as_collection_mutation().with_deserialized(*my_map_type, [] (collection_mutation_view_description muts) {
            BOOST_REQUIRE(muts.cells.size() == 3);
        });
        // FIXME: more strict tests
    });
}

SEASTAR_TEST_CASE(test_set_mutations) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto my_set_type = set_type_impl::get_instance(int32_type, true);
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_set_type}}, utf8_type);
        auto mt = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto& column = *s->get_column_definition("s1");
        auto mmut1 = make_collection_mutation({}, int32_type->decompose(101), make_atomic_cell());
        mutation m1(s, key);
        m1.set_static_cell(column, mmut1.serialize(*my_set_type));
        mt->apply(m1);
        auto mmut2 = make_collection_mutation({}, int32_type->decompose(102), make_atomic_cell());
        mutation m2(s, key);
        m2.set_static_cell(column, mmut2.serialize(*my_set_type));
        mt->apply(m2);
        auto mmut3 = make_collection_mutation({}, int32_type->decompose(103), make_atomic_cell());
        mutation m3(s, key);
        m3.set_static_cell(column, mmut3.serialize(*my_set_type));
        mt->apply(m3);
        auto mmut2o = make_collection_mutation({}, int32_type->decompose(102), make_atomic_cell());
        mutation m2o(s, key);
        m2o.set_static_cell(column, mmut2o.serialize(*my_set_type));
        mt->apply(m2o);

        auto p = get_partition(semaphore.make_permit(), *mt, key);
        lazy_row& r = p.static_row();
        auto i = r.find_cell(column.id);
        BOOST_REQUIRE(i);
        i->as_collection_mutation().with_deserialized(*my_set_type, [] (collection_mutation_view_description muts) {
            BOOST_REQUIRE(muts.cells.size() == 3);
        });
        // FIXME: more strict tests
    });
}

SEASTAR_TEST_CASE(test_list_mutations) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto my_list_type = list_type_impl::get_instance(int32_type, true);
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_list_type}}, utf8_type);
        auto mt = make_lw_shared<memtable>(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto& column = *s->get_column_definition("s1");
        auto make_key = [] { return timeuuid_type->decompose(utils::UUID_gen::get_time_UUID()); };
        auto mmut1 = make_collection_mutation({}, make_key(), make_collection_member(int32_type, 101));
        mutation m1(s, key);
        m1.set_static_cell(column, mmut1.serialize(*my_list_type));
        mt->apply(m1);
        auto mmut2 = make_collection_mutation({}, make_key(), make_collection_member(int32_type, 102));
        mutation m2(s, key);
        m2.set_static_cell(column, mmut2.serialize(*my_list_type));
        mt->apply(m2);
        auto mmut3 = make_collection_mutation({}, make_key(), make_collection_member(int32_type, 103));
        mutation m3(s, key);
        m3.set_static_cell(column, mmut3.serialize(*my_list_type));
        mt->apply(m3);
        auto mmut2o = make_collection_mutation({}, make_key(), make_collection_member(int32_type, 102));
        mutation m2o(s, key);
        m2o.set_static_cell(column, mmut2o.serialize(*my_list_type));
        mt->apply(m2o);

        auto p = get_partition(semaphore.make_permit(), *mt, key);
        lazy_row& r = p.static_row();
        auto i = r.find_cell(column.id);
        BOOST_REQUIRE(i);
        i->as_collection_mutation().with_deserialized(*my_list_type, [] (collection_mutation_view_description muts) {
            BOOST_REQUIRE(muts.cells.size() == 4);
        });
        // FIXME: more strict tests
    });
}

SEASTAR_THREAD_TEST_CASE(test_udt_mutations) {
    tests::reader_concurrency_semaphore_wrapper semaphore;

    // (a int, b text, c long, d text)
    auto ut = user_type_impl::get_instance("ks", to_bytes("ut"),
            {to_bytes("a"), to_bytes("b"), to_bytes("c"), to_bytes("d")},
            {int32_type, utf8_type, long_type, utf8_type},
            true);

    auto s = make_shared_schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", ut}}, utf8_type);
    auto mt = make_lw_shared<memtable>(s);
    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto& column = *s->get_column_definition("s1");

    // {a: 0, c: 2}
    auto mut1 = make_collection_mutation({}, serialize_field_index(0), make_collection_member(int32_type, 0),
            serialize_field_index(2), make_collection_member(long_type, int64_t(2)));
    mutation m1(s, key);
    m1.set_static_cell(column, mut1.serialize(*ut));
    mt->apply(m1);

    // {d: "text"}
    auto mut2 = make_collection_mutation({}, serialize_field_index(3), make_collection_member(utf8_type, "text"));
    mutation m2(s, key);
    m2.set_static_cell(column, mut2.serialize(*ut));
    mt->apply(m2);

    // {c: 3}
    auto mut3 = make_collection_mutation({}, serialize_field_index(2), make_collection_member(long_type, int64_t(3)));
    mutation m3(s, key);
    m3.set_static_cell(column, mut3.serialize(*ut));
    mt->apply(m3);

    auto p = get_partition(semaphore.make_permit(), *mt, key);
    lazy_row& r = p.static_row();
    auto i = r.find_cell(column.id);
    BOOST_REQUIRE(i);
    i->as_collection_mutation().with_deserialized(*ut, [&] (collection_mutation_view_description m) {
        // one cell for each field that has been set. mut3 and mut1 should have been merged
        BOOST_REQUIRE(m.cells.size() == 3);
        BOOST_REQUIRE(std::all_of(m.cells.begin(), m.cells.end(), [] (const auto& c) { return c.second.is_live(); }));

        auto cells_equal = [] (const auto& c1, const auto& c2) {
            return c1.first == c2.first && c1.second.value().linearize() == c2.second.value().linearize();
        };

        auto cell_a = std::make_pair(serialize_field_index(0), make_collection_member(int32_type, 0));
        BOOST_REQUIRE(cells_equal(m.cells[0], std::pair<bytes_view, atomic_cell_view>(cell_a.first, cell_a.second)));

        auto cell_c = std::make_pair(serialize_field_index(2), make_collection_member(long_type, int64_t(3)));
        BOOST_REQUIRE(cells_equal(m.cells[1], std::pair<bytes_view, atomic_cell_view>(cell_c.first, cell_c.second)));

        auto cell_d = std::make_pair(serialize_field_index(3), make_collection_member(utf8_type, "text"));
        BOOST_REQUIRE(cells_equal(m.cells[2], std::pair<bytes_view, atomic_cell_view>(cell_d.first, cell_d.second)));

        auto mm = m.materialize(*ut);
        BOOST_REQUIRE(mm.cells.size() == 3);

        BOOST_REQUIRE(cells_equal(mm.cells[0], cell_a));
        BOOST_REQUIRE(cells_equal(mm.cells[1], cell_c));
        BOOST_REQUIRE(cells_equal(mm.cells[2], cell_d));
    });
}

// Verify that serializing and unserializing a large collection doesn't
// trigger any large allocations.
// We create a 8MB collection, composed of key/value pairs of varying
// size, apply it to a memtable and verify that during usual memtable
// operations like merging two collections and compaction query results
// there are no allocations larger than our usual 128KB buffer size.
SEASTAR_THREAD_TEST_CASE(test_large_collection_allocation) {
    tests::reader_concurrency_semaphore_wrapper semaphore;

    const auto key_type = int32_type;
    const auto value_type = utf8_type;
    const auto collection_type = map_type_impl::get_instance(key_type, value_type, true);

    auto schema = schema_builder("test", "test_large_collection_allocation")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("v", collection_type)
        .build();

    const std::array sizes_kb{size_t(1), size_t(10), size_t(64)};
    auto mt = make_lw_shared<memtable>(schema);

    auto make_mutation_with_collection = [&schema, &semaphore, collection_type] (partition_key pk, collection_mutation_description cmd) {
        const auto& cdef = schema->column_at(column_kind::regular_column, 0);

        mutation mut(schema, pk);

        row r;
        r.apply(cdef, atomic_cell_or_collection(cmd.serialize(*collection_type)));
        mut.apply(mutation_fragment(*schema, semaphore.make_permit(), clustering_row(clustering_key_prefix::make_empty(), {}, {}, std::move(r))));

        return mut;
    };

    for (size_t i = 0; i != sizes_kb.size(); ++i) {
        const auto pk = partition_key::from_single_value(*schema, int32_type->decompose(int(i)));
        const auto blob_size = sizes_kb[i] * 1024;
        const bytes blob(blob_size, 'a');

        const auto stats_before = memory::stats();
        const memory::scoped_large_allocation_warning_threshold _{128 * 1024};

        const api::timestamp_type ts1 = 1;
        const api::timestamp_type ts2 = 2;

        collection_mutation_description cmd1;
        collection_mutation_description cmd2;

        for (size_t j = 0; j < size_t(8 * 1024 * 1024) / blob_size; ++j) { // we want no more than 8MB total size
            cmd1.cells.emplace_back(int32_type->decompose(int(j)), atomic_cell::make_live(*value_type, ts1, blob, atomic_cell::collection_member::yes));
            cmd2.cells.emplace_back(int32_type->decompose(int(j)), atomic_cell::make_live(*value_type, ts2, blob, atomic_cell::collection_member::yes));
        }

        mt->apply(make_mutation_with_collection(pk, std::move(cmd1)));
        mt->apply(make_mutation_with_collection(pk, std::move(cmd2))); // this should trigger a merge of the two collections

        auto rd = mt->make_flat_reader(schema, semaphore.make_permit());
        auto close_rd = deferred_close(rd);
        auto res_mut_opt = read_mutation_from_flat_mutation_reader(rd).get0();
        BOOST_REQUIRE(res_mut_opt);

        res_mut_opt->partition().compact_for_query(*schema, res_mut_opt->decorated_key(), gc_clock::now(), {query::full_clustering_range}, true, false,
                std::numeric_limits<uint32_t>::max());

        const auto stats_after = memory::stats();
        BOOST_REQUIRE_EQUAL(stats_before.large_allocations(), stats_after.large_allocations());
    }
}

SEASTAR_THREAD_TEST_CASE(test_large_collection_serialization_exception_safety) {
#ifndef SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION
    std::cout << "Test case " << get_name() << " will not run because SEASTAR_ENABLE_ALLOC_FAILURE_INJECTION is not defined." << std::endl;
    return;
#endif
    const auto key_type = int32_type;
    const auto value_type = utf8_type;
    const auto collection_type = map_type_impl::get_instance(key_type, value_type, true);

    const auto blob_size = 1024;
    const bytes blob(blob_size, 'a');
    const api::timestamp_type ts = 1;

    collection_mutation_description cmd;

    for (size_t i = 0; i != 256; ++i) {
        cmd.cells.emplace_back(int32_type->decompose(int(i)), atomic_cell::make_live(*value_type, ts, blob, atomic_cell::collection_member::yes));
    }

    // We need an undisturbed run first to create all thread_local variables.
    cmd.serialize(*collection_type);

    memory::with_allocation_failures([&] {
        cmd.serialize(*collection_type);
    });
}

SEASTAR_TEST_CASE(test_multiple_memtables_one_partition) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
    auto s = make_shared_schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);

    auto cf_stats = make_lw_shared<replica::cf_stats>();
    replica::column_family::config cfg = column_family_test_config(env.manager(), env.semaphore());
    cfg.enable_disk_reads = false;
    cfg.enable_disk_writes = false;
    cfg.enable_incremental_backups = false;
    cfg.cf_stats = &*cf_stats;

    with_column_family(s, cfg, [s, &env] (replica::column_family& cf) {
        const column_definition& r1_col = *s->get_column_definition("r1");
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});

        auto insert_row = [&] (int32_t c1, int32_t r1) {
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type, r1));
            cf.apply(std::move(m));
            return cf.flush();
        };
        insert_row(1001, 2001).get();
        insert_row(1002, 2002).get();
        insert_row(1003, 2003).get();
        {
            auto verify_row = [&] (int32_t c1, int32_t r1) {
                auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
                auto p_key = dht::decorate_key(*s, key);
                auto r = cf.find_row(cf.schema(), env.make_reader_permit(), p_key, c_key).get0();
                {
                    BOOST_REQUIRE(r);
                    auto i = r->find_cell(r1_col.id);
                    BOOST_REQUIRE(i);
                    auto cell = i->as_atomic_cell(r1_col);
                    BOOST_REQUIRE(cell.is_live());
                    BOOST_REQUIRE(int32_type->equal(cell.value().linearize(), int32_type->decompose(r1)));
                }
            };
            verify_row(1001, 2001);
            verify_row(1002, 2002);
            verify_row(1003, 2003);
        }
        return make_ready_future<>();
    }).get();
    });
}

SEASTAR_TEST_CASE(test_flush_in_the_middle_of_a_scan) {
  return sstables::test_env::do_with([] (sstables::test_env& env) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    auto cf_stats = make_lw_shared<replica::cf_stats>();

    replica::column_family::config cfg = column_family_test_config(env.manager(), env.semaphore());
    cfg.enable_disk_reads = true;
    cfg.enable_disk_writes = true;
    cfg.enable_cache = true;
    cfg.enable_incremental_backups = false;
    cfg.cf_stats = &*cf_stats;

    return with_column_family(s, cfg, [&env, s](replica::column_family& cf) {
        return seastar::async([&env, s, &cf] {
            // populate
            auto new_key = [&] {
                static thread_local int next = 0;
                return dht::decorate_key(*s,
                    partition_key::from_single_value(*s, to_bytes(format("key{:d}", next++))));
            };
            auto make_mutation = [&] {
                mutation m(s, new_key());
                m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(to_bytes("value")), 1);
                return m;
            };

            std::vector<mutation> mutations;
            for (int i = 0; i < 1000; ++i) {
                auto m = make_mutation();
                cf.apply(m);
                mutations.emplace_back(std::move(m));
            }

            std::sort(mutations.begin(), mutations.end(), mutation_decorated_key_less_comparator());

            // Flush will happen in the middle of reading for this scanner
            auto assert_that_scanner1 = assert_that(cf.make_reader(s, env.make_reader_permit(),
                        query::full_partition_range));

            // Flush will happen before it is invoked
            auto assert_that_scanner2 = assert_that(cf.make_reader(s, env.make_reader_permit(),
                        query::full_partition_range));

            // Flush will happen after all data was read, but before EOS was consumed
            auto assert_that_scanner3 = assert_that(cf.make_reader(s, env.make_reader_permit(),
                        query::full_partition_range));

            assert_that_scanner1.produces(mutations[0]);
            assert_that_scanner1.produces(mutations[1]);

            for (unsigned i = 0; i < mutations.size(); ++i) {
                assert_that_scanner3.produces(mutations[i]);
            }

            memtable& m = cf.active_memtable(); // held by scanners

            auto flushed = cf.flush();

            while (!m.is_flushed()) {
                sleep(10ms).get();
            }

            for (unsigned i = 2; i < mutations.size(); ++i) {
                assert_that_scanner1.produces(mutations[i]);
            }
            assert_that_scanner1.produces_end_of_stream();

            for (unsigned i = 0; i < mutations.size(); ++i) {
                assert_that_scanner2.produces(mutations[i]);
            }
            assert_that_scanner2.produces_end_of_stream();

            assert_that_scanner3.produces_end_of_stream();

            flushed.get();
        });
    }).then([cf_stats] {});
  });
}

SEASTAR_TEST_CASE(test_multiple_memtables_multiple_partitions) {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
    auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type);

    auto cf_stats = make_lw_shared<replica::cf_stats>();

    replica::column_family::config cfg = column_family_test_config(env.manager(), env.semaphore());
    cfg.enable_disk_reads = false;
    cfg.enable_disk_writes = false;
    cfg.enable_incremental_backups = false;
    cfg.cf_stats = &*cf_stats;

    with_column_family(s, cfg, [s, &env] (auto& cf) mutable {
        std::map<int32_t, std::map<int32_t, int32_t>> shadow, result;

        const column_definition& r1_col = *s->get_column_definition("r1");

        api::timestamp_type ts = 0;
        auto insert_row = [&] (int32_t p1, int32_t c1, int32_t r1) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(p1)});
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
            mutation m(s, key);
            m.set_clustered_cell(c_key, r1_col, atomic_cell::make_live(*int32_type, ts++, int32_type->decompose(r1)));
            cf.apply(std::move(m));
            shadow[p1][c1] = r1;
        };
        std::minstd_rand random_engine;
        std::normal_distribution<> pk_distribution(0, 10);
        std::normal_distribution<> ck_distribution(0, 5);
        std::normal_distribution<> r_distribution(0, 100);
        for (unsigned i = 0; i < 10; ++i) {
            for (unsigned j = 0; j < 100; ++j) {
                insert_row(pk_distribution(random_engine), ck_distribution(random_engine), r_distribution(random_engine));
            }
            // In the background, cf.stop() will wait for this.
            (void)cf.flush();
        }

        return do_with(std::move(result), [&cf, s, &env, &r1_col, shadow] (auto& result) {
            return cf.for_all_partitions_slow(s, env.make_reader_permit(), [&, s] (const dht::decorated_key& pk, const mutation_partition& mp) {
                auto p1 = value_cast<int32_t>(int32_type->deserialize(pk._key.explode(*s)[0]));
                for (const rows_entry& re : mp.range(*s, nonwrapping_range<clustering_key_prefix>())) {
                    auto c1 = value_cast<int32_t>(int32_type->deserialize(re.key().explode(*s)[0]));
                    auto cell = re.row().cells().find_cell(r1_col.id);
                    if (cell) {
                        result[p1][c1] = value_cast<int32_t>(int32_type->deserialize(cell->as_atomic_cell(r1_col).value().linearize()));
                    }
                }
                return true;
            }).then([&result, shadow] (bool ok) {
                BOOST_REQUIRE(shadow == result);
            });
        });
    }).then([cf_stats] {}).get();
    });
}

SEASTAR_TEST_CASE(test_cell_ordering) {
    auto now = gc_clock::now();
    auto ttl_1 = gc_clock::duration(1);
    auto ttl_2 = gc_clock::duration(2);
    auto expiry_1 = now + ttl_1;
    auto expiry_2 = now + ttl_2;

    auto assert_order = [] (atomic_cell_view first, atomic_cell_view second) {
        if (compare_atomic_cell_for_merge(first, second) >= 0) {
            testlog.trace("Expected {} < {}", first, second);
            abort();
        }
        if (compare_atomic_cell_for_merge(second, first) <= 0) {
            testlog.trace("Expected {} < {}", second, first);
            abort();
        }
    };

    auto assert_equal = [] (atomic_cell_view c1, atomic_cell_view c2) {
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c1, c2) == 0);
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c2, c1) == 0);
    };

    assert_equal(
        atomic_cell::make_live(*bytes_type, 0, bytes("value")),
        atomic_cell::make_live(*bytes_type, 0, bytes("value")));

    assert_order(
        atomic_cell::make_live(*bytes_type, 1, bytes("value")),
        atomic_cell::make_live(*bytes_type, 1, bytes("value"), expiry_1, ttl_1));

    assert_equal(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_1));

    assert_order(
        atomic_cell::make_live(*bytes_type, 1, bytes()),
        atomic_cell::make_live(*bytes_type, 1, bytes(), expiry_2, ttl_2));

    // Origin doesn't compare ttl (is it wise?)
    assert_equal(
        atomic_cell::make_live(*bytes_type, 1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(*bytes_type, 1, bytes("value"), expiry_1, ttl_2));

    assert_order(
        atomic_cell::make_live(*bytes_type, 0, bytes("value1")),
        atomic_cell::make_live(*bytes_type, 0, bytes("value2")));

    assert_order(
        atomic_cell::make_live(*bytes_type, 0, bytes("value12")),
        atomic_cell::make_live(*bytes_type, 0, bytes("value2")));

    // Live cells are ordered first by timestamp...
    assert_order(
        atomic_cell::make_live(*bytes_type, 0, bytes("value2")),
        atomic_cell::make_live(*bytes_type, 1, bytes("value1")));

    // ..then by value
    assert_order(
        atomic_cell::make_live(*bytes_type, 1, bytes("value1"), expiry_2, ttl_2),
        atomic_cell::make_live(*bytes_type, 1, bytes("value2"), expiry_1, ttl_1));

    // ..then by expiry
    assert_order(
        atomic_cell::make_live(*bytes_type, 1, bytes(), expiry_1, ttl_1),
        atomic_cell::make_live(*bytes_type, 1, bytes(), expiry_2, ttl_1));

    // Dead wins
    assert_order(
        atomic_cell::make_live(*bytes_type, 1, bytes("value")),
        atomic_cell::make_dead(1, expiry_1));

    // Dead wins with expiring cell
    assert_order(
        atomic_cell::make_live(*bytes_type, 1, bytes("value"), expiry_2, ttl_2),
        atomic_cell::make_dead(1, expiry_1));

    // Deleted cells are ordered first by timestamp
    assert_order(
        atomic_cell::make_dead(1, expiry_2),
        atomic_cell::make_dead(2, expiry_1));

    // ...then by expiry
    assert_order(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_2));
    return make_ready_future<>();
}

static query::partition_slice make_full_slice(const schema& s) {
    return partition_slice_builder(s).build();
}

SEASTAR_TEST_CASE(test_querying_of_mutation) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        auto resultify = [s] (const mutation& m) -> query::result_set {
            auto slice = make_full_slice(*s);
            return query::result_set::from_raw_result(s, slice, query_mutation(mutation(m), slice));
        };

        mutation m(s, partition_key::from_single_value(*s, "key1"));
        m.set_clustered_cell(clustering_key::make_empty(), "v", data_value(bytes("v1")), 1);

        assert_that(resultify(m))
            .has_only(a_row()
                .with_column("pk", data_value(bytes("key1")))
                .with_column("v", data_value(bytes("v1"))));

        m.partition().apply(tombstone(2, gc_clock::now()));

        assert_that(resultify(m)).is_empty();
    });
}

SEASTAR_TEST_CASE(test_partition_with_no_live_data_is_absent_in_data_query_results) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        mutation m(s, partition_key::from_single_value(*s, "key1"));
        m.partition().apply(tombstone(1, gc_clock::now()));
        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));
        m.set_clustered_cell(clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("A")))),
            *s->get_column_definition("v"), atomic_cell::make_dead(2, gc_clock::now()));

        auto slice = make_full_slice(*s);

        assert_that(query::result_set::from_raw_result(s, slice, query_mutation(mutation(m), slice)))
            .is_empty();
    });
}

SEASTAR_TEST_CASE(test_partition_with_live_data_in_static_row_is_present_in_the_results_even_if_static_row_was_not_queried) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        mutation m(s, partition_key::from_single_value(*s, "key1"));
        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("sc1:value")))));

        auto slice = partition_slice_builder(*s)
            .with_no_static_columns()
            .with_regular_column("v")
            .build();

        assert_that(query::result_set::from_raw_result(s, slice,
                query_mutation(mutation(m), slice)))
            .has_only(a_row()
                .with_column("pk", data_value(bytes("key1")))
                .with_column("v", data_value::make_null(bytes_type)));
    });
}

SEASTAR_TEST_CASE(test_query_result_with_one_regular_column_missing) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v1", bytes_type, column_kind::regular_column)
            .with_column("v2", bytes_type, column_kind::regular_column)
            .build();

        mutation m(s, partition_key::from_single_value(*s, "key1"));
        m.set_clustered_cell(clustering_key::from_single_value(*s, bytes("ck:A")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v1:value")))));

        auto slice = partition_slice_builder(*s).build();

        assert_that(query::result_set::from_raw_result(s, slice,
                query_mutation(mutation(m), slice)))
            .has_only(a_row()
                .with_column("pk", data_value(bytes("key1")))
                .with_column("ck", data_value(bytes("ck:A")))
                .with_column("v1", data_value(bytes("v1:value")))
                .with_column("v2", data_value::make_null(bytes_type)));
    });
}

SEASTAR_TEST_CASE(test_row_counting) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

        auto col_v = *s->get_column_definition("v");

        mutation m(s, partition_key::from_single_value(*s, "key1"));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        auto ckey1 = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("A"))));
        auto ckey2 = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("B"))));

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v:value")))));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("sc1:value")))));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_dead(2, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        m.partition().clustered_row(*s, ckey1).apply(row_marker(api::timestamp_type(3)));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().apply(tombstone(3, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_live(*bytes_type, 4, bytes_type->decompose(data_value(bytes("v:value")))));
        m.set_clustered_cell(ckey2, col_v, atomic_cell::make_live(*bytes_type, 4, bytes_type->decompose(data_value(bytes("v:value")))));

        BOOST_REQUIRE_EQUAL(2, m.live_row_count());
    });
}

SEASTAR_TEST_CASE(test_tombstone_apply) {
    auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

    auto pkey = partition_key::from_single_value(*s, "key1");

    mutation m1(s, pkey);

    BOOST_REQUIRE_EQUAL(m1.partition().partition_tombstone(), tombstone());

    mutation m2(s, pkey);
    auto tomb = tombstone(api::new_timestamp(), gc_clock::now());
    m2.partition().apply(tomb);
    BOOST_REQUIRE_EQUAL(m2.partition().partition_tombstone(), tomb);

    m1.apply(m2);

    BOOST_REQUIRE_EQUAL(m1.partition().partition_tombstone(), tomb);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_marker_apply) {
    auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v", bytes_type, column_kind::regular_column)
            .build();

    auto pkey = partition_key::from_single_value(*s, "pk1");
    auto ckey = clustering_key::from_single_value(*s, "ck1");

    auto mutation_with_marker = [&] (row_marker rm) {
        mutation m(s, pkey);
        m.partition().clustered_row(*s, ckey).marker() = rm;
        return m;
    };

    {
        mutation m(s, pkey);
        auto marker = row_marker(api::new_timestamp());
        auto mm = mutation_with_marker(marker);
        m.apply(mm);
        BOOST_REQUIRE_EQUAL(m.partition().clustered_row(*s, ckey).marker(), marker);
    }

    {
        mutation m(s, pkey);
        auto marker = row_marker(api::new_timestamp(), std::chrono::seconds(1), gc_clock::now());
        m.apply(mutation_with_marker(marker));
        BOOST_REQUIRE_EQUAL(m.partition().clustered_row(*s, ckey).marker(), marker);
    }

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_apply_monotonically_is_monotonic) {
    auto do_test = [](auto&& gen) {
        auto&& alloc = standard_allocator();
        with_allocator(alloc, [&] {
            mutation_application_stats app_stats;
            auto&& s = *gen.schema();
            mutation target = gen();
            mutation second = gen();

            target.partition().set_continuity(s, position_range::all_clustered_rows(), is_continuous::no);
            second.partition().set_continuity(s, position_range::all_clustered_rows(), is_continuous::no);

            // Mark random ranges as continuous in target and second.
            // Note that continuity merging rules mandate that the ranges are discjoint
            // between the two.
            {
                int which = 0;
                for (auto&& ck_range : gen.make_random_ranges(7)) {
                    bool use_second = which++ % 2;
                    mutation& dst = use_second ? second : target;
                    dst.partition().set_continuity(s, position_range::from_range(ck_range), is_continuous::yes);
                    // Continutiy merging rules mandate that continuous range in the newer verison
                    // contains all rows which are in the old versions.
                    if (use_second) {
                        second.partition().apply(s, target.partition().sliced(s, {ck_range}), app_stats);
                    }
                }
            }

            auto expected = target + second;

            mutation m = target;
            auto m2 = mutation_partition(*m.schema(), second.partition());
            memory::with_allocation_failures([&] {
                auto d = defer([&] {
                    auto&& s = *gen.schema();
                    auto c1 = m.partition().get_continuity(s);
                    auto c2 = m2.get_continuity(s);
                    clustering_interval_set actual;
                    actual.add(s, c1);
                    actual.add(s, c2);
                    auto expected_cont = expected.partition().get_continuity(s);
                    if (!actual.contained_in(expected_cont)) {
                        BOOST_FAIL(format("Continuity should be contained in the expected one, expected {} ({} + {}), got {} ({} + {})",
                            expected_cont, target.partition().get_continuity(s), second.partition().get_continuity(s),
                            actual, c1, c2));
                    }
                    m.partition().apply_monotonically(*m.schema(), std::move(m2), no_cache_tracker, app_stats);
                    assert_that(m).is_equal_to(expected);

                    m = target;
                    m2 = mutation_partition(*m.schema(), second.partition());
                });
                m.partition().apply_monotonically(*m.schema(), std::move(m2), no_cache_tracker, app_stats);
                d.cancel();
            });
            assert_that(m).is_equal_to(expected).has_same_continuity(expected);
        });
    };

    do_test(random_mutation_generator(random_mutation_generator::generate_counters::no));
    do_test(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_mutation_diff) {
    return seastar::async([] {
        mutation_application_stats app_stats;

        auto my_set_type = set_type_impl::get_instance(int32_type, true);
        auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("sc1", bytes_type, column_kind::static_column)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .with_column("v1", bytes_type, column_kind::regular_column)
            .with_column("v2", bytes_type, column_kind::regular_column)
            .with_column("v3", my_set_type, column_kind::regular_column)
            .build();

        auto ckey1 = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("A"))));
        auto ckey2 = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("B"))));

        mutation m1(s, partition_key::from_single_value(*s, "key1"));
        m1.set_static_cell(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));

        m1.partition().apply(tombstone { 1, gc_clock::now() });
        m1.set_clustered_cell(ckey1, *s->get_column_definition("v1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v1:value1")))));
        m1.set_clustered_cell(ckey1, *s->get_column_definition("v2"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v2:value2")))));

        m1.partition().clustered_row(*s, ckey2).apply(row_marker(3));
        m1.set_clustered_cell(ckey2, *s->get_column_definition("v2"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v2:value4")))));
        auto mset1 = make_collection_mutation({}, int32_type->decompose(1), make_atomic_cell(), int32_type->decompose(2), make_atomic_cell());
        m1.set_clustered_cell(ckey2, *s->get_column_definition("v3"), mset1.serialize(*my_set_type));

        mutation m2(s, partition_key::from_single_value(*s, "key1"));
        m2.set_clustered_cell(ckey1, *s->get_column_definition("v1"),
            atomic_cell::make_live(*bytes_type, 1, bytes_type->decompose(data_value(bytes("v1:value1a")))));
        m2.set_clustered_cell(ckey1, *s->get_column_definition("v2"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v2:value2")))));

        m2.set_clustered_cell(ckey2, *s->get_column_definition("v1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v1:value3")))));
        m2.set_clustered_cell(ckey2, *s->get_column_definition("v2"),
            atomic_cell::make_live(*bytes_type, 3, bytes_type->decompose(data_value(bytes("v2:value4a")))));
        auto mset2 = make_collection_mutation({}, int32_type->decompose(1), make_atomic_cell(), int32_type->decompose(3), make_atomic_cell());
        m2.set_clustered_cell(ckey2, *s->get_column_definition("v3"), mset2.serialize(*my_set_type));

        mutation m3(s, partition_key::from_single_value(*s, "key1"));
        m3.set_clustered_cell(ckey1, *s->get_column_definition("v1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v1:value1")))));

        m3.set_clustered_cell(ckey2, *s->get_column_definition("v1"),
            atomic_cell::make_live(*bytes_type, 2, bytes_type->decompose(data_value(bytes("v1:value3")))));
        m3.set_clustered_cell(ckey2, *s->get_column_definition("v2"),
            atomic_cell::make_live(*bytes_type, 3, bytes_type->decompose(data_value(bytes("v2:value4a")))));
        auto mset3 = make_collection_mutation({}, int32_type->decompose(1), make_atomic_cell());
        m3.set_clustered_cell(ckey2, *s->get_column_definition("v3"), mset3.serialize(*my_set_type));

        mutation m12(s, partition_key::from_single_value(*s, "key1"));
        m12.apply(m1);
        m12.apply(m2);

        auto m2_1 = m2.partition().difference(s, m1.partition());
        BOOST_REQUIRE_EQUAL(m2_1.partition_tombstone(), tombstone());
        BOOST_REQUIRE(!m2_1.static_row().size());
        BOOST_REQUIRE(!m2_1.find_row(*s, ckey1));
        BOOST_REQUIRE(m2_1.find_row(*s, ckey2));
        BOOST_REQUIRE(m2_1.find_row(*s, ckey2)->find_cell(2));
        auto cmv = m2_1.find_row(*s, ckey2)->find_cell(2)->as_collection_mutation();
        cmv.with_deserialized(*my_set_type, [] (collection_mutation_view_description cm) {
            BOOST_REQUIRE(cm.cells.size() == 1);
            BOOST_REQUIRE(cm.cells.front().first == int32_type->decompose(3));
        });

        mutation m12_1(s, partition_key::from_single_value(*s, "key1"));
        m12_1.apply(m1);
        m12_1.partition().apply(*s, m2_1, *s, app_stats);
        BOOST_REQUIRE_EQUAL(m12, m12_1);

        auto m1_2 = m1.partition().difference(s, m2.partition());
        BOOST_REQUIRE_EQUAL(m1_2.partition_tombstone(), m12.partition().partition_tombstone());
        BOOST_REQUIRE(m1_2.find_row(*s, ckey1));
        BOOST_REQUIRE(m1_2.find_row(*s, ckey2));
        BOOST_REQUIRE(!m1_2.find_row(*s, ckey1)->find_cell(1));
        BOOST_REQUIRE(!m1_2.find_row(*s, ckey2)->find_cell(0));
        BOOST_REQUIRE(!m1_2.find_row(*s, ckey2)->find_cell(1));
        cmv = m1_2.find_row(*s, ckey2)->find_cell(2)->as_collection_mutation();
        cmv.with_deserialized(*my_set_type, [] (collection_mutation_view_description cm) {
            BOOST_REQUIRE(cm.cells.size() == 1);
            BOOST_REQUIRE(cm.cells.front().first == int32_type->decompose(2));
        });

        mutation m12_2(s, partition_key::from_single_value(*s, "key1"));
        m12_2.apply(m2);
        m12_2.partition().apply(*s, m1_2, *s, app_stats);
        BOOST_REQUIRE_EQUAL(m12, m12_2);

        auto m3_12 = m3.partition().difference(s, m12.partition());
        BOOST_REQUIRE(m3_12.empty());

        auto m12_3 = m12.partition().difference(s, m3.partition());
        BOOST_REQUIRE_EQUAL(m12_3.partition_tombstone(), m12.partition().partition_tombstone());

        mutation m123(s, partition_key::from_single_value(*s, "key1"));
        m123.apply(m3);
        m123.partition().apply(*s, m12_3, *s, app_stats);
        BOOST_REQUIRE_EQUAL(m12, m123);
    });
}

SEASTAR_TEST_CASE(test_large_blobs) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {{"s1", bytes_type}}, utf8_type);

        auto mt = make_lw_shared<memtable>(s);

        auto blob1 = make_blob(1234567);
        auto blob2 = make_blob(2345678);


        const column_definition& s1_col = *s->get_column_definition("s1");
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});

        mutation m(s, key);
        m.set_static_cell(s1_col, make_atomic_cell(bytes_type, data_value(blob1)));
        mt->apply(std::move(m));

        auto p = get_partition(semaphore.make_permit(), *mt, key);
        lazy_row& r = p.static_row();
        auto i = r.find_cell(s1_col.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_atomic_cell(s1_col);
        BOOST_REQUIRE(cell.is_live());
        BOOST_REQUIRE(bytes_type->equal(cell.value().linearize(), bytes_type->decompose(data_value(blob1))));

        // Stress managed_bytes::linearize and scatter by merging a value into the cell
        mutation m2(s, key);
        m2.set_static_cell(s1_col, atomic_cell::make_live(*bytes_type, 7, bytes_type->decompose(data_value(blob2))));
        mt->apply(std::move(m2));

        auto p2 = get_partition(semaphore.make_permit(), *mt, key);
        lazy_row& r2 = p2.static_row();
        auto i2 = r2.find_cell(s1_col.id);
        BOOST_REQUIRE(i2);
        auto cell2 = i2->as_atomic_cell(s1_col);
        BOOST_REQUIRE(cell2.is_live());
        BOOST_REQUIRE(bytes_type->equal(cell2.value().linearize(), bytes_type->decompose(data_value(blob2))));
    });
}

SEASTAR_TEST_CASE(test_mutation_equality) {
    return seastar::async([] {
        for_each_mutation_pair([] (auto&& m1, auto&& m2, are_equal eq) {
            if (eq) {
                assert_that(m1).is_equal_to(m2);
            } else {
                assert_that(m1).is_not_equal_to(m2);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_mutation_hash) {
    return seastar::async([] {
        for_each_mutation_pair([] (auto&& m1, auto&& m2, are_equal eq) {
            auto test_with_hasher = [&] (auto hasher) {
                auto get_hash = [&] (const mutation &m) {
                    auto h = hasher;
                    feed_hash(h, m);
                    return h.finalize();
                };
                auto h1 = get_hash(m1);
                auto h2 = get_hash(m2);
                if (eq) {
                    if (h1 != h2) {
                        BOOST_FAIL(format("Hash should be equal for {} and {}", m1, m2));
                    }
                } else {
                    // We're using a strong hasher, collision should be unlikely
                    if (h1 == h2) {
                        BOOST_FAIL(format("Hash should be different for {} and {}", m1, m2));
                    }
                }
            };
            test_with_hasher(md5_hasher());
            test_with_hasher(xx_hasher());
        });
    });
}

static mutation compacted(const mutation& m) {
    auto result = m;
    result.partition().compact_for_compaction(*result.schema(), always_gc, result.decorated_key(), gc_clock::now());
    return result;
}

SEASTAR_TEST_CASE(test_query_digest) {
    return seastar::async([] {
        auto check_digests_equal = [] (const mutation& m1, const mutation& m2) {
            auto ps1 = partition_slice_builder(*m1.schema()).build();
            auto ps2 = partition_slice_builder(*m2.schema()).build();
            auto digest1 = *query_mutation(mutation(m1), ps1, query::max_rows, gc_clock::now(),
                    query::result_options::only_digest(query::digest_algorithm::xxHash)).digest();
            auto digest2 = *query_mutation( mutation(m2), ps2, query::max_rows, gc_clock::now(),
                    query::result_options::only_digest(query::digest_algorithm::xxHash)).digest();

            if (digest1 != digest2) {
                BOOST_FAIL(format("Digest should be the same for {} and {}", m1, m2));
            }
        };

        for_each_mutation_pair([&] (const mutation& m1, const mutation& m2, are_equal eq) {
            if (m1.schema()->version() != m2.schema()->version()) {
                return;
            }

            if (eq) {
                check_digests_equal(compacted(m1), m2);
                check_digests_equal(m1, compacted(m2));
            } else {
                testlog.info("If not equal, they should become so after applying diffs mutually");

                mutation_application_stats app_stats;
                schema_ptr s = m1.schema();

                auto m3 = m2;
                {
                    auto diff = m1.partition().difference(s, m2.partition());
                    m3.partition().apply(*m3.schema(), std::move(diff), app_stats);
                }

                auto m4 = m1;
                {
                    auto diff = m2.partition().difference(s, m1.partition());
                    m4.partition().apply(*m4.schema(), std::move(diff), app_stats);
                }

                check_digests_equal(m3, m4);
            }
        });
    });
}

SEASTAR_TEST_CASE(test_mutation_upgrade_of_equal_mutations) {
    return seastar::async([] {
        for_each_mutation_pair([](auto&& m1, auto&& m2, are_equal eq) {
            if (eq == are_equal::yes) {
                assert_that(m1).is_upgrade_equivalent(m2.schema());
                assert_that(m2).is_upgrade_equivalent(m1.schema());
            }
        });
    });
}

SEASTAR_TEST_CASE(test_mutation_upgrade) {
    return seastar::async([] {
        auto make_builder = [] {
            return schema_builder("ks", "cf")
                    .with_column("pk", bytes_type, column_kind::partition_key)
                    .with_column("ck", bytes_type, column_kind::clustering_key);
        };

        auto s = make_builder()
                .with_column("sc1", bytes_type, column_kind::static_column)
                .with_column("v1", bytes_type, column_kind::regular_column)
                .with_column("v2", bytes_type, column_kind::regular_column)
                .build();

        auto pk = partition_key::from_singular(*s, data_value(bytes("key1")));
        auto ckey1 = clustering_key::from_singular(*s, data_value(bytes("A")));

        {
            mutation m(s, pk);
            m.set_clustered_cell(ckey1, "v2", data_value(bytes("v2:value")), 1);

            assert_that(m).is_upgrade_equivalent(
                    make_builder() // without v1
                            .with_column("sc1", bytes_type, column_kind::static_column)
                            .with_column("v2", bytes_type, column_kind::regular_column)
                            .build());

            assert_that(m).is_upgrade_equivalent(
                    make_builder() // without sc1
                            .with_column("v1", bytes_type, column_kind::static_column)
                            .with_column("v2", bytes_type, column_kind::regular_column)
                            .build());

            assert_that(m).is_upgrade_equivalent(
                    make_builder() // with v1 recreated as static
                            .with_column("sc1", bytes_type, column_kind::static_column)
                            .with_column("v1", bytes_type, column_kind::static_column)
                            .with_column("v2", bytes_type, column_kind::regular_column)
                            .build());

            assert_that(m).is_upgrade_equivalent(
                    make_builder() // with new column inserted before v1
                            .with_column("sc1", bytes_type, column_kind::static_column)
                            .with_column("v0", bytes_type, column_kind::regular_column)
                            .with_column("v1", bytes_type, column_kind::regular_column)
                            .with_column("v2", bytes_type, column_kind::regular_column)
                            .build());

            assert_that(m).is_upgrade_equivalent(
                    make_builder() // with new column inserted after v2
                            .with_column("sc1", bytes_type, column_kind::static_column)
                            .with_column("v0", bytes_type, column_kind::regular_column)
                            .with_column("v2", bytes_type, column_kind::regular_column)
                            .with_column("v3", bytes_type, column_kind::regular_column)
                            .build());
        }

        {
            mutation m(s, pk);
            m.set_clustered_cell(ckey1, "v1", data_value(bytes("v2:value")), 1);
            m.set_clustered_cell(ckey1, "v2", data_value(bytes("v2:value")), 1);

            auto s2 = make_builder() // v2 changed into a static column, v1 removed
                    .with_column("v2", bytes_type, column_kind::static_column)
                    .build();

            m.upgrade(s2);

            mutation m2(s2, pk);
            m2.partition().clustered_row(*s2, ckey1);
            assert_that(m).is_equal_to(m2);
        }

        {
            mutation m(make_builder()
                    .with_column("v1", bytes_type, column_kind::regular_column)
                    .with_column("v2", bytes_type, column_kind::regular_column)
                    .with_column("v3", bytes_type, column_kind::regular_column)
                    .build(), pk);
            m.set_clustered_cell(ckey1, "v1", data_value(bytes("v1:value")), 1);
            m.set_clustered_cell(ckey1, "v2", data_value(bytes("v2:value")), 1);
            m.set_clustered_cell(ckey1, "v3", data_value(bytes("v3:value")), 1);

            auto s2 = make_builder() // v2 changed into a static column
                    .with_column("v1", bytes_type, column_kind::regular_column)
                    .with_column("v2", bytes_type, column_kind::static_column)
                    .with_column("v3", bytes_type, column_kind::regular_column)
                    .build();

            m.upgrade(s2);

            mutation m2(s2, pk);
            m2.set_clustered_cell(ckey1, "v1", data_value(bytes("v1:value")), 1);
            m2.set_clustered_cell(ckey1, "v3", data_value(bytes("v3:value")), 1);

            assert_that(m).is_equal_to(m2);
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_mutation_upgrade_type_change) {
    auto make_builder = [] {
        return schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key);
    };

    auto s1 = make_builder()
            .with_column("v1", int32_type)
            .build();

    auto s2 = make_builder()
            .with_column("v1", bytes_type)
            .build();

    auto pk = partition_key::from_singular(*s1, data_value(bytes("key1")));
    auto ck1 = clustering_key::from_singular(*s1, data_value(bytes("A")));

    mutation m(s1, pk);
    m.set_clustered_cell(ck1, "v1", data_value(int32_t(0x1234abcd)), 1);
    m.upgrade(s2);

    mutation m2(s2, pk);
    m2.set_clustered_cell(ck1, "v1", data_value(from_hex("1234abcd")), 1);

    assert_that(m).is_equal_to(m2);
}

// This test checks the behavior of row_marker::{is_live, is_dead, compact_and_expire}. Those functions have some
// duplicated logic that decides if a row is expired, and this test verifies that they behave the same with respect
// to TTL.
SEASTAR_THREAD_TEST_CASE(test_row_marker_expiry) {
    can_gc_fn never_gc = [] (tombstone) { return false; };

    auto must_be_alive = [&] (row_marker mark, gc_clock::time_point t) {
        testlog.trace("must_be_alive({}, {})", mark, t);
        BOOST_REQUIRE(mark.is_live(tombstone(), t));
        BOOST_REQUIRE(mark.is_missing() || !mark.is_dead(t));
        BOOST_REQUIRE(mark.compact_and_expire(tombstone(), t, never_gc, gc_clock::time_point()));
    };

    auto must_be_dead = [&] (row_marker mark, gc_clock::time_point t) {
        testlog.trace("must_be_dead({}, {})", mark, t);
        BOOST_REQUIRE(!mark.is_live(tombstone(), t));
        BOOST_REQUIRE(mark.is_missing() || mark.is_dead(t));
        BOOST_REQUIRE(!mark.compact_and_expire(tombstone(), t, never_gc, gc_clock::time_point()));
    };

    const auto timestamp = api::timestamp_type(1);
    const auto t0 = gc_clock::now();
    const auto t1 = t0 + 1s;
    const auto t2 = t0 + 2s;
    const auto t3 = t0 + 3s;

    // Without timestamp the marker is missing (doesn't exist)
    const row_marker m1;
    must_be_dead(m1, t0);
    must_be_dead(m1, t1);
    must_be_dead(m1, t2);
    must_be_dead(m1, t3);

    // With timestamp and without ttl, a row_marker is always alive
    const row_marker m2(timestamp);
    must_be_alive(m2, t0);
    must_be_alive(m2, t1);
    must_be_alive(m2, t2);
    must_be_alive(m2, t3);

    // A row_marker becomes dead exactly at the moment of expiry
    // Reproduces #4263, #5290
    const auto ttl = 1s;
    const row_marker m3(timestamp, ttl, t2);
    must_be_alive(m3, t0);
    must_be_alive(m3, t1);
    must_be_dead(m3, t2);
    must_be_dead(m3, t3);
}

SEASTAR_THREAD_TEST_CASE(test_querying_expired_rows) {
    auto s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("ck", bytes_type, column_kind::clustering_key)
            .build();

    auto pk = partition_key::from_singular(*s, data_value(bytes("key1")));
    auto ckey1 = clustering_key::from_singular(*s, data_value(bytes("A")));
    auto ckey2 = clustering_key::from_singular(*s, data_value(bytes("B")));
    auto ckey3 = clustering_key::from_singular(*s, data_value(bytes("C")));

    auto ttl = 1s;
    auto t0 = gc_clock::now();
    auto t1 = t0 + 1s;
    auto t2 = t0 + 2s;
    auto t3 = t0 + 3s;

    auto results_at_time = [s] (const mutation& m, gc_clock::time_point t) {
        auto slice = partition_slice_builder(*s)
                .without_partition_key_columns()
                .build();
        auto opts = query::result_options{query::result_request::result_and_digest, query::digest_algorithm::xxHash};
        return query::result_set::from_raw_result(s, slice, query_mutation(mutation(m), slice, query::max_rows, t, opts));
    };

    mutation m(s, pk);
    m.partition().clustered_row(*m.schema(), ckey1).apply(row_marker(api::new_timestamp(), ttl, t1));
    m.partition().clustered_row(*m.schema(), ckey2).apply(row_marker(api::new_timestamp(), ttl, t2));
    m.partition().clustered_row(*m.schema(), ckey3).apply(row_marker(api::new_timestamp(), ttl, t3));

    assert_that(results_at_time(m, t0))
            .has_size(3)
            .has(a_row().with_column("ck", data_value(bytes("A"))))
            .has(a_row().with_column("ck", data_value(bytes("B"))))
            .has(a_row().with_column("ck", data_value(bytes("C"))));

    assert_that(results_at_time(m, t1))
            .has_size(2)
            .has(a_row().with_column("ck", data_value(bytes("B"))))
            .has(a_row().with_column("ck", data_value(bytes("C"))));

    assert_that(results_at_time(m, t2))
            .has_size(1)
            .has(a_row().with_column("ck", data_value(bytes("C"))));

    assert_that(results_at_time(m, t3)).is_empty();
}

SEASTAR_TEST_CASE(test_querying_expired_cells) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key)
                .with_column("s1", bytes_type, column_kind::static_column)
                .with_column("s2", bytes_type, column_kind::static_column)
                .with_column("s3", bytes_type, column_kind::static_column)
                .with_column("v1", bytes_type)
                .with_column("v2", bytes_type)
                .with_column("v3", bytes_type)
                .build();

        auto pk = partition_key::from_singular(*s, data_value(bytes("key1")));
        auto ckey1 = clustering_key::from_singular(*s, data_value(bytes("A")));

        auto ttl = std::chrono::seconds(1);
        auto t1 = gc_clock::now();
        auto t0 = t1 - std::chrono::seconds(1);
        auto t2 = t1 + std::chrono::seconds(1);
        auto t3 = t2 + std::chrono::seconds(1);

        auto v1 = data_value(bytes("1"));
        auto v2 = data_value(bytes("2"));
        auto v3 = data_value(bytes("3"));

        auto results_at_time = [s] (const mutation& m, gc_clock::time_point t) {
            auto slice = partition_slice_builder(*s)
                    .with_regular_column("v1")
                    .with_regular_column("v2")
                    .with_regular_column("v3")
                    .with_static_column("s1")
                    .with_static_column("s2")
                    .with_static_column("s3")
                    .without_clustering_key_columns()
                    .without_partition_key_columns()
                    .build();
            auto opts = query::result_options{query::result_request::result_and_digest, query::digest_algorithm::xxHash};
            return query::result_set::from_raw_result(s, slice, query_mutation(mutation(m), slice, query::max_rows, t, opts));
        };

        {
            mutation m(s, pk);
            m.set_clustered_cell(ckey1, *s->get_column_definition("v1"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v1.serialize_nonnull(), t1, ttl));
            m.set_clustered_cell(ckey1, *s->get_column_definition("v2"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v2.serialize_nonnull(), t2, ttl));
            m.set_clustered_cell(ckey1, *s->get_column_definition("v3"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v3.serialize_nonnull(), t3, ttl));
            m.set_static_cell(*s->get_column_definition("s1"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v1.serialize_nonnull(), t1, ttl));
            m.set_static_cell(*s->get_column_definition("s2"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v2.serialize_nonnull(), t2, ttl));
            m.set_static_cell(*s->get_column_definition("s3"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v3.serialize_nonnull(), t3, ttl));

            assert_that(results_at_time(m, t0))
                    .has_only(a_row()
                         .with_column("s1", v1)
                         .with_column("s2", v2)
                         .with_column("s3", v3)
                         .with_column("v1", v1)
                         .with_column("v2", v2)
                         .with_column("v3", v3)
                         .and_only_that());

            assert_that(results_at_time(m, t1))
                    .has_only(a_row()
                         .with_column("s2", v2)
                         .with_column("s3", v3)
                         .with_column("v2", v2)
                         .with_column("v3", v3)
                         .and_only_that());

            assert_that(results_at_time(m, t2))
                    .has_only(a_row()
                         .with_column("s3", v3)
                         .with_column("v3", v3)
                         .and_only_that());

            assert_that(results_at_time(m, t3)).is_empty();
        }

        {
            mutation m(s, pk);
            m.set_clustered_cell(ckey1, *s->get_column_definition("v1"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v1.serialize_nonnull(), t1, ttl));
            m.set_static_cell(*s->get_column_definition("s1"), atomic_cell::make_live(*bytes_type, api::new_timestamp(), v1.serialize_nonnull(), t3, ttl));

            assert_that(results_at_time(m, t2))
                    .has_only(a_row().with_column("s1", v1).and_only_that());

            assert_that(results_at_time(m, t3)).is_empty();
        }
    });
}

SEASTAR_TEST_CASE(test_tombstone_purge) {
    auto builder = schema_builder("tests", "tombstone_purge")
        .with_column("id", utf8_type, column_kind::partition_key)
        .with_column("value", int32_type);
    builder.set_gc_grace_seconds(0);
    auto s = builder.build();

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    const column_definition& col = *s->get_column_definition("value");

    mutation m(s, key);
    m.set_clustered_cell(clustering_key::make_empty(), col, make_atomic_cell(int32_type, 1));
    tombstone tomb(api::new_timestamp(), gc_clock::now() - std::chrono::seconds(1));
    m.partition().apply(tomb);
    BOOST_REQUIRE(!m.partition().empty());
    m.partition().compact_for_compaction(*s, always_gc, m.decorated_key(), gc_clock::now());
    // Check that row was covered by tombstone.
    BOOST_REQUIRE(m.partition().empty());
    // Check that tombstone was purged after compact_for_compaction().
    BOOST_REQUIRE(!m.partition().partition_tombstone());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_slicing_mutation) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v", int32_type)
        .build();

    auto pk = partition_key::from_exploded(*s, { int32_type->decompose(0) });
    mutation m(s, pk);
    constexpr auto row_count = 8;
    for (auto i = 0; i < row_count; i++) {
        m.set_clustered_cell(clustering_key_prefix::from_single_value(*s, int32_type->decompose(i)),
                             to_bytes("v"), data_value(i), api::new_timestamp());
    }

    auto verify_rows = [&] (mutation_partition& mp, std::vector<int> rows) {
        std::deque<clustering_key> cks;
        for (auto&& cr : rows) {
            cks.emplace_back(clustering_key_prefix::from_single_value(*s, int32_type->decompose(cr)));
        }
        clustering_key::equality ck_eq(*s);
        for (auto&& cr : mp.clustered_rows()) {
            BOOST_REQUIRE(ck_eq(cr.key(), cks.front()));
            cks.pop_front();
        }
    };

    auto test_slicing = [&] (query::clustering_row_ranges ranges, std::vector<int> expected_rows) {
        mutation_partition mp1(m.partition(), *s, ranges);
        auto mp_temp = mutation_partition(*s, m.partition());
        mutation_partition mp2(std::move(mp_temp), *s, ranges);

        BOOST_REQUIRE(mp1.equal(*s, mp2));
        verify_rows(mp1, expected_rows);
    };

    test_slicing(query::clustering_row_ranges {
            query::clustering_range {
                { },
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(2)), false },
            },
            clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)),
            query::clustering_range {
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(7)) },
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(10)) },
            },
        },
        std::vector<int> { 0, 1, 5, 7 });

    test_slicing(query::clustering_row_ranges {
            query::clustering_range {
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(1)) },
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(2)) },
            },
            query::clustering_range {
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(4)), false },
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(6)) },
            },
            query::clustering_range {
                query::clustering_range::bound { clustering_key_prefix::from_single_value(*s, int32_type->decompose(7)), false },
                { },
            },
        },
        std::vector<int> { 1, 2, 5, 6 });

    test_slicing(query::clustering_row_ranges {
            query::clustering_range {
                { },
                { },
            },
        },
        std::vector<int> { 0, 1, 2, 3, 4, 5, 6, 7 });

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_trim_rows) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
                .with_column("pk", int32_type, column_kind::partition_key)
                .with_column("ck", int32_type, column_kind::clustering_key)
                .with_column("v", int32_type)
                .build();

        auto pk = partition_key::from_exploded(*s, { int32_type->decompose(0) });
        mutation m(s, pk);
        constexpr auto row_count = 8;
        for (auto i = 0; i < row_count; i++) {
            m.set_clustered_cell(clustering_key_prefix::from_single_value(*s, int32_type->decompose(i)),
                                 to_bytes("v"), data_value(i), api::new_timestamp() - 5);
        }
        m.partition().apply(tombstone(api::new_timestamp(), gc_clock::now()));

        auto now = gc_clock::now() + gc_clock::duration(std::chrono::hours(1));

        auto compact_and_expect_empty = [&] (mutation m, std::vector<query::clustering_range> ranges) {
            mutation m2 = m;
            m.partition().compact_for_query(*s, m.decorated_key(), now, ranges, false, false, query::max_rows);
            BOOST_REQUIRE(m.partition().clustered_rows().empty());

            std::reverse(ranges.begin(), ranges.end());
            m2.partition().compact_for_query(*s, m2.decorated_key(), now, ranges, false, true, query::max_rows);
            BOOST_REQUIRE(m2.partition().clustered_rows().empty());
        };

        std::vector<query::clustering_range> ranges = {
                query::clustering_range::make_starting_with(clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)))
        };
        compact_and_expect_empty(m, ranges);

        ranges = {
            query::clustering_range::make_starting_with(clustering_key_prefix::from_single_value(*s, int32_type->decompose(50)))
        };
        compact_and_expect_empty(m, ranges);

        ranges = {
            query::clustering_range::make_ending_with(clustering_key_prefix::from_single_value(*s, int32_type->decompose(5)))
        };
        compact_and_expect_empty(m, ranges);

        ranges = {
            query::clustering_range::make_open_ended_both_sides()
        };
        compact_and_expect_empty(m, ranges);
    });
}

SEASTAR_TEST_CASE(test_collection_cell_diff) {
    return seastar::async([] {
        auto s = make_shared_schema({}, some_keyspace, some_column_family,
            {{"p", utf8_type}}, {}, {{"v", list_type_impl::get_instance(bytes_type, true)}}, {}, utf8_type);

        auto& col = s->column_at(column_kind::regular_column, 0);
        auto k = dht::decorate_key(*s, partition_key::from_single_value(*s, to_bytes("key")));
        mutation m1(s, k);
        auto uuid = utils::UUID_gen::get_time_UUID_bytes();
        collection_mutation_description mcol1;
        mcol1.cells.emplace_back(
                bytes(reinterpret_cast<const int8_t*>(uuid.data()), uuid.size()),
                atomic_cell::make_live(*bytes_type, api::timestamp_type(1), to_bytes("element")));
        m1.set_clustered_cell(clustering_key::make_empty(), col, mcol1.serialize(*col.type));

        mutation m2(s, k);
        collection_mutation_description mcol2;
        mcol2.tomb = tombstone(api::timestamp_type(2), gc_clock::now());
        m2.set_clustered_cell(clustering_key::make_empty(), col, mcol2.serialize(*col.type));

        mutation m12 = m1;
        m12.apply(m2);

        auto diff = m12.partition().difference(s, m1.partition());
        BOOST_REQUIRE(!diff.empty());
        BOOST_REQUIRE(m2.partition().equal(*s, diff));
    });
}

SEASTAR_TEST_CASE(test_apply_is_commutative) {
    return seastar::async([] {
        for_each_mutation_pair([] (auto&& m1, auto&& m2, are_equal eq) {
            auto s = m1.schema();
            if (s != m2.schema()) {
                return; // mutations with different schemas not commutative
            }
            assert_that(m1 + m2).is_equal_to(m2 + m1);
        });
    });
}

SEASTAR_TEST_CASE(test_mutation_diff_with_random_generator) {
    return seastar::async([] {
        auto check_partitions_match = [] (const mutation_partition& mp1, const mutation_partition& mp2, const schema& s) {
            if (!mp1.equal(s, mp2)) {
                BOOST_FAIL(format("Partitions don't match, got: {}\n...and: {}", mutation_partition::printer(s, mp1), mutation_partition::printer(s, mp2)));
            }
        };
        const auto now = gc_clock::now();
        can_gc_fn never_gc = [] (tombstone) { return false; };
        for_each_mutation_pair([&] (auto m1, auto m2, are_equal eq) {
            mutation_application_stats app_stats;
            auto s = m1.schema();
            if (s != m2.schema()) {
                return;
            }
            m1.partition().compact_for_compaction(*s, never_gc, m1.decorated_key(), now);
            m2.partition().compact_for_compaction(*s, never_gc, m2.decorated_key(), now);
            auto m12 = m1;
            m12.apply(m2);
            auto m12_with_diff = m1;
            m12_with_diff.partition().apply(*s, m2.partition().difference(s, m1.partition()), app_stats);
            check_partitions_match(m12.partition(), m12_with_diff.partition(), *s);
            check_partitions_match(mutation_partition{s}, m1.partition().difference(s, m1.partition()), *s);
            check_partitions_match(m1.partition(), m1.partition().difference(s, mutation_partition{s}), *s);
            check_partitions_match(mutation_partition{s}, mutation_partition{s}.difference(s, m1.partition()), *s);
        });
    });
}

SEASTAR_TEST_CASE(test_continuity_merging_of_complete_mutations) {
    random_mutation_generator gen(random_mutation_generator::generate_counters::no);

    mutation m1 = gen();
    m1.partition().make_fully_continuous();
    mutation m2 = gen();
    m2.partition().make_fully_continuous();
    mutation m3 = m1 + m2;

    assert_that(m3).is_continuous(position_range::all_clustered_rows(), is_continuous::yes);

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_continuity_merging) {
    return seastar::async([] {
        simple_schema table;
        auto&& s = *table.schema();

        auto new_mutation = [&] {
            return mutation(table.schema(), table.make_pkey(0));
        };

        {
            auto left = new_mutation();
            auto right = new_mutation();
            auto result = new_mutation();

            left.partition().clustered_row(s, table.make_ckey(0), is_dummy::no, is_continuous::yes);
            right.partition().clustered_row(s, table.make_ckey(0), is_dummy::no, is_continuous::no);
            result.partition().clustered_row(s, table.make_ckey(0), is_dummy::no, is_continuous::yes);

            left.partition().clustered_row(s, table.make_ckey(1), is_dummy::yes, is_continuous::yes);
            right.partition().clustered_row(s, table.make_ckey(2), is_dummy::yes, is_continuous::no);
            result.partition().clustered_row(s, table.make_ckey(1), is_dummy::yes, is_continuous::yes);
            result.partition().clustered_row(s, table.make_ckey(2), is_dummy::yes, is_continuous::no);

            left.partition().clustered_row(s, table.make_ckey(3), is_dummy::yes, is_continuous::yes);
            right.partition().clustered_row(s, table.make_ckey(3), is_dummy::no, is_continuous::no);
            result.partition().clustered_row(s, table.make_ckey(3), is_dummy::no, is_continuous::yes);

            left.partition().clustered_row(s, table.make_ckey(4), is_dummy::no, is_continuous::no);
            right.partition().clustered_row(s, table.make_ckey(4), is_dummy::no, is_continuous::yes);
            result.partition().clustered_row(s, table.make_ckey(4), is_dummy::no, is_continuous::yes);

            left.partition().clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::no);
            right.partition().clustered_row(s, table.make_ckey(5), is_dummy::yes, is_continuous::yes);
            result.partition().clustered_row(s, table.make_ckey(5), is_dummy::no, is_continuous::yes);

            left.partition().clustered_row(s, table.make_ckey(6), is_dummy::no, is_continuous::yes);
            right.partition().clustered_row(s, table.make_ckey(6), is_dummy::yes, is_continuous::no);
            result.partition().clustered_row(s, table.make_ckey(6), is_dummy::no, is_continuous::yes);

            left.partition().clustered_row(s, table.make_ckey(7), is_dummy::yes, is_continuous::yes);
            right.partition().clustered_row(s, table.make_ckey(7), is_dummy::yes, is_continuous::no);
            result.partition().clustered_row(s, table.make_ckey(7), is_dummy::yes, is_continuous::yes);

            left.partition().clustered_row(s, table.make_ckey(8), is_dummy::yes, is_continuous::no);
            right.partition().clustered_row(s, table.make_ckey(8), is_dummy::yes, is_continuous::yes);
            result.partition().clustered_row(s, table.make_ckey(8), is_dummy::yes, is_continuous::yes);

            assert_that(right + left).has_same_continuity(result);
        }

        // static row continuity
        {
            auto complete = mutation(table.schema(), table.make_pkey(0));
            auto incomplete = mutation(table.schema(), table.make_pkey(0));
            incomplete.partition().set_static_row_continuous(false);

            assert_that(complete + complete).has_same_continuity(complete);
            assert_that(complete + incomplete).has_same_continuity(complete);
            assert_that(incomplete + complete).has_same_continuity(complete);
            assert_that(incomplete + incomplete).has_same_continuity(incomplete);
        }
    });
}

class measuring_allocator final : public allocation_strategy {
    size_t _allocated_bytes = 0;
public:
    measuring_allocator() {
        _preferred_max_contiguous_allocation = standard_allocator().preferred_max_contiguous_allocation();
    }
    virtual void* alloc(migrate_fn mf, size_t size, size_t alignment) override {
        _allocated_bytes += size;
        return standard_allocator().alloc(mf, size, alignment);
    }
    virtual void free(void* ptr, size_t size) override {
        standard_allocator().free(ptr, size);
    }
    virtual void free(void* ptr) override {
        standard_allocator().free(ptr);
    }
    virtual size_t object_memory_size_in_allocator(const void* obj) const noexcept override {
        return standard_allocator().object_memory_size_in_allocator(obj);
    }
    size_t allocated_bytes() const { return _allocated_bytes; }
};

SEASTAR_THREAD_TEST_CASE(test_external_memory_usage) {
    measuring_allocator alloc;
    auto s = simple_schema();

    auto generate = [&s] {
        size_t data_size = 0;

        auto m = mutation(s.schema(), s.make_pkey("pk"));

        auto row_count = tests::random::get_int(1, 16);
        for (auto i = 0; i < row_count; i++) {
            auto ck_value = to_hex(tests::random::get_bytes(tests::random::get_int(1023) + 1));
            data_size += ck_value.size();
            auto ck = s.make_ckey(ck_value);

            auto value = to_hex(tests::random::get_bytes(tests::random::get_int(128 * 1024)));
            data_size += value.size();
            s.add_row(m, ck, value);
        }

        return std::pair(std::move(m), data_size);
    };

    for (auto i = 0; i < 16; i++) {
        auto m_and_size = generate();
        auto&& m = m_and_size.first;
        auto&& size = m_and_size.second;

        with_allocator(alloc, [&] {
            auto before = alloc.allocated_bytes();
            auto m2 = m;
            auto after = alloc.allocated_bytes();

            BOOST_CHECK_EQUAL(m.partition().external_memory_usage(*s.schema()),
                              m2.partition().external_memory_usage(*s.schema()));

            BOOST_CHECK_GE(m.partition().external_memory_usage(*s.schema()), size);
            BOOST_CHECK_EQUAL(m.partition().external_memory_usage(*s.schema()), after - before);
        });
    }
}

SEASTAR_THREAD_TEST_CASE(test_cell_equals) {
    auto now = gc_clock::now();
    auto ttl = gc_clock::duration(0);

    auto c1 = atomic_cell_or_collection(atomic_cell::make_live(*bytes_type, 1, bytes(1, 'a'), now, ttl));
    auto c2 = atomic_cell_or_collection(atomic_cell::make_dead(1, now));
    BOOST_REQUIRE(!c1.equals(*bytes_type, c2));
    BOOST_REQUIRE(!c2.equals(*bytes_type, c1));

    auto c3 = atomic_cell_or_collection(atomic_cell::make_live_counter_update(1, 2));
    auto c4 = atomic_cell_or_collection(atomic_cell::make_live(*bytes_type, 1, bytes(1, 'a')));
    BOOST_REQUIRE(!c3.equals(*bytes_type, c4));
    BOOST_REQUIRE(!c4.equals(*bytes_type, c3));

    BOOST_REQUIRE(!c1.equals(*bytes_type, c4));
    BOOST_REQUIRE(!c4.equals(*bytes_type, c1));
}

// Global to avoid elimination by the compiler; see below for use
thread_local data_type force_type_thread_local_init_evaluation [[gnu::used]];

SEASTAR_THREAD_TEST_CASE(test_cell_external_memory_usage) {
    measuring_allocator alloc;

    // Force evaluation of int32_type and all the other types. This is so
    // the compiler doesn't reorder it into the with_allocator section, below,
    // and any managed_bytes instances creates during that operation interfere
    // with the measurements.
    force_type_thread_local_init_evaluation = int32_type;
    // optimization barrier
    std::atomic_signal_fence(std::memory_order_seq_cst);

    auto test_live_atomic_cell = [&] (data_type dt, bytes_view bv) {
        with_allocator(alloc, [&] {
            auto before = alloc.allocated_bytes();
            auto ac = atomic_cell_or_collection(atomic_cell::make_live(*dt, 1, bv));
            auto after = alloc.allocated_bytes();
            BOOST_CHECK_EQUAL(ac.external_memory_usage(*dt), after - before);
        });
    };

    test_live_atomic_cell(int32_type, { });
    test_live_atomic_cell(int32_type, int32_type->decompose(int32_t(1)));

    test_live_atomic_cell(bytes_type, { });
    test_live_atomic_cell(bytes_type, bytes(1, 'a'));
    test_live_atomic_cell(bytes_type, bytes(16, 'a'));
    test_live_atomic_cell(bytes_type, bytes(32, 'a'));
    test_live_atomic_cell(bytes_type, bytes(1024, 'a'));
    test_live_atomic_cell(bytes_type, bytes(64 * 1024 - 1, 'a'));
    test_live_atomic_cell(bytes_type, bytes(64 * 1024, 'a'));
    test_live_atomic_cell(bytes_type, bytes(64 * 1024 + 1, 'a'));
    test_live_atomic_cell(bytes_type, bytes(1024 * 1024, 'a'));

    auto test_collection = [&] (bytes_view bv) {
        auto collection_type = map_type_impl::get_instance(int32_type, bytes_type, true);

        auto m = make_collection_mutation({ }, int32_type->decompose(0), make_collection_member(bytes_type, data_value(bytes(bv))));
        auto cell = atomic_cell_or_collection(m.serialize(*collection_type));

        with_allocator(alloc, [&] {
            auto before = alloc.allocated_bytes();
            auto cell2 = cell.copy(*collection_type);
            auto after = alloc.allocated_bytes();
            BOOST_CHECK_EQUAL(cell2.external_memory_usage(*collection_type), cell.external_memory_usage(*collection_type));
            BOOST_CHECK_EQUAL(cell2.external_memory_usage(*collection_type), after - before);
        });
    };

    test_collection({ });
    test_collection(bytes(1, 'a'));
    test_collection(bytes(16, 'a'));
    test_collection(bytes(32, 'a'));
    test_collection(bytes(1024, 'a'));
    test_collection(bytes(64 * 1024 - 1, 'a'));
    test_collection(bytes(64 * 1024, 'a'));
    test_collection(bytes(64 * 1024 + 1, 'a'));
    test_collection(bytes(1024 * 1024, 'a'));
}

// external_memory_usage() must be invariant to the merging order,
// so that accounting of a clustering_row produced by partition_snapshot_flat_reader
// doesn't give a greater result than what is used by the memtable region, possibly
// after all MVCC versions are merged.
// Overaccounting leads to assertion failure in ~flush_memory_accounter.
SEASTAR_THREAD_TEST_CASE(test_row_size_is_immune_to_application_order) {
    auto s = schema_builder("ks", "cf")
            .with_column("pk", utf8_type, column_kind::partition_key)
            .with_column("v1", utf8_type)
            .with_column("v2", utf8_type)
            .with_column("v3", utf8_type)
            .with_column("v4", utf8_type)
            .with_column("v5", utf8_type)
            .with_column("v6", utf8_type)
            .with_column("v7", utf8_type)
            .with_column("v8", utf8_type)
            .with_column("v9", utf8_type)
            .build();

    auto value = utf8_type->decompose(data_value("value"));

    row r1;
    r1.append_cell(7, make_atomic_cell(value));

    row r2;
    r2.append_cell(8, make_atomic_cell(value));

    auto size1 = [&] {
        auto r3 = row(*s, column_kind::regular_column, r1);
        r3.apply(*s, column_kind::regular_column, r2);
        return r3.external_memory_usage(*s, column_kind::regular_column);
    }();

    auto size2 = [&] {
        auto r3 = row(*s, column_kind::regular_column, r2);
        r3.apply(*s, column_kind::regular_column, r1);
        return r3.external_memory_usage(*s, column_kind::regular_column);
    }();

    BOOST_REQUIRE_EQUAL(size1, size2);
}

SEASTAR_THREAD_TEST_CASE(test_schema_changes) {
    for_each_schema_change([] (schema_ptr base, const std::vector<mutation>& base_mutations,
                               schema_ptr changed, const std::vector<mutation>& changed_mutations) {
        BOOST_REQUIRE_EQUAL(base_mutations.size(), changed_mutations.size());
        for (auto bc : boost::range::combine(base_mutations, changed_mutations)) {
            auto b = boost::get<0>(bc);
            b.upgrade(changed);
            BOOST_CHECK_EQUAL(b, boost::get<1>(bc));
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_collection_compaction) {
    auto key = to_bytes("key");
    auto value = data_value(to_bytes("value"));

    // No collection tombstone, row tombstone covers all cells
    auto cmut = make_collection_mutation({}, key, make_collection_member(bytes_type, value));
    auto row_tomb = row_tombstone(tombstone { 1, gc_clock::time_point() });
    auto any_live = cmut.compact_and_expire(0, row_tomb, gc_clock::time_point(), always_gc, gc_clock::time_point());
    BOOST_CHECK(!any_live);
    BOOST_CHECK(!cmut.tomb);
    BOOST_CHECK(cmut.cells.empty());

    // No collection tombstone, row tombstone doesn't cover anything
    cmut = make_collection_mutation({}, key, make_collection_member(bytes_type, value));
    row_tomb = row_tombstone(tombstone { -1, gc_clock::time_point() });
    any_live = cmut.compact_and_expire(0, row_tomb, gc_clock::time_point(), always_gc, gc_clock::time_point());
    BOOST_CHECK(any_live);
    BOOST_CHECK(!cmut.tomb);
    BOOST_CHECK_EQUAL(cmut.cells.size(), 1);

    // Collection tombstone covers everything
    cmut = make_collection_mutation(tombstone { 2, gc_clock::time_point() }, key, make_collection_member(bytes_type, value));
    row_tomb = row_tombstone(tombstone { 1, gc_clock::time_point() });
    any_live = cmut.compact_and_expire(0, row_tomb, gc_clock::time_point(), always_gc, gc_clock::time_point());
    BOOST_CHECK(!any_live);
    BOOST_CHECK(cmut.tomb);
    BOOST_CHECK_EQUAL(cmut.tomb.timestamp, 2);
    BOOST_CHECK(cmut.cells.empty());

    // Collection tombstone covered by row tombstone
    cmut = make_collection_mutation(tombstone { 2, gc_clock::time_point() }, key, make_collection_member(bytes_type, value));
    row_tomb = row_tombstone(tombstone { 3, gc_clock::time_point() });
    any_live = cmut.compact_and_expire(0, row_tomb, gc_clock::time_point(), always_gc, gc_clock::time_point());
    BOOST_CHECK(!any_live);
    BOOST_CHECK(!cmut.tomb);
    BOOST_CHECK(cmut.cells.empty());
}

namespace {

template <bool OnlyPurged>
class basic_compacted_fragments_consumer_base {
    const schema& _schema;
    gc_clock::time_point _query_time;
    gc_clock::time_point _gc_before;
    std::function<api::timestamp_type(const dht::decorated_key&)> _get_max_purgeable;
    api::timestamp_type _max_purgeable;

    std::vector<mutation> _mutations;
    std::optional<mutation> _mutation;

private:
    bool can_gc(tombstone t) {
        if (!t) {
            return true;
        }
        return t.timestamp < _max_purgeable;
    }
    bool is_tombstone_purgeable(const tombstone& t) {
        return t.deletion_time < _gc_before && can_gc(t);
    }
    bool is_tombstone_purgeable(const row_tombstone& t) {
        return t.max_deletion_time() < _gc_before && can_gc(t.tomb());
    }
    bool is_marker_purgeable(const row_marker& marker, tombstone tomb) {
        return marker.timestamp() <= tomb.timestamp ||
            (marker.is_dead(_query_time) && marker.expiry() < _gc_before && can_gc(tombstone(marker.timestamp(), marker.expiry())));
    }
    bool is_cell_purgeable(const atomic_cell_view& cell) {
        return (cell.has_expired(_query_time) || !cell.is_live()) &&
                cell.deletion_time() < _gc_before &&
                can_gc(tombstone(cell.timestamp(), cell.deletion_time()));
    }
    void examine_cell(const column_definition& cdef, const atomic_cell_or_collection& cell_or_collection, const row_tombstone& tomb) {
        if (cdef.type->is_atomic()) {
            auto cell = cell_or_collection.as_atomic_cell(cdef);
            if constexpr (OnlyPurged) {
                BOOST_REQUIRE(!cell.is_covered_by(tomb.tomb(), cdef.is_counter()));
            }
            BOOST_REQUIRE_EQUAL(is_cell_purgeable(cell), OnlyPurged);
        } else if (cdef.type->is_collection() || cdef.type->is_user_type()) {
            auto cell = cell_or_collection.as_collection_mutation();
            cell.with_deserialized(*cdef.type, [&] (collection_mutation_view_description m_view) {
                BOOST_REQUIRE(m_view.tomb.timestamp == api::missing_timestamp || m_view.tomb.timestamp > tomb.tomb().timestamp ||
                        is_tombstone_purgeable(m_view.tomb) == OnlyPurged);
                auto t = m_view.tomb;
                t.apply(tomb.tomb());
                for (const auto& [key, cell] : m_view.cells) {
                    if constexpr (OnlyPurged) {
                        BOOST_REQUIRE(!cell.is_covered_by(t, false));
                    }
                    BOOST_REQUIRE_EQUAL(is_cell_purgeable(cell), OnlyPurged);
                }
            });
        } else {
            throw std::runtime_error(fmt::format("Cannot check cell {} of unknown type {}", cdef.name_as_text(), cdef.type->name()));
        }
    }
    void examine_row(column_kind kind, const row& r, const row_tombstone& tomb) {
        r.for_each_cell([&, this, kind] (column_id id, const atomic_cell_or_collection& cell) {
            examine_cell(_schema.column_at(kind, id), cell, tomb);
        });
    }

public:
    basic_compacted_fragments_consumer_base(const schema& schema, gc_clock::time_point query_time,
            std::function<api::timestamp_type(const dht::decorated_key&)> get_max_purgeable)
        : _schema(schema)
        , _query_time(query_time)
        , _gc_before(saturating_subtract(query_time, _schema.gc_grace_seconds()))
        , _get_max_purgeable(std::move(get_max_purgeable)) {
    }
    void consume_new_partition(const dht::decorated_key& dk) {
        _max_purgeable = _get_max_purgeable(dk);
        BOOST_REQUIRE(!_mutation);
        _mutation.emplace(_schema.shared_from_this(), dk);
    }
    void consume(tombstone t) {
        BOOST_REQUIRE(t);
        BOOST_REQUIRE_EQUAL(is_tombstone_purgeable(t), OnlyPurged);

        BOOST_REQUIRE(_mutation);
        _mutation->partition().apply(t);
    }
    stop_iteration consume(static_row&& sr, tombstone tomb, bool is_live) {
        BOOST_REQUIRE(!OnlyPurged || !is_live);

        examine_row(column_kind::static_column, sr.cells(), row_tombstone(tomb));

        BOOST_REQUIRE(_mutation);
        _mutation->partition().static_row().apply(_schema, column_kind::static_column, std::move(sr.cells()));

        return stop_iteration::no;
    }
    stop_iteration consume(clustering_row&& cr, row_tombstone tomb, bool is_live) {
        BOOST_REQUIRE(!OnlyPurged || !is_live);

        if (!cr.marker().is_missing()) {
            BOOST_REQUIRE_EQUAL(is_marker_purgeable(cr.marker(), tomb.tomb()), OnlyPurged);
        }
        if (cr.tomb().regular()) {
            BOOST_REQUIRE_EQUAL(is_tombstone_purgeable(cr.tomb()), OnlyPurged);
        }
        examine_row(column_kind::regular_column, cr.cells(), tomb);

        BOOST_REQUIRE(_mutation);
        auto& dr = _mutation->partition().clustered_row(_schema, std::move(cr.key()));
        dr.apply(_schema, std::move(cr).as_deletable_row());

        return stop_iteration::no;
    }
    stop_iteration consume(range_tombstone&& rt) {
        BOOST_REQUIRE_EQUAL(is_tombstone_purgeable(rt.tomb), OnlyPurged);

        BOOST_REQUIRE(_mutation);
        _mutation->partition().apply_row_tombstone(_schema, std::move(rt));

        return stop_iteration::no;
    }
    stop_iteration consume_end_of_partition() {
        BOOST_REQUIRE(_mutation);
        _mutations.emplace_back(std::move(*_mutation));
        _mutation.reset();

        return stop_iteration::no;
    }
    std::vector<mutation> consume_end_of_stream() {
        BOOST_REQUIRE(!_mutation);

        return _mutations;
    }
};

using survived_compacted_fragments_consumer = basic_compacted_fragments_consumer_base<false>;
using purged_compacted_fragments_consumer = basic_compacted_fragments_consumer_base<true>;

void run_compaction_data_stream_split_test(const schema& schema, reader_permit permit, gc_clock::time_point query_time,
        std::vector<mutation> mutations) {
    auto never_gc = std::function<bool(tombstone)>([] (tombstone) { return false; });
    for (auto& mut : mutations) {
        mut.partition().compact_for_compaction(schema, never_gc, mut.decorated_key(), query_time);
    }

    auto reader = make_flat_mutation_reader_from_mutations_v2(schema.shared_from_this(), std::move(permit), mutations);
    auto close_reader = deferred_close(reader);
    auto get_max_purgeable = [] (const dht::decorated_key&) {
        return api::max_timestamp;
    };
    auto gc_grace_seconds = schema.gc_grace_seconds();
    auto consumer = compact_for_compaction<survived_compacted_fragments_consumer, purged_compacted_fragments_consumer>(
            schema,
            query_time,
            get_max_purgeable,
            survived_compacted_fragments_consumer(schema, query_time, get_max_purgeable),
            purged_compacted_fragments_consumer(schema, query_time, get_max_purgeable));

    auto [survived_muts, purged_muts] = reader.consume(std::move(consumer)).get0();

    auto survived_muts_it = survived_muts.begin();
    const auto survived_muts_end = survived_muts.end();
    auto purged_muts_it = purged_muts.begin();
    const auto purged_muts_end = purged_muts.end();
    for (const auto& expected_mut : mutations) {
        const auto& dkey = expected_mut.decorated_key();
        auto actual_mut = mutation(schema.shared_from_this(), dkey);
        if (survived_muts_it != survived_muts_end && survived_muts_it->decorated_key().equal(schema, dkey)) {
            actual_mut.apply(*survived_muts_it++);
        }
        if (purged_muts_it != purged_muts_end && purged_muts_it->decorated_key().equal(schema, dkey)) {
            actual_mut.apply(*purged_muts_it++);
        }
        BOOST_REQUIRE_EQUAL(actual_mut, expected_mut);
    }
    BOOST_REQUIRE(survived_muts_it == survived_muts_end);
    BOOST_REQUIRE(purged_muts_it == purged_muts_end);
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_compaction_data_stream_split) {
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto spec = tests::make_random_schema_specification(get_name());

    tests::random_schema random_schema(tests::random::get_int<uint32_t>(), *spec);
    const auto& schema = *random_schema.schema();

    testlog.info("Random schema:\n{}", random_schema.cql());

    const auto query_time = gc_clock::now();
    const auto ttl = gc_clock::duration{schema.gc_grace_seconds().count() * 4};
    const std::uniform_int_distribution<size_t> partition_count_dist = std::uniform_int_distribution<size_t>(16, 128);
    const std::uniform_int_distribution<size_t> clustering_row_count_dist = std::uniform_int_distribution<size_t>(2, 32);

    // Random data
    {
        testlog.info("Random data");
        const auto ts_gen = tests::default_timestamp_generator();
        // Half of the tombstones are gcable.
        // Half of the cells are expiring. Half of those is expired.
        const auto exp_gen = [query_time, ttl, schema] (std::mt19937& engine, tests::timestamp_destination destination)
                -> std::optional<tests::expiry_info> {
            const auto is_tombstone = (destination == tests::timestamp_destination::partition_tombstone ||
                    destination == tests::timestamp_destination::row_tombstone ||
                    destination == tests::timestamp_destination::range_tombstone ||
                    destination == tests::timestamp_destination::collection_tombstone);
            if (!is_tombstone && tests::random::get_bool(engine)) {
                return std::nullopt;
            }
            const auto offset = (is_tombstone ? schema.gc_grace_seconds().count() : ttl.count()) / 2;
            auto offset_dist = std::uniform_int_distribution<gc_clock::duration::rep>(-offset, offset);
            return tests::expiry_info{ttl, query_time + gc_clock::duration{offset_dist(engine)}};
        };
        const auto mutations = tests::generate_random_mutations(random_schema, ts_gen, exp_gen, partition_count_dist,
                clustering_row_count_dist).get0();
        run_compaction_data_stream_split_test(schema, semaphore.make_permit(), query_time, mutations);
    }

    // All data is purged
    {
        testlog.info("All data is purged");
        const auto ts_gen = [] (std::mt19937& engine, tests::timestamp_destination destination, api::timestamp_type min_timestamp) {
            static const api::timestamp_type tomb_ts_min = 10000;
            static const api::timestamp_type tomb_ts_max = 99999;
            static const api::timestamp_type collection_tomb_ts_min = 100;
            static const api::timestamp_type collection_tomb_ts_max = 999;
            static const api::timestamp_type other_ts_min = 1000;
            static const api::timestamp_type other_ts_max = 9999;

            if (destination == tests::timestamp_destination::partition_tombstone ||
                    destination == tests::timestamp_destination::row_tombstone ||
                    destination == tests::timestamp_destination::range_tombstone) {
                assert(min_timestamp < tomb_ts_max);
                return tests::random::get_int<api::timestamp_type>(tomb_ts_min, tomb_ts_max, engine);
            } else if (destination == tests::timestamp_destination::collection_tombstone) {
                assert(min_timestamp < collection_tomb_ts_max);
                return tests::random::get_int<api::timestamp_type>(collection_tomb_ts_min, collection_tomb_ts_max, engine);
            } else {
                assert(min_timestamp < other_ts_max);
                return tests::random::get_int<api::timestamp_type>(other_ts_min, other_ts_max, engine);
            }
        };
        const auto all_purged_exp_gen = [query_time, ttl, schema] (std::mt19937& engine, tests::timestamp_destination destination)
                -> std::optional<tests::expiry_info> {
            const auto offset = std::max(ttl.count(), schema.gc_grace_seconds().count());
            auto offset_dist = std::uniform_int_distribution<gc_clock::duration::rep>(-offset * 2, -offset);
            return tests::expiry_info{ttl, query_time + gc_clock::duration{offset_dist(engine)}};
        };
        const auto mutations = tests::generate_random_mutations(random_schema, ts_gen, all_purged_exp_gen, partition_count_dist,
                clustering_row_count_dist).get0();
        run_compaction_data_stream_split_test(schema, semaphore.make_permit(), query_time, mutations);
    }

    // No data is purged
    {
        testlog.info("No data is purged");
        const auto ts_gen = [] (std::mt19937& engine, tests::timestamp_destination destination, api::timestamp_type min_timestamp) {
            static const api::timestamp_type tomb_ts_min = 100;
            static const api::timestamp_type tomb_ts_max = 999;
            static const api::timestamp_type collection_tomb_ts_min = 1000;
            static const api::timestamp_type collection_tomb_ts_max = 9999;
            static const api::timestamp_type other_ts_min = 10000;
            static const api::timestamp_type other_ts_max = 99999;

            if (destination == tests::timestamp_destination::partition_tombstone ||
                    destination == tests::timestamp_destination::row_tombstone ||
                    destination == tests::timestamp_destination::range_tombstone) {
                assert(min_timestamp < tomb_ts_max);
                return tests::random::get_int<api::timestamp_type>(tomb_ts_min, tomb_ts_max, engine);
            } else if (destination == tests::timestamp_destination::collection_tombstone) {
                assert(min_timestamp < tomb_ts_max);
                return tests::random::get_int<api::timestamp_type>(collection_tomb_ts_min, collection_tomb_ts_max, engine);
            } else {
                assert(min_timestamp < other_ts_max);
                return tests::random::get_int<api::timestamp_type>(other_ts_min, other_ts_max, engine);
            }
        };
        const auto mutations = tests::generate_random_mutations(random_schema, ts_gen, tests::no_expiry_expiry_generator(), partition_count_dist,
                clustering_row_count_dist).get0();
        run_compaction_data_stream_split_test(schema, semaphore.make_permit(), query_time, mutations);
    }
}

// Reproducer for #4567: "appending_hash<row> ignores cells after first null"
SEASTAR_THREAD_TEST_CASE(test_appending_hash_row_4567) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("ck", bytes_type, column_kind::clustering_key)
        .with_column("r1", bytes_type)
        .with_column("r2", bytes_type)
        .with_column("r3", bytes_type)
        .build();

    auto r1 = row();
    r1.append_cell(0, atomic_cell::make_live(*bytes_type, 1, bytes{}));
    r1.append_cell(2, atomic_cell::make_live(*bytes_type, 1, to_bytes("aaa")));

    auto r2 = row();
    r2.append_cell(0, atomic_cell::make_live(*bytes_type, 1, bytes{}));
    r2.append_cell(2, atomic_cell::make_live(*bytes_type, 1, to_bytes("bbb")));

    auto r3 = row();
    r3.append_cell(0, atomic_cell::make_live(*bytes_type, 1, bytes{}));
    r3.append_cell(1, atomic_cell::make_live(*bytes_type, 1, to_bytes("bbb")));

    BOOST_CHECK(!r1.equal(column_kind::regular_column, *s, r2, *s));

    auto compute_legacy_hash = [&] (const row& r, const query::column_id_vector& columns) {
        auto hasher = legacy_xx_hasher_without_null_digest{};
        max_timestamp ts;
        appending_hash<row>{}(hasher, r, *s, column_kind::regular_column, columns, ts);
        return hasher.finalize_uint64();
    };
    auto compute_hash = [&] (const row& r, const query::column_id_vector& columns) {
        auto hasher = xx_hasher{};
        max_timestamp ts;
        appending_hash<row>{}(hasher, r, *s, column_kind::regular_column, columns, ts);
        return hasher.finalize_uint64();
    };

    BOOST_CHECK_EQUAL(compute_hash(r1, { 1 }), compute_hash(r2, { 1 }));
    BOOST_CHECK_EQUAL(compute_hash(r1, { 0, 1 }), compute_hash(r2, { 0, 1 }));
    BOOST_CHECK_NE(compute_hash(r1, { 0, 2 }), compute_hash(r2, { 0, 2 }));
    BOOST_CHECK_NE(compute_hash(r1, { 0, 1, 2 }), compute_hash(r2, { 0, 1, 2 }));
    // Additional test for making sure that {"", NULL, "bbb"} is not equal to {"", "bbb", NULL}
    // due to ignoring NULL in a hash
    BOOST_CHECK_NE(compute_hash(r2, { 0, 1, 2 }), compute_hash(r3, { 0, 1, 2 }));
    // Legacy check which shows incorrect handling of NULL values.
    // These checks are meaningful because legacy hashing is still used for old nodes.
    BOOST_CHECK_EQUAL(compute_legacy_hash(r1, { 0, 1, 2 }), compute_legacy_hash(r2, { 0, 1, 2 }));
}

SEASTAR_THREAD_TEST_CASE(test_mutation_consume) {
    std::mt19937 engine(tests::random::get_int<uint32_t>());

    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    auto rnd_schema_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(1, 8));
    auto rnd_schema = tests::random_schema(engine(), *rnd_schema_spec);

    auto forward_schema = rnd_schema.schema();
    auto reverse_schema = forward_schema->make_reversed();

    const auto muts = tests::generate_random_mutations(
            rnd_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(1, 10)).get();


    testlog.info("Forward");
    {
        for (const auto& mut : muts) {
            mutation_rebuilder_v2 rebuilder(forward_schema);
            auto rebuilt_mut = *mutation(mut).consume(rebuilder, consume_in_reverse::no).result;
            assert_that(std::move(rebuilt_mut)).is_equal_to(mut);
        }
    }
    testlog.info("Legacy half reverse");
    {
        for (const auto& mut : muts) {
            mutation_rebuilder_v2 rebuilder(forward_schema);
            auto rebuilt_mut = *mutation(mut).consume(rebuilder, consume_in_reverse::legacy_half_reverse).result;
            assert_that(std::move(rebuilt_mut)).is_equal_to(mut);
        }
    }
    testlog.info("Reverse");
    {
        for (const auto& mut : muts) {
            mutation_rebuilder_v2 rebuilder(reverse_schema);
            auto rebuilt_mut = *mutation(mut).consume(rebuilder, consume_in_reverse::yes).result;
            assert_that(reverse(std::move(rebuilt_mut))).is_equal_to(mut);
        }
    }
}

SEASTAR_THREAD_TEST_CASE(test_mutation_consume_position_monotonicity) {
    std::mt19937 engine(tests::random::get_int<uint32_t>());

    tests::reader_concurrency_semaphore_wrapper semaphore;
    auto permit = semaphore.make_permit();

    auto rnd_schema_spec = tests::make_random_schema_specification(
            get_name(),
            std::uniform_int_distribution<size_t>(1, 2),
            std::uniform_int_distribution<size_t>(1, 8));
    auto rnd_schema = tests::random_schema(engine(), *rnd_schema_spec);

    auto forward_schema = rnd_schema.schema();
    auto reverse_schema = forward_schema->make_reversed();

    const auto muts = tests::generate_random_mutations(
            rnd_schema,
            tests::default_timestamp_generator(),
            tests::no_expiry_expiry_generator(),
            std::uniform_int_distribution<size_t>(1, 1)).get();

    class validating_consumer {
        mutation_fragment_stream_validator _validator;

    public:
        explicit validating_consumer(const schema& s) : _validator(s) { }

        void consume_new_partition(const dht::decorated_key&) {
            BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::partition_start, position_in_partition_view(position_in_partition_view::partition_start_tag_t{})));
        }
        void consume(tombstone) { }
        stop_iteration consume(static_row&& sr) {
            BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::static_row, sr.position()));
            return stop_iteration::no;
        }
        stop_iteration consume(clustering_row&& cr) {
            BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::clustering_row, cr.position()));
            return stop_iteration::no;
        }
        stop_iteration consume(range_tombstone_change&& rtc) {
            BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::range_tombstone_change, rtc.position()));
            return stop_iteration::no;
        }
        stop_iteration consume_end_of_partition() {
            BOOST_REQUIRE(_validator(mutation_fragment_v2::kind::partition_end, position_in_partition_view(position_in_partition_view::end_of_partition_tag_t{})));
            return stop_iteration::no;
        }
        void consume_end_of_stream() {
            BOOST_REQUIRE(_validator.on_end_of_stream());
        }
    };

    BOOST_TEST_MESSAGE("Forward");
    {
        auto mut = muts.front();
        validating_consumer consumer(*forward_schema);
        std::move(mut).consume(consumer, consume_in_reverse::no);
    }
    BOOST_TEST_MESSAGE("Reverse");
    {
        auto mut = muts.front();
        validating_consumer consumer(*reverse_schema);
        std::move(mut).consume(consumer, consume_in_reverse::yes);
    }
}

SEASTAR_THREAD_TEST_CASE(test_position_in_partition_reversal) {
    using p_i_p = position_in_partition;

    simple_schema ss;
    auto pk = ss.make_pkey();

    position_in_partition::tri_compare fwd_cmp(*ss.schema());
    auto rev_s = ss.schema()->make_reversed();
    position_in_partition::tri_compare rev_cmp(*rev_s);

    BOOST_REQUIRE(fwd_cmp(p_i_p::before_key(ss.make_ckey(1)), p_i_p::after_key(ss.make_ckey(1))) < 0);
    BOOST_REQUIRE(fwd_cmp(p_i_p::before_key(ss.make_ckey(1)), ss.make_ckey(0)) > 0);
    BOOST_REQUIRE(fwd_cmp(p_i_p::after_key(ss.make_ckey(1)), p_i_p::before_key(ss.make_ckey(1))) > 0);
    BOOST_REQUIRE(fwd_cmp(p_i_p::after_key(ss.make_ckey(1)), ss.make_ckey(2)) < 0);

    BOOST_REQUIRE(rev_cmp(p_i_p::before_key(ss.make_ckey(1)).reversed(), p_i_p::after_key(ss.make_ckey(1)).reversed()) > 0);
    BOOST_REQUIRE(rev_cmp(p_i_p::before_key(ss.make_ckey(1)).reversed(), ss.make_ckey(0)) < 0);
    BOOST_REQUIRE(rev_cmp(p_i_p::after_key(ss.make_ckey(1)).reversed(), p_i_p::before_key(ss.make_ckey(1)).reversed()) < 0);
    BOOST_REQUIRE(rev_cmp(p_i_p::after_key(ss.make_ckey(1)).reversed(), ss.make_ckey(2)) > 0);

    // Test reversal-invariant positions

    BOOST_REQUIRE(rev_cmp(p_i_p::for_partition_start().reversed(),
                          p_i_p::for_partition_start()) == 0);

    BOOST_REQUIRE(rev_cmp(p_i_p(p_i_p::end_of_partition_tag_t()).reversed(),
                          p_i_p(p_i_p::end_of_partition_tag_t())) == 0);

    BOOST_REQUIRE(rev_cmp(p_i_p::for_static_row().reversed(),
                          p_i_p::for_static_row()) == 0);
}

SEASTAR_THREAD_TEST_CASE(test_compactor_range_tombstone_spanning_many_pages) {
    simple_schema ss;
    auto pk = ss.make_pkey();
    auto s = ss.schema();

    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto permit = semaphore.make_permit();

    const auto marker_ts = ss.new_timestamp();
    const auto tomb_ts = ss.new_timestamp();
    const auto row_ts = ss.new_timestamp();

    auto make_frags = [&] {
        std::deque<mutation_fragment_v2> frags;

        frags.emplace_back(*s, permit, partition_start(pk, {}));

        const auto& v_def = *s->get_column_definition(to_bytes("v"));

        frags.emplace_back(*s, permit, range_tombstone_change(position_in_partition::before_key(ss.make_ckey(0)), tombstone{tomb_ts, {}}));

        for (uint32_t ck = 0; ck < 10; ++ck) {
            auto row = clustering_row(ss.make_ckey(ck));
            row.cells().apply(v_def, atomic_cell::make_live(*v_def.type, row_ts, serialized("v")));
            row.marker() = row_marker(marker_ts);
            frags.emplace_back(mutation_fragment_v2(*s, permit, std::move(row)));
        }

        frags.emplace_back(*s, permit, range_tombstone_change(position_in_partition::after_key(ss.make_ckey(10)), tombstone{}));

        frags.emplace_back(*s, permit, partition_end{});

        return frags;
    };

    auto restore_state = [&] (flat_mutation_reader_v2& reader, detached_compaction_state&& state) {
        if (auto rt_opt = state.current_tombstone) {
            reader.unpop_mutation_fragment(mutation_fragment_v2(*s, permit, std::move(*rt_opt)));
        }
        if (state.static_row) {
            reader.unpop_mutation_fragment(mutation_fragment_v2(*s, permit, std::move(*state.static_row)));
        }
        reader.unpop_mutation_fragment(mutation_fragment_v2(*s, permit, std::move(state.partition_start)));
    };

    const auto query_time = gc_clock::now();
    const auto max_rows = std::numeric_limits<uint64_t>::max();
    const auto max_partitions = std::numeric_limits<uint32_t>::max();

    mutation ref_mut(s, pk);
    {
        auto reader = make_flat_mutation_reader_from_fragments(s, permit, make_frags());
        auto close_reader = deferred_close(reader);
        auto mut_opt = reader.consume(mutation_rebuilder_v2(s)).get();
        BOOST_REQUIRE(mut_opt);
        ref_mut = std::move(*mut_opt);
        ref_mut.partition().compact_for_query(*s, pk, query_time, {query::clustering_range::make_open_ended_both_sides()}, true, false, max_rows);
    }

    struct consumer {
        reader_permit permit;
        mutation& mut;
        const uint64_t row_limit;
        uint64_t rows = 0;

        void consume_new_partition(const dht::decorated_key& dk) {
            BOOST_REQUIRE(mut.decorated_key().equal(*mut.schema(), dk));
        }
        void consume(const tombstone& t) {
            BOOST_REQUIRE_EQUAL(t, mut.partition().partition_tombstone());
        }
        stop_iteration consume(static_row&& sr, tombstone, bool) {
            mut.apply(mutation_fragment(*mut.schema(), permit, std::move(sr)));
            return stop_iteration(++rows >= row_limit);
        }
        stop_iteration consume(clustering_row&& cr, row_tombstone t, bool is_alive) {
            mut.apply(mutation_fragment(*mut.schema(), permit, std::move(cr)));
            return stop_iteration(++rows >= row_limit);
        }
        stop_iteration consume(range_tombstone&& rt) {
            mut.apply(mutation_fragment(*mut.schema(), permit, std::move(rt)));
            return stop_iteration(++rows >= row_limit);
        }
        stop_iteration consume_end_of_partition() {
            return stop_iteration::yes;
        }
        void consume_end_of_stream() {
        }
    };

    testlog.info("non-paged");
    {
        mutation res_mut(s, pk);
        auto c = compact_for_query<emit_only_live_rows::no, consumer>(*s, query_time, s->full_slice(), max_rows, max_partitions, consumer{permit, res_mut, max_rows});
        auto reader = make_flat_mutation_reader_from_fragments(s, permit, make_frags());
        auto close_reader = deferred_close(reader);

        reader.consume(std::move(c)).get();

        BOOST_REQUIRE_EQUAL(res_mut, ref_mut);
    }

    testlog.info("limited pages");
    {
        mutation res_mut(s, pk);
        auto compaction_state = make_lw_shared<compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::no>>(*s, query_time, s->full_slice(), 1, max_partitions);
        auto reader = make_flat_mutation_reader_from_fragments(s, permit, make_frags());
        auto close_reader = deferred_close(reader);

        while (!reader.is_buffer_empty() || !reader.is_end_of_stream()) {
            auto c = consumer{permit, res_mut, max_rows};
            compaction_state->start_new_page(1, max_partitions, query_time, reader.peek().get()->position().region(), c);
            reader.consume(compact_for_query<emit_only_live_rows::no, consumer>(compaction_state, std::move(c))).get();
        }

        BOOST_REQUIRE_EQUAL(res_mut, ref_mut);
    }

    testlog.info("short pages");
    {
        mutation res_mut(s, pk);
        auto compaction_state = make_lw_shared<compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::no>>(*s, query_time, s->full_slice(), max_rows, max_partitions);
        auto reader = make_flat_mutation_reader_from_fragments(s, permit, make_frags());
        auto close_reader = deferred_close(reader);

        while (!reader.is_buffer_empty() || !reader.is_end_of_stream()) {
            auto c = consumer{permit, res_mut, 2};
            compaction_state->start_new_page(max_rows, max_partitions, query_time, reader.peek().get()->position().region(), c);
            reader.consume(compact_for_query<emit_only_live_rows::no, consumer>(compaction_state, std::move(c))).get();
        }

        BOOST_REQUIRE_EQUAL(res_mut, ref_mut);
    }

    testlog.info("limited pages - detach state");
    {
        mutation res_mut(s, pk);
        auto reader = make_flat_mutation_reader_from_fragments(s, permit, make_frags());
        auto close_reader = deferred_close(reader);

        std::optional<detached_compaction_state> detached_state;

        while (!reader.is_buffer_empty() || !reader.is_end_of_stream()) {
            if (detached_state) {
                restore_state(reader, std::move(*detached_state));
            }
            auto compaction_state = make_lw_shared<compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::no>>(*s, query_time, s->full_slice(), 1, max_partitions);
            auto c = consumer{permit, res_mut, max_rows};
            reader.consume(compact_for_query<emit_only_live_rows::no, consumer>(compaction_state, std::move(c))).get();
            detached_state = std::move(*compaction_state).detach_state();
        }

        BOOST_REQUIRE_EQUAL(res_mut, ref_mut);
    }

    testlog.info("short pages - detach state");
    {
        mutation res_mut(s, pk);
        auto reader = make_flat_mutation_reader_from_fragments(s, permit, make_frags());
        auto close_reader = deferred_close(reader);

        std::optional<detached_compaction_state> detached_state;

        while (!reader.is_buffer_empty() || !reader.is_end_of_stream()) {
            if (detached_state) {
                restore_state(reader, std::move(*detached_state));
            }
            auto compaction_state = make_lw_shared<compact_mutation_state<emit_only_live_rows::no, compact_for_sstables::no>>(*s, query_time, s->full_slice(), max_rows, max_partitions);
            auto c = consumer{permit, res_mut, 2};
            reader.consume(compact_for_query<emit_only_live_rows::no, consumer>(compaction_state, std::move(c))).get();
            detached_state = std::move(*compaction_state).detach_state();
        }

        BOOST_REQUIRE_EQUAL(res_mut, ref_mut);
    }
}
