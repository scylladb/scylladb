/*
 * Copyright 2015 Cloudius Systems
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#define BOOST_TEST_DYN_LINK

#include <random>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include "md5_hasher.hh"

#include "core/sstring.hh"
#include "core/do_with.hh"
#include "core/thread.hh"

#include "database.hh"
#include "utils/UUID_gen.hh"
#include "mutation_reader.hh"
#include "schema_builder.hh"
#include "query-result-set.hh"
#include "query-result-reader.hh"
#include "partition_slice_builder.hh"
#include "tmpdir.hh"

#include "tests/test-utils.hh"
#include "tests/mutation_assertions.hh"
#include "tests/mutation_reader_assertions.hh"
#include "tests/result_set_assertions.hh"
#include "mutation_source_test.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

using namespace std::chrono_literals;

static sstring some_keyspace("ks");
static sstring some_column_family("cf");

static atomic_cell make_atomic_cell(bytes value) {
    return atomic_cell::make_live(0, std::move(value));
};

static mutation_partition get_partition(memtable& mt, const partition_key& key) {
    auto dk = dht::global_partitioner().decorate_key(*mt.schema(), key);
    auto reader = mt.make_reader(mt.schema(), query::partition_range::make_singular(dk));
    auto mo = reader().get0();
    BOOST_REQUIRE(bool(mo));
    return std::move(mo->partition());
}

template <typename Func>
future<>
with_column_family(schema_ptr s, column_family::config cfg, Func func) {
    auto cm = make_lw_shared<compaction_manager>();
    auto cf = make_lw_shared<column_family>(s, cfg, column_family::no_commitlog(), *cm);
    cf->mark_ready_for_writes();
    return func(*cf).then([cf, cm] {
        return cf->stop();
    }).finally([cf, cm] {});
}

SEASTAR_TEST_CASE(test_mutation_is_applied) {
    return seastar::async([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

        memtable mt(s);

        const column_definition& r1_col = *s->get_column_definition("r1");
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(2)});

        mutation m(key, s);
        m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(3)));
        mt.apply(std::move(m));

        auto p = get_partition(mt, key);
        row& r = p.clustered_row(c_key).cells();
        auto i = r.find_cell(r1_col.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_atomic_cell();
        BOOST_REQUIRE(cell.is_live());
        BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(3)));
    });
}

SEASTAR_TEST_CASE(test_multi_level_row_tombstones) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}},
        {{"c1", int32_type}, {"c2", int32_type}, {"c3", int32_type}},
        {{"r1", int32_type}}, {}, utf8_type));

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(partition_key::from_exploded(*s, {to_bytes("key1")}), s);

    auto make_prefix = [s] (const std::vector<data_value>& v) {
        return clustering_key_prefix::from_deeply_exploded(*s, v);
    };
    auto make_key = [s] (const std::vector<data_value>& v) {
        return clustering_key::from_deeply_exploded(*s, v);
    };

    m.partition().apply_row_tombstone(*s, make_prefix({1, 2}), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 3})), tombstone(9, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 3}), tombstone(8, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(9, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(8, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1}), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(11, ttl));

    m.partition().apply_row_tombstone(*s, make_prefix({1, 4}), tombstone(6, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 2, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 3, 0})), tombstone(11, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, make_key({1, 4, 0})), tombstone(11, ttl));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_row_tombstone_updates) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}, {"c2", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    memtable mt(s);

    auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
    auto c_key1 = clustering_key::from_deeply_exploded(*s, {1, 0});
    auto c_key1_prefix = clustering_key_prefix::from_deeply_exploded(*s, {1});
    auto c_key2 = clustering_key::from_deeply_exploded(*s, {2, 0});
    auto c_key2_prefix = clustering_key_prefix::from_deeply_exploded(*s, {2});

    auto ttl = gc_clock::now() + std::chrono::seconds(1);

    mutation m(key, s);
    m.partition().apply_row_tombstone(*s, c_key1_prefix, tombstone(1, ttl));
    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(0, ttl));

    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key1), tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), tombstone(0, ttl));

    m.partition().apply_row_tombstone(*s, c_key2_prefix, tombstone(1, ttl));
    BOOST_REQUIRE_EQUAL(m.partition().tombstone_for_row(*s, c_key2), tombstone(1, ttl));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_map_mutations) {
    return seastar::async([] {
        auto my_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_map_type}}, utf8_type));
        memtable mt(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto& column = *s->get_column_definition("s1");
        map_type_impl::mutation mmut1{{}, {{int32_type->decompose(101), make_atomic_cell(utf8_type->decompose(sstring("101")))}}};
        mutation m1(key, s);
        m1.set_static_cell(column, my_map_type->serialize_mutation_form(mmut1));
        mt.apply(m1);
        map_type_impl::mutation mmut2{{}, {{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102")))}}};
        mutation m2(key, s);
        m2.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2));
        mt.apply(m2);
        map_type_impl::mutation mmut3{{}, {{int32_type->decompose(103), make_atomic_cell(utf8_type->decompose(sstring("103")))}}};
        mutation m3(key, s);
        m3.set_static_cell(column, my_map_type->serialize_mutation_form(mmut3));
        mt.apply(m3);
        map_type_impl::mutation mmut2o{{}, {{int32_type->decompose(102), make_atomic_cell(utf8_type->decompose(sstring("102 override")))}}};
        mutation m2o(key, s);
        m2o.set_static_cell(column, my_map_type->serialize_mutation_form(mmut2o));
        mt.apply(m2o);

        auto p = get_partition(mt, key);
        row& r = p.static_row();
        auto i = r.find_cell(column.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_collection_mutation();
        auto muts = my_map_type->deserialize_mutation_form(cell);
        BOOST_REQUIRE(muts.cells.size() == 3);
        // FIXME: more strict tests
    });
}

SEASTAR_TEST_CASE(test_set_mutations) {
    return seastar::async([] {
        auto my_set_type = set_type_impl::get_instance(int32_type, true);
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_set_type}}, utf8_type));
        memtable mt(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto& column = *s->get_column_definition("s1");
        map_type_impl::mutation mmut1{{}, {{int32_type->decompose(101), make_atomic_cell({})}}};
        mutation m1(key, s);
        m1.set_static_cell(column, my_set_type->serialize_mutation_form(mmut1));
        mt.apply(m1);
        map_type_impl::mutation mmut2{{}, {{int32_type->decompose(102), make_atomic_cell({})}}};
        mutation m2(key, s);
        m2.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2));
        mt.apply(m2);
        map_type_impl::mutation mmut3{{}, {{int32_type->decompose(103), make_atomic_cell({})}}};
        mutation m3(key, s);
        m3.set_static_cell(column, my_set_type->serialize_mutation_form(mmut3));
        mt.apply(m3);
        map_type_impl::mutation mmut2o{{}, {{int32_type->decompose(102), make_atomic_cell({})}}};
        mutation m2o(key, s);
        m2o.set_static_cell(column, my_set_type->serialize_mutation_form(mmut2o));
        mt.apply(m2o);

        auto p = get_partition(mt, key);
        row& r = p.static_row();
        auto i = r.find_cell(column.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_collection_mutation();
        auto muts = my_set_type->deserialize_mutation_form(cell);
        BOOST_REQUIRE(muts.cells.size() == 3);
        // FIXME: more strict tests
    });
}

SEASTAR_TEST_CASE(test_list_mutations) {
    return seastar::async([] {
        auto my_list_type = list_type_impl::get_instance(int32_type, true);
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {{"c1", int32_type}}, {}, {{"s1", my_list_type}}, utf8_type));
        memtable mt(s);
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});
        auto& column = *s->get_column_definition("s1");
        auto make_key = [] { return timeuuid_type->decompose(utils::UUID_gen::get_time_UUID()); };
        collection_type_impl::mutation mmut1{{}, {{make_key(), make_atomic_cell(int32_type->decompose(101))}}};
        mutation m1(key, s);
        m1.set_static_cell(column, my_list_type->serialize_mutation_form(mmut1));
        mt.apply(m1);
        collection_type_impl::mutation mmut2{{}, {{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
        mutation m2(key, s);
        m2.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2));
        mt.apply(m2);
        collection_type_impl::mutation mmut3{{}, {{make_key(), make_atomic_cell(int32_type->decompose(103))}}};
        mutation m3(key, s);
        m3.set_static_cell(column, my_list_type->serialize_mutation_form(mmut3));
        mt.apply(m3);
        collection_type_impl::mutation mmut2o{{}, {{make_key(), make_atomic_cell(int32_type->decompose(102))}}};
        mutation m2o(key, s);
        m2o.set_static_cell(column, my_list_type->serialize_mutation_form(mmut2o));
        mt.apply(m2o);

        auto p = get_partition(mt, key);
        row& r = p.static_row();
        auto i = r.find_cell(column.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_collection_mutation();
        auto muts = my_list_type->deserialize_mutation_form(cell);
        BOOST_REQUIRE(muts.cells.size() == 4);
        // FIXME: more strict tests
    });
}

SEASTAR_TEST_CASE(test_multiple_memtables_one_partition) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
        {{"p1", utf8_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    auto cf_stats = make_lw_shared<::cf_stats>();
    column_family::config cfg;
    cfg.enable_disk_reads = false;
    cfg.enable_disk_writes = false;
    cfg.enable_incremental_backups = false;
    cfg.cf_stats = &*cf_stats;

    return with_column_family(s, cfg, [s] (column_family& cf) {
        const column_definition& r1_col = *s->get_column_definition("r1");
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});

        auto insert_row = [&] (int32_t c1, int32_t r1) {
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
            mutation m(key, s);
            m.set_clustered_cell(c_key, r1_col, make_atomic_cell(int32_type->decompose(r1)));
            cf.apply(std::move(m));
            return cf.flush();
        };
        return when_all(
                insert_row(1001, 2001),
                insert_row(1002, 2002),
                insert_row(1003, 2003)).discard_result().then([s, &r1_col, &cf, key] {
            auto verify_row = [&] (int32_t c1, int32_t r1) {
                auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
                return cf.find_row(cf.schema(), dht::global_partitioner().decorate_key(*s, key), std::move(c_key)).then([r1, r1_col] (auto r) {
                    BOOST_REQUIRE(r);
                    auto i = r->find_cell(r1_col.id);
                    BOOST_REQUIRE(i);
                    auto cell = i->as_atomic_cell();
                    BOOST_REQUIRE(cell.is_live());
                    BOOST_REQUIRE(int32_type->equal(cell.value(), int32_type->decompose(r1)));
                });
            };
            verify_row(1001, 2001);
            verify_row(1002, 2002);
            verify_row(1003, 2003);
        });
    }).then([cf_stats] {});
}

SEASTAR_TEST_CASE(test_flush_in_the_middle_of_a_scan) {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", bytes_type, column_kind::partition_key)
        .with_column("v", bytes_type)
        .build();

    auto dir = make_lw_shared<tmpdir>();
    auto cf_stats = make_lw_shared<::cf_stats>();

    column_family::config cfg;
    cfg.datadir = { dir->path };
    cfg.enable_disk_reads = true;
    cfg.enable_disk_writes = true;
    cfg.enable_cache = true;
    cfg.enable_incremental_backups = false;
    cfg.cf_stats = &*cf_stats;

    return with_column_family(s, cfg, [s](column_family& cf) {
        return seastar::async([s, &cf] {
            // populate
            auto new_key = [&] {
                static thread_local int next = 0;
                return dht::global_partitioner().decorate_key(*s,
                    partition_key::from_single_value(*s, to_bytes(sprint("key%d", next++))));
            };
            auto make_mutation = [&] {
                mutation m(new_key(), s);
                m.set_clustered_cell(clustering_key::make_empty(*s), "v", data_value(to_bytes("value")), 1);
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
            auto assert_that_scanner1 = assert_that(cf.make_reader(s, query::full_partition_range));

            // Flush will happen before it is invoked
            auto assert_that_scanner2 = assert_that(cf.make_reader(s, query::full_partition_range));

            // Flush will happen after all data was read, but before EOS was consumed
            auto assert_that_scanner3 = assert_that(cf.make_reader(s, query::full_partition_range));

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
    }).then([dir, cf_stats] {});
}

SEASTAR_TEST_CASE(test_multiple_memtables_multiple_partitions) {
    auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", int32_type}}, {{"c1", int32_type}}, {{"r1", int32_type}}, {}, utf8_type));

    auto cf_stats = make_lw_shared<::cf_stats>();

    column_family::config cfg;
    cfg.enable_disk_reads = false;
    cfg.enable_disk_writes = false;
    cfg.enable_incremental_backups = false;
    cfg.cf_stats = &*cf_stats;
    return with_column_family(s, cfg, [s] (auto& cf) mutable {
        std::map<int32_t, std::map<int32_t, int32_t>> shadow, result;

        const column_definition& r1_col = *s->get_column_definition("r1");

        api::timestamp_type ts = 0;
        auto insert_row = [&] (int32_t p1, int32_t c1, int32_t r1) {
            auto key = partition_key::from_exploded(*s, {int32_type->decompose(p1)});
            auto c_key = clustering_key::from_exploded(*s, {int32_type->decompose(c1)});
            mutation m(key, s);
            m.set_clustered_cell(c_key, r1_col, atomic_cell::make_live(ts++, int32_type->decompose(r1)));
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
            cf.flush();
        }

        return do_with(std::move(result), [&cf, s, &r1_col, shadow] (auto& result) {
            return cf.for_all_partitions_slow(s, [&, s] (const dht::decorated_key& pk, const mutation_partition& mp) {
                auto p1 = value_cast<int32_t>(int32_type->deserialize(pk._key.explode(*s)[0]));
                for (const rows_entry& re : mp.range(*s, query::range<clustering_key_prefix>())) {
                    auto c1 = value_cast<int32_t>(int32_type->deserialize(re.key().explode(*s)[0]));
                    auto cell = re.row().cells().find_cell(r1_col.id);
                    if (cell) {
                        result[p1][c1] = value_cast<int32_t>(int32_type->deserialize(cell->as_atomic_cell().value()));
                    }
                }
                return true;
            }).then([&result, shadow] (bool ok) {
                BOOST_REQUIRE(shadow == result);
            });
        });
    }).then([cf_stats] {});
}

SEASTAR_TEST_CASE(test_cell_ordering) {
    auto now = gc_clock::now();
    auto ttl_1 = gc_clock::duration(1);
    auto ttl_2 = gc_clock::duration(2);
    auto expiry_1 = now + ttl_1;
    auto expiry_2 = now + ttl_2;

    auto assert_order = [] (atomic_cell_view first, atomic_cell_view second) {
        if (compare_atomic_cell_for_merge(first, second) >= 0) {
            BOOST_FAIL(sprint("Expected %s < %s", first, second));
        }
        if (compare_atomic_cell_for_merge(second, first) <= 0) {
            BOOST_FAIL(sprint("Expected %s < %s", second, first));
        }
    };

    auto assert_equal = [] (atomic_cell_view c1, atomic_cell_view c2) {
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c1, c2) == 0);
        BOOST_REQUIRE(compare_atomic_cell_for_merge(c2, c1) == 0);
    };

    assert_equal(
        atomic_cell::make_live(0, bytes("value")),
        atomic_cell::make_live(0, bytes("value")));

    assert_equal(
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes("value")));

    assert_equal(
        atomic_cell::make_dead(1, expiry_1),
        atomic_cell::make_dead(1, expiry_1));

    // If one cell doesn't have an expiry, Origin considers them equal.
    assert_equal(
        atomic_cell::make_live(1, bytes(), expiry_2, ttl_2),
        atomic_cell::make_live(1, bytes()));

    // Origin doesn't compare ttl (is it wise?)
    assert_equal(
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes("value"), expiry_1, ttl_2));

    assert_order(
        atomic_cell::make_live(0, bytes("value1")),
        atomic_cell::make_live(0, bytes("value2")));

    assert_order(
        atomic_cell::make_live(0, bytes("value12")),
        atomic_cell::make_live(0, bytes("value2")));

    // Live cells are ordered first by timestamp...
    assert_order(
        atomic_cell::make_live(0, bytes("value2")),
        atomic_cell::make_live(1, bytes("value1")));

    // ..then by value
    assert_order(
        atomic_cell::make_live(1, bytes("value1"), expiry_2, ttl_2),
        atomic_cell::make_live(1, bytes("value2"), expiry_1, ttl_1));

    // ..then by expiry
    assert_order(
        atomic_cell::make_live(1, bytes(), expiry_1, ttl_1),
        atomic_cell::make_live(1, bytes(), expiry_2, ttl_1));

    // Dead wins
    assert_order(
        atomic_cell::make_live(1, bytes("value")),
        atomic_cell::make_dead(1, expiry_1));

    // Dead wins with expiring cell
    assert_order(
        atomic_cell::make_live(1, bytes("value"), expiry_2, ttl_2),
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
            return query::result_set::from_raw_result(s, slice, m.query(slice));
        };

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.set_clustered_cell(clustering_key::make_empty(*s), "v", data_value(bytes("v1")), 1);

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

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.partition().apply(tombstone(1, gc_clock::now()));
        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));
        m.set_clustered_cell(clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("A")))),
            *s->get_column_definition("v"), atomic_cell::make_dead(2, gc_clock::now()));

        auto slice = make_full_slice(*s);

        assert_that(query::result_set::from_raw_result(s, slice, m.query(slice)))
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

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("sc1:value")))));

        auto slice = partition_slice_builder(*s)
            .with_no_static_columns()
            .with_regular_column("v")
            .build();

        assert_that(query::result_set::from_raw_result(s, slice, m.query(slice)))
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

        mutation m(partition_key::from_single_value(*s, "key1"), s);
        m.set_clustered_cell(clustering_key::from_single_value(*s, bytes("ck:A")),
            *s->get_column_definition("v1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v1:value")))));

        auto slice = partition_slice_builder(*s).build();

        assert_that(query::result_set::from_raw_result(s, slice, m.query(slice)))
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

        mutation m(partition_key::from_single_value(*s, "key1"), s);

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        auto ckey1 = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("A"))));
        auto ckey2 = clustering_key::from_single_value(*s, bytes_type->decompose(data_value(bytes("B"))));

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v:value")))));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("sc1:value")))));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_dead(2, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().static_row().apply(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        m.partition().clustered_row(*s, ckey1).apply(api::timestamp_type(3));

        BOOST_REQUIRE_EQUAL(1, m.live_row_count());

        m.partition().apply(tombstone(3, gc_clock::now()));

        BOOST_REQUIRE_EQUAL(0, m.live_row_count());

        m.set_clustered_cell(ckey1, col_v, atomic_cell::make_live(4, bytes_type->decompose(data_value(bytes("v:value")))));
        m.set_clustered_cell(ckey2, col_v, atomic_cell::make_live(4, bytes_type->decompose(data_value(bytes("v:value")))));

        BOOST_REQUIRE_EQUAL(2, m.live_row_count());
    });
}

SEASTAR_TEST_CASE(test_mutation_diff) {
    return seastar::async([] {
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

        mutation m1(partition_key::from_single_value(*s, "key1"), s);
        m1.set_static_cell(*s->get_column_definition("sc1"),
            atomic_cell::make_dead(2, gc_clock::now()));

        m1.partition().apply(tombstone { 1, gc_clock::now() });
        m1.set_clustered_cell(ckey1, *s->get_column_definition("v1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v1:value1")))));
        m1.set_clustered_cell(ckey1, *s->get_column_definition("v2"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v2:value2")))));

        m1.partition().clustered_row(ckey2).apply(row_marker(3));
        m1.set_clustered_cell(ckey2, *s->get_column_definition("v2"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v2:value4")))));
        map_type_impl::mutation mset1 {{}, {{int32_type->decompose(1), make_atomic_cell({})}, {int32_type->decompose(2), make_atomic_cell({})}}};
        m1.set_clustered_cell(ckey2, *s->get_column_definition("v3"),
            my_set_type->serialize_mutation_form(mset1));

        mutation m2(partition_key::from_single_value(*s, "key1"), s);
        m2.set_clustered_cell(ckey1, *s->get_column_definition("v1"),
            atomic_cell::make_live(1, bytes_type->decompose(data_value(bytes("v1:value1a")))));
        m2.set_clustered_cell(ckey1, *s->get_column_definition("v2"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v2:value2")))));

        m2.set_clustered_cell(ckey2, *s->get_column_definition("v1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v1:value3")))));
        m2.set_clustered_cell(ckey2, *s->get_column_definition("v2"),
            atomic_cell::make_live(3, bytes_type->decompose(data_value(bytes("v2:value4a")))));
        map_type_impl::mutation mset2 {{}, {{int32_type->decompose(1), make_atomic_cell({})}, {int32_type->decompose(3), make_atomic_cell({})}}};
        m2.set_clustered_cell(ckey2, *s->get_column_definition("v3"),
            my_set_type->serialize_mutation_form(mset2));

        mutation m3(partition_key::from_single_value(*s, "key1"), s);
        m3.set_clustered_cell(ckey1, *s->get_column_definition("v1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v1:value1")))));

        m3.set_clustered_cell(ckey2, *s->get_column_definition("v1"),
            atomic_cell::make_live(2, bytes_type->decompose(data_value(bytes("v1:value3")))));
        m3.set_clustered_cell(ckey2, *s->get_column_definition("v2"),
            atomic_cell::make_live(3, bytes_type->decompose(data_value(bytes("v2:value4a")))));
        map_type_impl::mutation mset3 {{}, {{int32_type->decompose(1), make_atomic_cell({})}}};
        m3.set_clustered_cell(ckey2, *s->get_column_definition("v3"),
            my_set_type->serialize_mutation_form(mset3));

        mutation m12(partition_key::from_single_value(*s, "key1"), s);
        m12.apply(m1);
        m12.apply(m2);

        auto m2_1 = m2.partition().difference(s, m1.partition());
        BOOST_REQUIRE_EQUAL(m2_1.partition_tombstone(), tombstone());
        BOOST_REQUIRE(!m2_1.static_row().size());
        BOOST_REQUIRE(!m2_1.find_row(ckey1));
        BOOST_REQUIRE(m2_1.find_row(ckey2));
        BOOST_REQUIRE(m2_1.find_row(ckey2)->find_cell(2));
        auto cmv = m2_1.find_row(ckey2)->find_cell(2)->as_collection_mutation();
        auto cm = my_set_type->deserialize_mutation_form(cmv);
        BOOST_REQUIRE(cm.cells.size() == 1);
        BOOST_REQUIRE(cm.cells.front().first == int32_type->decompose(3));

        mutation m12_1(partition_key::from_single_value(*s, "key1"), s);
        m12_1.apply(m1);
        m12_1.partition().apply(*s, m2_1, *s);
        BOOST_REQUIRE_EQUAL(m12, m12_1);

        auto m1_2 = m1.partition().difference(s, m2.partition());
        BOOST_REQUIRE_EQUAL(m1_2.partition_tombstone(), m12.partition().partition_tombstone());
        BOOST_REQUIRE(m1_2.find_row(ckey1));
        BOOST_REQUIRE(m1_2.find_row(ckey2));
        BOOST_REQUIRE(!m1_2.find_row(ckey1)->find_cell(1));
        BOOST_REQUIRE(!m1_2.find_row(ckey2)->find_cell(0));
        BOOST_REQUIRE(!m1_2.find_row(ckey2)->find_cell(1));
        cmv = m1_2.find_row(ckey2)->find_cell(2)->as_collection_mutation();
        cm = my_set_type->deserialize_mutation_form(cmv);
        BOOST_REQUIRE(cm.cells.size() == 1);
        BOOST_REQUIRE(cm.cells.front().first == int32_type->decompose(2));

        mutation m12_2(partition_key::from_single_value(*s, "key1"), s);
        m12_2.apply(m2);
        m12_2.partition().apply(*s, m1_2, *s);
        BOOST_REQUIRE_EQUAL(m12, m12_2);

        auto m3_12 = m3.partition().difference(s, m12.partition());
        BOOST_REQUIRE(m3_12.empty());

        auto m12_3 = m12.partition().difference(s, m3.partition());
        BOOST_REQUIRE_EQUAL(m12_3.partition_tombstone(), m12.partition().partition_tombstone());

        mutation m123(partition_key::from_single_value(*s, "key1"), s);
        m123.apply(m3);
        m123.partition().apply(*s, m12_3, *s);
        BOOST_REQUIRE_EQUAL(m12, m123);
    });
}

SEASTAR_TEST_CASE(test_large_blobs) {
    return seastar::async([] {
        auto s = make_lw_shared(schema({}, some_keyspace, some_column_family,
            {{"p1", utf8_type}}, {}, {}, {{"s1", bytes_type}}, utf8_type));

        auto mt = make_lw_shared<memtable>(s);

        auto make_blob = [] (size_t blob_size) -> bytes {
            bytes big_blob(bytes::initialized_later(), blob_size);
            std::independent_bits_engine<std::default_random_engine, 8, uint8_t> random_bytes;
            for (auto&& b : big_blob) {
                b = random_bytes();
            }
            return big_blob;
        };

        auto blob1 = make_blob(1234567);
        auto blob2 = make_blob(2345678);


        const column_definition& s1_col = *s->get_column_definition("s1");
        auto key = partition_key::from_exploded(*s, {to_bytes("key1")});

        mutation m(key, s);
        m.set_static_cell(s1_col, make_atomic_cell(bytes_type->decompose(data_value(blob1))));
        mt->apply(std::move(m));

        auto p = get_partition(*mt, key);
        row& r = p.static_row();
        auto i = r.find_cell(s1_col.id);
        BOOST_REQUIRE(i);
        auto cell = i->as_atomic_cell();
        BOOST_REQUIRE(cell.is_live());
        BOOST_REQUIRE(bytes_type->equal(cell.value(), bytes_type->decompose(data_value(blob1))));

        // Stress managed_bytes::linearize and scatter by merging a value into the cell
        mutation m2(key, s);
        m2.set_static_cell(s1_col, atomic_cell::make_live(7, bytes_type->decompose(data_value(blob2))));
        mt->apply(std::move(m2));

        auto p2 = get_partition(*mt, key);
        row& r2 = p2.static_row();
        auto i2 = r2.find_cell(s1_col.id);
        BOOST_REQUIRE(i2);
        auto cell2 = i2->as_atomic_cell();
        BOOST_REQUIRE(cell2.is_live());
        BOOST_REQUIRE(bytes_type->equal(cell2.value(), bytes_type->decompose(data_value(blob2))));
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
            auto get_hash = [] (const mutation& m) {
                md5_hasher h;
                feed_hash(h, m);
                return h.finalize();
            };
            auto h1 = get_hash(m1);
            auto h2 = get_hash(m2);
            if (eq) {
                if (h1 != h2) {
                    BOOST_FAIL(sprint("Hash should be equal for %s and %s", m1, m2));
                }
            } else {
                // We're using a strong hasher, collision should be unlikely
                if (h1 == h2) {
                    BOOST_FAIL(sprint("Hash should be different for %s and %s", m1, m2));
                }
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
            mutation m(pk, s);
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
            mutation m(pk, s);
            m.set_clustered_cell(ckey1, "v1", data_value(bytes("v2:value")), 1);
            m.set_clustered_cell(ckey1, "v2", data_value(bytes("v2:value")), 1);

            auto s2 = make_builder() // v2 changed into a static column, v1 removed
                    .with_column("v2", bytes_type, column_kind::static_column)
                    .build();

            m.upgrade(s2);

            mutation m2(pk, s2);
            m2.partition().clustered_row(ckey1);
            assert_that(m).is_equal_to(m2);
        }

        {
            mutation m(pk, make_builder()
                    .with_column("v1", bytes_type, column_kind::regular_column)
                    .with_column("v2", bytes_type, column_kind::regular_column)
                    .with_column("v3", bytes_type, column_kind::regular_column)
                    .build());
            m.set_clustered_cell(ckey1, "v1", data_value(bytes("v1:value")), 1);
            m.set_clustered_cell(ckey1, "v2", data_value(bytes("v2:value")), 1);
            m.set_clustered_cell(ckey1, "v3", data_value(bytes("v3:value")), 1);

            auto s2 = make_builder() // v2 changed into a static column
                    .with_column("v1", bytes_type, column_kind::regular_column)
                    .with_column("v2", bytes_type, column_kind::static_column)
                    .with_column("v3", bytes_type, column_kind::regular_column)
                    .build();

            m.upgrade(s2);

            mutation m2(pk, s2);
            m2.set_clustered_cell(ckey1, "v1", data_value(bytes("v1:value")), 1);
            m2.set_clustered_cell(ckey1, "v3", data_value(bytes("v3:value")), 1);

            assert_that(m).is_equal_to(m2);
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

    mutation m(key, s);
    m.set_clustered_cell(clustering_key::make_empty(*s), col, make_atomic_cell(int32_type->decompose(1)));
    tombstone tomb(api::new_timestamp(), gc_clock::now() - std::chrono::seconds(1));
    m.partition().apply(tomb);
    BOOST_REQUIRE(!m.partition().empty());
    m.partition().compact_for_compaction(*s, api::max_timestamp, gc_clock::now());
    // Check that row was covered by tombstone.
    BOOST_REQUIRE(m.partition().empty());
    // Check that tombstone was purged after compact_for_compaction().
    BOOST_REQUIRE(!m.partition().partition_tombstone());

    return make_ready_future<>();
}
