/*
 * Copyright (C) 2015 ScyllaDB
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


#include <boost/test/unit_test.hpp>
#include "service/priority_manager.hh"
#include "database.hh"
#include "utils/UUID_gen.hh"
#include "tests/test-utils.hh"
#include "schema_builder.hh"

#include "core/thread.hh"
#include "memtable.hh"
#include "mutation_source_test.hh"
#include "mutation_assertions.hh"
#include "flat_mutation_reader_assertions.hh"
#include "flat_mutation_reader.hh"

static api::timestamp_type next_timestamp() {
    static thread_local api::timestamp_type next_timestamp = 1;
    return next_timestamp++;
}

static bytes make_unique_bytes() {
    return to_bytes(utils::UUID_gen::get_time_UUID().to_sstring());
}

static void set_column(mutation& m, const sstring& column_name) {
    assert(m.schema()->get_column_definition(to_bytes(column_name))->type == bytes_type);
    auto value = data_value(make_unique_bytes());
    m.set_clustered_cell(clustering_key::make_empty(), to_bytes(column_name), value, next_timestamp());
}

static
mutation make_unique_mutation(schema_ptr s) {
    return mutation(s, partition_key::from_single_value(*s, make_unique_bytes()));
}

// Returns a vector of empty mutations in ring order
std::vector<mutation> make_ring(schema_ptr s, int n_mutations) {
    std::vector<mutation> ring;
    for (int i = 0; i < n_mutations; ++i) {
        ring.push_back(make_unique_mutation(s));
    }
    std::sort(ring.begin(), ring.end(), mutation_decorated_key_less_comparator());
    return ring;
}

SEASTAR_TEST_CASE(test_memtable_conforms_to_mutation_source) {
    return seastar::async([] {
        run_mutation_source_tests([](schema_ptr s, const std::vector<mutation>& partitions) {
            auto mt = make_lw_shared<memtable>(s);

            for (auto&& m : partitions) {
                mt->apply(m);
            }

            logalloc::shard_tracker().full_compaction();

            return mt->as_data_source();
        });
    });
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source) {
    return seastar::async([] {
        lw_shared_ptr<memtable> mt;
        std::vector<flat_mutation_reader> readers;
        run_mutation_source_tests([&] (schema_ptr s, const std::vector<mutation>& muts) {
            readers.clear();
            mt = make_lw_shared<memtable>(s);

            for (auto&& m : muts) {
                mt->apply(m);
                // Create reader so that each mutation is in a separate version
                flat_mutation_reader rd = mt->make_flat_reader(s, dht::partition_range::make_singular(m.decorated_key()));
                rd.set_max_buffer_size(1);
                rd.fill_buffer(db::no_timeout).get();
                readers.push_back(std::move(rd));
            }

            return mt->as_data_source();
        });
    });
}

SEASTAR_TEST_CASE(test_memtable_flush_reader) {
    // Memtable flush reader is severly limited, it always assumes that
    // the full partition range is being read and that
    // streamed_mutation::forwarding is set to no. Therefore, we cannot use
    // run_mutation_source_tests() to test it.
    return seastar::async([] {
        auto make_memtable = [] (dirty_memory_manager& mgr, std::vector<mutation> muts) {
            assert(!muts.empty());
            auto mt = make_lw_shared<memtable>(muts.front().schema(), mgr);
            for (auto& m : muts) {
                mt->apply(m);
            }
            return mt;
        };

        auto test_random_streams = [&] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                dirty_memory_manager mgr;
                auto muts = gen(4);

                BOOST_TEST_MESSAGE("Simple read");
                auto mt = make_memtable(mgr, muts);
                assert_that(mt->make_flush_reader(gen.schema(), default_priority_class()))
                    .produces_partition(muts[0])
                    .produces_partition(muts[1])
                    .produces_partition(muts[2])
                    .produces_partition(muts[3])
                    .produces_end_of_stream();

                BOOST_TEST_MESSAGE("Read with next_partition() calls between partition");
                mt = make_memtable(mgr, muts);
                assert_that(mt->make_flush_reader(gen.schema(), default_priority_class()))
                    .next_partition()
                    .produces_partition(muts[0])
                    .next_partition()
                    .produces_partition(muts[1])
                    .next_partition()
                    .produces_partition(muts[2])
                    .next_partition()
                    .produces_partition(muts[3])
                    .next_partition()
                    .produces_end_of_stream();

                BOOST_TEST_MESSAGE("Read with next_partition() calls inside partitions");
                mt = make_memtable(mgr, muts);
                assert_that(mt->make_flush_reader(gen.schema(), default_priority_class()))
                    .produces_partition(muts[0])
                    .produces_partition_start(muts[1].decorated_key(), muts[1].partition().partition_tombstone())
                    .next_partition()
                    .produces_partition(muts[2])
                    .next_partition()
                    .produces_partition_start(muts[3].decorated_key(), muts[3].partition().partition_tombstone())
                    .next_partition()
                    .produces_end_of_stream();
            }
        };

        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::no));
        test_random_streams(random_mutation_generator(random_mutation_generator::generate_counters::yes));
    });
}

SEASTAR_TEST_CASE(test_adding_a_column_during_reading_doesnt_affect_read_result) {
    return seastar::async([] {
        auto common_builder = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key);

        auto s1 = common_builder
                .with_column("v2", bytes_type, column_kind::regular_column)
                .build();

        auto s2 = common_builder
                .with_column("v1", bytes_type, column_kind::regular_column) // new column
                .with_column("v2", bytes_type, column_kind::regular_column)
                .build();

        auto mt = make_lw_shared<memtable>(s1);

        std::vector<mutation> ring = make_ring(s1, 3);

        for (auto&& m : ring) {
            set_column(m, "v2");
            mt->apply(m);
        }

        auto check_rd_s1 = assert_that(mt->make_flat_reader(s1));
        auto check_rd_s2 = assert_that(mt->make_flat_reader(s2));
        check_rd_s1.next_mutation().has_schema(s1).is_equal_to(ring[0]);
        check_rd_s2.next_mutation().has_schema(s2).is_equal_to(ring[0]);
        mt->set_schema(s2);
        check_rd_s1.next_mutation().has_schema(s1).is_equal_to(ring[1]);
        check_rd_s2.next_mutation().has_schema(s2).is_equal_to(ring[1]);
        check_rd_s1.next_mutation().has_schema(s1).is_equal_to(ring[2]);
        check_rd_s2.next_mutation().has_schema(s2).is_equal_to(ring[2]);
        check_rd_s1.produces_end_of_stream();
        check_rd_s2.produces_end_of_stream();

        assert_that(mt->make_flat_reader(s1))
            .produces(ring[0])
            .produces(ring[1])
            .produces(ring[2])
            .produces_end_of_stream();

        assert_that(mt->make_flat_reader(s2))
            .produces(ring[0])
            .produces(ring[1])
            .produces(ring[2])
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_virtual_dirty_accounting_on_flush) {
    return seastar::async([] {
        schema_ptr s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("col", bytes_type, column_kind::regular_column)
                .build();

        dirty_memory_manager mgr;

        auto mt = make_lw_shared<memtable>(s, mgr);

        std::vector<mutation> ring = make_ring(s, 3);
        std::vector<mutation> current_ring;

        for (auto&& m : ring) {
            auto m_with_cell = m;
            m_with_cell.set_clustered_cell(clustering_key::make_empty(), to_bytes("col"),
                                           data_value(bytes(bytes::initialized_later(), 4096)), next_timestamp());
            mt->apply(m_with_cell);
            current_ring.push_back(m_with_cell);
        }

        // Create a reader which will cause many partition versions to be created
        flat_mutation_reader_opt rd1 = mt->make_flat_reader(s);
        rd1->set_max_buffer_size(1);
        rd1->fill_buffer(db::no_timeout).get();

        // Override large cell value with a short one
        {
            auto part0_update = ring[0];
            part0_update.set_clustered_cell(clustering_key::make_empty(), to_bytes("col"),
                                            data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());
            mt->apply(std::move(part0_update));
            current_ring[0] = part0_update;
        }

        std::vector<size_t> virtual_dirty_values;
        virtual_dirty_values.push_back(mgr.virtual_dirty_memory());

        auto flush_reader_check = assert_that(mt->make_flush_reader(s, service::get_local_priority_manager().memtable_flush_priority()));
        flush_reader_check.produces_partition(current_ring[0]);
        virtual_dirty_values.push_back(mgr.virtual_dirty_memory());
        flush_reader_check.produces_partition(current_ring[1]);
        virtual_dirty_values.push_back(mgr.virtual_dirty_memory());

        while ((*rd1)(db::no_timeout).get0()) ;
        rd1 = {};

        logalloc::shard_tracker().full_compaction();

        flush_reader_check.produces_partition(current_ring[2]);
        virtual_dirty_values.push_back(mgr.virtual_dirty_memory());
        flush_reader_check.produces_end_of_stream();
        virtual_dirty_values.push_back(mgr.virtual_dirty_memory());

        std::reverse(virtual_dirty_values.begin(), virtual_dirty_values.end());
        BOOST_REQUIRE(std::is_sorted(virtual_dirty_values.begin(), virtual_dirty_values.end()));
    });
}

// Reproducer for #1753
SEASTAR_TEST_CASE(test_partition_version_consistency_after_lsa_compaction_happens) {
    return seastar::async([] {
        schema_ptr s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key)
                .with_column("col", bytes_type, column_kind::regular_column)
                .build();

        auto mt = make_lw_shared<memtable>(s);

        auto empty_m = make_unique_mutation(s);
        auto ck1 = clustering_key::from_single_value(*s, data_value(make_unique_bytes()).serialize());
        auto ck2 = clustering_key::from_single_value(*s, data_value(make_unique_bytes()).serialize());
        auto ck3 = clustering_key::from_single_value(*s, data_value(make_unique_bytes()).serialize());

        auto m1 = empty_m;
        m1.set_clustered_cell(ck1, to_bytes("col"), data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());

        auto m2 = empty_m;
        m2.set_clustered_cell(ck2, to_bytes("col"), data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());

        auto m3 = empty_m;
        m3.set_clustered_cell(ck3, to_bytes("col"), data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());

        mt->apply(m1);
        stdx::optional<flat_reader_assertions> rd1 = assert_that(mt->make_flat_reader(s));
        rd1->set_max_buffer_size(1);
        rd1->fill_buffer().get();

        mt->apply(m2);
        stdx::optional<flat_reader_assertions> rd2 = assert_that(mt->make_flat_reader(s));
        rd2->set_max_buffer_size(1);
        rd2->fill_buffer().get();

        mt->apply(m3);
        stdx::optional<flat_reader_assertions> rd3 = assert_that(mt->make_flat_reader(s));
        rd3->set_max_buffer_size(1);
        rd3->fill_buffer().get();

        logalloc::shard_tracker().full_compaction();

        auto rd4 = assert_that(mt->make_flat_reader(s));
        rd4.set_max_buffer_size(1);
        rd4.fill_buffer().get();
        auto rd5 = assert_that(mt->make_flat_reader(s));
        rd5.set_max_buffer_size(1);
        rd5.fill_buffer().get();
        auto rd6 = assert_that(mt->make_flat_reader(s));
        rd6.set_max_buffer_size(1);
        rd6.fill_buffer().get();

        rd1->next_mutation().is_equal_to(m1);
        rd2->next_mutation().is_equal_to(m1 + m2);
        rd3->next_mutation().is_equal_to(m1 + m2 + m3);
        rd3 = {};

        rd4.next_mutation().is_equal_to(m1 + m2 + m3);
        rd1 = {};

        rd5.next_mutation().is_equal_to(m1 + m2 + m3);
        rd2 = {};

        rd6.next_mutation().is_equal_to(m1 + m2 + m3);
    });
}

// Reproducer for #1746
SEASTAR_TEST_CASE(test_segment_migration_during_flush) {
    return seastar::async([] {
        schema_ptr s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key)
                .with_column("col", bytes_type, column_kind::regular_column)
                .build();

        dirty_memory_manager mgr;

        auto mt = make_lw_shared<memtable>(s, mgr);

        const int rows_per_partition = 300;
        const int partitions = 3;
        std::vector<mutation> ring = make_ring(s, partitions);

        for (auto& m : ring) {
            for (int i = 0; i < rows_per_partition; ++i) {
                auto ck = clustering_key::from_single_value(*s, data_value(make_unique_bytes()).serialize());
                auto col_value = data_value(bytes(bytes::initialized_later(), 8));
                m.set_clustered_cell(ck, to_bytes("col"), col_value, next_timestamp());
            }
            mt->apply(m);
        }

        std::vector<size_t> virtual_dirty_values;
        virtual_dirty_values.push_back(mgr.virtual_dirty_memory());

        auto rd = mt->make_flush_reader(s, service::get_local_priority_manager().memtable_flush_priority());

        for (int i = 0; i < partitions; ++i) {
            auto mfopt = rd(db::no_timeout).get0();
            BOOST_REQUIRE(bool(mfopt));
            BOOST_REQUIRE(mfopt->is_partition_start());
            while (!mfopt->is_end_of_partition()) {
                logalloc::shard_tracker().full_compaction();
                mfopt = rd(db::no_timeout).get0();
            }
            virtual_dirty_values.push_back(mgr.virtual_dirty_memory());
        }

        BOOST_REQUIRE(!rd(db::no_timeout).get0());

        std::reverse(virtual_dirty_values.begin(), virtual_dirty_values.end());
        BOOST_REQUIRE(std::is_sorted(virtual_dirty_values.begin(), virtual_dirty_values.end()));
    });
}

// Reproducer for #2854
SEASTAR_TEST_CASE(test_fast_forward_to_after_memtable_is_flushed) {
    return seastar::async([] {
        schema_ptr s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("col", bytes_type, column_kind::regular_column)
            .build();

        auto mt = make_lw_shared<memtable>(s);
        auto mt2 = make_lw_shared<memtable>(s);

        std::vector<mutation> ring = make_ring(s, 5);

        for (auto& m : ring) {
            mt->apply(m);
            mt2->apply(m);
        }

        auto rd = assert_that(mt->make_flat_reader(s));
        rd.produces(ring[0]);
        mt->mark_flushed(mt2->as_data_source());
        rd.produces(ring[1]);
        auto range = dht::partition_range::make_starting_with(dht::ring_position(ring[3].decorated_key()));
        rd.fast_forward_to(range);
        rd.produces(ring[3]).produces(ring[4]).produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_partition_range_reads) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        std::vector<mutation> ms = gen(2);

        auto mt = make_lw_shared<memtable>(s);
        for (auto& m : ms) {
            mt->apply(m);
        }

        auto& injector = memory::local_failure_injector();
        uint64_t i = 0;
        do {
            try {
                injector.fail_after(i++);
                assert_that(mt->make_flat_reader(s, query::full_partition_range))
                    .produces(ms);
                injector.cancel();
            } catch (const std::bad_alloc&) {
                // expected
            }
        } while (injector.failed());
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_flush_reads) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        std::vector<mutation> ms = gen(2);

        auto mt = make_lw_shared<memtable>(s);
        for (auto& m : ms) {
            mt->apply(m);
        }

        auto& injector = memory::local_failure_injector();
        uint64_t i = 0;
        do {
            try {
                injector.fail_after(i++);
                assert_that(mt->make_flush_reader(s, default_priority_class()))
                    .produces(ms);
                injector.cancel();
            } catch (const std::bad_alloc&) {
                // expected
            }
            mt->revert_flushed_memory();
        } while (injector.failed());
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_single_partition_reads) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        std::vector<mutation> ms = gen(2);

        auto mt = make_lw_shared<memtable>(s);
        for (auto& m : ms) {
            mt->apply(m);
        }

        auto& injector = memory::local_failure_injector();
        uint64_t i = 0;
        do {
            try {
                injector.fail_after(i++);
                assert_that(mt->make_flat_reader(s, dht::partition_range::make_singular(ms[1].decorated_key())))
                    .produces(ms[1]);
                injector.cancel();
            } catch (const std::bad_alloc&) {
                // expected
            }
        } while (injector.failed());
    });
}

SEASTAR_TEST_CASE(test_hash_is_cached) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v", bytes_type, column_kind::regular_column)
                .build();

        auto mt = make_lw_shared<memtable>(s);

        auto m = make_unique_mutation(s);
        set_column(m, "v");
        mt->apply(m);

        {
            auto rd = mt->make_flat_reader(s);
            rd(db::no_timeout).get0()->as_partition_start();
            clustering_row row = std::move(rd(db::no_timeout).get0()->as_mutable_clustering_row());
            BOOST_REQUIRE(!row.cells().cell_hash_for(0));
        }

        {
            auto slice = s->full_slice();
            slice.options.set<query::partition_slice::option::with_digest>();
            auto rd = mt->make_flat_reader(s, query::full_partition_range, slice);
            rd(db::no_timeout).get0()->as_partition_start();
            clustering_row row = std::move(rd(db::no_timeout).get0()->as_mutable_clustering_row());
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        {
            auto rd = mt->make_flat_reader(s);
            rd(db::no_timeout).get0()->as_partition_start();
            clustering_row row = std::move(rd(db::no_timeout).get0()->as_mutable_clustering_row());
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        set_column(m, "v");
        mt->apply(m);

        {
            auto rd = mt->make_flat_reader(s);
            rd(db::no_timeout).get0()->as_partition_start();
            clustering_row row = std::move(rd(db::no_timeout).get0()->as_mutable_clustering_row());
            BOOST_REQUIRE(!row.cells().cell_hash_for(0));
        }

        {
            auto slice = s->full_slice();
            slice.options.set<query::partition_slice::option::with_digest>();
            auto rd = mt->make_flat_reader(s, query::full_partition_range, slice);
            rd(db::no_timeout).get0()->as_partition_start();
            clustering_row row = std::move(rd(db::no_timeout).get0()->as_mutable_clustering_row());
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        {
            auto rd = mt->make_flat_reader(s);
            rd(db::no_timeout).get0()->as_partition_start();
            clustering_row row = std::move(rd(db::no_timeout).get0()->as_mutable_clustering_row());
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }
    });
}
