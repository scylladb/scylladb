/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/test/unit_test.hpp>
#include <boost/test/framework.hpp>
#include "replica/database.hh"
#include "db/config.hh"
#include "utils/assert.hh"
#include "utils/UUID_gen.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include "schema/schema_builder.hh"
#include <seastar/util/closeable.hh>
#include "service/migration_manager.hh"

#include <fmt/ranges.h>
#include <seastar/core/thread.hh>
#include "replica/memtable.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/mutation_assertions.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/data_model.hh"
#include "test/lib/eventually.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/log.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/key_utils.hh"
#include "test/lib/sstable_utils.hh"
#include "utils/error_injection.hh"
#include "db/commitlog/commitlog.hh"
#include "test/lib/make_random_string.hh"
#include "db/extensions.hh"
#include "db/config.hh"
#include "service/storage_service.hh"

using namespace std::literals::chrono_literals;

static api::timestamp_type next_timestamp() {
    static thread_local api::timestamp_type next_timestamp = 1;
    return next_timestamp++;
}

static bytes make_unique_bytes() {
    return to_bytes(fmt::to_string(utils::UUID_gen::get_time_UUID()));
}

static void set_column(mutation& m, const sstring& column_name) {
    SCYLLA_ASSERT(m.schema()->get_column_definition(to_bytes(column_name))->type == bytes_type);
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
            auto mt = make_memtable(s, partitions);
            logalloc::shard_tracker().full_compaction();
            return mt->as_data_source();
        });
    });
}

static future<> test_memtable(void (*run_tests)(populate_fn_ex, bool)) {
    return seastar::async([run_tests] {
        tests::reader_concurrency_semaphore_wrapper semaphore;
        lw_shared_ptr<replica::memtable> mt;
        std::vector<mutation_reader> readers;
        auto clear_readers = [&readers] {
            parallel_for_each(readers, [] (mutation_reader& rd) {
                return rd.close();
            }).finally([&readers] {
                readers.clear();
            }).get();
        };
        auto cleanup_readers = defer([&] { clear_readers(); });
        std::deque<dht::partition_range> ranges_storage;
        lw_shared_ptr<bool> finished = make_lw_shared(false);
        auto full_compaction_in_background = seastar::do_until([finished] {return *finished;}, [] {
            // do_refresh_state is called when we detect a new partition snapshot version.
            // If snapshot version changes in process of reading mutation fragments from a
            // clustering range, the partition_snapshot_reader state is refreshed with saved
            // last position of emitted row and range tombstone. full_compaction increases the
            // change mark.
            logalloc::shard_tracker().full_compaction();
            return seastar::sleep(100us);
        });
        run_tests([&] (schema_ptr s, const std::vector<mutation>& muts, gc_clock::time_point) {
            clear_readers();
            mt = make_lw_shared<replica::memtable>(s);

            for (auto&& m : muts) {
                mt->apply(m);
                // Create reader so that each mutation is in a separate version
                auto rd = mt->make_flat_reader(s, semaphore.make_permit(), ranges_storage.emplace_back(dht::partition_range::make_singular(m.decorated_key())));
                rd.set_max_buffer_size(1);
                rd.fill_buffer().get();
                readers.emplace_back(std::move(rd));
            }

            return mt->as_data_source();
        }, true);
        *finished = true;
        full_compaction_in_background.get();
    });
}

// plain
SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_basic) {
    return test_memtable(run_mutation_source_tests_plain_basic);
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_plain_reader_conversion) {
    return test_memtable(run_mutation_source_tests_plain_reader_conversion);
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_plain_fragments_monotonic) {
    return test_memtable(run_mutation_source_tests_plain_fragments_monotonic);
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_plain_read_back) {
    return test_memtable(run_mutation_source_tests_plain_read_back);
}

// reverse
SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_reverse_basic) {
    return test_memtable(run_mutation_source_tests_reverse_basic);
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_reverse_reader_conversion) {
    return test_memtable(run_mutation_source_tests_reverse_reader_conversion);
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_reverse_fragments_monotonic) {
    return test_memtable(run_mutation_source_tests_reverse_fragments_monotonic);
}

SEASTAR_TEST_CASE(test_memtable_with_many_versions_conforms_to_mutation_source_reverse_read_back) {
    return test_memtable(run_mutation_source_tests_reverse_read_back);
}

SEASTAR_TEST_CASE(test_memtable_flush_reader) {
    // Memtable flush reader is severely limited, it always assumes that
    // the full partition range is being read and that
    // streamed_mutation::forwarding is set to no. Therefore, we cannot use
    // run_mutation_source_tests() to test it.
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto make_memtable = [] (replica::dirty_memory_manager& mgr, replica::memtable_table_shared_data& table_shared_data, replica::table_stats& tbl_stats, std::vector<mutation> muts) {
            SCYLLA_ASSERT(!muts.empty());
            auto mt = make_lw_shared<replica::memtable>(muts.front().schema(), mgr, table_shared_data, tbl_stats);
            for (auto& m : muts) {
                mt->apply(m);
            }
            return mt;
        };

        auto test_random_streams = [&] (random_mutation_generator&& gen) {
            for (auto i = 0; i < 4; i++) {
                replica::table_stats tbl_stats;
                replica::memtable_table_shared_data table_shared_data;
                replica::dirty_memory_manager mgr;
                const auto muts = gen(4);
                const auto now = gc_clock::now();
                auto compacted_muts = muts;
                for (auto& mut : compacted_muts) {
                    mut.partition().compact_for_compaction(*mut.schema(), always_gc, mut.decorated_key(), now, tombstone_gc_state(nullptr));
                }

                testlog.info("Simple read");
                auto mt = make_memtable(mgr, table_shared_data, tbl_stats, muts);

                assert_that(mt->make_flush_reader(gen.schema(), semaphore.make_permit()))
                    .produces_compacted(compacted_muts[0], now)
                    .produces_compacted(compacted_muts[1], now)
                    .produces_compacted(compacted_muts[2], now)
                    .produces_compacted(compacted_muts[3], now)
                    .produces_end_of_stream();

                testlog.info("Read with next_partition() calls between partition");
                mt = make_memtable(mgr, table_shared_data, tbl_stats, muts);
                assert_that(mt->make_flush_reader(gen.schema(), semaphore.make_permit()))
                    .next_partition()
                    .produces_compacted(compacted_muts[0], now)
                    .next_partition()
                    .produces_compacted(compacted_muts[1], now)
                    .next_partition()
                    .produces_compacted(compacted_muts[2], now)
                    .next_partition()
                    .produces_compacted(compacted_muts[3], now)
                    .next_partition()
                    .produces_end_of_stream();

                testlog.info("Read with next_partition() calls inside partitions");
                mt = make_memtable(mgr, table_shared_data, tbl_stats, muts);
                assert_that(mt->make_flush_reader(gen.schema(), semaphore.make_permit()))
                    .produces_compacted(compacted_muts[0], now)
                    .produces_partition_start(muts[1].decorated_key(), muts[1].partition().partition_tombstone())
                    .next_partition()
                    .produces_compacted(compacted_muts[2], now)
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

        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto mt = make_lw_shared<replica::memtable>(s1);

        std::vector<mutation> ring = make_ring(s1, 3);

        for (auto&& m : ring) {
            set_column(m, "v2");
            mt->apply(m);
        }

        auto check_rd_s1 = assert_that(mt->make_flat_reader(s1, semaphore.make_permit()));
        auto check_rd_s2 = assert_that(mt->make_flat_reader(s2, semaphore.make_permit()));
        check_rd_s1.next_mutation().has_schema(s1).is_equal_to(ring[0]);
        check_rd_s2.next_mutation().has_schema(s2).is_equal_to(ring[0]);
        mt->set_schema(s2);
        check_rd_s1.next_mutation().has_schema(s1).is_equal_to(ring[1]);
        check_rd_s2.next_mutation().has_schema(s2).is_equal_to(ring[1]);
        check_rd_s1.next_mutation().has_schema(s1).is_equal_to(ring[2]);
        check_rd_s2.next_mutation().has_schema(s2).is_equal_to(ring[2]);
        check_rd_s1.produces_end_of_stream();
        check_rd_s2.produces_end_of_stream();

        assert_that(mt->make_flat_reader(s1, semaphore.make_permit()))
            .produces(ring[0])
            .produces(ring[1])
            .produces(ring[2])
            .produces_end_of_stream();

        assert_that(mt->make_flat_reader(s2, semaphore.make_permit()))
            .produces(ring[0])
            .produces(ring[1])
            .produces(ring[2])
            .produces_end_of_stream();
    });
}

SEASTAR_TEST_CASE(test_unspooled_dirty_accounting_on_flush) {
    return seastar::async([] {
        schema_ptr s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("col", bytes_type, column_kind::regular_column)
                .build();

        tests::reader_concurrency_semaphore_wrapper semaphore;

        replica::dirty_memory_manager mgr;
        replica::memtable_table_shared_data table_shared_data;
        replica::table_stats tbl_stats;

        auto mt = make_lw_shared<replica::memtable>(s, mgr, table_shared_data, tbl_stats);

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
        mutation_reader_opt rd1 = mt->make_flat_reader(s, semaphore.make_permit());
        auto close_rd1 = deferred_close(*rd1);
        rd1->set_max_buffer_size(1);
        rd1->fill_buffer().get();

        // Override large cell value with a short one
        {
            auto part0_update = ring[0];
            part0_update.set_clustered_cell(clustering_key::make_empty(), to_bytes("col"),
                                            data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());
            mt->apply(std::move(part0_update));
            current_ring[0] = part0_update;
        }

        std::vector<size_t> unspooled_dirty_values;
        unspooled_dirty_values.push_back(mgr.unspooled_dirty_memory());

        auto flush_reader_check = assert_that(mt->make_flush_reader(s, semaphore.make_permit()));
        flush_reader_check.produces_partition(current_ring[0]);
        unspooled_dirty_values.push_back(mgr.unspooled_dirty_memory());
        flush_reader_check.produces_partition(current_ring[1]);
        unspooled_dirty_values.push_back(mgr.unspooled_dirty_memory());

        while ((*rd1)().get()) ;
        close_rd1.close_now();

        logalloc::shard_tracker().full_compaction();

        flush_reader_check.produces_partition(current_ring[2]);
        unspooled_dirty_values.push_back(mgr.unspooled_dirty_memory());
        flush_reader_check.produces_end_of_stream();
        unspooled_dirty_values.push_back(mgr.unspooled_dirty_memory());

        std::reverse(unspooled_dirty_values.begin(), unspooled_dirty_values.end());
        BOOST_REQUIRE(std::is_sorted(unspooled_dirty_values.begin(), unspooled_dirty_values.end()));
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

        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto mt = make_lw_shared<replica::memtable>(s);

        auto empty_m = make_unique_mutation(s);
        auto ck1 = clustering_key::from_single_value(*s, serialized(make_unique_bytes()));
        auto ck2 = clustering_key::from_single_value(*s, serialized(make_unique_bytes()));
        auto ck3 = clustering_key::from_single_value(*s, serialized(make_unique_bytes()));

        auto m1 = empty_m;
        m1.set_clustered_cell(ck1, to_bytes("col"), data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());

        auto m2 = empty_m;
        m2.set_clustered_cell(ck2, to_bytes("col"), data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());

        auto m3 = empty_m;
        m3.set_clustered_cell(ck3, to_bytes("col"), data_value(bytes(bytes::initialized_later(), 8)), next_timestamp());

        mt->apply(m1);
        std::optional<flat_reader_assertions_v2> rd1 = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
        rd1->set_max_buffer_size(1);
        rd1->fill_buffer().get();

        mt->apply(m2);
        std::optional<flat_reader_assertions_v2> rd2 = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
        rd2->set_max_buffer_size(1);
        rd2->fill_buffer().get();

        mt->apply(m3);
        std::optional<flat_reader_assertions_v2> rd3 = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
        rd3->set_max_buffer_size(1);
        rd3->fill_buffer().get();

        logalloc::shard_tracker().full_compaction();

        auto rd4 = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
        rd4.set_max_buffer_size(1);
        rd4.fill_buffer().get();
        auto rd5 = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
        rd5.set_max_buffer_size(1);
        rd5.fill_buffer().get();
        auto rd6 = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
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

        tests::reader_concurrency_semaphore_wrapper semaphore;

        replica::table_stats tbl_stats;
        replica::memtable_table_shared_data table_shared_data;
        replica::dirty_memory_manager mgr;

        auto mt = make_lw_shared<replica::memtable>(s, mgr, table_shared_data, tbl_stats);

        const int rows_per_partition = 300;
        const int partitions = 3;
        std::vector<mutation> ring = make_ring(s, partitions);

        for (auto& m : ring) {
            for (int i = 0; i < rows_per_partition; ++i) {
                auto ck = clustering_key::from_single_value(*s, serialized(make_unique_bytes()));
                auto col_value = data_value(bytes(bytes::initialized_later(), 8));
                m.set_clustered_cell(ck, to_bytes("col"), col_value, next_timestamp());
            }
            mt->apply(m);
        }

        auto rd = mt->make_flush_reader(s, semaphore.make_permit());
        auto close_rd = deferred_close(rd);

        for (int i = 0; i < partitions; ++i) {
            auto mfopt = rd().get();
            BOOST_REQUIRE(bool(mfopt));
            BOOST_REQUIRE(mfopt->is_partition_start());
            while (!mfopt->is_end_of_partition()) {
                logalloc::shard_tracker().full_compaction();
                mfopt = rd().get();
            }
            BOOST_REQUIRE_LE(mgr.unspooled_dirty_memory(), mgr.real_dirty_memory());
        }

        BOOST_REQUIRE(!rd().get());
    });
}

// Reproducer for #2854
SEASTAR_TEST_CASE(test_fast_forward_to_after_memtable_is_flushed) {
    return seastar::async([] {
        schema_ptr s = schema_builder("ks", "cf")
            .with_column("pk", bytes_type, column_kind::partition_key)
            .with_column("col", bytes_type, column_kind::regular_column)
            .build();

        tests::reader_concurrency_semaphore_wrapper semaphore;

        std::vector<mutation> ring = make_ring(s, 5);
        auto mt = make_memtable(s, ring);
        auto mt2 = make_memtable(s, ring);

        auto rd = assert_that(mt->make_flat_reader(s, semaphore.make_permit()));
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
        tests::reader_concurrency_semaphore_wrapper semaphore;
        std::vector<mutation> ms = gen(2);

        auto mt = make_memtable(s, ms);
        memory::with_allocation_failures([&] {
            assert_that(mt->make_flat_reader(s, semaphore.make_permit(), query::full_partition_range))
                .produces(ms);
        });
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_flush_reads) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        std::vector<mutation> ms = gen(2);

        auto mt = make_memtable(s, ms);
        memory::with_allocation_failures([&] {
            auto revert = defer([&] {
                mt->revert_flushed_memory();
            });
            assert_that(mt->make_flush_reader(s, semaphore.make_permit()))
                .produces(ms);
        });
    });
}

SEASTAR_TEST_CASE(test_exception_safety_of_single_partition_reads) {
    return seastar::async([] {
        random_mutation_generator gen(random_mutation_generator::generate_counters::no);
        auto s = gen.schema();
        tests::reader_concurrency_semaphore_wrapper semaphore;
        std::vector<mutation> ms = gen(2);

        auto mt = make_memtable(s, ms);
        memory::with_allocation_failures([&] {
            assert_that(mt->make_flat_reader(s, semaphore.make_permit(), dht::partition_range::make_singular(ms[1].decorated_key())))
                .produces(ms[1]);
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_tombstone_compaction_during_flush) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    simple_schema ss;
    auto s = ss.schema();
    auto mt = make_lw_shared<replica::memtable>(ss.schema());

    auto pk = ss.make_pkey(0);
    auto pr = dht::partition_range::make_singular(pk);
    int n_rows = 10000;
    {
        mutation m(ss.schema(), pk);
        for (int i = 0; i < n_rows; ++i) {
            ss.add_row(m, ss.make_ckey(i), "v1");
        }
        mt->apply(m);
    }

    auto rd1 = mt->make_flat_reader(ss.schema(), semaphore.make_permit(), pr, s->full_slice(),
                                    nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_rd1 = defer([&] { rd1.close().get(); });

    rd1.fill_buffer().get();

    mutation rt_m(ss.schema(), pk);
    auto rt = ss.delete_range(rt_m, ss.make_ckey_range(0, n_rows));
    mt->apply(rt_m);

    auto rd2 = mt->make_flat_reader(ss.schema(), semaphore.make_permit(), pr, s->full_slice(),
                                    nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_rd2 = defer([&] { rd2.close().get(); });

    rd2.fill_buffer().get();

    mt->apply(rt_m); // whatever

    auto flush_rd = mt->make_flush_reader(ss.schema(), semaphore.make_permit());
    auto close_flush_rd = defer([&] { flush_rd.close().get(); });

    while (!flush_rd.is_end_of_stream()) {
        flush_rd().get();
    }

    { auto close_rd = std::move(close_rd2); }
    { auto rd = std::move(rd2); }

    { auto close_rd = std::move(close_rd1); }
    { auto rd = std::move(rd1); }

    mt->cleaner().drain().get();
}

SEASTAR_THREAD_TEST_CASE(test_tombstone_merging_with_multiple_versions) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    simple_schema ss;
    auto s = ss.schema();
    auto mt = make_lw_shared<replica::memtable>(ss.schema());

    auto pk = ss.make_pkey(0);
    auto pr = dht::partition_range::make_singular(pk);

    auto t0 = ss.new_tombstone();
    auto t1 = ss.new_tombstone();
    auto t2 = ss.new_tombstone();
    auto t3 = ss.new_tombstone();

    mutation m1(s, pk);
    ss.delete_range(m1, *position_range_to_clustering_range(position_range(
                position_in_partition::before_key(ss.make_ckey(0)),
                position_in_partition::for_key(ss.make_ckey(3))), *s), t1);
    ss.add_row(m1, ss.make_ckey(0), "v");
    ss.add_row(m1, ss.make_ckey(1), "v");

    // Fill so that rd1 stays in the partition snapshot
    int n_rows = 1000;
    auto v = make_random_string(512);
    for (int i = 0; i < n_rows; ++i) {
        ss.add_row(m1, ss.make_ckey(i), v);
    }

    mutation m2(s, pk);
    ss.delete_range(m2, *position_range_to_clustering_range(position_range(
            position_in_partition::before_key(ss.make_ckey(0)),
            position_in_partition::before_key(ss.make_ckey(1))), *s), t2);
    ss.delete_range(m2, *position_range_to_clustering_range(position_range(
            position_in_partition::before_key(ss.make_ckey(1)),
            position_in_partition::for_key(ss.make_ckey(3))), *s), t3);

    mutation m3(s, pk);
    ss.delete_range(m3, *position_range_to_clustering_range(position_range(
            position_in_partition::before_key(ss.make_ckey(0)),
            position_in_partition::for_key(ss.make_ckey(4))), *s), t0);

    mt->apply(m1);

    auto rd1 = mt->make_flat_reader(s, semaphore.make_permit(), pr, s->full_slice(),
                                    nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_rd1 = defer([&] { rd1.close().get(); });

    rd1.fill_buffer().get();
    BOOST_REQUIRE(!rd1.is_end_of_stream()); // rd1 must keep the m1 version alive

    mt->apply(m2);

    auto rd2 = mt->make_flat_reader(s, semaphore.make_permit(), pr, s->full_slice(),
                                    nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_r2 = defer([&] { rd2.close().get(); });

    rd2.fill_buffer().get();
    BOOST_REQUIRE(!rd2.is_end_of_stream()); // rd2 must keep the m1 version alive

    mt->apply(m3);

    assert_that(mt->make_flat_reader(s, semaphore.make_permit(), pr))
        .has_monotonic_positions();

    assert_that(mt->make_flat_reader(s, semaphore.make_permit(), pr))
        .produces(m1 + m2 + m3);
}

SEASTAR_THREAD_TEST_CASE(test_tombstone_merging_with_mvcc_and_preemption) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    simple_schema ss;
    auto s = ss.schema();
    auto mt = make_lw_shared<replica::memtable>(ss.schema());

    auto pk = ss.make_pkey(0);
    auto pr = dht::partition_range::make_singular(pk);

    // Produce large m0 so that merging range tombstone from m1 into m0 is likely to be preempted in the middle.
    int n_tombstones = 10000;
    int key_delta_per_tombstone = 3;
    mutation m0(s, pk);
    {
        int key = 0;
        for (int i = 0; i < n_tombstones; ++i) {
            ss.add_row(m0, ss.make_ckey(key), "value");
            key += key_delta_per_tombstone;
        }
    }
    mt->apply(m0);

    std::optional<mutation_reader> rd0 = mt->make_flat_reader(
            s, semaphore.make_permit(), pr, s->full_slice(),
            nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_rd0 = defer([&] { rd0->close().get(); });
    rd0->fill_buffer().get();
    BOOST_REQUIRE(!rd0->is_end_of_stream());

    auto k1 = n_tombstones * key_delta_per_tombstone / 3;
    auto k2 = k1 + n_tombstones * key_delta_per_tombstone / 2;

    mutation m1(s, pk);
    ss.delete_range(m1, ss.make_ckey_range(k1, k2));
    mt->apply(m1);

    std::optional<mutation_reader> rd1 = mt->make_flat_reader(
            s, semaphore.make_permit(), pr, s->full_slice(),
            nullptr, streamed_mutation::forwarding::no, mutation_reader::forwarding::no);
    auto close_rd1 = defer([&] { rd1->close().get(); });
    rd1->fill_buffer().get();
    BOOST_REQUIRE(!rd1->is_end_of_stream());

    // Trigger merging of m1 into m0.
    close_rd0.cancel();
    rd0->close().get();
    rd0 = {};

    mutation m2(s, pk);
    ss.delete_range(m2, ss.make_ckey_range(0, 1));
    // Shadow earlier range tombstone in m1 to test whether applying this range tombstone
    // to m1 (from m2) while m1 is still merging its range tombstones to m0 doesn't
    // lead to loss of information from m2 due to the way preemption is handled in m1 -> m0 merging.
    ss.delete_range(m2, ss.make_ckey_range(k1, k2));
    mt->apply(m2);

    // Trigger merging of m2 into m1.
    // Some of it will complete immediately.
    // Let's see if updates of m1 from m2 are not lost while merging of m1 into m0 is still in progress.
    close_rd1.cancel();
    rd1->close().get();
    rd1 = {};

    // Wait for merging to complete so that we read the final result later.
    mt->cleaner().drain().get();

    assert_that(mt->make_flat_reader(s, semaphore.make_permit(), pr))
            .produces(m0 + m1 + m2);
}

SEASTAR_THREAD_TEST_CASE(test_range_tombstones_are_compacted_with_data) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    simple_schema ss;
    auto s = ss.schema();
    auto mt = make_lw_shared<replica::memtable>(ss.schema());

    auto pk = ss.make_pkey(0);
    auto pr = dht::partition_range::make_singular(pk);

    auto old_tombstone = ss.new_tombstone(); // older than any write, does not cover anything

    {
        mutation m(ss.schema(), pk);
        ss.add_row(m, ss.make_ckey(1), "v1");
        ss.add_row(m, ss.make_ckey(2), "v1");
        ss.add_row(m, ss.make_ckey(3), "v1");
        ss.add_row(m, ss.make_ckey(4), "v1");
        mt->apply(m);
    }

    mutation rt_m(ss.schema(), pk);
    auto rt = ss.delete_range(rt_m, ss.make_ckey_range(2,3));
    mt->apply(rt_m);
    mt->cleaner().drain().get();

    assert_that(mt->make_flat_reader(ss.schema(), semaphore.make_permit(), pr))
            .produces_partition_start(pk)
            .produces_row_with_key(ss.make_ckey(1))
            .produces_range_tombstone_change({rt.position(), rt.tomb})
            .produces_range_tombstone_change({rt.end_position(), {}})
            .produces_row_with_key(ss.make_ckey(4))
            .produces_partition_end()
            .produces_end_of_stream();

    {
        mutation m(ss.schema(), pk);
        m.partition().apply(old_tombstone);
        mt->apply(m);
        mt->cleaner().drain().get();
    }

    // No change
    assert_that(mt->make_flat_reader(ss.schema(), semaphore.make_permit(), pr))
            .produces_partition_start(pk, {old_tombstone})
            .produces_row_with_key(ss.make_ckey(1))
            .produces_range_tombstone_change({rt.position(), rt.tomb})
            .produces_range_tombstone_change({rt.end_position(), {}})
            .produces_row_with_key(ss.make_ckey(4))
            .produces_partition_end()
            .produces_end_of_stream();

    auto new_tomb = ss.new_tombstone();

    {
        mutation m(ss.schema(), pk);
        m.partition().apply(new_tomb);
        mt->apply(m);
        mt->cleaner().drain().get();
    }

    assert_that(mt->make_flat_reader(ss.schema(), semaphore.make_permit(), pr))
            .produces_partition_start(pk, {new_tomb})
            .produces_range_tombstone_change({rt.position(), rt.tomb})
            .produces_range_tombstone_change({rt.end_position(), {}})
            .produces_partition_end()
            .produces_end_of_stream();
}

SEASTAR_TEST_CASE(test_hash_is_cached) {
    return seastar::async([] {
        auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("v", bytes_type, column_kind::regular_column)
                .build();
        tests::reader_concurrency_semaphore_wrapper semaphore;

        auto mt = make_lw_shared<replica::memtable>(s);

        auto m = make_unique_mutation(s);
        set_column(m, "v");
        mt->apply(m);

        {
            auto rd = mt->make_flat_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(!row.cells().cell_hash_for(0));
        }

        {
            auto slice = s->full_slice();
            slice.options.set<query::partition_slice::option::with_digest>();
            auto rd = mt->make_flat_reader(s, semaphore.make_permit(), query::full_partition_range, slice);
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        {
            auto rd = mt->make_flat_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        set_column(m, "v");
        mt->apply(m);

        {
            auto rd = mt->make_flat_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(!row.cells().cell_hash_for(0));
        }

        {
            auto slice = s->full_slice();
            slice.options.set<query::partition_slice::option::with_digest>();
            auto rd = mt->make_flat_reader(s, semaphore.make_permit(), query::full_partition_range, slice);
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }

        {
            auto rd = mt->make_flat_reader(s, semaphore.make_permit());
            auto close_rd = deferred_close(rd);
            rd().get()->as_partition_start();
            clustering_row row = std::move(*rd().get()).as_clustering_row();
            BOOST_REQUIRE(row.cells().cell_hash_for(0));
        }
    });
}

SEASTAR_THREAD_TEST_CASE(test_collecting_encoding_stats) {
    auto random_int32_value = [] {
        return int32_type->decompose(tests::random::get_int<int32_t>());
    };

    auto now = gc_clock::now();

    auto td = tests::data_model::table_description({ { "pk", int32_type } }, { { "ck", utf8_type } });

    auto td1 = td;
    td1.add_static_column("s1", int32_type);
    td1.add_regular_column("v1", int32_type);
    td1.add_regular_column("v2", int32_type);
    auto built_schema = td1.build();
    auto s = built_schema.schema;

    auto md1 = tests::data_model::mutation_description({ to_bytes("pk1") });
    md1.add_clustered_row_marker({ to_bytes("ck1") });
    md1.add_clustered_cell({ to_bytes("ck1") }, "v1", random_int32_value());
    auto m1 = md1.build(s);

    auto md2 = tests::data_model::mutation_description({ to_bytes("pk2") });
    auto md2_ttl = gc_clock::duration(std::chrono::seconds(1));
    md2.add_clustered_row_marker({ to_bytes("ck1") }, -10);
    md2.add_clustered_cell({ to_bytes("ck1") }, "v1", random_int32_value());
    md2.add_clustered_cell({ to_bytes("ck2") }, "v2",
            tests::data_model::mutation_description::atomic_value(random_int32_value(), tests::data_model::data_timestamp, md2_ttl, now + md2_ttl));
    auto m2 = md2.build(s);

    auto md3 = tests::data_model::mutation_description({ to_bytes("pk3") });
    auto md3_ttl = gc_clock::duration(std::chrono::seconds(2));
    auto md3_expiry_point = now - std::chrono::hours(8);
    md3.add_static_cell("s1",
            tests::data_model::mutation_description::atomic_value(random_int32_value(), tests::data_model::data_timestamp, md3_ttl, md3_expiry_point));
    auto m3 = md3.build(s);

    auto mt = make_lw_shared<replica::memtable>(s);

    auto stats = mt->get_encoding_stats();
    BOOST_CHECK(stats.min_local_deletion_time == gc_clock::time_point::max());
    BOOST_CHECK_EQUAL(stats.min_timestamp, api::max_timestamp);
    BOOST_CHECK(stats.min_ttl == gc_clock::duration::max());

    mt->apply(m1);
    stats = mt->get_encoding_stats();
    BOOST_CHECK(stats.min_local_deletion_time == gc_clock::time_point::max());
    BOOST_CHECK_EQUAL(stats.min_timestamp, tests::data_model::data_timestamp);
    BOOST_CHECK(stats.min_ttl == gc_clock::duration::max());

    mt->apply(m2);
    stats = mt->get_encoding_stats();
    BOOST_CHECK(stats.min_local_deletion_time == now + md2_ttl);
    BOOST_CHECK_EQUAL(stats.min_timestamp, -10);
    BOOST_CHECK(stats.min_ttl == md2_ttl);

    mt->apply(m3);
    stats = mt->get_encoding_stats();
    BOOST_CHECK(stats.min_local_deletion_time == md3_expiry_point);
    BOOST_CHECK_EQUAL(stats.min_timestamp, -10);
    BOOST_CHECK(stats.min_ttl == md2_ttl);
}


SEASTAR_TEST_CASE(memtable_flush_compresses_mutations) {
    auto db_config = make_shared<db::config>();
    db_config->enable_cache.set(false);
    return do_with_cql_env_thread([](cql_test_env& env) {
        // Create table and insert some data
        char const* ks_name = "keyspace_name";
        char const* table_name = "table_name";
        env.execute_cql(format("CREATE KEYSPACE {} WITH REPLICATION = {{'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1}};", ks_name)).get();
        env.execute_cql(format("CREATE TABLE {}.{} (pk int, ck int, id int, PRIMARY KEY(pk, ck));", ks_name, table_name)).get();

        replica::database& db = env.local_db();
        replica::table& t = db.find_column_family(ks_name, table_name);
        tests::reader_concurrency_semaphore_wrapper semaphore;
        schema_ptr s = t.schema();

        // Build expected mutation with partition key: 1, clustering_key: 2 and value of id column: 3
        dht::decorated_key pk = dht::decorate_key(*s, partition_key::from_single_value(*s, serialized(1)));
        clustering_key ck = clustering_key::from_single_value(*s, serialized(2));

        mutation m1 = mutation(s, pk);
        m1.set_clustered_cell(ck, to_bytes("id"), data_value(3), api::new_timestamp());

        mutation m2 = mutation(s, pk);
        m2.partition().apply_delete(*s, clustering_key_prefix::from_singular(*s, 2), tombstone{api::new_timestamp(), gc_clock::now()});

        t.apply(m1);
        t.apply(m2);

        // Flush to make sure all the modifications make it to disk
        t.flush().get();

        // Treat the table as mutation_source and SCYLLA_ASSERT we get the expected mutation and end of stream
        mutation_source ms = t.as_mutation_source();
        assert_that(ms.make_reader_v2(s, semaphore.make_permit()))
            .produces(m2)
            .produces_end_of_stream();
    }, db_config);
}

SEASTAR_TEST_CASE(sstable_compaction_does_not_resurrect_data) {
    auto db_config = make_shared<db::config>();
    db_config->enable_cache.set(false);
    return do_with_cql_env_thread([](cql_test_env& env) {
        replica::database& db = env.local_db();
        service::migration_manager& mm = env.migration_manager().local();

        sstring ks_name = "ks";
        sstring table_name = "table_name";

        schema_ptr s = schema_builder(ks_name, table_name)
            .with_column(to_bytes("pk"), int32_type, column_kind::partition_key)
            .with_column(to_bytes("ck"), int32_type, column_kind::clustering_key)
            .with_column(to_bytes("id"), int32_type)
            .set_gc_grace_seconds(1)
            .build();
        auto group0_guard = mm.start_group0_operation().get();
        auto ts = group0_guard.write_timestamp();
        mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s, ts).get(), std::move(group0_guard), "").get();

        replica::table& t = db.find_column_family(ks_name, table_name);

        dht::decorated_key pk = dht::decorate_key(*s, partition_key::from_single_value(*s, serialized(1)));
        clustering_key ck_to_delete = clustering_key::from_single_value(*s, serialized(2));
        clustering_key ck = clustering_key::from_single_value(*s, serialized(3));

        api::timestamp_type insertion_timestamp_before_delete = api::new_timestamp();
        forward_jump_clocks(1s);
        api::timestamp_type deletion_timestamp = api::new_timestamp();
        forward_jump_clocks(1s);
        api::timestamp_type insertion_timestamp_after_delete = api::new_timestamp();

        mutation m_delete = mutation(s, pk);
        m_delete.partition().apply_delete(
            *s,
            ck_to_delete,
            tombstone{deletion_timestamp, gc_clock::now()});
        t.apply(m_delete);

        // Insert data that won't be removed by tombstone to prevent compaction from skipping whole partition
        mutation m_insert = mutation(s, pk);
        m_insert.set_clustered_cell(ck, to_bytes("id"), data_value(3), insertion_timestamp_after_delete);
        t.apply(m_insert);

        // Flush and wait until the gc_grace_seconds pass
        t.flush().get();
        forward_jump_clocks(2s);

        // Apply the past mutation to memtable to simulate repair. This row should be deleted by tombstone
        mutation m_past_insert = mutation(s, pk);
        m_past_insert.set_clustered_cell(
            ck_to_delete,
            to_bytes("id"),
            data_value(4),
            insertion_timestamp_before_delete);
        t.apply(m_past_insert);

        // Trigger compaction. If all goes well, compaction should check if a relevant row is in the memtable
        // and should not purge the tombstone.
        t.compact_all_sstables(tasks::task_info{}).get();

        // If we get additional row (1, 2, 4), that means the tombstone was purged and data was resurrected
        assert_that(env.execute_cql(format("SELECT * FROM {}.{};", ks_name, table_name)).get())
            .is_rows()
            .with_rows_ignore_order({
                {serialized(1), serialized(3), serialized(3)}, 
            });
    }, db_config);
}

SEASTAR_TEST_CASE(failed_flush_prevents_writes) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
    return make_ready_future<>();
#else
    auto db_config = make_shared<db::config>();
    db_config->unspooled_dirty_soft_limit.set(1.0);

    return do_with_cql_env_thread([](cql_test_env& env) {
        replica::database& db = env.local_db();
        service::migration_manager& mm = env.migration_manager().local();

        simple_schema ss;
        schema_ptr s = ss.schema();
        auto group0_guard = mm.start_group0_operation().get();
        auto ts = group0_guard.write_timestamp();
        mm.announce(service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s, ts).get(), std::move(group0_guard), "").get();

        replica::table& t = db.find_column_family("ks", "cf");
        auto memtables = active_memtables(t);

        // Insert something so that we have data in memtable to flush
        // it has to be somewhat large, as automatic flushing picks the
        // largest memtable to flush
        mutation mt = {s, tests::generate_partition_key(s)};
        for (uint32_t i = 0; i < 1000; ++i) {
            ss.add_row(mt, ss.make_ckey(i), format("{}", i));
        }
        t.apply(mt);

        auto failed_memtables_flushes_count = db.cf_stats()->failed_memtables_flushes_count;

        utils::get_local_injector().enable("table_seal_active_memtable_add_memtable", true /* oneshot */);
        utils::get_local_injector().enable("table_seal_active_memtable_start_op", true /* oneshot */);
        utils::get_local_injector().enable("table_seal_active_memtable_try_flush", true /* oneshot */);
        utils::get_local_injector().enable("table_seal_active_memtable_reacquire_write_permit");

        // Trigger flush
        auto f = t.flush();

        BOOST_REQUIRE(eventually_true([&] {
            return db.cf_stats()->failed_memtables_flushes_count - failed_memtables_flushes_count >= 4;
        }));

        // The flush failed, make sure there is still data in memtable.
        BOOST_REQUIRE_LT(t.min_memtable_timestamp(), api::max_timestamp);
        utils::get_local_injector().disable("table_seal_active_memtable_reacquire_write_permit");

        BOOST_REQUIRE(eventually_true([&] {
            // The error above is no longer being injected, so
            // seal_active_memtable retry loop should eventually succeed
            return t.min_memtable_timestamp() == api::max_timestamp;
        }));

        std::move(f).get();
    }, db_config);
#endif
}

SEASTAR_TEST_CASE(flushing_rate_is_reduced_if_compaction_doesnt_keep_up) {
    BOOST_ASSERT(smp::count == 2);
    // The test simulates a situation where 2 threads issue flushes to 2
    // tables. Both issue small flushes, but one has injected reactor stalls.
    // This can lead to a situation where lots of small sstables accumulate on
    // disk, and, if compaction never has a chance to keep up, resources can be
    // exhausted.
    return do_with_cql_env([](cql_test_env& env) -> future<> {
        struct flusher {
            cql_test_env& env;
            const int num_flushes;
            const int sleep_ms;

            static sstring cf_name(unsigned thread_id) {
                return format("cf_{}", thread_id);
            }

            static sstring ks_name() {
                return "ks";
            }

            future<> create_table(schema_ptr s) {
                return env.migration_manager().invoke_on(0, [s = global_schema_ptr(std::move(s))] (service::migration_manager& mm) -> future<> {
                    auto group0_guard = co_await mm.start_group0_operation();
                    auto ts = group0_guard.write_timestamp();
                    auto announcement = co_await service::prepare_new_column_family_announcement(mm.get_storage_proxy(), s, ts);
                    co_await mm.announce(std::move(announcement), std::move(group0_guard), "");
                });
            }

            future<> drop_table() {
                return env.migration_manager().invoke_on(0, [shard = this_shard_id()] (service::migration_manager& mm) -> future<> {
                    auto group0_guard = co_await mm.start_group0_operation();
                    auto ts = group0_guard.write_timestamp();
                    auto announcement = co_await service::prepare_column_family_drop_announcement(mm.get_storage_proxy(), ks_name(), cf_name(shard), ts);
                    co_await mm.announce(std::move(announcement), std::move(group0_guard), "");
                });
            }

            future<> operator()() {
                const sstring ks_name = this->ks_name();
                const sstring cf_name = this->cf_name(this_shard_id());
                random_mutation_generator gen{
                    random_mutation_generator::generate_counters::no,
                    local_shard_only::yes,
                    random_mutation_generator::generate_uncompactable::no,
                    std::nullopt,
                    ks_name.c_str(),
                    cf_name.c_str()
                };
                schema_ptr s = gen.schema();

                co_await create_table(s);
                replica::database& db = env.local_db();
                replica::table& t = db.find_column_family(ks_name, cf_name);

                for ([[maybe_unused]] int value : boost::irange<int>(0, num_flushes)) {
                    ::usleep(sleep_ms * 1000);
                    co_await db.apply(t.schema(), freeze(gen()), tracing::trace_state_ptr(), db::commitlog::force_sync::yes, db::no_timeout);
                    co_await t.flush();
                    BOOST_ASSERT(t.sstables_count() < size_t(t.schema()->max_compaction_threshold() * 2));
                }
                co_await drop_table();
            }
        };

        int sleep_ms = 2;
        for ([[maybe_unused]] int i : boost::irange<int>(8)) {
            future<> f0 = smp::submit_to(0, flusher{.env=env, .num_flushes=100, .sleep_ms=0});
            future<> f1 = smp::submit_to(1, flusher{.env=env, .num_flushes=3, .sleep_ms=sleep_ms});
            co_await std::move(f0);
            co_await std::move(f1);
            sleep_ms *= 2;
        }
    });
}

static future<> exceptions_in_flush_helper(std::unique_ptr<sstables::file_io_extension> mep, bool& should_fail, const bool& did_fail, bool expect_isolate) {
    auto ext = std::make_shared<db::extensions>();
    auto cfg = seastar::make_shared<db::config>(ext);

    ext->add_sstable_file_io_extension("test", std::move(mep));

    co_await do_with_cql_env([&](cql_test_env& env) -> future<> {

        co_await env.execute_cql(fmt::format("create table t0 (pk text primary key, v text)"));

        should_fail = true;

        int i = 0;

        testlog.debug("Wait for fail");

        auto f = make_ready_future<>();

        while (!did_fail) {
            std::string pk = "apa" + std::to_string(i++);
            std::string v = "ko";
            co_await env.execute_cql(fmt::format("insert into ks.t0 (pk, v) values ('{}', '{}')", pk, v));

            f = f.then([&] {
                return env.db().invoke_on_all([] (replica::database& db) {
                    return db.flush_all_memtables();
                });
            });
        }

        BOOST_REQUIRE(did_fail);
        testlog.debug("Reset fail trigger");

        should_fail = false;

        if (expect_isolate) {
            bool isolated = false;
            // can't use eventually_true here, because neither we nor the invoke on shard 0 is in seastar
            // thread.
            for (int i = 0; i < 10; ++i) {
                isolated = co_await env.get_storage_service().invoke_on(0, [&](service::storage_service& ss) {
                    return ss.is_isolated(); 
                });
                if (isolated) {
                    break;
                }
                // isolation is not syncnronous;
                co_await sleep(2s);
            }

            BOOST_REQUIRE(isolated);
        }

        testlog.debug("Trying to stop");

        co_await std::move(f);
    }, cfg);
}

static future<> exceptions_in_flush_on_sstable_write_helper(std::function<void()> throw_func, bool expect_isolate = true) {
    class myext : public sstables::file_io_extension {
    public:
        bool should_fail = false;
        bool did_fail = false;
        std::function<void()> throw_func;

        future<file> wrap_file(sstable& t, component_type type, file f, open_flags flags) override {
            if (should_fail) {
                class myimpl : public seastar::file_impl {
                    file _file;
                    myext& _myext;
                public:
                    myimpl(file f, myext& ext)
                        : _file(std::move(f))
                        , _myext(ext)
                    {}
                    void fail() const {
                        if (_myext.should_fail) {
                            _myext.did_fail = true;
                            testlog.debug("Throwing exception");
                            _myext.throw_func();
                        }
                    }
                    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
                        fail();
                        return get_file_impl(_file)->write_dma(pos, buffer, len, intent);
                    }
                    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
                        fail();
                        return get_file_impl(_file)->write_dma(pos, std::move(iov), intent);
                    }
                    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
                        fail();
                        return get_file_impl(_file)->read_dma(pos, buffer, len, intent);
                    }
                    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
                        fail();
                        return get_file_impl(_file)->read_dma(pos, std::move(iov), intent);
                    }
                    future<> flush(void) override {
                        fail();
                        return get_file_impl(_file)->flush();
                    }
                    future<struct stat> stat(void) override {
                        fail();
                        return get_file_impl(_file)->stat();
                    }
                    future<> truncate(uint64_t length) override {
                        fail();
                        return get_file_impl(_file)->truncate(length);
                    }
                    future<> discard(uint64_t offset, uint64_t length) override {
                        fail();
                        return get_file_impl(_file)->discard(offset, length);
                    }
                    future<> allocate(uint64_t position, uint64_t length) override {
                        fail();
                        return get_file_impl(_file)->allocate(position, length);
                    }
                    future<uint64_t> size(void) override {
                        fail();
                        return get_file_impl(_file)->size();
                    }
                    future<> close() override {
                        fail();
                        return get_file_impl(_file)->close();
                    }
                    std::unique_ptr<seastar::file_handle_impl> dup() override {
                        fail();
                        return get_file_impl(_file)->dup();
                    }
                    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
                        fail();
                        return get_file_impl(_file)->list_directory(std::move(next));
                    }
                    future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
                        fail();
                        return get_file_impl(_file)->dma_read_bulk(offset, range_size, intent);
                    }
                };
                co_return file(make_shared<myimpl>(std::move(f), *this));
            }
            co_return f;
        }
    };
    auto mep = std::make_unique<myext>();
    auto& me = *mep;
    me.throw_func = std::move(throw_func);
    co_await exceptions_in_flush_helper(std::move(mep), me.should_fail, me.did_fail, expect_isolate);
}

SEASTAR_TEST_CASE(test_exceptions_in_flush_on_sstable_write) {
    co_await exceptions_in_flush_on_sstable_write_helper(
        [] { throw std::system_error(EACCES, std::system_category()); }
    );
}

SEASTAR_TEST_CASE(test_ext_permission_exceptions_in_flush_on_sstable_write) {
    co_await exceptions_in_flush_on_sstable_write_helper(
        [] { throw db::extension_storage_permission_error(get_name()); }
    );
}

SEASTAR_TEST_CASE(test_ext_resource_exceptions_in_flush_on_sstable_write) {
    co_await exceptions_in_flush_on_sstable_write_helper(
        [] { throw db::extension_storage_resource_unavailable(get_name()); }
        , false // equal no ENOENT
    );
}

SEASTAR_TEST_CASE(test_ext_config_exceptions_in_flush_on_sstable_write) {
    co_await exceptions_in_flush_on_sstable_write_helper(
        [] { throw db::extension_storage_misconfigured(get_name()); }
    );
}

static future<> exceptions_in_flush_on_sstable_open_helper(std::function<void()> throw_func, bool expect_isolate = true) {
    auto ext = std::make_shared<db::extensions>();
    auto cfg = seastar::make_shared<db::config>(ext);

    class myext : public sstables::file_io_extension {
    public:
        bool should_fail = false;
        bool did_fail = false;
        std::function<void()> throw_func;

        future<file> wrap_file(sstable& t, component_type type, file f, open_flags flags) override {
            if (should_fail) {
                did_fail = true;
                testlog.debug("Throwing exception");
                throw_func();
            }
            co_return f;
        }
    };
    auto mep = std::make_unique<myext>();
    auto& me = *mep;
    me.throw_func = std::move(throw_func);;
    co_await exceptions_in_flush_helper(std::move(mep), me.should_fail, me.did_fail, expect_isolate);
}

SEASTAR_TEST_CASE(test_exceptions_in_flush_on_sstable_open) {
    co_await exceptions_in_flush_on_sstable_open_helper(
        [] { throw std::system_error(EACCES, std::system_category()); }
    );
}

SEASTAR_TEST_CASE(test_ext_permission_exceptions_in_flush_on_sstable_open) {
    co_await exceptions_in_flush_on_sstable_open_helper(
        [] { throw db::extension_storage_permission_error(get_name()); }
    );
}

SEASTAR_TEST_CASE(test_ext_resource_exceptions_in_flush_on_sstable_open) {
    co_await exceptions_in_flush_on_sstable_open_helper(
        [] { throw db::extension_storage_resource_unavailable(get_name()); }
        , false // equal no ENOENT
    );
}

SEASTAR_TEST_CASE(test_ext_config_exceptions_in_flush_on_sstable_open) {
    co_await exceptions_in_flush_on_sstable_open_helper(
        [] { throw db::extension_storage_misconfigured(get_name()); }
    );
}
