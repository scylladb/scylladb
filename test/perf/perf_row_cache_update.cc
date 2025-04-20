/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "utils/UUID_gen.hh"
#include "db/row_cache.hh"
#include "utils/log.hh"
#include "schema/schema_builder.hh"
#include "readers/combined.hh"
#include "readers/mutation_fragment_v1_stream.hh"
#include "replica/memtable.hh"
#include "dht/i_partitioner.hh"
#include "test/perf/perf.hh"
#include "test/lib/reader_concurrency_semaphore.hh"

static const int update_iterations = 16;
static const int cell_size = 128;
static bool cancelled = false;

template<typename MutationGenerator>
void run_test(const sstring& name, schema_ptr s, MutationGenerator&& gen) {
    tests::reader_concurrency_semaphore_wrapper semaphore;
    cache_tracker tracker;
    row_cache cache(s, make_empty_snapshot_source(), tracker, is_continuous::yes);

    size_t memtable_size = seastar::memory::stats().total_memory() / 4;

    std::cout << name << ":" << std::endl;

    for (int i = 0; i < update_iterations; ++i) {
        auto MB = 1024 * 1024;
        const auto prefill_stats = logalloc::shard_tracker().statistics();
        auto prefill_compacted = prefill_stats.memory_compacted;
        auto prefill_allocated = prefill_stats.memory_allocated;

        scheduling_latency_measurer memtable_slm;
        memtable_slm.start();

        auto mt = make_lw_shared<replica::memtable>(s);
        auto fill_d = duration_in_seconds([&] {
            while (mt->occupancy().total_space() < memtable_size) {
                mutation m = gen();
                mt->apply(m);
                seastar::thread::maybe_yield();
                if (cancelled) {
                    return;
                }
            }
        });
        memtable_slm.stop();
        std::cout << format("Memtable fill took {:.6f} [ms], {}", fill_d.count() * 1000, memtable_slm) << std::endl;

        std::cout << "Draining..." << std::endl;
        auto drain_d = duration_in_seconds([&] {
            mt->cleaner().drain().get();
        });
        std::cout << format("took {:.6f} [ms]", drain_d.count() * 1000) << std::endl;

        const auto prev_stats = logalloc::shard_tracker().statistics();
        auto prev_compacted = prev_stats.memory_compacted;
        auto prev_allocated = prev_stats.memory_allocated;
        auto prev_rows_processed_from_memtable = tracker.get_stats().rows_processed_from_memtable;
        auto prev_rows_merged_from_memtable = tracker.get_stats().rows_merged_from_memtable;
        auto prev_rows_dropped_from_memtable = tracker.get_stats().rows_dropped_from_memtable;

        std::cout << format("cache: {:d}/{:d} [MB], memtable: {:d}/{:d} [MB], alloc/comp: {:d}/{:d} [MB] (amp: {:.3f})",
            tracker.region().occupancy().used_space() / MB,
            tracker.region().occupancy().total_space() / MB,
            mt->occupancy().used_space() / MB,
            mt->occupancy().total_space() / MB,
            (prev_allocated - prefill_allocated) / MB,
            (prev_compacted - prefill_compacted) / MB,
            float((prev_compacted - prefill_compacted)) / (prev_allocated - prefill_allocated)) << std::endl;

        auto permit = semaphore.make_permit();
        // Create a reader which tests the case of memtable snapshots
        // going away after memtable was merged to cache.
        auto rd = std::make_unique<mutation_fragment_v1_stream>(
            mutation_fragment_v1_stream(make_combined_reader(s, permit, cache.make_reader(s, permit), mt->make_mutation_reader(s, permit))));
        auto close_rd = defer([&rd] { rd->close().get(); });
        rd->set_max_buffer_size(1);
        rd->fill_buffer().get();

        scheduling_latency_measurer slm;
        slm.start();
        auto d = duration_in_seconds([&] {
            cache.update(row_cache::external_updater([] {}), *mt).get();
        });

        rd->set_max_buffer_size(1024*1024);
        rd->consume_pausable([] (mutation_fragment) {
            return stop_iteration::no;
        }).get();

        mt = {};

        close_rd.cancel();
        rd->close().get();
        rd = {};

        slm.stop();

        const auto stats = logalloc::shard_tracker().statistics();
        auto compacted = stats.memory_compacted - prev_compacted;
        auto allocated = stats.memory_allocated - prev_allocated;

        std::cout << format("update: {:.6f} [ms], preemption: {}, cache: {:d}/{:d} [MB], alloc/comp: {:d}/{:d} [MB] (amp: {:.3f}), pr/me/dr {:d}/{:d}/{:d}\n",
            d.count() * 1000,
            slm,
            tracker.region().occupancy().used_space() / MB,
            tracker.region().occupancy().total_space() / MB,
            allocated / MB, compacted / MB, float(compacted)/allocated,
            tracker.get_stats().rows_processed_from_memtable - prev_rows_processed_from_memtable,
            tracker.get_stats().rows_merged_from_memtable - prev_rows_merged_from_memtable,
            tracker.get_stats().rows_dropped_from_memtable - prev_rows_dropped_from_memtable);
    }

    scheduling_latency_measurer invalidate_slm;
    invalidate_slm.start();
    auto d = duration_in_seconds([&] {
        cache.invalidate(row_cache::external_updater([] {})).get();
    });
    invalidate_slm.stop();

    std::cout << format("invalidation: {:.6f} [ms], preemption: {}", d.count() * 1000, invalidate_slm) << "\n";
}

static void test_small_partitions() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    run_test("Small partitions, no overwrites", s, [&] {
        auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s,
            serialized(utils::UUID_gen::get_time_UUID())));
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        m.set_clustered_cell(clustering_key::make_empty(), "v1", val, api::new_timestamp());
        m.set_clustered_cell(clustering_key::make_empty(), "v2", val, api::new_timestamp());
        m.set_clustered_cell(clustering_key::make_empty(), "v3", val, api::new_timestamp());
        return m;
    });
}

static void test_partition_with_lots_of_small_rows() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s,
        serialized(utils::UUID_gen::get_time_UUID())));
    int ck_idx = 0;

    run_test("Large partition, lots of small rows", s, [&] {
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        auto ck = clustering_key::from_single_value(*s, serialized(ck_idx++));
        m.set_clustered_cell(ck, "v1", val, api::new_timestamp());
        m.set_clustered_cell(ck, "v2", val, api::new_timestamp());
        m.set_clustered_cell(ck, "v3", val, api::new_timestamp());
        return m;
    });
}

static void test_partition_with_few_small_rows() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    run_test("Small partition with a few rows", s, [&] {
        auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s,
            serialized(utils::UUID_gen::get_time_UUID())));

        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));

        for (int i = 0; i < 3; ++i) {
            auto ck = clustering_key::from_single_value(*s, serialized(i));
            m.set_clustered_cell(ck, "v1", val, api::new_timestamp());
            m.set_clustered_cell(ck, "v2", val, api::new_timestamp());
            m.set_clustered_cell(ck, "v3", val, api::new_timestamp());
        }
        return m;
    });
}

static void test_partition_with_lots_of_range_tombstones() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s,
        serialized(utils::UUID_gen::get_time_UUID())));
    int ck_idx = 0;

    run_test("Large partition, lots of range tombstones", s, [&] {
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        auto ck = clustering_key::from_single_value(*s, serialized(ck_idx++));
        auto r = query::clustering_range::make({ck}, {ck});
        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_row_tombstone(*s, range_tombstone(bound_view::from_range_start(r), bound_view::from_range_end(r), tomb));
        return m;
    });
}

// This test case stresses handling of overlapping range tombstones
static void test_partition_with_lots_of_range_tombstones_with_residuals() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", int32_type, column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s,
        serialized(utils::UUID_gen::get_time_UUID())));
    int ck_idx = 0;

    run_test("Large partition, lots of range tombstones with residuals", s, [&] {
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        auto ck = clustering_key::from_single_value(*s, serialized(ck_idx++));
        auto r = query::clustering_range::make({ck}, {ck});
        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_row_tombstone(*s, range_tombstone(bound_view::from_range_start(r), bound_view::top(), tomb));

        // Stress range tombstone overlapping with lots of range tombstones
        auto stride = 1'000'000;
        if (ck_idx == stride) {
            ck = clustering_key::from_single_value(*s, serialized(ck_idx - stride));
            r = query::clustering_range::make({ck}, {ck});
            m.partition().apply_row_tombstone(*s, range_tombstone(bound_view::from_range_start(r), bound_view::top(), tomb));
        }

        return m;
    });
}

namespace perf {

int scylla_row_cache_update_main(int argc, char** argv) {
    app_template app;
    return app.run(argc, argv, [] {
        return seastar::async([&] {
            auto stop_test = defer([] {
                cancelled = true;
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            test_small_partitions();
            test_partition_with_few_small_rows();
            test_partition_with_lots_of_small_rows();
            test_partition_with_lots_of_range_tombstones();
            test_partition_with_lots_of_range_tombstones_with_residuals();
        });
    });
}

} // namespace perf
