/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/sharded.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "utils/UUID_gen.hh"
#include "db/row_cache.hh"
#include "partition_slice_builder.hh"
#include "schema/schema_builder.hh"
#include "replica/memtable.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include "test/perf/perf.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/simple_schema.hh"
#include "replica/querier.hh"
#include "query/query-result-writer.hh"
#include "types/types.hh"
#include "types/set.hh"
#include "mutation/collection_mutation.hh"
#include "reader_concurrency_semaphore.hh"

/// Tests read scenarios from cache.
///
/// Watch out for max preemption period growing above 0.5 ms.
///
/// Example run:
///
///    $ build/release/test/perf/perf_row_cache_reads_g  -c1 -m200M
///    Filling memtable
///    rows in cache: 0
///    Populating with dummy rows
///    rows in cache: 373929
///    Scanning
///    read: 156.288986 [ms], preemption: {count: 702, 99%: 0.545791 [ms], max: 0.537537 [ms]}, cache: 99/100 [MB]
///    read: 106.480766 [ms], preemption: {count: 6, 99%: 0.006866 [ms], max: 106.496168 [ms]}, cache: 99/100 [MB]
///
/// The second row which starts with "read:" has high max latency (106 ms),
/// which is an indication of the following bug: https://github.com/scylladb/scylla/issues/8153
///

static const int cell_size = 128;
static bool cancelled = false;
static const auto MB = 1024 * 1024;

void test_scans_with_dummy_entries() {
    std::cout << __FUNCTION__<< std::endl;

    auto s = schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", uuid_type, column_kind::partition_key)
            .with_column("st", bytes_type, column_kind::static_column)
            .with_column("ck", reversed_type_impl::get_instance(uuid_type), column_kind::clustering_key)
            .with_column("v1", bytes_type, column_kind::regular_column)
            .build();
    tests::reader_concurrency_semaphore_wrapper semaphore;

    auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s,
                                            serialized(utils::UUID_gen::get_time_UUID())));

    cache_tracker tracker;
    memtable_snapshot_source mss(s);

    auto make_random_ck = [&] {
        return clustering_key::from_single_value(*s, serialized(utils::make_random_uuid()));
    };

    auto val = data_value(bytes(bytes::initialized_later(), cell_size));

    // Make the partition non-empty so that it's not marked as continuous by the first read
    // on population.
    mutation m(s, pk);
    m.set_static_cell("st", val, api::new_timestamp());
    mss.apply(m);

    row_cache cache(s, snapshot_source([&] { return mss(); }), tracker, is_continuous::no);

    std::cout << "Rows in cache: " << tracker.get_stats().rows << std::endl;
    std::cout << "Populating with dummy rows" << std::endl;

    const size_t cache_size = seastar::memory::stats().total_memory() / 2;
    auto pr = dht::partition_range::make_singular(pk);
    while (tracker.region().occupancy().total_space() < cache_size) {
        auto slice = partition_slice_builder(*s)
                .with_range(query::clustering_range::make_starting_with(make_random_ck()))
                .build();

        auto rd = cache.make_reader(s, semaphore.make_permit(), pr, slice);
        auto close_reader = deferred_close(rd);
        rd.set_max_buffer_size(1);
        rd.fill_buffer().get();
        seastar::thread::maybe_yield();

        if (cancelled) {
            return;
        }
    }

    std::cout << "Rows in cache: " << tracker.get_stats().rows << std::endl;
    std::cout << "Scanning" << std::endl;

    auto test_read = [&] {
        auto rd = cache.make_reader(s, semaphore.make_permit(), pr);
        auto close_reader = deferred_close(rd);
        scheduling_latency_measurer slm;
        slm.start();
        auto d = duration_in_seconds([&] {
            rd.consume_pausable([](mutation_fragment_v2) {
                return stop_iteration(cancelled);
            }).get();
        });
        slm.stop();

        fmt::print(std::cout, "read: {:.6f} [ms], preemption: {}, cache: {:d}/{:d} [MB]\n",
                   d.count() * 1000,
                   slm,
                   tracker.region().occupancy().used_space() / MB,
                   tracker.region().occupancy().total_space() / MB);
    };

    // The first scan populates the cache with continuity
    // It reads from underlying so will be preemptible.
    test_read();

    // If dummy entries are not removed by the first scan, the second scan will hit
    // https://github.com/scylladb/scylla/issues/8153
    test_read();

    // Clean gently to avoid reactor stalls in destructors
    cache.invalidate(row_cache::external_updater([]{})).get();
    tracker.cleaner().drain().get();
}

void test_scan_with_range_delete_over_rows() {
    std::cout << __FUNCTION__<< std::endl;

    simple_schema ss;
    auto s = ss.schema();
    tests::reader_concurrency_semaphore_wrapper semaphore;

    cache_tracker tracker;
    memtable_snapshot_source mss(s);

    auto pk = ss.make_pkey(0);
    auto val = sstring(sstring::initialized_later(), cell_size);

    std::cout << "Populating with rows" << std::endl;

    const size_t cache_size = seastar::memory::stats().total_memory() / 4;
    auto pr = dht::partition_range::make_singular(pk);
    size_t ck_index = 0;
    while (mss.used_space() < cache_size) {
        mutation m(s, pk);
        ss.add_row(m, ss.make_ckey(ck_index++), val);
        mss.apply(m);

        if (cancelled) {
            return;
        }
    }

    mutation m(s, pk);
    ss.delete_range(m, ss.make_ckey_range(0, ck_index));
    mss.apply(m);

    row_cache cache(s, snapshot_source([&] { return mss(); }), tracker, is_continuous::no);

    auto cache_ms = mutation_source([&] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace,
            streamed_mutation::forwarding fwd) {
        return cache.make_reader(s, permit, range, slice, std::move(trace), std::move(fwd));
    });

    std::cout << "Rows: " << ck_index << std::endl;
    std::cout << "Scanning..." << std::endl;

    auto test_read = [&] {
        scheduling_latency_measurer slm;
        slm.start();

        auto d = duration_in_seconds([&] {
            auto slice = partition_slice_builder(*s).build();
            auto q = replica::querier(cache_ms, s, semaphore.make_permit(), pr, slice, nullptr, tombstone_gc_state::for_tests());
            auto close_q = deferred_close(q);
            q.consume_page(noop_compacted_fragments_consumer(),
                           std::numeric_limits<uint32_t>::max(),
                           std::numeric_limits<uint32_t>::max(),
                           gc_clock::now()).get();
        });

        slm.stop();

        fmt::print(std::cout, "read: {:.6f} [ms], preemption: {}, cache: {:d}/{:d} [MB]\n",
                   d.count() * 1000,
                   slm,
                   tracker.region().occupancy().used_space() / MB,
                   tracker.region().occupancy().total_space() / MB);
    };

    // The first scan populates the cache with continuity
    // It reads from underlying so will be preemptible.
    test_read();

    // This should be a pure cache scan.
    test_read();

    // Clean gently to avoid reactor stalls in destructors
    cache.invalidate(row_cache::external_updater([]{})).get();
    tracker.cleaner().drain().get();
}

// Point reads of single-row partitions resident in cache, through the replica query path.
// With digest, results are built as for a CL>ONE data read (result_and_digest).
void test_point_reads(unsigned columns, size_t value_size, unsigned collection_elements, bool digest) {
    // Keep the total number of collection elements at or below 10M.
    const int partitions = collection_elements ? std::min<size_t>(10000, 10'000'000 / collection_elements) : 10000;
    std::cout << __FUNCTION__ << " columns=" << columns << " value_size=" << value_size << " collection_elements=" << collection_elements
              << " partitions=" << partitions << " digest=" << digest << std::endl;

    auto sb = schema_builder(this_smp_shard_count(), "ks", "cf")
            .with_column("pk", int32_type, column_kind::partition_key);
    for (unsigned i = 0; i < columns; ++i) {
        sb.with_column(to_bytes(format("v{}", i)), bytes_type, column_kind::regular_column);
    }
    if (collection_elements) {
        sb.with_column("coll", set_type_impl::get_instance(bytes_type, true), column_kind::regular_column);
    }
    auto s = sb.build();
    tests::reader_concurrency_semaphore_wrapper semaphore;

    cache_tracker tracker;
    memtable_snapshot_source mss(s);

    std::optional<collection_mutation> coll;
    if (collection_elements) {
        collection_mutation_writer w({});
        for (unsigned i = 0; i < collection_elements; ++i) {
            auto e = int32_type->decompose(int32_t(i)); // big-endian, already sorted
            e.resize(32);
            w.push_back(managed_bytes_view(e), atomic_cell::make_live(*empty_type, 1 + i % 1000, bytes_view(), atomic_cell::collection_member::yes));
        }
        coll.emplace(std::move(w).finish());
    }
    std::vector<dht::partition_range> ranges;
    auto val = data_value(tests::random::get_bytes(value_size));
    for (int i = 0; i < partitions; ++i) {
        auto pk = dht::decorate_key(*s, partition_key::from_single_value(*s, serialized(i)));
        mutation m(s, pk);
        for (unsigned c = 0; c < columns; ++c) {
            m.set_clustered_cell(clustering_key::make_empty(), to_bytes(format("v{}", c)), val, api::new_timestamp());
        }
        if (coll) {
            m.set_clustered_cell(clustering_key::make_empty(), *s->get_column_definition(to_bytes("coll")),
                    atomic_cell_or_collection(collection_mutation(collection_mutation_view(*coll))));
        }
        mss.apply(m);
        ranges.push_back(dht::partition_range::make_singular(pk));
    }

    row_cache cache(s, snapshot_source([&] { return mss(); }), tracker, is_continuous::no);
    auto cache_ms = mutation_source([&] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& range,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace,
            streamed_mutation::forwarding fwd) {
        return cache.make_reader(s, permit, range, slice, std::move(trace), std::move(fwd));
    });

    auto sl = partition_slice_builder(*s);
    if (digest) {
        sl.with_option<query::partition_slice::option::with_digest>();
    }
    const auto slice = sl.build();
    const auto opts = digest
            ? query::result_options{query::result_request::result_and_digest, query::digest_algorithm::xxHash}
            : query::result_options::only_result();
    // 64 MiB covers the largest case (a ~4 MB row) with short reads off.
    const auto max_size = query::max_result_size(64 << 20);
    query::result_memory_limiter limiter(max_size.hard_limit * 100);

    auto read = [&] (const dht::partition_range& pr) -> future<> {
        query::result::builder rb(slice, opts, co_await limiter.new_data_read(max_size, query::short_read::no), query::max_tombstones);
        auto q = replica::querier(cache_ms, s, semaphore.make_permit(), pr, slice, nullptr, tombstone_gc_state::for_tests());
        std::exception_ptr ex;
        try {
            co_await q.consume_page(query_result_builder(*s, rb), query::max_rows, query::max_partitions, gc_clock::now());
        } catch (...) {
            ex = std::current_exception();
        }
        co_await q.close();
        if (ex) {
            std::rethrow_exception(ex);
        }
        rb.build();
    };

    // Populate the cache using the same slice as the measured reads.
    for (const auto& pr : ranges) {
        read(pr).get();
    }
    auto misses = cache.stats().misses.count();
    std::cout << "Rows in cache: " << tracker.get_stats().rows << std::endl;

    size_t idx = 0;
    auto results = time_parallel([&] {
        idx = (idx + 1) % ranges.size();
        return read(ranges[idx]);
    }, 1, 5);
    std::cout << aggregated_perf_results(results) << std::endl;
    std::cout << "Cache misses during measurement: " << cache.stats().misses.count() - misses << std::endl;

    cache.invalidate(row_cache::external_updater([]{})).get();
    tracker.cleaner().drain().get();
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("point-reads", "run only the cached point-read benchmark")
        ("digest", "request a digest along with point-read results")
        ("columns", bpo::value<unsigned>()->default_value(5), "regular columns for point reads")
        ("collection-elements", bpo::value<unsigned>()->default_value(0), "elements of a non-frozen set column for point reads (0: none)")
        ("value-size", bpo::value<size_t>()->default_value(8), "bytes per cell value for point reads");
    return app.run(argc, argv, [&app] {
        return seastar::async([&] {
            auto stop_test = defer([] noexcept {
                cancelled = true;
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            auto& cfg = app.configuration();
            if (cfg.contains("point-reads")) {
                test_point_reads(cfg["columns"].as<unsigned>(), cfg["value-size"].as<size_t>(), cfg["collection-elements"].as<unsigned>(), cfg.contains("digest"));
                return;
            }
            test_scans_with_dummy_entries();
            test_scan_with_range_delete_over_rows();
        });
    });
}
