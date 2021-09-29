/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <seastar/core/distributed.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>

#include "utils/managed_bytes.hh"
#include "utils/logalloc.hh"
#include "utils/UUID_gen.hh"
#include "row_cache.hh"
#include "partition_slice_builder.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include "test/perf/perf.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/simple_schema.hh"
#include "querier.hh"
#include "types.hh"
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

    auto s = schema_builder("ks", "cf")
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
            rd.consume_pausable([](mutation_fragment) {
                return stop_iteration(cancelled);
            }).get();
        });
        slm.stop();

        std::cout << format("read: {:.6f} [ms], preemption: {}, cache: {:d}/{:d} [MB]\n",
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
            const io_priority_class& pc,
            tracing::trace_state_ptr trace,
            streamed_mutation::forwarding fwd) {
        return cache.make_reader(s, permit, range, slice, pc, std::move(trace), std::move(fwd));
    });

    std::cout << "Rows: " << ck_index << std::endl;
    std::cout << "Scanning..." << std::endl;

    auto test_read = [&] {
        scheduling_latency_measurer slm;
        slm.start();

        auto d = duration_in_seconds([&] {
            auto slice = partition_slice_builder(*s).build();
            auto q = query::querier<emit_only_live_rows::yes>(cache_ms, s, semaphore.make_permit(), pr, slice,
                                                              default_priority_class(), nullptr);
            auto close_q = deferred_close(q);
            q.consume_page(noop_compacted_fragments_consumer(),
                           std::numeric_limits<uint32_t>::max(),
                           std::numeric_limits<uint32_t>::max(),
                           gc_clock::now()).get();
        });

        slm.stop();

        std::cout << format("read: {:.6f} [ms], preemption: {}, cache: {:d}/{:d} [MB]\n",
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

int main(int argc, char** argv) {
    app_template app;
    return app.run(argc, argv, [&app] {
        return seastar::async([&] {
            engine().at_exit([] {
                cancelled = true;
                return make_ready_future();
            });
            logalloc::prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
            test_scans_with_dummy_entries();
            test_scan_with_range_delete_over_rows();
        });
    });
}
