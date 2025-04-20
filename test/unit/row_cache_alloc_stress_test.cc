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
#include <seastar/util/closeable.hh>

#include "utils/assert.hh"
#include "utils/logalloc.hh"
#include "db/row_cache.hh"
#include "utils/log.hh"
#include "schema/schema_builder.hh"
#include "replica/memtable.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "dht/i_partitioner.hh"

static
partition_key new_key(schema_ptr s) {
    static thread_local int next = 0;
    return partition_key::from_single_value(*s, to_bytes(format("key{:d}", next++)));
}

static
clustering_key new_ckey(schema_ptr s) {
    static thread_local int next = 0;
    return clustering_key::from_single_value(*s, to_bytes(format("ckey{:d}", next++)));
}

void *leak;

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("debug", "enable debug logging");

    return app.run(argc, argv, [&app] {
        if (app.configuration().contains("debug")) {
            logging::logger_registry().set_all_loggers_level(logging::log_level::debug);
        }

        // This test is supposed to verify that when we're low on memory but
        // we still have plenty of evictable memory in cache, we should be
        // able to populate cache with large mutations This test works only
        // with seastar's allocator.
        return seastar::async([] {
            auto s = schema_builder("ks", "cf")
                .with_column("pk", bytes_type, column_kind::partition_key)
                .with_column("ck", bytes_type, column_kind::clustering_key)
                .with_column("v", bytes_type, column_kind::regular_column)
                .build();
            tests::reader_concurrency_semaphore_wrapper semaphore;

            cache_tracker tracker;
            row_cache cache(s, make_empty_snapshot_source(), tracker);

            auto mt = make_lw_shared<replica::memtable>(s);
            std::vector<dht::decorated_key> keys;

            size_t cell_size = 1024;
            size_t row_count = 40 * 1024; // 40M mutations
            size_t large_cell_size = cell_size * row_count;

            auto make_small_mutation = [&] {
                mutation m(s, new_key(s));
                m.set_clustered_cell(new_ckey(s), "v", data_value(bytes(bytes::initialized_later(), cell_size)), 1);
                return m;
            };

            auto make_large_mutation = [&] {
                mutation m(s, new_key(s));
                m.set_clustered_cell(new_ckey(s), "v", data_value(bytes(bytes::initialized_later(), large_cell_size)), 2);
                return m;
            };

            std::random_device random;
            std::default_random_engine random_engine(random());

            for (int i = 0; i < 10; i++) {
                auto key = dht::decorate_key(*s, new_key(s));

                mutation m1(s, key);
                m1.set_clustered_cell(new_ckey(s), "v", data_value(bytes(bytes::initialized_later(), cell_size)), 1);
                cache.populate(m1);

                // Putting large mutations into the memtable. Should take about row_count*cell_size each.
                mutation m2(s, key);
                for (size_t j = 0; j < row_count; j++) {
                    m2.set_clustered_cell(new_ckey(s), "v", data_value(bytes(bytes::initialized_later(), cell_size)), 2);
                }

                mt->apply(m2);
                keys.push_back(key);
            }

            auto reclaimable_memory = [] {
                return memory::stats().free_memory() + logalloc::shard_tracker().occupancy().free_space();
            };

            fmt::print("memtable occupancy: {}\n", mt->occupancy());
            fmt::print("Cache occupancy: {}\n", tracker.region().occupancy());
            fmt::print("Reclaimable memory: {}\n", reclaimable_memory());

            // We need to have enough Free memory to copy memtable into cache
            // When this assertion fails, increase amount of memory
            SCYLLA_ASSERT(mt->occupancy().used_space() < reclaimable_memory());

            std::deque<dht::decorated_key> cache_stuffing;
            auto fill_cache_to_the_top = [&] {
                std::cout << "Filling up memory with evictable data\n";
                // Ensure that entries matching memtable partitions are not evicted,
                // we want to hit the merge path in row_cache::update()
                for (auto&& key : keys) {
                    cache.unlink_from_lru(key);
                }
                while (true) {
                    auto evictions_before = tracker.get_stats().partition_evictions;
                    auto m = make_small_mutation();
                    cache_stuffing.push_back(m.decorated_key());
                    cache.populate(m);
                    if (tracker.get_stats().partition_evictions > evictions_before) {
                        break;
                    }
                }
                std::cout << "Shuffling..\n";
                // Evict in random order to create fragmentation.
                std::shuffle(cache_stuffing.begin(), cache_stuffing.end(), random_engine);
                for (auto&& key : cache_stuffing) {
                    cache.touch(key);
                }
                // Ensure that entries matching memtable partitions are evicted
                // last, we want to hit the merge path in row_cache::update()
                for (auto&& key : keys) {
                    cache.touch(key);
                }
                fmt::print("Reclaimable memory: {}\n", reclaimable_memory());
                fmt::print("Cache occupancy: {}\n", tracker.region().occupancy());
            };

            std::deque<std::unique_ptr<char[]>> stuffing;
            auto fragment_free_space = [&] {
                stuffing.clear();
                fmt::print("Reclaimable memory: {}\n", reclaimable_memory());
                fmt::print("Free memory: {}\n", memory::stats().free_memory());
                fmt::print("Cache occupancy: {}\n", tracker.region().occupancy());

                // Induce memory fragmentation by taking down cache segments,
                // which should be evicted in random order, and inducing high
                // waste level in them. Should leave around up to 100M free,
                // but no LSA segment should fit.
                for (unsigned i = 0; i < 100 * 1024 * 1024 / (logalloc::segment_size / 2); ++i) {
                    stuffing.emplace_back(std::make_unique<char[]>(logalloc::segment_size / 2 + 1));
                }

                fmt::print("After fragmenting:\n");
                fmt::print("Reclaimable memory: {}\n", reclaimable_memory());
                fmt::print("Free memory: {}\n", memory::stats().free_memory());
                fmt::print("Cache occupancy: {}\n", tracker.region().occupancy());
            };

            fill_cache_to_the_top();

            fragment_free_space();

            cache.update(row_cache::external_updater([] {}), *mt).get();

            stuffing.clear();
            cache_stuffing.clear();

            // Verify that all mutations from memtable went through
            for (auto&& key : keys) {
                auto range = dht::partition_range::make_singular(key);
                auto reader = cache.make_reader(s, semaphore.make_permit(), range);
                auto close_reader = deferred_close(reader);
                auto mo = read_mutation_from_mutation_reader(reader).get();
                SCYLLA_ASSERT(mo);
                SCYLLA_ASSERT(mo->partition().live_row_count(*s) ==
                       row_count + 1 /* one row was already in cache before update()*/);
            }

            std::cout << "Testing reading from cache.\n";

            fill_cache_to_the_top();

            for (auto&& key : keys) {
                cache.touch(key);
            }

            for (auto&& key : keys) {
                auto range = dht::partition_range::make_singular(key);
                auto reader = cache.make_reader(s, semaphore.make_permit(), range);
                auto close_reader = deferred_close(reader);
                auto mfopt = reader().get();
                SCYLLA_ASSERT(mfopt);
                SCYLLA_ASSERT(mfopt->is_partition_start());
            }

            std::cout << "Testing reading when memory can't be reclaimed.\n";
            // We want to check that when we really can't reserve memory, allocating_section
            // throws rather than enter infinite loop.
            {
                stuffing.clear();
                cache_stuffing.clear();
                tracker.clear();

                // eviction victims
                for (unsigned i = 0; i < logalloc::segment_size / cell_size; ++i) {
                    cache.populate(make_small_mutation());
                }

                const mutation& m = make_large_mutation();
                auto range = dht::partition_range::make_singular(m.decorated_key());

                cache.populate(m);

                logalloc::shard_tracker().reclaim_all_free_segments();

                {
                    logalloc::reclaim_lock _(tracker.region());
                    try {
                        while (true) {
                            stuffing.emplace_back(std::make_unique<char[]>(logalloc::segment_size));
                        }
                    } catch (const std::bad_alloc&) {
                        //expected
                    }
                }

                try {
                    auto reader = cache.make_reader(s, semaphore.make_permit(), range);
                    auto close_reader = deferred_close(reader);
                    SCYLLA_ASSERT(!reader().get());
                    auto evicted_from_cache = logalloc::segment_size + large_cell_size;
                    // GCC's -fallocation-dce can remove dead calls to new and malloc, so
                    // assign the result to a global variable to disable it.
                    leak = new char[evicted_from_cache + logalloc::segment_size];
                    SCYLLA_ASSERT(false); // The test is not invoking the case which it's supposed to test
                } catch (const std::bad_alloc&) {
                    // expected
                }
            }
        });
    });
}
