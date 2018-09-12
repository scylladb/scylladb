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

#include <chrono>
#include <core/distributed.hh>
#include <core/app-template.hh>
#include <core/sstring.hh>
#include <core/thread.hh>
#include <seastar/core/weak_ptr.hh>

#include "utils/managed_bytes.hh"
#include "utils/extremum_tracking.hh"
#include "utils/logalloc.hh"
#include "row_cache.hh"
#include "log.hh"
#include "schema_builder.hh"
#include "memtable.hh"
#include "tests/perf/perf.hh"

static const int update_iterations = 16;
static const int cell_size = 128;
static bool cancelled = false;

template<typename Func>
auto duration_in_seconds(Func&& f) {
    using clk = std::chrono::steady_clock;
    auto start = clk::now();
    f();
    auto end = clk::now();
    return std::chrono::duration_cast<std::chrono::duration<float>>(end - start);
}

class scheduling_latency_measurer : public weakly_referencable<scheduling_latency_measurer> {
    using clk = std::chrono::steady_clock;
    clk::time_point _last = clk::now();
    utils::estimated_histogram _hist{300};
    min_max_tracker<clk::duration> _minmax;
    bool _stop = false;
private:
    void schedule_tick();
    void tick() {
        auto old = _last;
        _last = clk::now();
        auto latency = _last - old;
        _minmax.update(latency);
        _hist.add(latency.count());
        if (!_stop) {
            schedule_tick();
        }
    }
public:
    void start() {
        schedule_tick();
    }
    void stop() {
        _stop = true;
        later().get(); // so that the last scheduled tick is counted
    }
    const utils::estimated_histogram& histogram() const {
        return _hist;
    }
    clk::duration min() const { return _minmax.min(); }
    clk::duration max() const { return _minmax.max(); }
};

void scheduling_latency_measurer::schedule_tick() {
    seastar::schedule(make_task(default_scheduling_group(), [self = weak_from_this()] () mutable {
        if (self) {
            self->tick();
        }
    }));
}

std::ostream& operator<<(std::ostream& out, const scheduling_latency_measurer& slm) {
    auto to_ms = [] (int64_t nanos) {
        return float(nanos) / 1e6;
    };
    return out << sprint("{count: %d, "
                         //"min: %.6f [ms], "
                         //"50%%: %.6f [ms], "
                         //"90%%: %.6f [ms], "
                         "99%%: %.6f [ms], "
                         "max: %.6f [ms]}",
        slm.histogram().count(),
        //to_ms(slm.min().count()),
        //to_ms(slm.histogram().percentile(0.5)),
        //to_ms(slm.histogram().percentile(0.9)),
        to_ms(slm.histogram().percentile(0.99)),
        to_ms(slm.max().count()));
}

template<typename MutationGenerator>
void run_test(const sstring& name, schema_ptr s, MutationGenerator&& gen) {
    cache_tracker tracker;
    row_cache cache(s, make_empty_snapshot_source(), tracker, is_continuous::yes);

    size_t memtable_size = seastar::memory::stats().total_memory() / 4;

    std::cout << name << ":\n";

    for (int i = 0; i < update_iterations; ++i) {
        auto MB = 1024 * 1024;
        auto prefill_compacted = logalloc::memory_compacted();
        auto prefill_allocated = logalloc::memory_allocated();

        auto mt = make_lw_shared<memtable>(s);
        while (mt->occupancy().total_space() < memtable_size) {
            auto pk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s,
                data_value(utils::UUID_gen::get_time_UUID()).serialize()));
            mutation m = gen();
            mt->apply(m);
            if (cancelled) {
                return;
            }
        }

        auto prev_compacted = logalloc::memory_compacted();
        auto prev_allocated = logalloc::memory_allocated();
        auto prev_rows_processed_from_memtable = tracker.get_stats().rows_processed_from_memtable;
        auto prev_rows_merged_from_memtable = tracker.get_stats().rows_merged_from_memtable;
        auto prev_rows_dropped_from_memtable = tracker.get_stats().rows_dropped_from_memtable;

        std::cout << sprint("cache: %d/%d [MB], memtable: %d/%d [MB], alloc/comp: %d/%d [MB] (amp: %.3f)\n",
            tracker.region().occupancy().used_space() / MB,
            tracker.region().occupancy().total_space() / MB,
            mt->occupancy().used_space() / MB,
            mt->occupancy().total_space() / MB,
            (prev_allocated - prefill_allocated) / MB,
            (prev_compacted - prefill_compacted) / MB,
            float((prev_compacted - prefill_compacted)) / (prev_allocated - prefill_allocated)
        );

        // Create a reader which tests the case of memtable snapshots
        // going away after memtable was merged to cache.
        auto rd = std::make_unique<flat_mutation_reader>(
            make_combined_reader(s, cache.make_reader(s), mt->make_flat_reader(s)));
        rd->set_max_buffer_size(1);
        rd->fill_buffer(db::no_timeout).get();

        scheduling_latency_measurer slm;
        slm.start();
        auto d = duration_in_seconds([&] {
            cache.update([] {}, *mt).get();
        });

        rd->set_max_buffer_size(1024*1024);
        rd->consume_pausable([] (mutation_fragment) {
            return stop_iteration::no;
        }, db::no_timeout).get();

        mt = {};
        rd = {};

        slm.stop();

        auto compacted = logalloc::memory_compacted() - prev_compacted;
        auto allocated = logalloc::memory_allocated() - prev_allocated;

        std::cout << sprint("update: %.6f [ms], stall: %s, cache: %d/%d [MB], alloc/comp: %d/%d [MB] (amp: %.3f), pr/me/dr %d/%d/%d\n",
            d.count() * 1000,
            slm,
            tracker.region().occupancy().used_space() / MB,
            tracker.region().occupancy().total_space() / MB,
            allocated / MB, compacted / MB, float(compacted)/allocated,
            tracker.get_stats().rows_processed_from_memtable - prev_rows_processed_from_memtable,
            tracker.get_stats().rows_merged_from_memtable - prev_rows_merged_from_memtable,
            tracker.get_stats().rows_dropped_from_memtable - prev_rows_dropped_from_memtable);
    }

    auto d = duration_in_seconds([&] {
        cache.invalidate([] {}).get();
    });

    std::cout << sprint("invalidation: %.6f [ms]", d.count() * 1000) << "\n";
}

void test_small_partitions() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    run_test("Small partitions, no overwrites", s, [&] {
        auto pk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s,
            data_value(utils::UUID_gen::get_time_UUID()).serialize()));
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        m.set_clustered_cell(clustering_key::make_empty(), "v1", val, api::new_timestamp());
        m.set_clustered_cell(clustering_key::make_empty(), "v2", val, api::new_timestamp());
        m.set_clustered_cell(clustering_key::make_empty(), "v3", val, api::new_timestamp());
        return m;
    });
}

void test_partition_with_lots_of_small_rows() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    auto pk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s,
        data_value(utils::UUID_gen::get_time_UUID()).serialize()));
    int ck_idx = 0;

    run_test("Large partition, lots of small rows", s, [&] {
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        auto ck = clustering_key::from_single_value(*s, data_value(ck_idx++).serialize());
        m.set_clustered_cell(ck, "v1", val, api::new_timestamp());
        m.set_clustered_cell(ck, "v2", val, api::new_timestamp());
        m.set_clustered_cell(ck, "v3", val, api::new_timestamp());
        return m;
    });
}

void test_partition_with_few_small_rows() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    run_test("Small partition with a few rows", s, [&] {
        auto pk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s,
            data_value(utils::UUID_gen::get_time_UUID()).serialize()));

        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));

        for (int i = 0; i < 3; ++i) {
            auto ck = clustering_key::from_single_value(*s, data_value(i).serialize());
            m.set_clustered_cell(ck, "v1", val, api::new_timestamp());
            m.set_clustered_cell(ck, "v2", val, api::new_timestamp());
            m.set_clustered_cell(ck, "v3", val, api::new_timestamp());
        }
        return m;
    });
}

void test_partition_with_lots_of_range_tombstones() {
    auto s = schema_builder("ks", "cf")
        .with_column("pk", uuid_type, column_kind::partition_key)
        .with_column("ck", reversed_type_impl::get_instance(int32_type), column_kind::clustering_key)
        .with_column("v1", bytes_type, column_kind::regular_column)
        .with_column("v2", bytes_type, column_kind::regular_column)
        .with_column("v3", bytes_type, column_kind::regular_column)
        .build();

    auto pk = dht::global_partitioner().decorate_key(*s, partition_key::from_single_value(*s,
        data_value(utils::UUID_gen::get_time_UUID()).serialize()));
    int ck_idx = 0;

    run_test("Large partition, lots of range tombstones", s, [&] {
        mutation m(s, pk);
        auto val = data_value(bytes(bytes::initialized_later(), cell_size));
        auto ck = clustering_key::from_single_value(*s, data_value(ck_idx++).serialize());
        auto r = query::clustering_range::make({ck}, {ck});
        tombstone tomb(api::new_timestamp(), gc_clock::now());
        m.partition().apply_row_tombstone(*s, range_tombstone(bound_view::from_range_start(r), bound_view::from_range_end(r), tomb));
        return m;
    });
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
            test_small_partitions();
            test_partition_with_few_small_rows();
            test_partition_with_lots_of_small_rows();
            // Takes a huge amount of time due to https://github.com/scylladb/scylla/issues/2581#issuecomment-398030186,
            // disable until fixed.
            // test_partition_with_lots_of_range_tombstones();
        });
    });
}
