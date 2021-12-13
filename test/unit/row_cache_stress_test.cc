/*
 * Copyright (C) 2017-present ScyllaDB
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

#include <boost/range/irange.hpp>
#include "seastarx.hh"
#include "test/lib/simple_schema.hh"
#include "test/lib/log.hh"
#include <seastar/core/app-template.hh>
#include "memtable.hh"
#include "row_cache.hh"
#include "partition_slice_builder.hh"
#include "utils/int_range.hh"
#include "utils/div_ceil.hh"
#include "test/lib/memtable_snapshot_source.hh"
#include <seastar/core/reactor.hh>

static thread_local bool cancelled = false;

using namespace std::chrono_literals;

namespace row_cache_stress_test {

struct table {
    simple_schema s;
    reader_concurrency_semaphore semaphore;
    std::vector<dht::decorated_key> p_keys;
    std::vector<api::timestamp_type> p_writetime; // committed writes
    std::vector<clustering_key> c_keys;
    uint64_t mutation_phase = 0;
    uint64_t mutations = 0;
    uint64_t reads_started = 0;
    uint64_t scans_started = 0;

    lw_shared_ptr<memtable> mt;
    lw_shared_ptr<memtable> prev_mt;
    memtable_snapshot_source underlying;
    cache_tracker tracker;
    row_cache cache;

    table(unsigned partitions, unsigned rows)
        : semaphore(reader_concurrency_semaphore::no_limits{}, __FILE__)
        , mt(make_lw_shared<memtable>(s.schema()))
        , underlying(s.schema())
        , cache(s.schema(), snapshot_source([this] { return underlying(); }), tracker)
    {
        p_keys = s.make_pkeys(partitions);
        p_writetime.resize(p_keys.size());
        c_keys = s.make_ckeys(rows);
    }

    reader_permit make_permit() {
        return semaphore.make_tracking_only_permit(s.schema().get(), "test", db::no_timeout);
    }
    future<> stop() noexcept {
        return semaphore.stop();
    }

    void set_schema(schema_ptr new_s) {
        s.set_schema(new_s);
        mt->set_schema(new_s);
        if (prev_mt) {
            prev_mt->set_schema(new_s);
        }
        cache.set_schema(new_s);
        underlying.set_schema(new_s);
    }

    size_t index_of_key(const dht::decorated_key& dk) {
        for (auto i : boost::irange<size_t>(0, p_keys.size())) {
            if (p_keys[i].equal(*s.schema(), dk)) {
                return i;
            }
        }
        throw std::runtime_error(format("key not found: {}", dk));
    }

    sstring value_tag(int key, uint64_t phase) {
        return format("k_0x{:x}_p_0x{:x}", key, phase);
    }

    mutation get_mutation(int key, api::timestamp_type t, const sstring& tag) {
        mutation m(s.schema(), p_keys[key]);
        for (auto ck : c_keys) {
            s.add_row(m, ck, tag, t);
        }
        return m;
    }

    // Must not be called concurrently
    void flush() {
        testlog.trace("flushing");
        prev_mt = std::exchange(mt, make_lw_shared<memtable>(s.schema()));
        auto flushed = make_lw_shared<memtable>(s.schema());
        flushed->apply(*prev_mt, make_permit()).get();
        prev_mt->mark_flushed(flushed->as_data_source());
        testlog.trace("updating cache");
        cache.update(row_cache::external_updater([&] {
            underlying.apply(flushed);
        }), *prev_mt).get();
        testlog.trace("flush done");
        prev_mt = {};
    }

    void mutate_next_phase() {
        testlog.trace("mutating, phase={}", mutation_phase);
        for (auto i : boost::irange<int>(0, p_keys.size())) {
            auto t = s.new_timestamp();
            auto tag = value_tag(i, mutation_phase);
            auto m = get_mutation(i, t, tag);
            mt->apply(std::move(m));
            p_writetime[i] = t;
            testlog.trace("updated key {}, {} @{}", i, tag, t);
            ++mutations;
            later().get();
        }
        testlog.trace("mutated whole ring");
        ++mutation_phase;
        // FIXME: mutate concurrently with flush
        flush();
    }

    struct reader {
        dht::partition_range pr;
        query::partition_slice slice;
        flat_mutation_reader rd;

        reader(dht::partition_range pr_, query::partition_slice slice_) noexcept
            : pr(std::move(pr_))
            , slice(std::move(slice_))
            , rd(nullptr)
        { }
        ~reader() {
            rd.close().get();
        }
    };

    void alter_schema() {
        static thread_local int col_id = 0;
        auto new_s = schema_builder(s.schema())
            .with_column(to_bytes(format("_a{}", col_id++)), byte_type)
            .build();
        testlog.trace("changing schema to {}", *new_s);
        set_schema(new_s);
    }

    std::unique_ptr<reader> make_reader(dht::partition_range pr, query::partition_slice slice) {
        testlog.trace("making reader, pk={} ck={}", pr, slice);
        auto r = std::make_unique<reader>(std::move(pr), std::move(slice));
        std::vector<flat_mutation_reader_v2> rd;
        auto permit = make_permit();
        if (prev_mt) {
            rd.push_back(upgrade_to_v2(prev_mt->make_flat_reader(s.schema(), permit, r->pr, r->slice, default_priority_class(), nullptr,
                streamed_mutation::forwarding::no, mutation_reader::forwarding::no)));
        }
        rd.push_back(upgrade_to_v2(mt->make_flat_reader(s.schema(), permit, r->pr, r->slice, default_priority_class(), nullptr,
            streamed_mutation::forwarding::no, mutation_reader::forwarding::no)));
        rd.push_back(upgrade_to_v2(cache.make_reader(s.schema(), permit, r->pr, r->slice, default_priority_class(), nullptr,
            streamed_mutation::forwarding::no, mutation_reader::forwarding::no)));
        r->rd = downgrade_to_v1(make_combined_reader(s.schema(), permit, std::move(rd), streamed_mutation::forwarding::no, mutation_reader::forwarding::no));
        return r;
    }

    std::unique_ptr<reader> make_single_key_reader(int pk, int_range ck_range) {
        ++reads_started;
        auto slice = partition_slice_builder(*s.schema())
            .with_range(ck_range.transform([this] (int key) { return c_keys[key]; }))
            .build();
        auto pr = dht::partition_range::make_singular(p_keys[pk]);
        return make_reader(std::move(pr), std::move(slice));
    }

    std::unique_ptr<reader> make_scanning_reader() {
        ++scans_started;
        return make_reader(query::full_partition_range, s.schema()->full_slice());
    }
};

struct reader_id {
    sstring name;

    friend std::ostream& operator<<(std::ostream& out, reader_id id) {
        return out << id.name;
    }
};

class validating_consumer {
    table& _t;
    reader_id _id;
    std::optional<sstring> _value;
    size_t _row_count = 0;
    size_t _key = 0;
    std::vector<api::timestamp_type> _writetimes;
    schema_ptr _s;
public:
    validating_consumer(table& t, reader_id id, schema_ptr s)
        : _t(t)
        , _id(id)
        , _writetimes(t.p_writetime)
        , _s(s)
    { }

    void consume_new_partition(const dht::decorated_key& key) {
        testlog.trace("reader {}: enters partition {}", _id, key);
        _value = {};
        _key = _t.index_of_key(key);
    }

    stop_iteration consume_end_of_partition() { return stop_iteration::no; }
    stop_iteration consume(tombstone) { return stop_iteration::no; }
    stop_iteration consume(const static_row&) { return stop_iteration::no; }
    stop_iteration consume(const range_tombstone&) { return stop_iteration::no; }

    stop_iteration consume(const clustering_row& row) {
        ++_row_count;
        sstring value;
        api::timestamp_type t;
        std::tie(value, t) = _t.s.get_value(*_s, row);
        testlog.trace("reader {}: {} @{}, {}", _id, value, t, clustering_row::printer(*_s, row));
        if (_value && value != _value) {
            throw std::runtime_error(format("Saw values from two different writes in partition {:d}: {} and {}", _key, _value, value));
        }
        auto lowest_timestamp = _writetimes[_key];
        if (t < lowest_timestamp) {
            throw std::runtime_error(format("Expected to see the write @{:d}, but saw @{:d} ({}), c_key={}", lowest_timestamp, t, value, row.key()));
        }
        _value = std::move(value);
        return stop_iteration::no;
    }

    size_t consume_end_of_stream() {
        testlog.trace("reader {}: done, {} rows", _id, _row_count);
        return _row_count;
    }
};

template<typename T>
class monotonic_counter {
    std::function<T()> _getter;
    T _prev;
public:
    monotonic_counter(std::function<T()> getter)
        : _getter(std::move(getter)) {
        _prev = _getter();
    }
    // Return change in value since the last call to change() or rate().
    auto change() {
        auto now = _getter();
        return now - std::exchange(_prev, now);
    }
};

}

using namespace row_cache_stress_test;

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template app;
    app.add_options()
        ("trace", "Enables trace-level logging for the test actions")
        ("concurrency", bpo::value<unsigned>()->default_value(10), "Number of concurrent single partition readers")
        ("scan-concurrency", bpo::value<unsigned>()->default_value(2), "Number of concurrent ring scanners")
        ("partitions", bpo::value<unsigned>()->default_value(10), "Number of partitions")
        ("rows", bpo::value<unsigned>()->default_value(10000), "Number of rows in each partitions")
        ("seconds", bpo::value<unsigned>()->default_value(600), "Duration [s] after which the test terminates with a success")
        ;

    return app.run(argc, argv, [&app] {
        if (app.configuration().contains("trace")) {
            testlog.set_level(seastar::log_level::trace);
        }

        return seastar::async([&app] {
            auto concurrency = app.configuration()["concurrency"].as<unsigned>();
            auto scan_concurrency = app.configuration()["scan-concurrency"].as<unsigned>();
            auto partitions = app.configuration()["partitions"].as<unsigned>();
            auto rows = app.configuration()["rows"].as<unsigned>();
            auto seconds = app.configuration()["seconds"].as<unsigned>();

            row_cache_stress_test::table t(partitions, rows);
            auto stop_t = deferred_stop(t);

            engine().at_exit([] {
                cancelled = true;
                return make_ready_future();
            });

            timer<> completion_timer;
            completion_timer.set_callback([&] {
                testlog.info("Test done.");
                cancelled = true;
            });
            completion_timer.arm(std::chrono::seconds(seconds));

            auto fail = [&] (sstring msg) {
                testlog.error("{}", msg);
                cancelled = true;
                completion_timer.cancel();
            };

            // Stats printer
            timer<> stats_printer;
            monotonic_counter<uint64_t> reads([&] { return t.reads_started; });
            monotonic_counter<uint64_t> scans([&] { return t.scans_started; });
            monotonic_counter<uint64_t> mutations([&] { return t.mutations; });
            monotonic_counter<uint64_t> flushes([&] { return t.mutation_phase; });
            stats_printer.set_callback([&] {
                auto MB = 1024 * 1024;
                testlog.info("reads/s: {}, scans/s: {}, mutations/s: {}, flushes/s: {}, Cache: {}/{} [MB], LSA: {}/{} [MB], std free: {} [MB]",
                    reads.change(), scans.change(), mutations.change(), flushes.change(),
                    t.tracker.region().occupancy().used_space() / MB,
                    t.tracker.region().occupancy().total_space() / MB,
                    logalloc::shard_tracker().region_occupancy().used_space() / MB,
                    logalloc::shard_tracker().region_occupancy().total_space() / MB,
                    seastar::memory::stats().free_memory() / MB);
            });
            stats_printer.arm_periodic(1s);

            auto single_partition_reader = [&] (int i, reader_id id) {
                auto n_keys = t.c_keys.size();

                // Assign ranges so that there is ~30% overlap between adjacent readers.
                auto len = div_ceil(n_keys, concurrency);
                len = std::min(n_keys, len + div_ceil(len, 3)); // so that read ranges overlap
                auto start = (n_keys - len) * i / (std::max(concurrency - 1, 1u));
                int_range ck_range = make_int_range(start, start + len);

                int pk = t.p_keys.size() / 2; // FIXME: spread over 3 consecutive partitions
                testlog.info("{} is using pk={} ck={}", id, pk, ck_range);
                while (!cancelled) {
                    testlog.trace("{}: starting read", id);
                    auto rd = t.make_single_key_reader(pk, ck_range);
                    auto row_count = rd->rd.consume(validating_consumer(t, id, t.s.schema())).get0();
                    if (row_count != len) {
                        throw std::runtime_error(format("Expected {:d} fragments, got {:d}", len, row_count));
                    }
                }
            };

            auto scanning_reader = [&] (reader_id id) {
                auto expected_row_count = t.p_keys.size() * t.c_keys.size();
                while (!cancelled) {
                    testlog.trace("{}: starting read", id);
                    auto rd = t.make_scanning_reader();
                    auto row_count = rd->rd.consume(validating_consumer(t, id, t.s.schema())).get0();
                    if (row_count != expected_row_count) {
                        throw std::runtime_error(format("Expected {:d} fragments, got {:d}", expected_row_count, row_count));
                    }
                }
            };

            // populate the initial phase, readers expect constant fragment count.
            t.mutate_next_phase();

            auto readers = parallel_for_each(boost::irange(0u, concurrency), [&] (auto i) {
                reader_id id{format("single-{:d}", i)};
                return seastar::async([&, i, id] {
                    single_partition_reader(i, id);
                }).handle_exception([&, id] (auto e) {
                    fail(format("{} failed: {}", id, e));
                });
            });

            auto scanning_readers = parallel_for_each(boost::irange(0u, scan_concurrency), [&] (auto i) {
                reader_id id{format("scan-{:d}", i)};
                return seastar::async([&, id] {
                    scanning_reader(id);
                }).handle_exception([&, id] (auto e) {
                    fail(format("{} failed: {}", id, e));
                });
            });

            timer<> evictor;
            evictor.set_callback([&] {
                testlog.trace("evicting");
                t.cache.evict();
            });
            evictor.arm_periodic(3s);

            timer<> schema_changer;
            schema_changer.set_callback([&] {
                t.alter_schema();
            });
            schema_changer.arm_periodic(1s);

            // Mutator
            while (!cancelled) {
                t.mutate_next_phase();
            }

            stats_printer.cancel();
            completion_timer.cancel();
            evictor.cancel();
            readers.get();
            scanning_readers.get();

            t.cache.evict();
            t.tracker.cleaner().drain().get();
            t.tracker.memtable_cleaner().drain().get();

            assert(t.tracker.get_stats().partitions == 0);
            assert(t.tracker.get_stats().rows == 0);
        });
    });
}
