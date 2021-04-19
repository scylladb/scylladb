/*
 * Copyright (C) 2021 ScyllaDB
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

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/testing/random.hh>
#include <seastar/util/memory_diagnostics.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/sleep.hh>
#include "utils/logalloc.hh"
#include "utils/managed_bytes.hh"
#include "utils/managed_ref.hh"
#include "test/perf/perf.hh"
#include <boost/intrusive/list.hpp>
#include <fmt/ostream.h>

using namespace logalloc;
using namespace std::chrono_literals;
namespace bi = boost::intrusive;

using test_clock = std::chrono::steady_clock;
using key_type = uint64_t;

void print_seastar_memstats() {
    std::ostream& out = std::cout;
    out << seastar::memory::generate_memory_diagnostics_report() << '\n';
    seastar::memory::statistics stats = seastar::memory::stats();
    out << "stats.mallocs() " << stats.mallocs() << '\n';
    out << "stats.frees() " << stats.frees() << '\n';
    //out << "stats.cross_cpu_frees() " << stats.cross_cpu_frees() << '\n';
    out << "stats.live_objects() " << stats.live_objects() << '\n';
    out << "stats.free_memory() " << stats.free_memory() << '\n';
    out << "stats.allocated_memory() " << stats.allocated_memory() << '\n';
    out << "stats.total_memory() " << stats.total_memory() << '\n';
    out << "stats.reclaims() " << stats.reclaims() << '\n';
    out << "stats.large_allocations() " << stats.large_allocations() << '\n';
    //out << "stats.foreign_mallocs() " << stats.foreign_mallocs() << '\n';
    //out << "stats.foreign_frees() " << stats.foreign_frees() << '\n';
    //out << "stats.foreign_cross_frees() " << stats.foreign_cross_frees() << '\n';
}

template <std::invocable F>
test_clock::duration timed(F&& f) {
    auto start = test_clock::now();
    std::forward<F>(f)();
    auto end = test_clock::now();
    return test_clock::now() - start;
}

template <std::invocable F>
auto with_stall_detection_disabled(F&& f) {
    auto old = engine().get_blocked_reactor_notify_ms();
    smp::invoke_on_all([] { engine().update_blocked_reactor_notify_ms(1000s); }).get();
    std::forward<F>(f)();
    smp::invoke_on_all([&] { engine().update_blocked_reactor_notify_ms(old); }).get();
}


struct size_distribution {
    virtual size_t next_size() = 0;
};
struct uniform_size_distribution final : size_distribution {
    std::uniform_int_distribution<size_t> _dist;
    uniform_size_distribution(size_t a, size_t b) : _dist(a, b) {}
    size_t next_size() override { return _dist(seastar::testing::local_random_engine); }
};


struct access_pattern {
    virtual key_type next_key() = 0;
};
struct uniform_random_access_pattern final : access_pattern {
    std::uniform_int_distribution<key_type> _dist;
    uniform_random_access_pattern(key_type n_keys) : _dist(0, n_keys - 1) {}
    key_type next_key() override { return _dist(seastar::testing::local_random_engine); }
};
struct sequential_access_pattern final : access_pattern {
    key_type _n_keys;
    key_type _next_key = 0;
    sequential_access_pattern(key_type n_keys) : _n_keys(n_keys) {}
    key_type next_key() override {
        key_type retval = _next_key;
        _next_key = (_next_key + 1) % _n_keys;
        return retval;
    }
};


struct model_stats {
    size_t reads = 0;
    size_t misses = 0;
    size_t hits = 0;

    size_t writes = 0;
    size_t overwritten_values = 0;
    size_t overwritten_bytes = 0;
    size_t invalidated_values = 0;
    size_t invalidated_bytes = 0;
    size_t fresh_values = 0;
    size_t fresh_bytes = 0;

    size_t allocated_bytes = 0;
    size_t allocated_values = 0;
    size_t sync_evicted_bytes = 0;
    size_t sync_evicted_values = 0;
    size_t background_evicted_bytes = 0;
    size_t background_evicted_values = 0;
    size_t moved_bytes = 0;
    size_t moved_values = 0;

    size_t all_entries = 0;
    size_t active_entries = 0;

    void print() const {
        fmt::print("misses {}\n", misses);
        fmt::print("hits {}\n", hits);

        fmt::print("writes {}\n", writes);
        fmt::print("overwritten_values {}\n", overwritten_values);
        fmt::print("overwritten_bytes {}\n", overwritten_bytes);
        fmt::print("invalidated_values {}\n", invalidated_values);
        fmt::print("invalidated_bytes {}\n", invalidated_bytes);
        fmt::print("fresh_values {}\n", fresh_values);
        fmt::print("fresh_bytes {}\n", fresh_bytes);

        fmt::print("allocated_bytes {}\n", allocated_bytes);
        fmt::print("allocated_values {}\n", allocated_values);
        fmt::print("sync_evicted_bytes {}\n", sync_evicted_bytes);
        fmt::print("sync_evicted_values {}\n", sync_evicted_values);
        fmt::print("background_evicted_bytes {}\n", background_evicted_bytes);
        fmt::print("background_evicted_values {}\n", background_evicted_values);
        fmt::print("moved_bytes {}\n", moved_bytes);
        fmt::print("moved_values {}\n", moved_values);

        fmt::print("all_entries {}\n", all_entries);
        fmt::print("active_entries {}\n", active_entries);
    }

    model_stats operator-(const model_stats& other) const {
        return model_stats {
            .reads = reads - other.reads,
            .misses = misses - other.misses,
            .hits = hits - other.hits,

            .writes = writes - other.writes,
            .overwritten_values = overwritten_values - other.overwritten_values,
            .overwritten_bytes = overwritten_bytes - other.overwritten_bytes,
            .invalidated_values = invalidated_values - other.invalidated_values,
            .invalidated_bytes = invalidated_bytes - other.invalidated_bytes,
            .fresh_values = fresh_values - other.fresh_values,
            .fresh_bytes = fresh_bytes - other.fresh_bytes,

            .allocated_bytes = allocated_bytes - other.allocated_bytes,
            .allocated_values = allocated_values - other.allocated_values,
            .sync_evicted_bytes = sync_evicted_bytes - other.sync_evicted_bytes,
            .sync_evicted_values = sync_evicted_values - other.sync_evicted_values,
            .background_evicted_bytes = background_evicted_bytes - other.background_evicted_bytes,
            .background_evicted_values = background_evicted_values - other.background_evicted_values,
            .moved_bytes = moved_bytes - other.moved_bytes,
            .moved_values = moved_values - other.moved_values,

            .all_entries = all_entries,
            .active_entries = active_entries,
        };
    }
};

struct abstract_model {
    virtual void read(key_type, uint32_t default_size) = 0;
    virtual void write(key_type, uint32_t size) = 0;
    virtual void shuffle_lru() = 0;
    virtual memory::reclaiming_result evict_some() = 0;
    virtual const model_stats& stats() = 0;
    virtual ~abstract_model() {};
};

struct vector_model final : abstract_model {
    struct [[gnu::packed]] entry {
        using lru_link_type = bi::list_member_hook<bi::link_mode<bi::auto_unlink>>;
        lru_link_type _lru_link;

        vector_model& _owner;
        entry** _backref = nullptr;
        managed_bytes _value;

        entry() = default;
        entry(size_t value_size, vector_model& owner)
            : _owner(owner)
            , _value(managed_bytes::initialized_later(), value_size)
        {
            _owner._stats.allocated_bytes += memory_usage();
            _owner._stats.allocated_values += 1;
        }
        entry(entry&& other) noexcept
            : _owner(other._owner)
            , _backref(other._backref)
            , _value(std::move(other._value))
        {
            _owner._stats.moved_bytes += memory_usage();
            _owner._stats.moved_values += 1;
            _lru_link.swap_nodes(other._lru_link);
            *_backref = this;
            other._backref = nullptr;
        }
        ~entry() {
            if (_backref) {
                *_backref = nullptr;
            }
        }
        size_t memory_usage() {
            return sizeof(entry) + _value.external_memory_usage();
        }
    };
    struct entry_ptr {
        entry* _entry = nullptr;
        uint32_t _size = 0;
        uint32_t _generation = 0;
    };
    using lru_type = bi::list<entry,
        bi::member_hook<entry, entry::lru_link_type, &entry::_lru_link>,
        bi::constant_time_size<false>>;

    region _cache;
    region _memtable;
    uint32_t _generation = 1;
    model_stats _stats;
    std::vector<entry_ptr> _entries;
    std::vector<size_t> _shuffle_helper;
    lru_type _cache_lru;
    lru_type _memtable_lru;

    bool _currently_allocating = false;
    struct currently_allocating_guard {
        vector_model& _vm;
        currently_allocating_guard(vector_model& vm) : _vm(vm) {
            _vm._currently_allocating = true;
        }
        ~currently_allocating_guard() {
            _vm._currently_allocating = false;
        }
    };

    vector_model(key_type n_keys) {
        _entries.resize(n_keys);
        _shuffle_helper.resize(n_keys);
        std::iota(_shuffle_helper.begin(), _shuffle_helper.end(), 0);
        _cache.make_evictable([this] {
            return evict_some();
        });
        _stats.all_entries = n_keys;
    }

    void touch(entry_ptr& eptr) {
        if (eptr._generation == _generation) {
            _memtable_lru.erase(_memtable_lru.iterator_to(*eptr._entry));
            _memtable_lru.push_front(*eptr._entry);
        } else {
            _cache_lru.erase(_cache_lru.iterator_to(*eptr._entry));
            _cache_lru.push_front(*eptr._entry);
        }
    }

    void read(key_type k, uint32_t default_size) override {
        currently_allocating_guard guard(*this);
        _stats.reads += 1;
        entry_ptr& eptr = _entries.at(k);
        if (eptr._entry) {
            _stats.hits += 1;
            touch(eptr);
        } else {
            _stats.misses += 1;
            size_t size = eptr._size ? eptr._size : default_size;
            eptr._entry = _cache.allocator().construct<entry>(size, *this);
            eptr._entry->_backref = &eptr._entry;
            eptr._size = size;
            eptr._generation = _generation - 1;
            _cache_lru.push_front(*eptr._entry);
            _stats.active_entries += 1;
        }
    }

    void write(key_type k, uint32_t size) override {
        currently_allocating_guard guard(*this);
        _stats.writes += 1;
        entry_ptr& eptr = _entries.at(k);
        enum class write_type { overwrite, invalidate, fresh };
        write_type wt;
        if (eptr._entry) {
            if (eptr._generation == _generation) {
                _memtable.allocator().destroy<entry>(eptr._entry);
                wt = write_type::overwrite;
            } else {
                _cache.allocator().destroy<entry>(eptr._entry);
                wt = write_type::invalidate;
            }
        } else {
            wt = write_type::fresh;
        }
        assert(!eptr._entry);
        eptr._entry = _memtable.allocator().construct<entry>(size, *this);
        eptr._entry->_backref = &eptr._entry;
        eptr._size = size;
        eptr._generation = _generation;
        _memtable_lru.push_front(*eptr._entry);
        if (wt == write_type::overwrite) {
            _stats.overwritten_values += 1;
            _stats.overwritten_bytes += eptr._entry->memory_usage();
        } else if (wt == write_type::invalidate) {
            _stats.invalidated_values += 1;
            _stats.invalidated_bytes += eptr._entry->memory_usage();
        } else {
            _stats.fresh_values += 1;
            _stats.fresh_bytes += eptr._entry->memory_usage();
            _stats.active_entries += 1;
        }
        if (_memtable.occupancy().used_space() > 50'000'000) {
            merge_memtable();
        }
    }

    memory::reclaiming_result evict_some() override {
        if (_cache_lru.empty()) {
            return memory::reclaiming_result::reclaimed_nothing;
        } else {
            auto& candidate = _cache_lru.back();
            _cache_lru.pop_back();

            if (_currently_allocating) {
                _stats.sync_evicted_values += 1;
                _stats.sync_evicted_bytes += candidate.memory_usage();
            } else {
                _stats.background_evicted_values += 1;
                _stats.background_evicted_bytes  += candidate.memory_usage();
            }

            _cache.allocator().destroy<entry>(&candidate);
            _stats.active_entries -= 1;
            return memory::reclaiming_result::reclaimed_something;
        }
    }

    const model_stats& stats() { return _stats; }

    void shuffle_lru() {
        std::shuffle(_shuffle_helper.begin(), _shuffle_helper.end(), seastar::testing::local_random_engine);
        size_t count = 0;
        for (size_t index : _shuffle_helper) {
            if (_entries[index]._entry) {
                touch(_entries[index]);
                ++count;
            }
        }
        assert(_stats.active_entries == count);
    }

    void merge_memtable() {
        _cache.merge(_memtable);
        _memtable = region();
        _cache_lru.splice(_cache_lru.begin(), _memtable_lru);
        ++_generation;
    }

    ~vector_model() {
        merge_memtable();
        with_stall_detection_disabled([&] {
            while (evict_some() == memory::reclaiming_result::reclaimed_something) {
                //...
            }
            assert(_stats.active_entries == 0);
        });
    }
};

void test_model(vector_model& model, access_pattern& pattern, bool shuffle) {
    with_stall_detection_disabled([&] {
        prime_segment_pool(memory::stats().total_memory(), memory::min_free_memory()).get();
    });

    fmt::print("Total memory available for LSA: {}\n", shard_tracker().occupancy().free_space());

    // Setup background reclaim
    auto background_reclaim_scheduling_group = create_scheduling_group("background_reclaim", 50).get0();
    auto kill_sched_group = defer([&] {
        destroy_scheduling_group(background_reclaim_scheduling_group).get();
    });
    logalloc::tracker::config st_cfg;
    st_cfg.defragment_on_idle = false;
    st_cfg.abort_on_lsa_bad_alloc = false;
    st_cfg.lsa_reclamation_step = 1;
    st_cfg.background_reclaim_sched_group = background_reclaim_scheduling_group;
    logalloc::shard_tracker().configure(st_cfg);
    auto stop_lsa_background_reclaim = defer([&] {
        return logalloc::shard_tracker().stop().get();
    });

    const auto yield_interval = 100us;
    // How many operations per yield?
    const size_t phase_throughputs[] = {1000000, 0, 0, 10, 30, 100, 333, 1000, 3333, 10000, 33333};
    const test_clock::duration phase_durations[] = {20s, 3s, 1s, 1s, 1s, 1s, 1s, 1s, 1s, 1s, 1s};
    assert(std::size(phase_durations) == std::size(phase_throughputs));

    int current_phase = 0;

    bool shuffled = false;
    while (true) {
        fmt::print("\nPhase {}, duration {}s, target ops/s: {}\n",
                current_phase,
                std::chrono::duration<double>(phase_durations[current_phase]).count(),
                phase_throughputs[current_phase] * (1s / yield_interval));

        auto phase_start = test_clock::now();
        auto phase_end = phase_start + phase_durations[current_phase];
        test_clock::duration time_spent_allocating = 0s;
        test_clock::duration time_spent_in_background = 0s;

        model_stats old_stats = model.stats();

        scheduling_latency_measurer latency;
        latency.start();

        while (test_clock::now() < phase_end) {
            auto next_yield = test_clock::now() + yield_interval;
            try {
                time_spent_allocating += timed([&] {
                    for (size_t i = 0; i < phase_throughputs[current_phase]; ++i) {
                        model.write(pattern.next_key(), 100);
                        if (i % 10000 == 0) {
                            if (test_clock::now() >= next_yield) {
                                break;
                            }
                        }
                    }
                });
                while (test_clock::now() < next_yield) {
                    // Pretend to do work.
                }
            } catch (std::bad_alloc& e) {
                fmt::print("Bad alloc in phase {}. Skipping the phase and sleeping 1s.\n", current_phase);
                print_seastar_memstats();
                sleep(1s).get();
                break;
            }
            if (need_preempt()) {
                time_spent_in_background += timed([] { thread::maybe_yield(); });
            }
        }

        latency.stop();

        auto phase_duration = test_clock::now() - phase_start;
        fmt::print("LSA Occupancy {}\n", shard_tracker().occupancy().used_fraction());
        fmt::print("Total time {}s\n", std::chrono::duration<double>(phase_duration).count());
        fmt::print("Time spent allocating: {}s\n", std::chrono::duration<double>(time_spent_allocating).count());
        fmt::print("Time spent in background: {}s\n", std::chrono::duration<double>(time_spent_in_background).count());
        fmt::print("Peak latency: {}ms\n", std::chrono::duration<double, std::milli>(latency.max()).count());
        (model.stats() - old_stats).print();

        // Move to next phase.
        ++current_phase;
        if (current_phase == std::size(phase_durations)) {
            break;
        };
        if (shuffle && !shuffled) {
            with_stall_detection_disabled([&] { model.shuffle_lru(); });
            shuffled = true;
        }
    }
}

void run_test_1() {
    key_type n = 100'000'000;
    auto vd = vector_model(n);
    auto ap = uniform_random_access_pattern(n);
    //auto ap = sequential_access_pattern(n);
    test_model(vd, ap, true);
}

void run_tests() {
    run_test_1();
}

int main(int argc, char** argv) {
    app_template app;
    app.add_options()
        ("random-seed", boost::program_options::value<unsigned>(), "Random number generator seed")
        ;
    return app.run(argc, argv, [&app] () -> future<int> {
        auto conf_seed = app.configuration()["random-seed"];
        auto seed = conf_seed.empty() ? std::random_device()() : conf_seed.as<unsigned>();
        std::cout << "random-seed=" << seed << '\n';
        co_await smp::invoke_on_all([seed] {
            seastar::testing::local_random_engine.seed(seed + this_shard_id());
        });

        if (smp::count != 1) {
            throw std::runtime_error("This test has to be run with -c1");
        }

        if (memory::stats().total_memory() < size_t(4) * 1024 * 1024 * 1024) {
            throw std::runtime_error("Currently this test wants at least 4G of memory");
        }

        co_await seastar::async([] {
            run_tests();
        });

        co_return 0;
    });
}
