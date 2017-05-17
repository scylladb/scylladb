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

#include "row_cache.hh"
#include "core/memory.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include <seastar/core/metrics.hh>
#include <seastar/util/defer.hh>
#include "memtable.hh"
#include <chrono>
#include "utils/move.hh"
#include <boost/version.hpp>
#include <sys/sdt.h>

using namespace std::chrono_literals;

namespace stdx = std::experimental;

static logging::logger logger("cache");

thread_local seastar::thread_scheduling_group row_cache::_update_thread_scheduling_group(1ms, 0.2);

enum class is_wide_partition { yes, no };

future<is_wide_partition, mutation_opt>
try_to_read(uint64_t max_cached_partition_size_in_bytes, streamed_mutation_opt&& sm) {
    if (!sm) {
        return make_ready_future<is_wide_partition, mutation_opt>(is_wide_partition::no, mutation_opt());
    }
    return mutation_from_streamed_mutation_with_limit(std::move(*sm), max_cached_partition_size_in_bytes).then(
        [] (mutation_opt&& omo) mutable {
            if (omo) {
                return make_ready_future<is_wide_partition, mutation_opt>(is_wide_partition::no, std::move(omo));
            } else {
                return make_ready_future<is_wide_partition, mutation_opt>(is_wide_partition::yes, mutation_opt());
            }
        });
}

cache_tracker& global_cache_tracker() {
    static thread_local cache_tracker instance;
    return instance;
}

cache_tracker::cache_tracker() {
    setup_metrics();

    _region.make_evictable([this] {
        return with_allocator(_region.allocator(), [this] {
          // Removing a partition may require reading large keys when we rebalance
          // the rbtree, so linearize anything we read
          return with_linearized_managed_bytes([&] {
           try {
            auto evict_last = [this](lru_type& lru) {
                cache_entry& ce = lru.back();
                auto it = row_cache::partitions_type::s_iterator_to(ce);
                clear_continuity(*std::next(it));
                lru.pop_back_and_dispose(current_deleter<cache_entry>());
            };
            if (!_wide_partition_lru.empty() && (_normal_eviction_count == 0 || _lru.empty())) {
                evict_last(_wide_partition_lru);
                _normal_eviction_count = _normal_large_eviction_ratio;
                ++_stats.wide_partition_evictions;
            } else {
                if (_lru.empty()) {
                    return memory::reclaiming_result::reclaimed_nothing;
                }
                evict_last(_lru);
                if (_normal_eviction_count > 0) {
                    --_normal_eviction_count;
                }
            }
            --_stats.partitions;
            ++_stats.evictions;
            ++_stats.modification_count;
            return memory::reclaiming_result::reclaimed_something;
           } catch (std::bad_alloc&) {
            // Bad luck, linearization during partition removal caused us to
            // fail.  Drop the entire cache so we can make forward progress.
            clear();
            return memory::reclaiming_result::reclaimed_something;
           }
          });
        });
    });
}

cache_tracker::~cache_tracker() {
    clear();
}

void
cache_tracker::setup_metrics() {
    namespace sm = seastar::metrics;
    _metrics.add_group("cache", {
        sm::make_gauge("bytes_used", sm::description("current bytes used by the cache out of the total size of memory"), [this] { return _region.occupancy().used_space(); }),
        sm::make_gauge("bytes_total", sm::description("total size of memory for the cache"), [this] { return _region.occupancy().total_space(); }),
        sm::make_derive("total_operations_hits", sm::description("total number of operation hits"), _stats.hits),
        sm::make_derive("total_operations_misses", sm::description("total number of operation misses"), _stats.misses),
        sm::make_derive("total_operations_uncached_wide_partitions", sm::description("total number of operation of uncached wide partitions"), _stats.uncached_wide_partitions),
        sm::make_derive("total_operations_insertions", sm::description("total number of operation insert"), _stats.insertions),
        sm::make_derive("total_operations_concurrent_misses_same_key", sm::description("total number of operation with misses same key"), _stats.concurrent_misses_same_key),
        sm::make_derive("total_operations_merges", sm::description("total number of operation merged"), _stats.merges),
        sm::make_derive("total_operations_evictions", sm::description("total number of operation eviction"), _stats.evictions),
        sm::make_derive("total_operations_wide_partition_evictions", sm::description("total number of operation wide partition eviction"), _stats.wide_partition_evictions),
        sm::make_derive("total_operations_wide_partition_mispopulations", sm::description("total number of operation wide partition mispopulations"), _stats.wide_partition_mispopulations),
        sm::make_derive("total_operations_removals", sm::description("total number of operation removals"), _stats.removals),
        sm::make_gauge("objects_partitions", sm::description("total number of partition objects"), _stats.partitions)
    });
}

void cache_tracker::clear() {
    with_allocator(_region.allocator(), [this] {
        auto clear = [this] (lru_type& lru) {
            while (!lru.empty()) {
                cache_entry& ce = lru.back();
                auto it = row_cache::partitions_type::s_iterator_to(ce);
                while (it->is_evictable()) {
                    cache_entry& to_remove = *it;
                    ++it;
                    to_remove._lru_link.unlink();
                    current_deleter<cache_entry>()(&to_remove);
                }
                clear_continuity(*it);
            }
        };
        clear(_lru);
        clear(_wide_partition_lru);
    });
    _stats.removals += _stats.partitions;
    _stats.partitions = 0;
    ++_stats.modification_count;
}

void cache_tracker::touch(cache_entry& e) {
    auto move_to_front = [this] (lru_type& lru, cache_entry& e) {
        lru.erase(lru.iterator_to(e));
        lru.push_front(e);
    };
    move_to_front(e.wide_partition() ? _wide_partition_lru : _lru, e);
}

void cache_tracker::insert(cache_entry& entry) {
    ++_stats.insertions;
    ++_stats.partitions;
    ++_stats.modification_count;
    if (entry.wide_partition()) {
        _wide_partition_lru.push_front(entry);
    } else {
        _lru.push_front(entry);
    }
}

void cache_tracker::mark_wide(cache_entry& entry) {
    if (entry._lru_link.is_linked()) {
        entry._lru_link.unlink();
    }
    entry.set_wide_partition();
    _wide_partition_lru.push_front(entry);
}

void cache_tracker::on_erase() {
    --_stats.partitions;
    ++_stats.removals;
    ++_stats.modification_count;
}

void cache_tracker::on_merge() {
    ++_stats.merges;
}

void cache_tracker::on_hit() {
    ++_stats.hits;
}

void cache_tracker::on_miss() {
    ++_stats.misses;
}

void cache_tracker::on_miss_already_populated() {
    ++_stats.concurrent_misses_same_key;
}

void cache_tracker::on_uncached_wide_partition() {
    ++_stats.uncached_wide_partitions;
}

void cache_tracker::on_wide_partition_mispopulation() {
    ++_stats.wide_partition_mispopulations;
}

allocation_strategy& cache_tracker::allocator() {
    return _region.allocator();
}

logalloc::region& cache_tracker::region() {
    return _region;
}

const logalloc::region& cache_tracker::region() const {
    return _region;
}

// Reader which populates the cache using data from the delegate.
class single_partition_populating_reader final : public mutation_reader::impl {
    schema_ptr _schema;
    row_cache& _cache;
    mutation_source& _underlying;
    mutation_reader _delegate;
    const io_priority_class _pc;
    const query::partition_slice& _slice;
    dht::partition_range _large_partition_range;
    mutation_reader _large_partition_reader;
    tracing::trace_state_ptr _trace_state;
    streamed_mutation::forwarding _fwd;
public:
    single_partition_populating_reader(schema_ptr s,
            row_cache& cache,
            mutation_source& underlying,
            mutation_reader delegate,
            const io_priority_class pc,
            const query::partition_slice& slice,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding fwd)
        : _schema(std::move(s))
        , _cache(cache)
        , _underlying(underlying)
        , _delegate(std::move(delegate))
        , _pc(pc)
        , _slice(slice)
        , _trace_state(std::move(trace_state))
        , _fwd(fwd)
    { }

    virtual future<streamed_mutation_opt> operator()() override {
        auto op = _cache._populate_phaser.start();
        return _delegate().then([this, op = std::move(op)] (auto sm) mutable {
            if (!sm) {
                return make_ready_future<streamed_mutation_opt>(streamed_mutation_opt());
            }
            dht::decorated_key dk = sm->decorated_key();
            return try_to_read(_cache._max_cached_partition_size_in_bytes, std::move(sm)).then(
                [this, op = std::move(op), dk = std::move(dk)]
                (is_wide_partition wide_partition, mutation_opt&& mo) {
                    if (wide_partition == is_wide_partition::no) {
                        if (mo) {
                            _cache.populate(*mo);
                            mo->upgrade(_schema);
                            auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, mo->key());
                            auto filtered_partition = mutation_partition(std::move(mo->partition()), *(mo->schema()), std::move(ck_ranges));
                            mo->partition() = std::move(filtered_partition);
                            return make_ready_future<streamed_mutation_opt>(streamed_mutation_from_mutation(std::move(*mo), _fwd));
                        }
                        return make_ready_future<streamed_mutation_opt>(streamed_mutation_opt());
                    } else {
                        _cache.on_uncached_wide_partition();
                        _cache._tracker.on_wide_partition_mispopulation();
                        _cache.mark_partition_as_wide(dk);
                        _large_partition_range = dht::partition_range::make_singular(std::move(dk));
                        _large_partition_reader = _underlying(_schema, _large_partition_range, _slice, _pc, _trace_state, _fwd);
                        return _large_partition_reader();
                    }
                });
        });
    }
};

void cache_tracker::clear_continuity(cache_entry& ce) {
    ce.set_continuous(false);
}

void row_cache::on_hit() {
    _stats.hits.mark();
    _tracker.on_hit();
}

void row_cache::on_miss() {
    _stats.misses.mark();
    _tracker.on_miss();
}

void row_cache::on_uncached_wide_partition() {
    _tracker.on_uncached_wide_partition();
}

class just_cache_scanning_reader final {
    schema_ptr _schema;
    row_cache& _cache;
    row_cache::partitions_type::iterator _it;
    row_cache::partitions_type::iterator _end;
    const dht::partition_range* _range;
    stdx::optional<dht::decorated_key> _last;
    uint64_t _last_reclaim_count;
    size_t _last_modification_count;
    const query::partition_slice& _slice;
    const io_priority_class _pc;
    streamed_mutation::forwarding _fwd;
private:
    void update_iterators() {
        auto cmp = cache_entry::compare(_cache._schema);
        auto update_end = [&] {
            if (_range->end()) {
                if (_range->end()->is_inclusive()) {
                    _end = _cache._partitions.upper_bound(_range->end()->value(), cmp);
                } else {
                    _end = _cache._partitions.lower_bound(_range->end()->value(), cmp);
                }
            } else {
                _end = _cache.partitions_end();
            }
        };

        auto reclaim_count = _cache.get_cache_tracker().region().reclaim_counter();
        auto modification_count = _cache.get_cache_tracker().modification_count();
        if (!_last) {
            if (_range->start()) {
                if (_range->start()->is_inclusive()) {
                    _it = _cache._partitions.lower_bound(_range->start()->value(), cmp);
                } else {
                    _it = _cache._partitions.upper_bound(_range->start()->value(), cmp);
                }
            } else {
                _it = _cache._partitions.begin();
            }
            update_end();
        } else if (reclaim_count != _last_reclaim_count || modification_count != _last_modification_count) {
            _it = _cache._partitions.upper_bound(*_last, cmp);
            update_end();
        }
        _last_reclaim_count = reclaim_count;
        _last_modification_count = modification_count;
    }
public:
    struct cache_data {
        streamed_mutation_opt mut;
        bool continuous;
    };
    just_cache_scanning_reader(schema_ptr s,
        row_cache& cache,
        const dht::partition_range& range,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        streamed_mutation::forwarding fwd)
            : _schema(std::move(s)), _cache(cache), _range(&range), _slice(slice), _pc(pc), _fwd(fwd)
    { }
    future<cache_data> operator()() {
        return _cache._read_section(_cache._tracker.region(), [this] {
          return with_linearized_managed_bytes([&] {
            update_iterators();
            if (_it == _end) {
                return make_ready_future<cache_data>(cache_data { {}, _it->continuous() });
            }
            cache_entry& ce = *_it;
            ++_it;
            _last = ce.key();
            _cache.upgrade_entry(ce);
            _cache._tracker.touch(ce);
            _cache.on_hit();
            cache_data cd { { }, ce.continuous() };
            if (ce.wide_partition()) {
                return ce.read_wide(_cache, _schema, _slice, _pc, _fwd).then([this, cd = std::move(cd)] (auto smopt) mutable {
                    if (smopt) {
                        cd.mut = std::move(*smopt);
                    } else {
                        cd.mut = streamed_mutation_from_mutation(mutation(*_last, _schema), _fwd);
                    }
                    return std::move(cd);
                });
            }
            cd.mut = ce.read(_cache, _schema, _slice, _fwd);
            return make_ready_future<cache_data>(std::move(cd));
          });
        });
    }
    future<> fast_forward_to(const dht::partition_range& pr) {
        _last = {};
        _range = &pr;
        return make_ready_future<>();
    }
};

class range_populating_reader {
    row_cache& _cache;
    schema_ptr _schema;
    dht::partition_range _range;
    const query::partition_slice& _slice;
    utils::phased_barrier::phase_type _populate_phase;
    const io_priority_class& _pc;
    tracing::trace_state_ptr _trace_state;
    mutation_reader _reader;
    bool _reader_created = false;
    row_cache::previous_entry_pointer _last_key;
    dht::partition_range _large_partition_range;
    mutation_reader _large_partition_reader;
    streamed_mutation::forwarding _fwd;
private:
    void update_reader() {
        // TODO: allow updating sstables without fully recreating the reader
        if (_populate_phase != _cache._populate_phaser.phase()) {
            _populate_phase = _cache._populate_phaser.phase();
            if (_last_key._key) {
                auto cmp = dht::ring_position_comparator(*_schema);
                auto&& new_range = _range.split_after(*_last_key._key, cmp);
                if (new_range) {
                    _range = std::move(new_range).value();
                } else {
                    _reader = make_empty_reader();
                    _reader_created = false;
                    return;
                }
            }
            _reader = _cache._underlying(_cache._schema, _range, query::full_slice, _pc, _trace_state);
        }
    }

    future<streamed_mutation_opt> handle_large_partition(dht::decorated_key&& dk) {
        _cache.on_uncached_wide_partition();
        _cache._tracker.on_wide_partition_mispopulation();
        _cache.mark_partition_as_wide(dk, &_last_key);
        _last_key.reset(dk, _populate_phase);

        _large_partition_range = dht::partition_range::make_singular(dk);
        _large_partition_reader = _cache._underlying(_schema, _large_partition_range, _slice, _pc, _trace_state, _fwd);
        return _large_partition_reader().then([this, dk = std::move(dk)] (auto smopt) mutable -> streamed_mutation_opt {
            _large_partition_reader = {};
            if (!smopt) {
                // We cannot emit disengaged optional since this is a part of range
                // read and it would incorrectly interpreted as end of stream.
                // Produce empty mutation instead.
                return streamed_mutation_from_mutation(mutation(std::move(dk), _schema));
            }
            return smopt;
        });
    }

    void handle_end_of_stream() {
        if (_last_key._populate_phase != _populate_phase) {
            return;
        }
        if (!_range.end() || !_range.end()->is_inclusive()) {
            cache_entry::compare cmp(_cache._schema);
            auto it = _range.end() ? _cache._partitions.find(_range.end()->value(), cmp)
                                   : std::prev(_cache._partitions.end());
            if (it != _cache._partitions.end()) {
                if (it == _cache._partitions.begin()) {
                    if (!_last_key._key) {
                        it->set_continuous(true);
                    }
                } else {
                    auto prev = std::prev(it);
                    if (prev->key().equal(*_cache._schema, *_last_key._key)) {
                        it->set_continuous(true);
                    }
                }
            }
        }
    }
public:
    range_populating_reader(
        row_cache& cache,
        schema_ptr schema,
        const query::partition_slice& slice,
        const io_priority_class& pc,
        tracing::trace_state_ptr trace_state,
        streamed_mutation::forwarding fwd)
        : _cache(cache)
        , _schema(std::move(schema))
        , _slice(slice)
        , _pc(pc)
        , _trace_state(std::move(trace_state))
        , _fwd(fwd)
    {}

    future<streamed_mutation_opt> operator()() {
        update_reader();
        return _reader().then([this, op = _cache._populate_phaser.start()] (streamed_mutation_opt smopt) mutable {
            dht::decorated_key dk = smopt ? smopt->decorated_key() : dht::decorated_key{ {}, partition_key::make_empty() };
            return try_to_read(_cache._max_cached_partition_size_in_bytes, std::move(smopt)).then(
                    [this, op = std::move(op), dk = std::move(dk)] (is_wide_partition is_wide, mutation_opt&& mo) mutable {
                if (is_wide == is_wide_partition::yes) {
                    _cache.on_miss();
                    return handle_large_partition(std::move(dk));
                }

                if (!mo) {
                    handle_end_of_stream();
                    return make_ready_future<streamed_mutation_opt>();
                }

                _cache.on_miss();
                _cache.populate(*mo, &_last_key);
                _last_key.reset(mo->decorated_key(), _populate_phase);

                mo->upgrade(_schema);
                auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*_schema, _slice, mo->key());
                auto filtered_partition = mutation_partition(std::move(mo->partition()), *mo->schema(), std::move(ck_ranges));
                mo->partition() = std::move(filtered_partition);
                return make_ready_future<streamed_mutation_opt>(streamed_mutation_from_mutation(std::move(*mo), _fwd));
            });
        });
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        _range = pr;

        auto phase = _cache._populate_phaser.phase();
        if (!_range.start()) {
            _last_key.reset({ }, phase);
        } else if (!_range.start()->is_inclusive() && _range.start()->value().has_key()) {
            _last_key.reset(_range.start()->value().as_decorated_key(), phase);
        } else {
            // Inclusive start bound, cannot set continuity flag.
            _last_key.reset(stdx::nullopt, phase - 1);
        }

        if (!_reader_created || phase != _populate_phase) {
            _populate_phase = _cache._populate_phaser.phase();
            _reader = _cache._underlying(_cache._schema, _range, query::full_slice, _pc, _trace_state);
            _reader_created = true;
            return make_ready_future();
        }
        return _reader.fast_forward_to(_range);
    }
};

class scanning_and_populating_reader final : public mutation_reader::impl {
    const dht::partition_range* _pr;
    schema_ptr _schema;
    dht::partition_range _secondary_range;

    just_cache_scanning_reader _primary_reader;
    range_populating_reader _secondary_reader;
    streamed_mutation::forwarding _fwd;
    streamed_mutation_opt _next_primary;
    bool _secondary_in_progress = false;
    bool _first_element = true;
    stdx::optional<dht::decorated_key> _last_key;
private:
    void update_last_key(const streamed_mutation_opt& smopt) {
        if (smopt) {
            _last_key = smopt->decorated_key();
        }
    }

    bool is_inclusive_start_bound(const dht::decorated_key& dk) {
        if (!_first_element) {
            return false;
        }
        return _pr->start() && _pr->start()->is_inclusive() && _pr->start()->value().equal(*_schema, dk);
    }

    future<streamed_mutation_opt> read_from_primary() {
        return _primary_reader().then([this] (just_cache_scanning_reader::cache_data cd) {
            auto& smopt = cd.mut;
            if (cd.continuous || (smopt && is_inclusive_start_bound(smopt->decorated_key()))) {
                _first_element = false;
                update_last_key(smopt);
                return make_ready_future<streamed_mutation_opt>(std::move(smopt));
            } else {
                _next_primary = std::move(smopt);

                if (!_next_primary) {
                    if (!_last_key) {
                        _secondary_range = *_pr;
                    } else {
                        dht::ring_position_comparator cmp(*_schema);
                        auto&& new_range = _pr->split_after(*_last_key, cmp);
                        if (!new_range) {
                            return make_ready_future<streamed_mutation_opt>();
                        }
                        _secondary_range = std::move(*new_range);
                    }
                } else {
                    if (_last_key) {
                        _secondary_range = dht::partition_range::make({ *_last_key, false }, { _next_primary->decorated_key(), false });
                    } else {
                        if (!_pr->start()) {
                            _secondary_range = dht::partition_range::make_ending_with({ _next_primary->decorated_key(), false });
                        } else {
                            _secondary_range = dht::partition_range::make(*_pr->start(), { _next_primary->decorated_key(), false });
                        }
                    }
                }

                _secondary_in_progress = true;
                return _secondary_reader.fast_forward_to(_secondary_range).then([this] {
                    return read_from_secondary();
                });
            }
        });
    }

    future<streamed_mutation_opt> read_from_secondary() {
        return _secondary_reader().then([this] (streamed_mutation_opt smopt) {
            if (smopt) {
                return smopt;
            } else {
                _secondary_in_progress = false;
                update_last_key(_next_primary);
                return std::move(_next_primary);
            }
        });
    }
public:
    scanning_and_populating_reader(schema_ptr s,
                                    row_cache& cache,
                                    const dht::partition_range& range,
                                    const query::partition_slice& slice,
                                    const io_priority_class& pc,
                                    tracing::trace_state_ptr trace_state,
                                    streamed_mutation::forwarding fwd)
        : _pr(&range)
        , _schema(s)
        , _primary_reader(s, cache, range, slice, pc, fwd)
        , _secondary_reader(cache, s, slice, pc, trace_state, fwd)
        , _fwd(fwd)
    { }

    future<streamed_mutation_opt> operator()() {
        if (_secondary_in_progress) {
            return read_from_secondary();
        } else {
            return read_from_primary();
        }
    }

    future<> fast_forward_to(const dht::partition_range& pr) {
        _secondary_in_progress = false;
        _first_element = true;
        _pr = &pr;
        return _primary_reader.fast_forward_to(pr);
    }
};

mutation_reader
row_cache::make_scanning_reader(schema_ptr s,
                                const dht::partition_range& range,
                                const io_priority_class& pc,
                                const query::partition_slice& slice,
                                tracing::trace_state_ptr trace_state,
                                streamed_mutation::forwarding fwd) {
    return make_mutation_reader<scanning_and_populating_reader>(std::move(s), *this, range, slice, pc, std::move(trace_state), fwd);
}

mutation_reader
row_cache::make_reader(schema_ptr s,
                       const dht::partition_range& range,
                       const query::partition_slice& slice,
                       const io_priority_class& pc,
                       tracing::trace_state_ptr trace_state,
                       streamed_mutation::forwarding fwd) {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start()->value();

        if (!pos.has_key()) {
            return make_scanning_reader(std::move(s), range, pc, slice, std::move(trace_state), fwd);
        }

        return _read_section(_tracker.region(), [&] {
          return with_linearized_managed_bytes([&] {
            const dht::decorated_key& dk = pos.as_decorated_key();
            auto i = _partitions.find(dk, cache_entry::compare(_schema));
            if (i != _partitions.end()) {
                cache_entry& e = *i;
                _tracker.touch(e);
                upgrade_entry(e);
                mutation_reader reader;
                if (e.wide_partition()) {
                    reader = _underlying(s, range, slice, pc, std::move(trace_state), fwd);
                    _tracker.on_uncached_wide_partition();
                    on_miss();
                } else {
                    reader = make_reader_returning(e.read(*this, s, slice, fwd));
                    on_hit();
                }
                return reader;
            } else {
                auto reader = make_mutation_reader<single_partition_populating_reader>(s, *this, _underlying,
                    _underlying(_schema, range, query::full_slice, pc, trace_state), pc, slice, trace_state, fwd);
                on_miss();
                return reader;
            }
          });
        });
    }

    return make_scanning_reader(std::move(s), range, pc, slice, std::move(trace_state), fwd);
}

row_cache::~row_cache() {
    with_allocator(_tracker.allocator(), [this] {
        _partitions.clear_and_dispose([this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            if (!p->is_dummy_entry()) {
                _tracker.on_erase();
            }
            deleter(p);
        });
    });
}

void row_cache::clear_now() noexcept {
    with_allocator(_tracker.allocator(), [this] {
        auto it = _partitions.erase_and_dispose(_partitions.begin(), partitions_end(), [this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_erase();
            deleter(p);
        });
        _tracker.clear_continuity(*it);
    });
}

template<typename CreateEntry, typename VisitEntry>
//requires requires(CreateEntry create, VisitEntry visit, row_cache::partitions_type::iterator it) {
//        { create(it) } -> row_cache::partitions_type::iterator;
//        { visit(it) } -> void;
//    }
void row_cache::do_find_or_create_entry(const dht::decorated_key& key,
    const previous_entry_pointer* previous, CreateEntry&& create_entry, VisitEntry&& visit_entry)
{
    with_allocator(_tracker.allocator(), [&] {
        _populate_section(_tracker.region(), [&] {
            with_linearized_managed_bytes([&] {
                auto i = _partitions.lower_bound(key, cache_entry::compare(_schema));
                if (i == _partitions.end() || !i->key().equal(*_schema, key)) {
                    i = create_entry(i);
                } else {
                    visit_entry(i);
                }

                if (!previous || previous->_populate_phase != _populate_phaser.phase()) {
                    return;
                }

                if ((!previous->_key && i == _partitions.begin())
                    || (previous->_key && i != _partitions.begin()
                        && std::prev(i)->key().equal(*_schema, *previous->_key))) {
                    i->set_continuous(true);
                }
            });
        });
    });
}

void row_cache::mark_partition_as_wide(const dht::decorated_key& key, const previous_entry_pointer* previous) {
    do_find_or_create_entry(key, previous, [&] (auto i) {
        cache_entry* entry = current_allocator().construct<cache_entry>(
                _schema, key, cache_entry::wide_partition_tag{});
        _tracker.insert(*entry);
        return _partitions.insert(i, *entry);
    }, [&] (auto i) {
        _tracker.mark_wide(*i);
    });
}

void row_cache::populate(const mutation& m, const previous_entry_pointer* previous) {
    do_find_or_create_entry(m.decorated_key(), previous, [&] (auto i) {
        cache_entry* entry = current_allocator().construct<cache_entry>(
                m.schema(), m.decorated_key(), m.partition());
        upgrade_entry(*entry);
        _tracker.insert(*entry);
        return _partitions.insert(i, *entry);
    }, [&] (auto i) {
        _tracker.touch(*i);
        // We cache whole partitions right now, so if cache already has this partition,
        // it must be complete, so do nothing.
        _tracker.on_miss_already_populated();  // #1534
    });
}

future<> row_cache::clear() {
    return invalidate(query::full_partition_range);
}

future<> row_cache::update(memtable& m, partition_presence_checker presence_checker) {
    m.on_detach_from_region_group();
    _tracker.region().merge(m); // Now all data in memtable belongs to cache
    auto attr = seastar::thread_attributes();
    attr.scheduling_group = &_update_thread_scheduling_group;
    STAP_PROBE(scylla, row_cache_update_start);
    auto t = seastar::thread(attr, [this, &m, presence_checker = std::move(presence_checker)] {
        auto cleanup = defer([&] {
            with_allocator(_tracker.allocator(), [&m, this] () {
                logalloc::reclaim_lock _(_tracker.region());
                bool blow_cache = false;
                // Note: clear_and_dispose() ought not to look up any keys, so it doesn't require
                // with_linearized_managed_bytes(), but invalidate() does.
                m.partitions.clear_and_dispose([this, deleter = current_deleter<memtable_entry>(), &blow_cache] (memtable_entry* entry) {
                  with_linearized_managed_bytes([&] {
                   try {
                    invalidate_locked(entry->key());
                   } catch (...) {
                    blow_cache = true;
                   }
                   deleter(entry);
                  });
                });
                if (blow_cache) {
                    // We failed to invalidate the key, presumably due to with_linearized_managed_bytes()
                    // running out of memory.  Recover using clear_now(), which doesn't throw.
                    clear_now();
                }
            });
        });
        _populate_phaser.advance_and_await().get();
        while (!m.partitions.empty()) {
            with_allocator(_tracker.allocator(), [this, &m, &presence_checker] () {
                unsigned quota = 30;
                auto cmp = cache_entry::compare(_schema);
                {
                    _update_section(_tracker.region(), [&] {
                        STAP_PROBE(scylla, row_cache_update_one_batch_start);
                        unsigned quota_before = quota;
                        // FIXME: we should really be checking should_yield() here instead of
                        // need_preempt() + quota. However, should_yield() is currently quite
                        // expensive and we need to amortize it somehow.
                        do {
                          auto i = m.partitions.begin();
                          STAP_PROBE(scylla, row_cache_update_partition_start);
                          with_linearized_managed_bytes([&] {
                           {
                            memtable_entry& mem_e = *i;
                            // FIXME: Optimize knowing we lookup in-order.
                            auto cache_i = _partitions.lower_bound(mem_e.key(), cmp);
                            // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
                            // FIXME: keep a bitmap indicating which sstables we do cover, so we don't have to
                            //        search it.
                            if (cache_i != partitions_end() && cache_i->key().equal(*_schema, mem_e.key())) {
                              if (!cache_i->wide_partition()) {
                                cache_entry& entry = *cache_i;
                                upgrade_entry(entry);
                                entry.partition().apply(*_schema, std::move(mem_e.partition()), *mem_e.schema());
                                _tracker.touch(entry);
                                _tracker.on_merge();
                              }
                            } else if (presence_checker(mem_e.key()) ==
                                    partition_presence_checker_result::definitely_doesnt_exist) {
                                cache_entry* entry = current_allocator().construct<cache_entry>(
                                        mem_e.schema(), std::move(mem_e.key()), std::move(mem_e.partition()));
                                _tracker.insert(*entry);
                                _partitions.insert(cache_i, *entry);
                            } else {
                                _tracker.clear_continuity(*cache_i);
                            }
                            i = m.partitions.erase(i);
                            current_allocator().destroy(&mem_e);
                            --quota;
                           }
                          });
                          STAP_PROBE(scylla, row_cache_update_partition_end);
                        } while (!m.partitions.empty() && quota && !need_preempt());
                        STAP_PROBE1(scylla, row_cache_update_one_batch_end, quota_before - quota);
                    });
                    if (quota == 0 && seastar::thread::should_yield()) {
                        return;
                    }
                }
            });
            seastar::thread::yield();
        }
    });
    STAP_PROBE(scylla, row_cache_update_end);
    return do_with(std::move(t), [] (seastar::thread& t) {
        return t.join();
    });
}

void row_cache::touch(const dht::decorated_key& dk) {
 _read_section(_tracker.region(), [&] {
  with_linearized_managed_bytes([&] {
    auto i = _partitions.find(dk, cache_entry::compare(_schema));
    if (i != _partitions.end()) {
        _tracker.touch(*i);
    }
  });
 });
}

void row_cache::invalidate_locked(const dht::decorated_key& dk) {
    auto pos = _partitions.lower_bound(dk, cache_entry::compare(_schema));
    if (pos == partitions_end() || !pos->key().equal(*_schema, dk)) {
        _tracker.clear_continuity(*pos);
    } else {
        auto it = _partitions.erase_and_dispose(pos,
            [this, &dk, deleter = current_deleter<cache_entry>()](auto&& p) mutable {
                _tracker.on_erase();
                deleter(p);
            });
        _tracker.clear_continuity(*it);
    }
}

future<> row_cache::invalidate(const dht::decorated_key& dk) {
return _populate_phaser.advance_and_await().then([this, &dk] {
  _read_section(_tracker.region(), [&] {
    with_allocator(_tracker.allocator(), [this, &dk] {
      with_linearized_managed_bytes([&] {
        invalidate_locked(dk);
      });
    });
  });
});
}

future<> row_cache::invalidate(const dht::partition_range& range) {
    return _populate_phaser.advance_and_await().then([this, &range] {
        with_linearized_managed_bytes([&] {
            invalidate_unwrapped(range);
        });
    });
}

void row_cache::invalidate_unwrapped(const dht::partition_range& range) {
    logalloc::reclaim_lock _(_tracker.region());

    auto cmp = cache_entry::compare(_schema);
    auto begin = _partitions.begin();
    if (range.start()) {
        if (range.start()->is_inclusive()) {
            begin = _partitions.lower_bound(range.start()->value(), cmp);
        } else {
            begin = _partitions.upper_bound(range.start()->value(), cmp);
        }
    }
    auto end = partitions_end();
    if (range.end()) {
        if (range.end()->is_inclusive()) {
            end = _partitions.upper_bound(range.end()->value(), cmp);
        } else {
            end = _partitions.lower_bound(range.end()->value(), cmp);
        }
    }
    with_allocator(_tracker.allocator(), [this, begin, end] {
        auto it = _partitions.erase_and_dispose(begin, end, [this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_erase();
            deleter(p);
        });
        assert(it != _partitions.end());
        _tracker.clear_continuity(*it);
    });
}

row_cache::row_cache(schema_ptr s, mutation_source fallback_factory,
    cache_tracker& tracker, uint64_t max_cached_partition_size_in_bytes)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(cache_entry::compare(_schema))
    , _underlying(std::move(fallback_factory))
    , _max_cached_partition_size_in_bytes(max_cached_partition_size_in_bytes)
{
    with_allocator(_tracker.allocator(), [this] {
        cache_entry* entry = current_allocator().construct<cache_entry>(cache_entry::dummy_entry_tag());
        _partitions.insert(*entry);
    });
}

cache_entry::cache_entry(cache_entry&& o) noexcept
    : _schema(std::move(o._schema))
    , _key(std::move(o._key))
    , _pe(std::move(o._pe))
    , _flags(o._flags)
    , _lru_link()
    , _cache_link()
{
    if (o._lru_link.is_linked()) {
        auto prev = o._lru_link.prev_;
        o._lru_link.unlink();
        cache_tracker::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
    }

    {
        using container_type = row_cache::partitions_type;
        container_type::node_algorithms::replace_node(o._cache_link.this_ptr(), _cache_link.this_ptr());
        container_type::node_algorithms::init(o._cache_link.this_ptr());
    }
}

void row_cache::set_schema(schema_ptr new_schema) noexcept {
    _schema = std::move(new_schema);
}

future<streamed_mutation_opt> cache_entry::read_wide(row_cache& rc,
    schema_ptr s, const query::partition_slice& slice, const io_priority_class& pc, streamed_mutation::forwarding fwd)
{
    struct range_and_underlyig_reader {
        dht::partition_range _range;
        mutation_reader _reader;
        range_and_underlyig_reader(row_cache& rc, schema_ptr s, dht::partition_range pr,
                                   const query::partition_slice& slice, const io_priority_class& pc, streamed_mutation::forwarding fwd)
                : _range(std::move(pr))
                , _reader(rc._underlying(s, _range, slice, pc, nullptr, fwd))
        { }
        range_and_underlyig_reader(range_and_underlyig_reader&&) = delete;
    };
    rc._tracker.on_uncached_wide_partition();
    auto pr = dht::partition_range::make_singular(_key);
    auto rd_ptr = std::make_unique<range_and_underlyig_reader>(rc, s, std::move(pr), slice, pc, fwd);
    auto& r_a_ur = *rd_ptr;
    return r_a_ur._reader().finally([rd_ptr = std::move(rd_ptr)] {});
}

streamed_mutation cache_entry::read(row_cache& rc, const schema_ptr& s, streamed_mutation::forwarding fwd) {
    return read(rc, s, query::full_slice, fwd);
}

streamed_mutation cache_entry::read(row_cache& rc, const schema_ptr& s, const query::partition_slice& slice, streamed_mutation::forwarding fwd) {
    assert(!wide_partition());
    if (_schema->version() != s->version()) {
        auto ck_ranges = query::clustering_key_filter_ranges::get_ranges(*s, slice, _key.key());
        auto mp = mutation_partition(_pe.squashed(_schema, s), *s, std::move(ck_ranges));
        auto m = mutation(s, _key, std::move(mp));
        return streamed_mutation_from_mutation(std::move(m), fwd);
    }
    auto ckr = query::clustering_key_filter_ranges::get_ranges(*s, slice, _key.key());
    auto snp = _pe.read(_schema);
    return make_partition_snapshot_reader(_schema, _key, std::move(ckr), snp, rc._tracker.region(), rc._read_section, { }, fwd);
}

const schema_ptr& row_cache::schema() const {
    return _schema;
}

void row_cache::upgrade_entry(cache_entry& e) {
    if (e._schema != _schema) {
        if (e.wide_partition()) {
            e._schema = _schema;
            return;
        }
        auto& r = _tracker.region();
        assert(!r.reclaiming_enabled());
        with_allocator(r.allocator(), [this, &e] {
          with_linearized_managed_bytes([&] {
            e.partition().upgrade(e._schema, _schema);
            e._schema = _schema;
          });
        });
    }
}
