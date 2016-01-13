/*
 * Copyright 2015 Cloudius Systems
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
#include <seastar/core/scollectd.hh>
#include <seastar/util/defer.hh>
#include "memtable.hh"
#include <chrono>
#include "utils/move.hh"

using namespace std::chrono_literals;

namespace stdx = std::experimental;

static logging::logger logger("cache");

thread_local seastar::thread_scheduling_group row_cache::_update_thread_scheduling_group(1ms, 0.2);


cache_tracker& global_cache_tracker() {
    static thread_local cache_tracker instance;
    return instance;
}

cache_tracker::cache_tracker() {
    setup_collectd();

    _region.make_evictable([this] {
        return with_allocator(_region.allocator(), [this] {
            if (_lru.empty()) {
                return memory::reclaiming_result::reclaimed_nothing;
            }
            _lru.pop_back_and_dispose(current_deleter<cache_entry>());
            --_partitions;
            ++_modification_count;
            return memory::reclaiming_result::reclaimed_something;
        });
    });
}

cache_tracker::~cache_tracker() {
    clear();
}

void
cache_tracker::setup_collectd() {
    _collectd_registrations = std::make_unique<scollectd::registrations>(scollectd::registrations({
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "bytes", "used")
                , scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return _region.occupancy().used_space(); })
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "bytes", "total")
                , scollectd::make_typed(scollectd::data_type::GAUGE, [this] { return _region.occupancy().total_space(); })
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "hits")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _hits)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "misses")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _misses)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "insertions")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _insertions)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "total_operations", "merges")
                , scollectd::make_typed(scollectd::data_type::DERIVE, _merges)
        ),
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "objects", "partitions")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _partitions)
        ),
    }));
}

void cache_tracker::clear() {
    with_allocator(_region.allocator(), [this] {
        _lru.clear_and_dispose(current_deleter<cache_entry>());
    });
    _partitions = 0;
    ++_modification_count;
}

void cache_tracker::touch(cache_entry& e) {
    _lru.erase(_lru.iterator_to(e));
    _lru.push_front(e);
}

void cache_tracker::insert(cache_entry& entry) {
    ++_insertions;
    ++_partitions;
    ++_modification_count;
    _lru.push_front(entry);
}

void cache_tracker::on_erase() {
    --_partitions;
    ++_modification_count;
}

void cache_tracker::on_merge() {
    ++_merges;
}

void cache_tracker::on_hit() {
    ++_hits;
}

void cache_tracker::on_miss() {
    ++_misses;
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
class populating_reader final : public mutation_reader::impl {
    schema_ptr _schema;
    row_cache& _cache;
    mutation_reader _delegate;
public:
    populating_reader(schema_ptr s, row_cache& cache, mutation_reader delegate)
        : _schema(std::move(s))
        , _cache(cache)
        , _delegate(std::move(delegate))
    { }

    virtual future<mutation_opt> operator()() override {
        return _delegate().then([this, op = _cache._populate_phaser.start()] (mutation_opt&& mo) {
            if (mo) {
                _cache.populate(*mo);
                mo->upgrade(_schema);
            }
            return std::move(mo);
        });
    }
};

void row_cache::on_hit() {
    ++_stats.hits;
    _tracker.on_hit();
}

void row_cache::on_miss() {
    ++_stats.misses;
    _tracker.on_miss();
}

class just_cache_scanning_reader final : public mutation_reader::impl {
    schema_ptr _schema;
    row_cache& _cache;
    row_cache::partitions_type::iterator _it;
    row_cache::partitions_type::iterator _end;
    const query::partition_range& _range;
    stdx::optional<dht::decorated_key> _last;
    uint64_t _last_reclaim_count;
    size_t _last_modification_count;
private:
    void update_iterators() {
        auto cmp = cache_entry::compare(_cache._schema);
        auto update_end = [&] {
            if (_range.end()) {
                if (_range.end()->is_inclusive()) {
                    _end = _cache._partitions.upper_bound(_range.end()->value(), cmp);
                } else {
                    _end = _cache._partitions.lower_bound(_range.end()->value(), cmp);
                }
            } else {
                _end = _cache._partitions.end();
            }
        };

        auto reclaim_count = _cache.get_cache_tracker().region().reclaim_counter();
        auto modification_count = _cache.get_cache_tracker().modification_count();
        if (!_last) {
            if (_range.start()) {
                if (_range.start()->is_inclusive()) {
                    _it = _cache._partitions.lower_bound(_range.start()->value(), cmp);
                } else {
                    _it = _cache._partitions.upper_bound(_range.start()->value(), cmp);
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
    just_cache_scanning_reader(schema_ptr s, row_cache& cache, const query::partition_range& range)
        : _schema(std::move(s)), _cache(cache), _range(range)
    { }
    virtual future<mutation_opt> operator()() override {
        return _cache._read_section(_cache._tracker.region(), [this] {
            update_iterators();
            if (_it == _end) {
                return make_ready_future<mutation_opt>();
            }
            auto& ce = *_it;
            ++_it;
            _last = ce.key();
            _cache.upgrade_entry(ce);
            return make_ready_future<mutation_opt>(ce.read(_schema));
        });
    }
};

class scanning_and_populating_reader final : public mutation_reader::impl {
    row_cache& _cache;
    schema_ptr _schema;
    mutation_reader _primary;
    bool _secondary_only = false;
    mutation_opt _next_primary;
    mutation_source& _underlying;
    mutation_reader _secondary;
    utils::phased_barrier::phase_type _secondary_phase;
    const query::partition_range& _original_range;
    query::partition_range _range;
    key_source& _underlying_keys;
    key_reader _keys;
    dht::decorated_key_opt _next_key;
    dht::decorated_key_opt _last_secondary_key;
public:
    scanning_and_populating_reader(schema_ptr s, row_cache& cache, const query::partition_range& range)
        : _cache(cache), _schema(s),
          _primary(make_mutation_reader<just_cache_scanning_reader>(s, cache, range)),
          _underlying(cache._underlying), _original_range(range), _underlying_keys(cache._underlying_keys),
          _keys(_underlying_keys(range))
    { }
    virtual future<mutation_opt> operator()() override {
        // FIXME: store in cache information whether the immediate successor
        // of the current entry is present. As long as it is consulting
        // index_reader is not necessary.
        if (_secondary_only) {
            return next_secondary();
        }
        return next_key().then([this] (dht::decorated_key_opt dk) mutable {
            return _primary().then([this, dk = std::move(dk)] (mutation_opt&& mo) {
                if (!mo && !dk) {
                    return make_ready_future<mutation_opt>();
                }
                if (mo) {
                    auto cmp = dk ? dk->tri_compare(*_schema, mo->decorated_key()) : 0;
                    if (cmp >= 0) {
                        if (cmp) {
                            _next_key = std::move(dk);
                        }
                        _cache.on_hit();
                        return make_ready_future<mutation_opt>(std::move(mo));
                    }
                }
                _next_primary = std::move(mo);

                stdx::optional<query::partition_range::bound> end;
                if (_next_primary) {
                    end = query::partition_range::bound(_next_primary->decorated_key(), false);
                } else {
                    end = _original_range.end();
                }
                _range = query::partition_range(query::partition_range::bound { std::move(*dk), true }, std::move(end));
                _last_secondary_key = {};
                _secondary_phase = _cache._populate_phaser.phase();
                _secondary = _underlying(_cache._schema, _range);
                _secondary_only = true;
                return next_secondary();
            });
        });
    }
private:
    future<mutation_opt> next_secondary() {
        if (_secondary_phase != _cache._populate_phaser.phase()) {
            assert(_last_secondary_key);
            auto cmp = dht::ring_position_comparator(*_schema);
            _range = _range.split_after(*_last_secondary_key, cmp);
            _secondary_phase = _cache._populate_phaser.phase();
            _secondary = _underlying(_cache._schema, _range);
        }
        return _secondary().then([this, op = _cache._populate_phaser.start()] (mutation_opt&& mo) {
            if (!mo && _next_primary) {
                auto cmp = dht::ring_position_comparator(*_schema);
                _range = _original_range.split_after(_next_primary->decorated_key(), cmp);
                _keys = _underlying_keys(_range);
                _secondary_only = false;
                _cache.on_hit();
                return std::move(_next_primary);
            }
            if (mo) {
                _cache.populate(*mo);
                mo->upgrade(_schema);
                _last_secondary_key = mo->decorated_key();
            }
            _cache.on_miss();
            return std::move(mo);
        });
    }
    future<dht::decorated_key_opt> next_key() {
        if (_next_key) {
            return make_ready_future<dht::decorated_key_opt>(move_and_disengage(_next_key));
        }
        return _keys();
    }
};

mutation_reader
row_cache::make_scanning_reader(schema_ptr s, const query::partition_range& range) {
    return make_mutation_reader<scanning_and_populating_reader>(std::move(s), *this, range);
}

mutation_reader
row_cache::make_reader(schema_ptr s, const query::partition_range& range) {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start()->value();

        if (!pos.has_key()) {
            return make_scanning_reader(std::move(s), range);
        }

        return _read_section(_tracker.region(), [&] {
            const dht::decorated_key& dk = pos.as_decorated_key();
            auto i = _partitions.find(dk, cache_entry::compare(_schema));
            if (i != _partitions.end()) {
                cache_entry& e = *i;
                _tracker.touch(e);
                on_hit();
                upgrade_entry(e);
                return make_reader_returning(e.read(s));
            } else {
                on_miss();
                return make_mutation_reader<populating_reader>(s, *this, _underlying(_schema, range));
            }
        });
    }

    return make_scanning_reader(std::move(s), range);
}

row_cache::~row_cache() {
    clear();
}

void row_cache::populate(const mutation& m) {
    with_allocator(_tracker.allocator(), [this, &m] {
        _populate_section(_tracker.region(), [&] {
        auto i = _partitions.lower_bound(m.decorated_key(), cache_entry::compare(_schema));
        if (i == _partitions.end() || !i->key().equal(*_schema, m.decorated_key())) {
            cache_entry* entry = current_allocator().construct<cache_entry>(
                    m.schema(), m.decorated_key(), m.partition());
            upgrade_entry(*entry);
            _tracker.insert(*entry);
            _partitions.insert(i, *entry);
        } else {
            _tracker.touch(*i);
            // We cache whole partitions right now, so if cache already has this partition,
            // it must be complete, so do nothing.
        }
        });
    });
}

void row_cache::clear() {
    with_allocator(_tracker.allocator(), [this] {
        _partitions.clear_and_dispose([this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_erase();
            deleter(p);
        });
    });
}

future<> row_cache::update(memtable& m, partition_presence_checker presence_checker) {
    _tracker.region().merge(m._region); // Now all data in memtable belongs to cache
    auto attr = seastar::thread_attributes();
    attr.scheduling_group = &_update_thread_scheduling_group;
    auto t = seastar::thread(attr, [this, &m, presence_checker = std::move(presence_checker)] {
      auto cleanup = defer([&] {
          with_allocator(_tracker.allocator(), [&m, this] () {
            m.partitions.clear_and_dispose(current_deleter<partition_entry>());
          });
      });
      _populate_phaser.advance_and_await().get();
      while (!m.partitions.empty()) {
        with_allocator(_tracker.allocator(), [this, &m, &presence_checker] () {
            unsigned quota = 30;
            auto cmp = cache_entry::compare(_schema);
            try {
                _update_section(_tracker.region(), [&] {
                    auto i = m.partitions.begin();
                    while (i != m.partitions.end() && quota) {
                        partition_entry& mem_e = *i;
                        // FIXME: Optimize knowing we lookup in-order.
                        auto cache_i = _partitions.lower_bound(mem_e.key(), cmp);
                        // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
                        // FIXME: keep a bitmap indicating which sstables we do cover, so we don't have to
                        //        search it.
                        if (cache_i != _partitions.end() && cache_i->key().equal(*_schema, mem_e.key())) {
                            cache_entry& entry = *cache_i;
                            upgrade_entry(entry);
                            entry.partition().apply(*_schema, std::move(mem_e.partition()), *mem_e.schema());
                            _tracker.touch(entry);
                            _tracker.on_merge();
                        } else if (presence_checker(mem_e.key().key()) ==
                                   partition_presence_checker_result::definitely_doesnt_exist) {
                            cache_entry* entry = current_allocator().construct<cache_entry>(
                                mem_e.schema(), std::move(mem_e.key()), std::move(mem_e.partition()));
                            upgrade_entry(*entry);
                            _tracker.insert(*entry);
                            _partitions.insert(cache_i, *entry);
                        }
                        i = m.partitions.erase(i);
                        current_allocator().destroy(&mem_e);
                        --quota;
                    }
                });
                if (quota == 0 && seastar::thread::should_yield()) {
                    return;
                }
            } catch (const std::bad_alloc&) {
                // Cache entry may be in an incomplete state if
                // _update_section fails due to weak exception guarantees of
                // mutation_partition::apply().
                auto i = m.partitions.begin();
                auto cache_i = _partitions.find(i->key(), cmp);
                if (cache_i != _partitions.end()) {
                    _partitions.erase_and_dispose(cache_i, current_deleter<cache_entry>());
                    _tracker.on_erase();
                }
                throw;
            }
        });
        seastar::thread::yield();
      }
    });
    return do_with(std::move(t), [] (seastar::thread& t) {
        return t.join();
    });
}

void row_cache::touch(const dht::decorated_key& dk) {
    auto i = _partitions.find(dk, cache_entry::compare(_schema));
    if (i != _partitions.end()) {
        _tracker.touch(*i);
    }
}

void row_cache::invalidate(const dht::decorated_key& dk) {
    with_allocator(_tracker.allocator(), [this, &dk] {
        _partitions.erase_and_dispose(dk, cache_entry::compare(_schema), [this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_erase();
            deleter(p);
        });
    });
}

void row_cache::invalidate(const query::partition_range& range) {
    if (range.is_wrap_around(dht::ring_position_comparator(*_schema))) {
        auto unwrapped = range.unwrap();
        invalidate(unwrapped.first);
        invalidate(unwrapped.second);
        return;
    }

    auto cmp = cache_entry::compare(_schema);
    auto begin = _partitions.begin();
    if (range.start()) {
        if (range.start()->is_inclusive()) {
            begin = _partitions.lower_bound(range.start()->value(), cmp);
        } else {
            begin = _partitions.upper_bound(range.start()->value(), cmp);
        }
    }
    auto end = _partitions.end();
    if (range.end()) {
        if (range.end()->is_inclusive()) {
            end = _partitions.upper_bound(range.end()->value(), cmp);
        } else {
            end = _partitions.lower_bound(range.end()->value(), cmp);
        }
    }
    with_allocator(_tracker.allocator(), [this, begin, end] {
        _partitions.erase_and_dispose(begin, end, [this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_erase();
            deleter(p);
        });
    });
}

row_cache::row_cache(schema_ptr s, mutation_source fallback_factory, key_source underlying_keys,
    cache_tracker& tracker)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(cache_entry::compare(_schema))
    , _underlying(std::move(fallback_factory))
    , _underlying_keys(std::move(underlying_keys))
{ }

cache_entry::cache_entry(cache_entry&& o) noexcept
    : _schema(std::move(o._schema))
    , _key(std::move(o._key))
    , _p(std::move(o._p))
    , _lru_link()
    , _cache_link()
{
    {
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

mutation cache_entry::read(const schema_ptr& s) {
    auto m = mutation(_schema, _key, _p);
    if (_schema != s) {
        m.upgrade(s);
    }
    return m;
}

const schema_ptr& row_cache::schema() const {
    return _schema;
}

void row_cache::upgrade_entry(cache_entry& e) {
    if (e._schema != _schema) {
        auto& r = _tracker.region();
        assert(!r.reclaiming_enabled());
        with_allocator(r.allocator(), [this, &e] {
            e._p.upgrade(*e._schema, *_schema);
            e._schema = _schema;
        });
    }
}
