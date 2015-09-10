/*
 * Copyright 2015 Cloudius Systems
 */

#include "row_cache.hh"
#include "core/memory.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include <seastar/core/scollectd.hh>
#include "memtable.hh"

static logging::logger logger("cache");

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
}

void cache_tracker::touch(cache_entry& e) {
    _lru.erase(_lru.iterator_to(e));
    _lru.push_front(e);
}

void cache_tracker::insert(cache_entry& entry) {
    ++_insertions;
    ++_partitions;
    _lru.push_front(entry);
}

void cache_tracker::on_erase() {
    --_partitions;
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
    row_cache& _cache;
    mutation_reader _delegate;
public:
    populating_reader(row_cache& cache, mutation_reader delegate)
        : _cache(cache)
        , _delegate(std::move(delegate))
    { }

    virtual future<mutation_opt> operator()() override {
        return _delegate().then([this] (mutation_opt&& mo) {
            if (mo) {
                _cache.populate(*mo);
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

mutation_reader
row_cache::make_scanning_reader(const query::partition_range& range) {
    warn(unimplemented::cause::RANGE_QUERIES);
    return make_mutation_reader<populating_reader>(*this, _underlying(range));
}

mutation_reader
row_cache::make_reader(const query::partition_range& range) {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start()->value();

        if (!pos.has_key()) {
            return make_scanning_reader(range);
        }

        return _read_section(_tracker.region(), [&] {
            const dht::decorated_key& dk = pos.as_decorated_key();
            auto i = _partitions.find(dk, cache_entry::compare(_schema));
            if (i != _partitions.end()) {
                cache_entry& e = *i;
                _tracker.touch(e);
                on_hit();
                return make_reader_returning(mutation(_schema, dk, e.partition()));
            } else {
                on_miss();
                return make_mutation_reader<populating_reader>(*this, _underlying(range));
            }
        });
    }

    return make_scanning_reader(range);
}

row_cache::~row_cache() {
    with_allocator(_tracker.allocator(), [this] {
        _partitions.clear_and_dispose([this, deleter = current_deleter<cache_entry>()] (auto&& p) mutable {
            _tracker.on_erase();
            deleter(p);
        });
    });
}

void row_cache::populate(const mutation& m) {
    with_allocator(_tracker.allocator(), [this, &m] {
        logalloc::reclaim_lock _(_tracker.region());
        auto i = _partitions.lower_bound(m.decorated_key(), cache_entry::compare(_schema));
        if (i == _partitions.end() || !i->key().equal(*_schema, m.decorated_key())) {
            cache_entry* entry = current_allocator().construct<cache_entry>(m.decorated_key(), m.partition());
            _tracker.insert(*entry);
            _partitions.insert(i, *entry);
        } else {
            _tracker.touch(*i);
            // We cache whole partitions right now, so if cache already has this partition,
            // it must be complete, so do nothing.
        }
    });
}

future<> row_cache::update(memtable& m, partition_presence_checker presence_checker) {
    _tracker.region().merge(m._region); // Now all data in memtable belongs to cache
    return repeat([this, &m, presence_checker = std::move(presence_checker)] () mutable {
        return with_allocator(_tracker.allocator(), [this, &m, &presence_checker] () {
            unsigned quota = 30;
            auto cmp = cache_entry::compare(_schema);
            try {
                _update_section(_tracker.region(), [&] {
                    auto i = m.partitions.begin();
                    const schema& s = *m.schema();
                    while (i != m.partitions.end() && quota) {
                        partition_entry& mem_e = *i;
                        // FIXME: Optimize knowing we lookup in-order.
                        auto cache_i = _partitions.lower_bound(mem_e.key(), cmp);
                        // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
                        // FIXME: keep a bitmap indicating which sstables we do cover, so we don't have to
                        //        search it.
                        if (cache_i != _partitions.end() && cache_i->key().equal(s, mem_e.key())) {
                            cache_entry& entry = *cache_i;
                            entry.partition().apply(s, std::move(mem_e.partition()));
                            _tracker.touch(entry);
                            _tracker.on_merge();
                        } else if (presence_checker(mem_e.key().key()) ==
                                   partition_presence_checker_result::definitely_doesnt_exist) {
                            cache_entry* entry = current_allocator().construct<cache_entry>(
                                std::move(mem_e.key()), std::move(mem_e.partition()));
                            _tracker.insert(*entry);
                            _partitions.insert(cache_i, *entry);
                        }
                        i = m.partitions.erase(i);
                        current_allocator().destroy(&mem_e);
                        --quota;
                    }
                });
                return m.partitions.empty() ? stop_iteration::yes : stop_iteration::no;
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
    }).finally([&m, this] {
        with_allocator(_tracker.allocator(), [&m] () {
            m.partitions.clear_and_dispose(current_deleter<partition_entry>());
        });
    });
}

void row_cache::touch(const dht::decorated_key& dk) {
    auto i = _partitions.find(dk, cache_entry::compare(_schema));
    if (i != _partitions.end()) {
        _tracker.touch(*i);
    }
}

row_cache::row_cache(schema_ptr s, mutation_source fallback_factory, cache_tracker& tracker)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(cache_entry::compare(_schema))
    , _underlying(std::move(fallback_factory))
{ }

cache_entry::cache_entry(cache_entry&& o) noexcept
    : _key(std::move(o._key))
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
