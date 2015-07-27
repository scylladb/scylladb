/*
 * Copyright 2015 Cloudius Systems
 */

#include "row_cache.hh"
#include "core/memory.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include <seastar/core/scollectd.hh>

static logging::logger logger("cache");

cache_tracker& global_cache_tracker() {
    static thread_local cache_tracker instance;
    return instance;
}

cache_tracker::cache_tracker()
    : _reclaimer([this] {
        logger.warn("Clearing cache from reclaimer hook");
        // FIXME: perform incremental eviction. We should first switch to a
        // compacting memory allocator to avoid problems with memory
        // fragmentation.
        clear();
    }) {
    setup_collectd();
}

cache_tracker::~cache_tracker() {
    clear();
}

void
cache_tracker::setup_collectd() {
    _collectd_registrations = std::make_unique<scollectd::registrations>(scollectd::registrations({
        scollectd::add_polled_metric(scollectd::type_instance_id("cache"
                , scollectd::per_cpu_plugin_instance
                , "queue_length", "total_rows")
                , scollectd::make_typed(scollectd::data_type::GAUGE, _lru_len)
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
    }));
}

void cache_tracker::clear() {
    _lru_len = 0;
    _lru.clear_and_dispose(std::default_delete<cache_entry>());
}

void cache_tracker::touch(cache_entry& e) {
    ++_hits;
    _lru.erase(_lru.iterator_to(e));
    _lru.push_front(e);
}

void cache_tracker::insert(cache_entry& entry) {
    ++_misses;
    ++_lru_len;
    _lru.push_front(entry);
}

// Reader which populates the cache using data from the delegate.
class populating_reader {
    row_cache& _cache;
    mutation_reader _delegate;
public:
    populating_reader(row_cache& cache, mutation_reader delegate)
        : _cache(cache)
        , _delegate(std::move(delegate))
    { }

    future<mutation_opt> operator()() {
        return _delegate().then([this] (mutation_opt&& mo) {
            if (mo) {
                _cache.populate(*mo);
            }
            return std::move(mo);
        });
    }
};

mutation_reader
row_cache::make_reader(const query::partition_range& range) {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start()->value();

        if (!pos.has_key()) {
            warn(unimplemented::cause::RANGE_QUERIES);
            return populating_reader(*this, _underlying(range));
        }

        const dht::decorated_key& dk = pos.as_decorated_key();
        auto i = _partitions.find(dk, cache_entry::compare(_schema));
        if (i != _partitions.end()) {
            cache_entry& e = *i;
            _tracker.touch(e);
            ++_stats.hits;
            return make_reader_returning(mutation(_schema, dk, e.partition()));
        } else {
            ++_stats.misses;
            return populating_reader(*this, _underlying(range));
        }
    }

    warn(unimplemented::cause::RANGE_QUERIES);
    return populating_reader(*this, _underlying(range));
}

row_cache::~row_cache() {
    _partitions.clear_and_dispose(std::default_delete<cache_entry>());
}

void row_cache::populate(const mutation& m) {
    populate(mutation(m));
}

void row_cache::populate(mutation&& m) {
    auto i = _partitions.lower_bound(m.decorated_key(), cache_entry::compare(_schema));
    if (i == _partitions.end() || !i->key().equal(*_schema, m.decorated_key())) {
        cache_entry* entry = new cache_entry(m.decorated_key(), std::move(m.partition()));
        _tracker.insert(*entry);
        _partitions.insert(i, *entry);
    } else {
        cache_entry& entry = *i;
        _tracker.touch(entry);
        entry.partition().apply(*m.schema(), m.partition());
    }
}

future<> row_cache::update(mutation_reader reader) {
    return do_with(std::move(reader), [this] (mutation_reader& r) {
        return consume(r, [this] (mutation&& m) {
            auto i = _partitions.find(m.decorated_key(), cache_entry::compare(_schema));
            // If cache doesn't contain the entry we cannot insert it because the mutation may be incomplete.
            // FIXME: if the bloom filters say the data isn't in any sstable yet (other than the
            //        one we are caching now), we can.
            //        Alternatively, keep a bitmap indicating which sstables we do cover, so we don't have to
            //        search it.
            if (i != _partitions.end()) {
                cache_entry& entry = *i;
                _tracker.touch(entry);
                entry.partition().apply(*m.schema(), std::move(m.partition()));
            }
            return stop_iteration::no;
        });
    });
}

row_cache::row_cache(schema_ptr s, mutation_source fallback_factory, cache_tracker& tracker)
    : _tracker(tracker)
    , _schema(std::move(s))
    , _partitions(cache_entry::compare(_schema))
    , _underlying(std::move(fallback_factory))
{ }
