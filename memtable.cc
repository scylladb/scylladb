/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
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

#include "memtable.hh"
#include "frozen_mutation.hh"
#include "sstable_mutation_readers.hh"

namespace stdx = std::experimental;

memtable::memtable(schema_ptr schema, logalloc::region_group* dirty_memory_region_group)
        : _schema(std::move(schema))
        , _region(dirty_memory_region_group ? logalloc::region(*dirty_memory_region_group) : logalloc::region())
        , partitions(partition_entry::compare(_schema)) {
}

memtable::~memtable() {
    with_allocator(_region.allocator(), [this] {
        partitions.clear_and_dispose(current_deleter<partition_entry>());
    });
}

mutation_partition&
memtable::find_or_create_partition_slow(partition_key_view key) {
    assert(!_region.reclaiming_enabled());

    // FIXME: Perform lookup using std::pair<token, partition_key_view>
    // to avoid unconditional copy of the partition key.
    // We can't do it right now because std::map<> which holds
    // partitions doesn't support heterogeneous lookup.
    // We could switch to boost::intrusive_map<> similar to what we have for row keys.
    auto& outer = current_allocator();
    return with_allocator(standard_allocator(), [&, this] () -> mutation_partition& {
        auto dk = dht::global_partitioner().decorate_key(*_schema, key);
        return with_allocator(outer, [&dk, this] () -> mutation_partition& {
            return find_or_create_partition(dk);
        });
    });
}

mutation_partition&
memtable::find_or_create_partition(const dht::decorated_key& key) {
    assert(!_region.reclaiming_enabled());

    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key, partition_entry::compare(_schema));
    if (i == partitions.end() || !key.equal(*_schema, i->key())) {
        partition_entry* entry = current_allocator().construct<partition_entry>(
            dht::decorated_key(key), mutation_partition(_schema));
        i = partitions.insert(i, *entry);
    }
    return i->partition();
}

boost::iterator_range<memtable::partitions_type::const_iterator>
memtable::slice(const query::partition_range& range) const {
    if (query::is_single_partition(range)) {
        const query::ring_position& pos = range.start()->value();
        auto i = partitions.find(pos, partition_entry::compare(_schema));
        if (i != partitions.end()) {
            return boost::make_iterator_range(i, std::next(i));
        } else {
            return boost::make_iterator_range(i, i);
        }
    } else {
        auto cmp = partition_entry::compare(_schema);

        auto i1 = range.start()
                  ? (range.start()->is_inclusive()
                        ? partitions.lower_bound(range.start()->value(), cmp)
                        : partitions.upper_bound(range.start()->value(), cmp))
                  : partitions.cbegin();

        auto i2 = range.end()
                  ? (range.end()->is_inclusive()
                        ? partitions.upper_bound(range.end()->value(), cmp)
                        : partitions.lower_bound(range.end()->value(), cmp))
                  : partitions.cend();

        return boost::make_iterator_range(i1, i2);
    }
}

class scanning_reader final : public mutation_reader::impl {
    lw_shared_ptr<const memtable> _memtable;
    const query::partition_range& _range;
    stdx::optional<dht::decorated_key> _last;
    memtable::partitions_type::const_iterator _i;
    memtable::partitions_type::const_iterator _end;
    uint64_t _last_reclaim_counter;
    stdx::optional<query::partition_range> _delegate_range;
    mutation_reader _delegate;
private:
    memtable::partitions_type::const_iterator lookup_end() {
        auto cmp = partition_entry::compare(_memtable->_schema);
        return _range.end()
            ? (_range.end()->is_inclusive()
                ? _memtable->partitions.upper_bound(_range.end()->value(), cmp)
                : _memtable->partitions.lower_bound(_range.end()->value(), cmp))
            : _memtable->partitions.cend();
    }
    void update_iterators() {
        // We must be prepared that iterators may get invalidated during compaction.
        auto current_reclaim_counter = _memtable->_region.reclaim_counter();
        auto cmp = partition_entry::compare(_memtable->_schema);
        if (_last) {
            if (current_reclaim_counter != _last_reclaim_counter) {
                _i = _memtable->partitions.upper_bound(*_last, cmp);
                _end = lookup_end();
            }
        } else {
            // Initial lookup
            _i = _range.start()
                 ? (_range.start()->is_inclusive()
                    ? _memtable->partitions.lower_bound(_range.start()->value(), cmp)
                    : _memtable->partitions.upper_bound(_range.start()->value(), cmp))
                 : _memtable->partitions.cbegin();
            _end = lookup_end();
        }
        _last_reclaim_counter = current_reclaim_counter;
    }
public:
    scanning_reader(lw_shared_ptr<const memtable> m, const query::partition_range& range)
        : _memtable(std::move(m))
        , _range(range)
    { }

    virtual future<mutation_opt> operator()() override {
        if (_delegate_range) {
            return _delegate();
        }

        // We cannot run concurrently with row_cache::update().
        if (_memtable->is_flushed()) {
            // FIXME: Use cache. See column_family::make_reader().
            _delegate_range = _last ? _range.split_after(*_last, dht::ring_position_comparator(*_memtable->_schema)) : _range;
            _delegate = make_mutation_reader<sstable_range_wrapping_reader>(
                _memtable->_sstable, _memtable->_schema, *_delegate_range);
            _memtable = {};
            _last = {};
            return _delegate();
        }

        logalloc::reclaim_lock _(_memtable->_region);
        update_iterators();
        if (_i == _end) {
            return make_ready_future<mutation_opt>(stdx::nullopt);
        }
        const partition_entry& e = *_i;
        ++_i;
        _last = e.key();
        return make_ready_future<mutation_opt>(mutation(_memtable->_schema, e.key(), e.partition()));
    }
};

mutation_reader
memtable::make_reader(const query::partition_range& range) const {
    if (query::is_wrap_around(range, *_schema)) {
        fail(unimplemented::cause::WRAP_AROUND);
    }

    if (query::is_single_partition(range)) {
        const query::ring_position& pos = range.start()->value();
        auto i = partitions.find(pos, partition_entry::compare(_schema));
        if (i != partitions.end()) {
            logalloc::reclaim_lock _(_region);
            return make_reader_returning(mutation(_schema, i->key(), i->partition()));
        } else {
            return make_empty_reader();
        }
    } else {
        return make_mutation_reader<scanning_reader>(shared_from_this(), range);
    }
}

void
memtable::update(const db::replay_position& rp) {
    if (_replay_position < rp) {
        _replay_position = rp;
    }
}

void
memtable::apply(const mutation& m, const db::replay_position& rp) {
    with_allocator(_region.allocator(), [this, &m] {
        logalloc::reclaim_lock _(_region);
        mutation_partition& p = find_or_create_partition(m.decorated_key());
        p.apply(*_schema, m.partition());
    });
    update(rp);
}

void
memtable::apply(const frozen_mutation& m, const db::replay_position& rp) {
    with_allocator(_region.allocator(), [this, &m] {
        logalloc::reclaim_lock _(_region);
        mutation_partition& p = find_or_create_partition_slow(m.key(*_schema));
        p.apply(*_schema, m.partition());
    });
    update(rp);
}

logalloc::occupancy_stats memtable::occupancy() const {
    return _region.occupancy();
}

mutation_source memtable::as_data_source() {
    return [mt = shared_from_this()] (const query::partition_range& range) {
        return mt->make_reader(range);
    };
}

size_t memtable::partition_count() const {
    return partitions.size();
}

partition_entry::partition_entry(partition_entry&& o) noexcept
    : _link()
    , _key(std::move(o._key))
    , _p(std::move(o._p))
{
    using container_type = memtable::partitions_type;
    container_type::node_algorithms::replace_node(o._link.this_ptr(), _link.this_ptr());
    container_type::node_algorithms::init(o._link.this_ptr());
}

void memtable::mark_flushed(lw_shared_ptr<sstables::sstable> sst) {
    _sstable = std::move(sst);
}

bool memtable::is_flushed() const {
    return bool(_sstable);
}
