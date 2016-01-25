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
            _schema, dht::decorated_key(key), mutation_partition(_schema));
        i = partitions.insert(i, *entry);
        return entry->partition();
    } else {
        upgrade_entry(*i);
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
    lw_shared_ptr<memtable> _memtable;
    schema_ptr _schema;
    const query::partition_range& _range;
    stdx::optional<dht::decorated_key> _last;
    memtable::partitions_type::iterator _i;
    memtable::partitions_type::iterator _end;
    uint64_t _last_reclaim_counter;
    size_t _last_partition_count = 0;
    stdx::optional<query::partition_range> _delegate_range;
    mutation_reader _delegate;
private:
    memtable::partitions_type::iterator lookup_end() {
        auto cmp = partition_entry::compare(_memtable->_schema);
        return _range.end()
            ? (_range.end()->is_inclusive()
                ? _memtable->partitions.upper_bound(_range.end()->value(), cmp)
                : _memtable->partitions.lower_bound(_range.end()->value(), cmp))
            : _memtable->partitions.end();
    }
    void update_iterators() {
        // We must be prepared that iterators may get invalidated during compaction.
        auto current_reclaim_counter = _memtable->_region.reclaim_counter();
        auto cmp = partition_entry::compare(_memtable->_schema);
        if (_last) {
            if (current_reclaim_counter != _last_reclaim_counter ||
                  _last_partition_count != _memtable->partition_count()) {
                _i = _memtable->partitions.upper_bound(*_last, cmp);
                _end = lookup_end();
                _last_partition_count = _memtable->partition_count();
            }
        } else {
            // Initial lookup
            _i = _range.start()
                 ? (_range.start()->is_inclusive()
                    ? _memtable->partitions.lower_bound(_range.start()->value(), cmp)
                    : _memtable->partitions.upper_bound(_range.start()->value(), cmp))
                 : _memtable->partitions.begin();
            _end = lookup_end();
            _last_partition_count = _memtable->partition_count();
        }
        _last_reclaim_counter = current_reclaim_counter;
    }
public:
    scanning_reader(schema_ptr s, lw_shared_ptr<memtable> m, const query::partition_range& range)
        : _memtable(std::move(m))
        , _schema(std::move(s))
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
                _memtable->_sstable, _schema, *_delegate_range);
            _memtable = {};
            _last = {};
            return _delegate();
        }

        logalloc::reclaim_lock _(_memtable->_region);
        update_iterators();
        if (_i == _end) {
            return make_ready_future<mutation_opt>(stdx::nullopt);
        }
        partition_entry& e = *_i;
        ++_i;
        _last = e.key();
        _memtable->upgrade_entry(e);
        return make_ready_future<mutation_opt>(e.read(_schema));
    }
};

mutation_reader
memtable::make_reader(schema_ptr s, const query::partition_range& range) {
    if (query::is_wrap_around(range, *s)) {
        fail(unimplemented::cause::WRAP_AROUND);
    }

    if (query::is_single_partition(range)) {
        const query::ring_position& pos = range.start()->value();
        return _read_section(_region, [&] {
        auto i = partitions.find(pos, partition_entry::compare(_schema));
        if (i != partitions.end()) {
            upgrade_entry(*i);
            return make_reader_returning(i->read(s));
        } else {
            return make_empty_reader();
        }
        });
    } else {
        return make_mutation_reader<scanning_reader>(std::move(s), shared_from_this(), range);
    }
}

void
memtable::update(const db::replay_position& rp) {
    if (_replay_position < rp) {
        _replay_position = rp;
    }
}

future<>
memtable::apply(memtable& mt) {
    return do_with(mt.make_reader(_schema), [this] (auto&& rd) mutable {
        return consume(rd, [self = this->shared_from_this(), &rd] (mutation&& m) {
            self->apply(m);
            return stop_iteration::no;
        });
    });
}

void
memtable::apply(const mutation& m, const db::replay_position& rp) {
    with_allocator(_region.allocator(), [this, &m] {
        _allocating_section(_region, [&, this] {
            mutation_partition& p = find_or_create_partition(m.decorated_key());
            p.apply(*_schema, m.partition(), *m.schema());
        });
    });
    update(rp);
}

void
memtable::apply(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& rp) {
    with_allocator(_region.allocator(), [this, &m, &m_schema] {
        _allocating_section(_region, [&, this] {
            mutation_partition& p = find_or_create_partition_slow(m.key(*_schema));
            p.apply(*_schema, m.partition(), *m_schema);
        });
    });
    update(rp);
}

logalloc::occupancy_stats memtable::occupancy() const {
    return _region.occupancy();
}

mutation_source memtable::as_data_source() {
    return mutation_source([mt = shared_from_this()] (schema_ptr s, const query::partition_range& range) {
        return mt->make_reader(std::move(s), range);
    });
}

key_source memtable::as_key_source() {
    return [mt = shared_from_this()] (const query::partition_range& range) {
        return make_key_from_mutation_reader(mt->make_reader(mt->_schema, range));
    };
}

size_t memtable::partition_count() const {
    return partitions.size();
}

partition_entry::partition_entry(partition_entry&& o) noexcept
    : _link()
    , _schema(std::move(o._schema))
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

mutation partition_entry::read(const schema_ptr& target_schema) {
    auto m = mutation(_schema, _key, _p);
    m.upgrade(target_schema);
    return m;
}

void memtable::upgrade_entry(partition_entry& e) {
    if (e._schema != _schema) {
        assert(!_region.reclaiming_enabled());
        with_allocator(_region.allocator(), [this, &e] {
            e._p.upgrade(*e._schema, *_schema);
            e._schema = _schema;
        });
    }
}

void memtable::set_schema(schema_ptr new_schema) noexcept {
    _schema = std::move(new_schema);
}
