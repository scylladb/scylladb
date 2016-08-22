/*
 * Copyright (C) 2014 ScyllaDB
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
#include "database.hh"
#include "frozen_mutation.hh"
#include "sstable_mutation_readers.hh"

namespace stdx = std::experimental;

memtable::memtable(schema_ptr schema, logalloc::region_group* dirty_memory_region_group)
        : logalloc::region(dirty_memory_region_group ? logalloc::region(*dirty_memory_region_group) : logalloc::region())
        , _schema(std::move(schema))
        , partitions(memtable_entry::compare(_schema)) {
}

memtable::~memtable() {
    with_allocator(allocator(), [this] {
        partitions.clear_and_dispose(current_deleter<memtable_entry>());
    });
}

partition_entry&
memtable::find_or_create_partition_slow(partition_key_view key) {
    assert(!reclaiming_enabled());

    // FIXME: Perform lookup using std::pair<token, partition_key_view>
    // to avoid unconditional copy of the partition key.
    // We can't do it right now because std::map<> which holds
    // partitions doesn't support heterogeneous lookup.
    // We could switch to boost::intrusive_map<> similar to what we have for row keys.
    auto& outer = current_allocator();
    return with_allocator(standard_allocator(), [&, this] () -> partition_entry& {
        auto dk = dht::global_partitioner().decorate_key(*_schema, key);
        return with_allocator(outer, [&dk, this] () -> partition_entry& {
          return with_linearized_managed_bytes([&] () -> partition_entry& {
            return find_or_create_partition(dk);
          });
        });
    });
}

partition_entry&
memtable::find_or_create_partition(const dht::decorated_key& key) {
    assert(!reclaiming_enabled());

    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key, memtable_entry::compare(_schema));
    if (i == partitions.end() || !key.equal(*_schema, i->key())) {
        memtable_entry* entry = current_allocator().construct<memtable_entry>(
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
        auto i = partitions.find(pos, memtable_entry::compare(_schema));
        if (i != partitions.end()) {
            return boost::make_iterator_range(i, std::next(i));
        } else {
            return boost::make_iterator_range(i, i);
        }
    } else {
        auto cmp = memtable_entry::compare(_schema);

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

class iterator_reader: public mutation_reader::impl {
    lw_shared_ptr<memtable> _memtable;
    schema_ptr _schema;
    const query::partition_range& _range;
    stdx::optional<dht::decorated_key> _last;
    memtable::partitions_type::iterator _i;
    memtable::partitions_type::iterator _end;
    uint64_t _last_reclaim_counter;
    size_t _last_partition_count = 0;

    memtable::partitions_type::iterator lookup_end() {
        auto cmp = memtable_entry::compare(_memtable->_schema);
        return _range.end()
            ? (_range.end()->is_inclusive()
                ? _memtable->partitions.upper_bound(_range.end()->value(), cmp)
                : _memtable->partitions.lower_bound(_range.end()->value(), cmp))
            : _memtable->partitions.end();
    }
    void update_iterators() {
        // We must be prepared that iterators may get invalidated during compaction.
        auto current_reclaim_counter = _memtable->reclaim_counter();
        auto cmp = memtable_entry::compare(_memtable->_schema);
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
protected:
    iterator_reader(schema_ptr s,
                    lw_shared_ptr<memtable> m,
                    const query::partition_range& range)
        : _memtable(std::move(m))
        , _schema(std::move(s))
        , _range(range)
    { }

    memtable_entry* fetch_next_entry() {
        update_iterators();
        if (_i == _end) {
            return nullptr;
        } else {
            memtable_entry& e = *_i;
            ++_i;
            _last = e.key();
            _memtable->upgrade_entry(e);
            return &e;
        }
    }

    logalloc::allocating_section& read_section() {
        return _memtable->_read_section;
    }

    lw_shared_ptr<memtable> mtbl() {
        return _memtable;
    }

    schema_ptr schema() {
        return _schema;
    }

    logalloc::region& region() {
        return *_memtable;
    };

    std::experimental::optional<query::partition_range> get_delegate_range() {
        // We cannot run concurrently with row_cache::update().
        if (_memtable->is_flushed()) {
            return _last ? _range.split_after(*_last, dht::ring_position_comparator(*_memtable->_schema)) : _range;
        }
        return {};
    }

    mutation_reader delegate_reader(const query::partition_range& delegate,
                                    const query::partition_slice& slice,
                                    const io_priority_class& pc) {
        auto ret = make_mutation_reader<sstable_range_wrapping_reader>(
            _memtable->_sstable, _schema, delegate, slice, pc);
        _memtable = {};
        _last = {};
        return ret;
    }
};

class scanning_reader final: public iterator_reader {
    stdx::optional<query::partition_range> _delegate_range;
    mutation_reader _delegate;
    const io_priority_class& _pc;
    const query::partition_slice& _slice;
public:
     scanning_reader(schema_ptr s,
                     lw_shared_ptr<memtable> m,
                     const query::partition_range& range,
                     const query::partition_slice& slice,
                     const io_priority_class& pc)
         : iterator_reader(std::move(s), std::move(m), range)
         , _pc(pc)
         , _slice(slice)
     { }

    virtual future<streamed_mutation_opt> operator()() override {
        if (_delegate_range) {
            return _delegate();
        }

        // FIXME: Use cache. See column_family::make_reader().
        _delegate_range = get_delegate_range();
        if (_delegate_range) {
            _delegate = delegate_reader(*_delegate_range, _slice, _pc);
            return _delegate();
        }

        logalloc::reclaim_lock _(region());
        managed_bytes::linearization_context_guard lcg;
        memtable_entry* e = fetch_next_entry();
        if (!e) {
             return make_ready_future<streamed_mutation_opt>(stdx::nullopt);
        } else {
            return make_ready_future<streamed_mutation_opt>(e->read(mtbl(), schema(), _slice));
        }
    }
};

class flush_memory_accounter {
    uint64_t _bytes_read = 0;
    logalloc::region& _region;

public:
    void update_bytes_read(uint64_t delta) {
        _bytes_read += delta;
        dirty_memory_manager::from_region_group(_region.group()).account_potentially_cleaned_up_memory(delta);
    }

    explicit flush_memory_accounter(logalloc::region& region)
        : _region(region)
	{}

    ~flush_memory_accounter() {
        assert(_bytes_read <= _region.occupancy().used_space());
        dirty_memory_manager::from_region_group(_region.group()).revert_potentially_cleaned_up_memory(_bytes_read);
    }
    void account_component(memtable_entry& e) {
        auto delta = _region.allocator().object_memory_size_in_allocator(&e) +
                     _region.allocator().object_memory_size_in_allocator(&*(partition_snapshot(e.schema(), &(e.partition())).version())) +
                     e.memory_usage_without_rows();
        update_bytes_read(delta);
    }
};

class partition_snapshot_accounter {
    flush_memory_accounter& _accounter;
public:
    partition_snapshot_accounter(flush_memory_accounter& acct): _accounter(acct) {}

    // We will be passed mutation fragments here, and they are allocated using the standard
    // allocator. So we can't compute the size in memtable precisely. However, precise accounting is
    // hard anyway, since we may be holding multiple snapshots of the partitions, and the
    // partition_snapshot_reader may compose them. In doing so, we move memory to the standard
    // allocation. As long as our size read here is lesser or equal to the size in the memtables, we
    // are safe, and worst case we will allow a bit fewer requests in.
    void operator()(const range_tombstone& rt) {
        _accounter.update_bytes_read(sizeof(range_tombstone) + rt.memory_usage());
    }

    void operator()(const static_row& sr) {
        _accounter.update_bytes_read(sr.memory_usage());
    }

    void operator()(const clustering_row& cr) {
        // Every clustering row is stored in a rows_entry object, and that has some significant
        // overhead - so add it here. We will be a bit short on our estimate because we can't know
        // what is the size in the allocator for this rows_entry object: we may have many snapshots,
        // and we don't know which one(s) contributed to the generation of this mutation fragment.
        //
        // We will add the size of the struct here, and that should be good enough.
        _accounter.update_bytes_read(sizeof(rows_entry) + cr.memory_usage());
    }
};

class flush_reader final : public iterator_reader {
    flush_memory_accounter _flushed_memory;
public:
    flush_reader(schema_ptr s, lw_shared_ptr<memtable> m)
        : iterator_reader(std::move(s), std::move(m), query::full_partition_range)
        , _flushed_memory(region())
    {}
    flush_reader(const flush_reader&) = delete;
    flush_reader(flush_reader&&) = delete;
    flush_reader& operator=(flush_reader&&) = delete;
    flush_reader& operator=(const flush_reader&) = delete;

    virtual future<streamed_mutation_opt> operator()() override {
        logalloc::reclaim_lock _(region());
        managed_bytes::linearization_context_guard lcg;
        memtable_entry* e = fetch_next_entry();
        if (!e) {
            return make_ready_future<streamed_mutation_opt>(stdx::nullopt);
        } else {
            auto cr = query::clustering_key_filter_ranges::get_ranges(*schema(), query::full_slice, e->key().key());
            auto snp = e->partition().read(schema());
            auto mpsr = make_partition_snapshot_reader<partition_snapshot_accounter>(schema(), e->key(), std::move(cr), snp, region(), read_section(), mtbl(), _flushed_memory);
            _flushed_memory.account_component(*e);
            return make_ready_future<streamed_mutation_opt>(std::move(mpsr));
        }
    }
};

mutation_reader
memtable::make_reader(schema_ptr s,
                      const query::partition_range& range,
                      const query::partition_slice& slice,
                      const io_priority_class& pc) {
    if (query::is_wrap_around(range, *s)) {
        fail(unimplemented::cause::WRAP_AROUND);
    }

    if (query::is_single_partition(range)) {
        const query::ring_position& pos = range.start()->value();
        return _read_section(*this, [&] {
        managed_bytes::linearization_context_guard lcg;
        auto i = partitions.find(pos, memtable_entry::compare(_schema));
        if (i != partitions.end()) {
            upgrade_entry(*i);
            return make_reader_returning(i->read(shared_from_this(), s, slice));
        } else {
            return make_empty_reader();
        }
        });
    } else {
        return make_mutation_reader<scanning_reader>(std::move(s), shared_from_this(), range, slice, pc);
    }
}

mutation_reader
memtable::make_flush_reader(schema_ptr s) {
    return make_mutation_reader<flush_reader>(std::move(s), shared_from_this());
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
    with_allocator(allocator(), [this, &m] {
        _allocating_section(*this, [&, this] {
          with_linearized_managed_bytes([&] {
            auto& p = find_or_create_partition(m.decorated_key());
            p.apply(*_schema, m.partition(), *m.schema());
          });
        });
    });
    update(rp);
}

void
memtable::apply(const frozen_mutation& m, const schema_ptr& m_schema, const db::replay_position& rp) {
    with_allocator(allocator(), [this, &m, &m_schema] {
        _allocating_section(*this, [&, this] {
          with_linearized_managed_bytes([&] {
            auto& p = find_or_create_partition_slow(m.key(*_schema));
            p.apply(*_schema, m.partition(), *m_schema);
          });
        });
    });
    update(rp);
}

logalloc::occupancy_stats memtable::occupancy() const {
    return logalloc::region::occupancy();
}

mutation_source memtable::as_data_source() {
    return mutation_source([mt = shared_from_this()] (schema_ptr s, const query::partition_range& range) {
        return mt->make_reader(std::move(s), range);
    });
}

key_source memtable::as_key_source() {
    return key_source([mt = shared_from_this()] (const query::partition_range& range) {
        return make_key_from_mutation_reader(mt->make_reader(mt->_schema, range));
    });
}

size_t memtable::partition_count() const {
    return partitions.size();
}

memtable_entry::memtable_entry(memtable_entry&& o) noexcept
    : _link()
    , _schema(std::move(o._schema))
    , _key(std::move(o._key))
    , _pe(std::move(o._pe))
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

streamed_mutation
memtable_entry::read(lw_shared_ptr<memtable> mtbl, const schema_ptr& target_schema, const query::partition_slice& slice) {
    auto cr = query::clustering_key_filter_ranges::get_ranges(*_schema, slice, _key.key());
    if (_schema->version() != target_schema->version()) {
        auto mp = mutation_partition(_pe.squashed(_schema, target_schema), *target_schema, std::move(cr));
        mutation m = mutation(target_schema, _key, std::move(mp));
        return streamed_mutation_from_mutation(std::move(m));
    }
    auto snp = _pe.read(_schema);
    return make_partition_snapshot_reader(_schema, _key, std::move(cr), snp, *mtbl, mtbl->_read_section, mtbl);
}

void memtable::upgrade_entry(memtable_entry& e) {
    if (e._schema != _schema) {
        assert(!reclaiming_enabled());
        with_allocator(allocator(), [this, &e] {
          with_linearized_managed_bytes([&] {
            e.partition().upgrade(e._schema, _schema);
            e._schema = _schema;
          });
        });
    }
}

void memtable::set_schema(schema_ptr new_schema) noexcept {
    _schema = std::move(new_schema);
}
