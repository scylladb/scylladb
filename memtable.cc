/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "memtable.hh"
#include "frozen_mutation.hh"

namespace stdx = std::experimental;

memtable::memtable(schema_ptr schema)
        : _schema(std::move(schema))
        , partitions(partition_entry::compare(_schema)) {
}

memtable::~memtable() {
    with_allocator(_region.allocator(), [this] {
        partitions.clear_and_dispose(current_deleter<partition_entry>());
    });
}

mutation_partition&
memtable::find_or_create_partition_slow(partition_key_view key) {
    // FIXME: Perform lookup using std::pair<token, partition_key_view>
    // to avoid unconditional copy of the partition key.
    // We can't do it right now because std::map<> which holds
    // partitions doesn't support heterogenous lookup.
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

class scanning_reader {
    lw_shared_ptr<const memtable> _memtable;
    const query::partition_range& _range;
    stdx::optional<dht::decorated_key> _last;
private:
    memtable::partitions_type::const_iterator current() {
        // We must be prepared that iterators may get invalidated across deferring points,
        // due to LSA compactions.
        // FIXME: Avoid lookup if the iterators didn't get invalidated. We could consult
        // compaction counter of _memtable->_region.
        auto cmp = partition_entry::compare(_memtable->_schema);
        if (_last) {
            return _memtable->partitions.upper_bound(*_last, cmp);
        } else {
            auto i = _range.start()
                 ? (_range.start()->is_inclusive()
                    ? _memtable->partitions.lower_bound(_range.start()->value(), cmp)
                    : _memtable->partitions.upper_bound(_range.start()->value(), cmp))
                 : _memtable->partitions.cbegin();
            return i;
        }
    }
    memtable::partitions_type::const_iterator end() {
        // FIXME: Same comment as for current()
        auto cmp = partition_entry::compare(_memtable->_schema);
        return _range.end()
               ? (_range.end()->is_inclusive()
                  ? _memtable->partitions.upper_bound(_range.end()->value(), cmp)
                  : _memtable->partitions.lower_bound(_range.end()->value(), cmp))
               : _memtable->partitions.cend();
    }
public:
    scanning_reader(lw_shared_ptr<const memtable> m, const query::partition_range& range)
        : _memtable(std::move(m))
        , _range(range)
    { }

    future<mutation_opt> operator()() {
        auto i = current();
        if (i == end()) {
            return make_ready_future<mutation_opt>(stdx::nullopt);
        }
        _last = i->key();
        return make_ready_future<mutation_opt>(mutation(_memtable->_schema, i->key(), i->partition()));
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
            return make_reader_returning(mutation(_schema, i->key(), i->partition()));
        } else {
            return make_empty_reader();
        }
    } else {
        return scanning_reader(shared_from_this(), range);
    }
}

row&
memtable::find_or_create_row_slow(const partition_key& partition_key, const clustering_key& clustering_key) {
    mutation_partition& p = find_or_create_partition_slow(partition_key);
    return p.clustered_row(clustering_key).cells();
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
        mutation_partition& p = find_or_create_partition(m.decorated_key());
        p.apply(*_schema, m.partition());
    });
    update(rp);
}

void
memtable::apply(const frozen_mutation& m, const db::replay_position& rp) {
    with_allocator(_region.allocator(), [this, &m] {
        mutation_partition& p = find_or_create_partition_slow(m.key(*_schema));
        p.apply(*_schema, m.partition());
    });
    update(rp);
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
