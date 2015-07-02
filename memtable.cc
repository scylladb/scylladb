/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "memtable.hh"
#include "frozen_mutation.hh"

memtable::memtable(schema_ptr schema)
        : _schema(std::move(schema))
        , partitions(dht::decorated_key::less_comparator(_schema)) {
}

memtable::const_mutation_partition_ptr
memtable::find_partition(const dht::decorated_key& key) const {
    auto i = partitions.find(key);
    // FIXME: remove copy if only one data source
    return i == partitions.end() ? const_mutation_partition_ptr() : std::make_unique<const mutation_partition>(i->second);
}

mutation_partition&
memtable::find_or_create_partition_slow(partition_key_view key) {
    // FIXME: Perform lookup using std::pair<token, partition_key_view>
    // to avoid unconditional copy of the partition key.
    // We can't do it right now because std::map<> which holds
    // partitions doesn't support heterogenous lookup.
    // We could switch to boost::intrusive_map<> similar to what we have for row keys.
    return find_or_create_partition(dht::global_partitioner().decorate_key(*_schema, key));
}

mutation_partition&
memtable::find_or_create_partition(const dht::decorated_key& key) {
    // call lower_bound so we have a hint for the insert, just in case.
    auto i = partitions.lower_bound(key);
    if (i == partitions.end() || !key.equal(*_schema, i->first)) {
        i = partitions.emplace_hint(i, std::make_pair(std::move(key), mutation_partition(_schema)));
    }
    return i->second;
}

const memtable::partitions_type&
memtable::all_partitions() const {
    return partitions;
}

boost::iterator_range<memtable::partitions_type::const_iterator>
memtable::slice(const query::partition_range& range) const {
    if (range.is_singular()) {
        const query::ring_position& pos = range.start_value();

        if (!pos.has_key()) {
            fail(unimplemented::cause::RANGE_QUERIES);
        }

        auto i = partitions.find(pos.as_decorated_key());
        if (i != partitions.end()) {
            return boost::make_iterator_range(i, std::next(i));
        } else {
            return boost::make_iterator_range(i, i);
        }
    } else {
        if (!range.is_full()) {
            fail(unimplemented::cause::RANGE_QUERIES);
        }

        return boost::make_iterator_range(partitions.begin(), partitions.end());
    }
}

mutation_reader
memtable::make_reader(const query::partition_range& range) const {
    auto r = slice(range);
    return [begin = r.begin(), end = r.end(), self = shared_from_this()] () mutable {
        if (begin != end) {
            auto m = mutation(self->_schema, begin->first, begin->second);
            ++begin;
            return make_ready_future<mutation_opt>(std::experimental::make_optional(std::move(m)));
        } else {
            return make_ready_future<mutation_opt>();
        }
    };
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
    mutation_partition& p = find_or_create_partition(m.decorated_key());
    p.apply(*_schema, m.partition());
    update(rp);
}

void
memtable::apply(const frozen_mutation& m, const db::replay_position& rp) {
    mutation_partition& p = find_or_create_partition_slow(m.key(*_schema));
    p.apply(*_schema, m.partition());
    update(rp);
}
