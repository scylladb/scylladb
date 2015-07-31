/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "memtable.hh"
#include "frozen_mutation.hh"

memtable::memtable(schema_ptr schema)
        : _schema(std::move(schema))
        , partitions(partition_entry::compare(_schema)) {
}

memtable::~memtable() {
    partitions.clear_and_dispose(std::default_delete<partition_entry>());
}

memtable::const_mutation_partition_ptr
memtable::find_partition(const dht::decorated_key& key) const {
    auto i = partitions.find(key, partition_entry::compare(_schema));
    // FIXME: remove copy if only one data source
    return i == partitions.end() ? const_mutation_partition_ptr() : std::make_unique<const mutation_partition>(i->partition());
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
    auto i = partitions.lower_bound(key, partition_entry::compare(_schema));
    if (i == partitions.end() || !key.equal(*_schema, i->key())) {
        auto entry = std::make_unique<partition_entry>(std::move(key), mutation_partition(_schema));
        i = partitions.insert(i, *entry.release());
    }
    return i->partition();
}

boost::iterator_range<memtable::partitions_type::const_iterator>
memtable::slice(const query::partition_range& range) const {
    if (range.is_singular() && range.start()->value().has_key()) {
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

mutation_reader
memtable::make_reader(const query::partition_range& range) const {
    if (query::is_wrap_around(range, *_schema)) {
        fail(unimplemented::cause::WRAP_AROUND);
    }

    auto r = slice(range);
    return [begin = r.begin(), end = r.end(), self = shared_from_this()] () mutable {
        if (begin != end) {
            auto m = mutation(self->_schema, begin->key(), begin->partition());
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

mutation_source memtable::as_data_source() {
    return [mt = shared_from_this()] (const query::partition_range& range) {
        return mt->make_reader(range);
    };
}

size_t memtable::partition_count() const {
    return partitions.size();
}
