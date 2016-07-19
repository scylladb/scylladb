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

#include "mutation.hh"
#include "query-result-writer.hh"

mutation::data::data(dht::decorated_key&& key, schema_ptr&& schema)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(_schema)
{ }

mutation::data::data(partition_key&& key_, schema_ptr&& schema)
    : _schema(std::move(schema))
    , _dk(dht::global_partitioner().decorate_key(*_schema, std::move(key_)))
    , _p(_schema)
{ }

mutation::data::data(schema_ptr&& schema, dht::decorated_key&& key, const mutation_partition& mp)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(mp)
{ }

mutation::data::data(schema_ptr&& schema, dht::decorated_key&& key, mutation_partition&& mp)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(std::move(mp))
{ }

void mutation::set_static_cell(const column_definition& def, atomic_cell_or_collection&& value) {
    partition().static_row().apply(def, std::move(value));
}

void mutation::set_static_cell(const bytes& name, const data_value& value, api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    if (!column_def->is_static()) {
        throw std::runtime_error(sprint("column '%s' is not static", name));
    }
    partition().static_row().apply(*column_def, atomic_cell::make_live(timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value) {
    auto& row = partition().clustered_row(clustering_key::from_clustering_prefix(*schema(), prefix)).cells();
    row.apply(def, std::move(value));
}

void mutation::set_clustered_cell(const clustering_key& key, const bytes& name, const data_value& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    return set_clustered_cell(key, *column_def, atomic_cell::make_live(timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection&& value) {
    auto& row = partition().clustered_row(key).cells();
    row.apply(def, std::move(value));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const data_value& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    return set_cell(prefix, *column_def, atomic_cell::make_live(timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value) {
    if (def.is_static()) {
        set_static_cell(def, std::move(value));
    } else if (def.is_regular()) {
        set_clustered_cell(prefix, def, std::move(value));
    } else {
        throw std::runtime_error("attemting to store into a key cell");
    }
}

std::experimental::optional<atomic_cell_or_collection>
mutation::get_cell(const clustering_key& rkey, const column_definition& def) const {
    if (def.is_static()) {
        const atomic_cell_or_collection* cell = partition().static_row().find_cell(def.id);
        if (!cell) {
            return {};
        }
        return { *cell };
    } else {
        const row* r = partition().find_row(rkey);
        if (!r) {
            return {};
        }
        const atomic_cell_or_collection* cell = r->find_cell(def.id);
        return { *cell };
    }
}

bool mutation::operator==(const mutation& m) const {
    return decorated_key().equal(*schema(), m.decorated_key())
           && partition().equal(*schema(), m.partition(), *m.schema());
}

bool mutation::operator!=(const mutation& m) const {
    return !(*this == m);
}

void
mutation::query(query::result::builder& builder,
    const query::partition_slice& slice,
    gc_clock::time_point now,
    uint32_t row_limit) &&
{
    auto pb = builder.add_partition(*schema(), key());
    auto is_reversed = slice.options.contains<query::partition_slice::option::reversed>();
    mutation_partition& p = partition();
    auto limit = std::min(row_limit, slice.partition_row_limit());
    p.compact_for_query(*schema(), now, slice.row_ranges(*schema(), key()), is_reversed, limit);
    p.query_compacted(pb, *schema(), limit);
}

query::result
mutation::query(const query::partition_slice& slice,
    query::result_request request,
    gc_clock::time_point now, uint32_t row_limit) &&
{
    query::result::builder builder(slice, request);
    std::move(*this).query(builder, slice, now, row_limit);
    return builder.build();
}

query::result
mutation::query(const query::partition_slice& slice,
    query::result_request request,
    gc_clock::time_point now, uint32_t row_limit) const&
{
    return mutation(*this).query(slice, request, now, row_limit);
}

size_t
mutation::live_row_count(gc_clock::time_point query_time) const {
    return partition().live_row_count(*schema(), query_time);
}

bool
mutation_decorated_key_less_comparator::operator()(const mutation& m1, const mutation& m2) const {
    return m1.decorated_key().less_compare(*m1.schema(), m2.decorated_key());
}

boost::iterator_range<std::vector<mutation>::const_iterator>
slice(const std::vector<mutation>& partitions, const query::partition_range& r) {
    struct cmp {
        bool operator()(const dht::ring_position& pos, const mutation& m) const {
            return m.decorated_key().tri_compare(*m.schema(), pos) > 0;
        };
        bool operator()(const mutation& m, const dht::ring_position& pos) const {
            return m.decorated_key().tri_compare(*m.schema(), pos) < 0;
        };
    };

    return boost::make_iterator_range(
        r.start()
            ? (r.start()->is_inclusive()
                ? std::lower_bound(partitions.begin(), partitions.end(), r.start()->value(), cmp())
                : std::upper_bound(partitions.begin(), partitions.end(), r.start()->value(), cmp()))
            : partitions.cbegin(),
        r.end()
            ? (r.end()->is_inclusive()
              ? std::upper_bound(partitions.begin(), partitions.end(), r.end()->value(), cmp())
              : std::lower_bound(partitions.begin(), partitions.end(), r.end()->value(), cmp()))
            : partitions.cend());
}

void
mutation::upgrade(const schema_ptr& new_schema) {
    if (_ptr->_schema != new_schema) {
        schema_ptr s = new_schema;
        partition().upgrade(*schema(), *new_schema);
        _ptr->_schema = std::move(s);
    }
}

void mutation::apply(mutation&& m) {
    partition().apply(*schema(), std::move(m.partition()), *m.schema());
}

void mutation::apply(const mutation& m) {
    partition().apply(*schema(), m.partition(), *m.schema());
}

mutation& mutation::operator=(const mutation& m) {
    return *this = mutation(m);
}

enum class limit_mutation_size { yes, no };

template <limit_mutation_size with_limit>
class mutation_rebuilder {
    mutation _m;
    streamed_mutation& _sm;
    size_t _remaining_limit;

    template <typename T> bool check_remaining_limit(const T& e) {
        if (with_limit == limit_mutation_size::no) {
            return true;
        }
        size_t size = e.memory_usage();
        if (_remaining_limit <= size) {
            _remaining_limit = 0;
        } else {
            _remaining_limit -= size;
        }
        return _remaining_limit > 0;
    }
public:
    mutation_rebuilder(streamed_mutation& sm)
        : _m(sm.decorated_key(), sm.schema()), _sm(sm), _remaining_limit(0) {
        static_assert(with_limit == limit_mutation_size::no,
                     "This constructor should be used only for mutation_rebuildeer with no limit");
    }
    mutation_rebuilder(streamed_mutation& sm, size_t limit)
        : _m(sm.decorated_key(), sm.schema()), _sm(sm), _remaining_limit(limit) {
        static_assert(with_limit == limit_mutation_size::yes,
                      "This constructor should be used only for mutation_rebuildeer with limit");
        check_remaining_limit(_m.key());
    }

    stop_iteration consume(tombstone t) {
        _m.partition().apply(t);
        return stop_iteration::no;
    }

    stop_iteration consume(range_tombstone&& rt) {
        if (!check_remaining_limit(rt)) {
            return stop_iteration::yes;
        }
        _m.partition().apply_row_tombstone(*_m.schema(), std::move(rt));
        return stop_iteration::no;
    }

    stop_iteration consume(static_row&& sr) {
        if (!check_remaining_limit(sr)) {
            return stop_iteration::yes;
        }
        _m.partition().static_row().apply(*_m.schema(), column_kind::static_column, std::move(sr.cells()));
        return stop_iteration::no;
    }

    stop_iteration consume(clustering_row&& cr) {
        if (!check_remaining_limit(cr)) {
            return stop_iteration::yes;
        }
        auto& dr = _m.partition().clustered_row(std::move(cr.key()));
        dr.apply(cr.tomb());
        dr.apply(cr.marker());
        dr.cells().apply(*_m.schema(), column_kind::regular_column, std::move(cr.cells()));
        return stop_iteration::no;
    }

    mutation_opt consume_end_of_stream() {
        return with_limit == limit_mutation_size::yes && _remaining_limit == 0 ? mutation_opt()
                                                                               : mutation_opt(std::move(_m));
    }
};

future<mutation_opt>
mutation_from_streamed_mutation_with_limit(streamed_mutation sm, size_t limit) {
    return do_with(std::move(sm), [limit] (auto& sm) {
        return consume(sm, mutation_rebuilder<limit_mutation_size::yes>(sm, limit));
    });
}

future<mutation_opt> mutation_from_streamed_mutation(streamed_mutation_opt sm) {
    if (!sm) {
        return make_ready_future<mutation_opt>();
    }
    return do_with(std::move(*sm), [] (auto& sm) {
        return consume(sm, mutation_rebuilder<limit_mutation_size::no>(sm));
    });
}

