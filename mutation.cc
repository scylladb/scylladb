/*
 * Copyright (C) 2014-present ScyllaDB
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
#include "flat_mutation_reader.hh"
#include "mutation_rebuilder.hh"

mutation::data::data(dht::decorated_key&& key, schema_ptr&& schema)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(_schema)
{ }

mutation::data::data(partition_key&& key_, schema_ptr&& schema)
    : _schema(std::move(schema))
    , _dk(dht::decorate_key(*_schema, std::move(key_)))
    , _p(_schema)
{ }

mutation::data::data(schema_ptr&& schema, dht::decorated_key&& key, const mutation_partition& mp)
    : _schema(schema)
    , _dk(std::move(key))
    , _p(*schema, mp)
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
        throw std::runtime_error(format("no column definition found for '{}'", name));
    }
    if (!column_def->is_static()) {
        throw std::runtime_error(format("column '{}' is not static", name));
    }
    partition().static_row().apply(*column_def, atomic_cell::make_live(*column_def->type, timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const clustering_key& key, const bytes& name, const data_value& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(format("no column definition found for '{}'", name));
    }
    return set_clustered_cell(key, *column_def, atomic_cell::make_live(*column_def->type, timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection&& value) {
    auto& row = partition().clustered_row(*schema(), key).cells();
    row.apply(def, std::move(value));
}

void mutation::set_cell(const clustering_key_prefix& prefix, const bytes& name, const data_value& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema()->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(format("no column definition found for '{}'", name));
    }
    return set_cell(prefix, *column_def, atomic_cell::make_live(*column_def->type, timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_cell(const clustering_key_prefix& prefix, const column_definition& def, atomic_cell_or_collection&& value) {
    if (def.is_static()) {
        set_static_cell(def, std::move(value));
    } else if (def.is_regular()) {
        set_clustered_cell(prefix, def, std::move(value));
    } else {
        throw std::runtime_error("attemting to store into a key cell");
    }
}

bool mutation::operator==(const mutation& m) const {
    return decorated_key().equal(*schema(), m.decorated_key())
           && partition().equal(*schema(), m.partition(), *m.schema());
}

bool mutation::operator!=(const mutation& m) const {
    return !(*this == m);
}

uint64_t
mutation::live_row_count(gc_clock::time_point query_time) const {
    return partition().live_row_count(*schema(), query_time);
}

bool
mutation_decorated_key_less_comparator::operator()(const mutation& m1, const mutation& m2) const {
    return m1.decorated_key().less_compare(*m1.schema(), m2.decorated_key());
}

boost::iterator_range<std::vector<mutation>::const_iterator>
slice(const std::vector<mutation>& partitions, const dht::partition_range& r) {
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
    mutation_application_stats app_stats;
    partition().apply(*schema(), std::move(m.partition()), *m.schema(), app_stats);
}

void mutation::apply(const mutation& m) {
    mutation_application_stats app_stats;
    partition().apply(*schema(), m.partition(), *m.schema(), app_stats);
}

void mutation::apply(const mutation_fragment& mf) {
    partition().apply(*schema(), mf);
}

mutation& mutation::operator=(const mutation& m) {
    return *this = mutation(m);
}

mutation mutation::operator+(const mutation& other) const {
    auto m = *this;
    m.apply(other);
    return m;
}

mutation& mutation::operator+=(const mutation& other) {
    apply(other);
    return *this;
}

mutation& mutation::operator+=(mutation&& other) {
    apply(std::move(other));
    return *this;
}

mutation mutation::sliced(const query::clustering_row_ranges& ranges) const {
    return mutation(schema(), decorated_key(), partition().sliced(*schema(), ranges));
}

future<mutation_opt> read_mutation_from_flat_mutation_reader(flat_mutation_reader& r) {
    if (r.is_buffer_empty()) {
        if (r.is_end_of_stream()) {
            return make_ready_future<mutation_opt>();
        }
        return r.fill_buffer().then([&r] {
            return read_mutation_from_flat_mutation_reader(r);
        });
    }
    // r.is_buffer_empty() is always false at this point
    return r.consume(mutation_rebuilder(r.schema()));
}

mutation reverse(mutation mut) {
    auto reverse_schema = mut.schema()->make_reversed();
    mutation_rebuilder reverse_rebuilder(reverse_schema);
    return *std::move(mut).consume(reverse_rebuilder, consume_in_reverse::yes).result;
}

std::ostream& operator<<(std::ostream& os, const mutation& m) {
    const ::schema& s = *m.schema();
    const auto& dk = m.decorated_key();

    fmt_print(os, "{{table: '{}.{}', key: {{", s.ks_name(), s.cf_name());

    auto type_iterator = dk._key.get_compound_type(s)->types().begin();
    auto column_iterator = s.partition_key_columns().begin();

    for (auto&& e : dk._key.components(s)) {
        os << "'" << column_iterator->name_as_text() << "': " << (*type_iterator)->to_string(to_bytes(e)) << ", ";
        ++type_iterator;
        ++column_iterator;
    }

    fmt_print(os, "token: {}}}, ", dk._token);
    os << mutation_partition::printer(s, m.partition()) << "\n}";
    return os;
}
