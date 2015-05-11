/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "mutation.hh"

mutation::mutation(dht::decorated_key key, schema_ptr schema)
    : _schema(std::move(schema))
    , _dk(std::move(key))
    , _p(_schema)
{ }

mutation::mutation(partition_key key_, schema_ptr schema)
    : _schema(std::move(schema))
    , _dk(dht::global_partitioner().decorate_key(*_schema, std::move(key_)))
    , _p(_schema)
{ }

void mutation::set_static_cell(const column_definition& def, atomic_cell_or_collection value) {
    _p.static_row().apply(def, std::move(value));
}

void mutation::set_static_cell(const bytes& name, const boost::any& value, api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = _schema->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    if (!column_def->is_static()) {
        throw std::runtime_error(sprint("column '%s' is not static", name));
    }
    _p.static_row().apply(*column_def, atomic_cell::make_live(timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
    auto& row = _p.clustered_row(clustering_key::from_clustering_prefix(*_schema, prefix)).cells;
    row.apply(def, std::move(value));
}

void mutation::set_clustered_cell(const clustering_key& key, const bytes& name, const boost::any& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = _schema->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    return set_clustered_cell(key, *column_def, atomic_cell::make_live(timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value) {
    auto& row = _p.clustered_row(key).cells;
    row.apply(def, std::move(value));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = _schema->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    return set_cell(prefix, *column_def, atomic_cell::make_live(timestamp, column_def->type->decompose(value), ttl));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
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
        const atomic_cell_or_collection* cell = _p.static_row().find_cell(def.id);
        if (!cell) {
            return {};
        }
        return { *cell };
    } else {
        const row* r = _p.find_row(rkey);
        if (!r) {
            return {};
        }
        const atomic_cell_or_collection* cell = r->find_cell(def.id);
        return { *cell };
    }
}

bool mutation::operator==(const mutation& m) const {
    return _dk.equal(*_schema, m._dk) && _p.equal(*_schema, m._p);
}

bool mutation::operator!=(const mutation& m) const {
    return !(*this == m);
}
