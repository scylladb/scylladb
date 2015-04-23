/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "mutation.hh"


void mutation::set_static_cell(const column_definition& def, atomic_cell_or_collection value) {
    update_column(p.static_row(), def, std::move(value));
}

void mutation::set_clustered_cell(const exploded_clustering_prefix& prefix, const column_definition& def, atomic_cell_or_collection value) {
    auto& row = p.clustered_row(clustering_key::from_clustering_prefix(*schema, prefix)).cells;
    update_column(row, def, std::move(value));
}

void mutation::set_clustered_cell(const clustering_key& key, const column_definition& def, atomic_cell_or_collection value) {
    auto& row = p.clustered_row(key).cells;
    update_column(row, def, std::move(value));
}

void mutation::set_cell(const exploded_clustering_prefix& prefix, const bytes& name, const boost::any& value,
        api::timestamp_type timestamp, ttl_opt ttl) {
    auto column_def = schema->get_column_definition(name);
    if (!column_def) {
        throw std::runtime_error(sprint("no column definition found for '%s'", name));
    }
    return set_cell(prefix, *column_def, atomic_cell::make_live(timestamp, ttl, column_def->type->decompose(value)));
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
mutation::get_cell(const clustering_key& rkey, const column_definition& def) {
    auto find_cell = [&def] (row& r) {
        auto i = r.find(def.id);
        if (i == r.end()) {
            return std::experimental::optional<atomic_cell_or_collection>{};
        }
        return std::experimental::optional<atomic_cell_or_collection>{i->second};
    };
    if (def.is_static()) {
        return find_cell(p.static_row());
    } else {
        auto r = p.find_row(rkey);
        if (!r) {
            return {};
        }
        return find_cell(*r);
    }
}

void mutation::update_column(row& row, const column_definition& def, atomic_cell_or_collection&& value) {
    // our mutations are not yet immutable
    auto id = def.id;
    auto i = row.lower_bound(id);
    if (i == row.end() || i->first != id) {
        row.emplace_hint(i, id, std::move(value));
    } else {
        merge_column(def, i->second, value);
    }
}
