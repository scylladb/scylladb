/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

#include "mutation_partition_view.hh"
#include "schema.hh"

// Mutation partition visitor which applies visited data into
// existing mutation_partition. The visited data may be of a different schema.
// Data which is not representable in the new schema is dropped.
// Weak exception guarantees.
class converting_mutation_partition_applier : public mutation_partition_visitor {
    const schema& _p_schema;
    mutation_partition& _p;
    const column_mapping& _visited_column_mapping;
    deletable_row* _current_row;
private:
    static bool is_compatible(const column_definition& new_def, const data_type& old_type, column_kind kind) {
        return new_def.kind == kind && new_def.type->is_value_compatible_with(*old_type);
    }
    void accept_cell(row& dst, column_kind kind, const column_definition& new_def, const data_type& old_type, atomic_cell_view cell) {
        if (is_compatible(new_def, old_type, kind) && cell.timestamp() > new_def.dropped_at()) {
            dst.apply(new_def, atomic_cell_or_collection(cell));
        }
    }
    void accept_cell(row& dst, column_kind kind, const column_definition& new_def, const data_type& old_type, collection_mutation_view cell) {
        if (!is_compatible(new_def, old_type, kind)) {
            return;
        }
        auto&& ctype = static_pointer_cast<const collection_type_impl>(old_type);
        auto old_view = ctype->deserialize_mutation_form(cell);

        collection_type_impl::mutation_view new_view;
        if (old_view.tomb.timestamp > new_def.dropped_at()) {
            new_view.tomb = old_view.tomb;
        }
        for (auto& c : old_view.cells) {
            if (c.second.timestamp() > new_def.dropped_at()) {
                new_view.cells.emplace_back(std::move(c));
            }
        }
        dst.apply(new_def, ctype->serialize_mutation_form(std::move(new_view)));
    }
public:
    converting_mutation_partition_applier(
            const column_mapping& visited_column_mapping,
            const schema& target_schema,
            mutation_partition& target)
        : _p_schema(target_schema)
        , _p(target)
        , _visited_column_mapping(visited_column_mapping)
    { }

    virtual void accept_partition_tombstone(tombstone t) override {
        _p.apply(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        const column_mapping_entry& col = _visited_column_mapping.static_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def) {
            accept_cell(_p._static_row, column_kind::static_column, *def, col.type(), cell);
        }
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override {
        const column_mapping_entry& col = _visited_column_mapping.static_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def) {
            accept_cell(_p._static_row, column_kind::static_column, *def, col.type(), collection);
        }
    }

    virtual void accept_row_tombstone(clustering_key_prefix_view prefix, tombstone t) override {
        _p.apply_row_tombstone(_p_schema, prefix, t);
    }

    virtual void accept_row(clustering_key_view key, tombstone deleted_at, const row_marker& rm) override {
        deletable_row& r = _p.clustered_row(_p_schema, key);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        const column_mapping_entry& col = _visited_column_mapping.regular_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def) {
            accept_cell(_current_row->cells(), column_kind::regular_column, *def, col.type(), cell);
        }
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override {
        const column_mapping_entry& col = _visited_column_mapping.regular_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def) {
            accept_cell(_current_row->cells(), column_kind::regular_column, *def, col.type(), collection);
        }
    }
};
