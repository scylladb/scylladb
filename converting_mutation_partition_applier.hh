/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
        const column_mapping::column& col = _visited_column_mapping.static_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def && def->is_static() && def->type->is_value_compatible_with(*col.type())) {
            _p._static_row.apply(*def, atomic_cell_or_collection(cell));
        }
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override {
        const column_mapping::column& col = _visited_column_mapping.static_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def && def->is_static() && def->type->is_value_compatible_with(*col.type())) {
            _p._static_row.apply(*def, atomic_cell_or_collection(collection));
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
        const column_mapping::column& col = _visited_column_mapping.regular_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def && def->is_regular() && def->type->is_value_compatible_with(*col.type())) {
            _current_row->cells().apply(*def, atomic_cell_or_collection(cell));
        }
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override {
        const column_mapping::column& col = _visited_column_mapping.regular_column_at(id);
        const column_definition* def = _p_schema.get_column_definition(col.name());
        if (def && def->is_regular() && def->type->is_value_compatible_with(*col.type())) {
            _current_row->cells().apply(*def, atomic_cell_or_collection(collection));
        }
    }
};
