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
// existing mutation_partition.
class mutation_partition_applier : public mutation_partition_visitor {
    const schema& _schema;
    mutation_partition& _p;
    deletable_row* _current_row;
public:
    mutation_partition_applier(const schema& s, mutation_partition& target)
        : _schema(s), _p(target) { }

    virtual void accept_partition_tombstone(tombstone t) override {
        _p.apply(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema.static_column_at(id);
        _p._static_row.apply(_schema.column_at(column_kind::static_column, id), atomic_cell_or_collection(*cdef.type, cell));
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override {
        auto& ctype = *static_pointer_cast<const collection_type_impl>(_schema.static_column_at(id).type);
        _p._static_row.apply(_schema.column_at(column_kind::static_column, id), atomic_cell_or_collection(collection_mutation(ctype, collection)));
    }

    virtual void accept_row_tombstone(const range_tombstone& rt) override {
        _p.apply_row_tombstone(_schema, rt);
    }

    virtual void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
        deletable_row& r = _p.clustered_row(_schema, key, dummy, continuous);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema.regular_column_at(id);
        _current_row->cells().apply(_schema.column_at(column_kind::regular_column, id), atomic_cell_or_collection(*cdef.type, cell));
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override {
        auto& ctype = *static_pointer_cast<const collection_type_impl>(_schema.regular_column_at(id).type);
        _current_row->cells().apply(_schema.column_at(column_kind::regular_column, id), atomic_cell_or_collection(collection_mutation(ctype, collection)));
    }
};
