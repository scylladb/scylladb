/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
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
        _p._static_row.apply(_schema.column_at(column_kind::static_column, id), atomic_cell_or_collection(cell));
    }

    virtual void accept_static_cell(column_id id, collection_mutation::view collection) override {
        _p._static_row.apply(_schema.column_at(column_kind::static_column, id), atomic_cell_or_collection(collection));
    }

    virtual void accept_row_tombstone(clustering_key_prefix_view prefix, tombstone t) override {
        _p.apply_row_tombstone(_schema, prefix, t);
    }

    virtual void accept_row(clustering_key_view key, tombstone deleted_at, const row_marker& rm) override {
        deletable_row& r = _p.clustered_row(_schema, key);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        _current_row->cells().apply(_schema.column_at(column_kind::regular_column, id), atomic_cell_or_collection(cell));
    }

    virtual void accept_row_cell(column_id id, collection_mutation::view collection) override {
        _current_row->cells().apply(_schema.column_at(column_kind::regular_column, id), atomic_cell_or_collection(collection));
    }
};
