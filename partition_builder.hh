/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "mutation_partition.hh"
#include "mutation_partition_view.hh"

// Partition visitor which builds mutation_partition corresponding to the data its fed with.
class partition_builder : public mutation_partition_visitor {
private:
    const schema& _schema;
    mutation_partition& _partition;
    deletable_row* _current_row;
public:
    // @p will hold the result of building.
    // @p must be empty.
    partition_builder(const schema& s, mutation_partition& p)
        : _schema(s)
        , _partition(p)
    { }

    virtual void accept_partition_tombstone(tombstone t) override {
        _partition.apply(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        row& r = _partition.static_row();
        r.append_cell(id, atomic_cell_or_collection(cell));
    }

    virtual void accept_static_cell(column_id id, collection_mutation::view collection) override {
        row& r = _partition.static_row();
        r.append_cell(id, atomic_cell_or_collection(collection));
    }

    virtual void accept_row_tombstone(clustering_key_prefix_view prefix, tombstone t) override {
        _partition.apply_row_tombstone(_schema, prefix, t);
    }

    virtual void accept_row(clustering_key_view key, tombstone deleted_at, const row_marker& rm) override {
        deletable_row& r = _partition.clustered_row(_schema, key);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        row& r = _current_row->cells();
        r.append_cell(id, atomic_cell_or_collection(cell));
    }

    virtual void accept_row_cell(column_id id, collection_mutation::view collection) override {
        row& r = _current_row->cells();
        r.append_cell(id, atomic_cell_or_collection(collection));
    }
};
