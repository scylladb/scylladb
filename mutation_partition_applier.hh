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
private:
    template <typename ColumnIdResolver>
    void apply_cell(row& r, column_id id, atomic_cell_or_collection c, ColumnIdResolver&& resolver) {
        auto i = r.lower_bound(id);
        if (i == r.end() || i->first != id) {
            r.emplace_hint(i, id, std::move(c));
        } else {
            merge_column(resolver(id), i->second, std::move(c));
        }
    }
public:
    mutation_partition_applier(const schema& s, mutation_partition& target)
        : _schema(s), _p(target) { }

    virtual void accept_partition_tombstone(tombstone t) override {
        _p.apply(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        apply_cell(_p._static_row, id, atomic_cell_or_collection(cell),
            [this](column_id id) -> const column_definition& { return _schema.static_column_at(id); });
    }

    virtual void accept_static_cell(column_id id, collection_mutation::view collection) override {
        apply_cell(_p._static_row, id, atomic_cell_or_collection(collection),
            [this](column_id id) -> const column_definition& { return _schema.static_column_at(id); });
    }

    virtual void accept_row_tombstone(clustering_key_prefix_view prefix, tombstone t) override {
        _p.apply_row_tombstone(_schema, prefix, t);
    }

    virtual void accept_row(clustering_key_view key, api::timestamp_type created_at, tombstone deleted_at) override {
        deletable_row& r = _p.clustered_row(_schema, key);
        r.apply(created_at);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        apply_cell(_current_row->cells, id, atomic_cell_or_collection(cell),
            [this](column_id id) -> const column_definition& { return _schema.regular_column_at(id); });
    }

    virtual void accept_row_cell(column_id id, collection_mutation::view collection) override {
        apply_cell(_current_row->cells, id, atomic_cell_or_collection(collection),
            [this](column_id id) -> const column_definition& { return _schema.regular_column_at(id); });
    }
};
