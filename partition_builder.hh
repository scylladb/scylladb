/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "schema/schema.hh"
#include "mutation/mutation.hh"
#include "mutation/mutation_partition.hh"
#include "mutation/mutation_partition_visitor.hh"
#include "mutation/tombstone.hh"
#include "mutation/atomic_cell.hh"
#include "mutation/range_tombstone.hh"
#include "collection_mutation.hh"

// Partition visitor which builds mutation_partition corresponding to the data its fed with.
class partition_builder final : public mutation_partition_visitor {
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
        auto& cdef = _schema.static_column_at(id);
        accept_static_cell(id, atomic_cell(*cdef.type, cell));
    }

    void accept_static_cell(column_id id, atomic_cell&& cell) {
        row& r = _partition.static_row().maybe_create();
        r.append_cell(id, atomic_cell_or_collection(std::move(cell)));
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override {
        row& r = _partition.static_row().maybe_create();
        r.append_cell(id, collection_mutation(*_schema.static_column_at(id).type, std::move(collection)));
    }

    virtual void accept_row_tombstone(const range_tombstone& rt) override {
        _partition.apply_row_tombstone(_schema, rt);
    }

    virtual void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
        deletable_row& r = _partition.append_clustered_row(_schema, key, dummy, continuous);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema.regular_column_at(id);
        accept_row_cell(id, atomic_cell(*cdef.type, cell));
    }

    void accept_row_cell(column_id id, atomic_cell&& cell) {
        row& r = _current_row->cells();
        r.append_cell(id, std::move(cell));
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override {
        row& r = _current_row->cells();
        r.append_cell(id, collection_mutation(*_schema.regular_column_at(id).type, std::move(collection)));
    }
};

class partition_split_builder final : public mutation_partition_visitor {
    schema_ptr _schema;
    partition_key_view _key;
    size_t _max_rows;
    size_t _current_rows = 0;
    std::function<void(mutation)> _process_mutation;

    std::optional<mutation> _m;
    mutation_partition* _partition;
    deletable_row* _current_row;

    void make_mutation() {
        _m = mutation(_schema, _key);
        _partition = &_m->partition();
        _current_row = nullptr;
        _current_rows = 0;
    }

    void maybe_process_mutation() {
        if (_current_rows >= _max_rows) {
            _process_mutation(std::move(*_m));
            make_mutation();
        }
    }

public:
    partition_split_builder(schema_ptr s, partition_key_view key, size_t max_rows, std::function<void(mutation)> process_mutation)
            : _schema(s)
            , _key(key)
            , _max_rows(max_rows)
            , _process_mutation(process_mutation)
    {
        make_mutation();
    }

    virtual void accept_partition_tombstone(tombstone t) override {
        _partition->apply(t);
    }

    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema->static_column_at(id);
        accept_static_cell(id, atomic_cell(*cdef.type, cell));
    }

    void accept_static_cell(column_id id, atomic_cell&& cell) {
        row& r = _partition->static_row().maybe_create();
        r.append_cell(id, atomic_cell_or_collection(std::move(cell)));
    }

    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override {
        row& r = _partition->static_row().maybe_create();
        r.append_cell(id, collection_mutation(*_schema->static_column_at(id).type, std::move(collection)));
    }

    virtual void accept_row_tombstone(const range_tombstone& rt) override {
        maybe_process_mutation();
        _partition->apply_row_tombstone(*_schema, rt);
        ++_current_rows;
    }

    virtual void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override {
        maybe_process_mutation();
        deletable_row& r = _partition->append_clustered_row(*_schema, key, dummy, continuous);
        r.apply(rm);
        r.apply(deleted_at);
        _current_row = &r;
        ++_current_rows;
    }

    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override {
        auto& cdef = _schema->regular_column_at(id);
        accept_row_cell(id, atomic_cell(*cdef.type, cell));
    }

    void accept_row_cell(column_id id, atomic_cell&& cell) {
        row& r = _current_row->cells();
        r.append_cell(id, std::move(cell));
    }

    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override {
        row& r = _current_row->cells();
        r.append_cell(id, collection_mutation(*_schema->regular_column_at(id).type, std::move(collection)));
    }

    virtual void accept_end_of_partition() override {
        _process_mutation(std::move(*_m));
    }
};
