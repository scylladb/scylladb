/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "mutation/mutation_partition_visitor.hh"
#include "mutation/atomic_cell.hh"
#include "schema/schema.hh" // temporary: bring in definition of `column_kind`

class schema;
class row;
class mutation_partition;
class column_mapping;
class deletable_row;
class column_definition;
class abstract_type;
class atomic_cell_or_collection;

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
    static bool is_compatible(const column_definition& new_def, const abstract_type& old_type, column_kind kind);
    static atomic_cell upgrade_cell(const abstract_type& new_type, const abstract_type& old_type, atomic_cell_view cell,
                                    atomic_cell::collection_member cm = atomic_cell::collection_member::no);
    static void accept_cell(row& dst, column_kind kind, const column_definition& new_def, const abstract_type& old_type, atomic_cell_view cell);
    static void accept_cell(row& dst, column_kind kind, const column_definition& new_def, const abstract_type& old_type, collection_mutation_view cell);public:
    converting_mutation_partition_applier(
            const column_mapping& visited_column_mapping,
            const schema& target_schema,
            mutation_partition& target);
    virtual void accept_partition_tombstone(tombstone t) override;
    void accept_static_cell(column_id id, atomic_cell cell);
    virtual void accept_static_cell(column_id id, atomic_cell_view cell) override;
    virtual void accept_static_cell(column_id id, collection_mutation_view collection) override;
    virtual void accept_row_tombstone(const range_tombstone& rt) override;
    virtual void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm, is_dummy dummy, is_continuous continuous) override;
    void accept_row_cell(column_id id, atomic_cell cell);
    virtual void accept_row_cell(column_id id, atomic_cell_view cell) override;
    virtual void accept_row_cell(column_id id, collection_mutation_view collection) override;

    // Appends the cell to dst upgrading it to the new schema.
    // Cells must have monotonic names.
    static void append_cell(row& dst, column_kind kind, const column_definition& new_def, const column_definition& old_def, const atomic_cell_or_collection& cell);
};
