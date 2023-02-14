/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/bool_class.hh>

#include "schema/schema_fwd.hh"

class atomic_cell_view;
class collection_mutation_view;
class row_marker;
class row_tombstone;
class range_tombstone;
class tombstone;
class position_in_partition_view;

// When used on an entry, marks the range between this entry and the previous
// one as continuous or discontinuous, excluding the keys of both entries.
// This information doesn't apply to continuity of the entries themselves,
// that is specified by is_dummy flag.
// See class doc of mutation_partition.
using is_continuous = seastar::bool_class<class continuous_tag>;

// Dummy entry is an entry which is incomplete.
// Typically used for marking bounds of continuity range.
// See class doc of mutation_partition.
class dummy_tag {};
using is_dummy = seastar::bool_class<dummy_tag>;

// Guarantees:
//
// - any tombstones which affect cell's liveness are visited before that cell
//
// - rows are visited in ascending order with respect to their keys
//
// - row header (accept_row) is visited before that row's cells
//
// - row tombstones are visited in ascending order with respect to their key prefixes
//
// - cells in given row are visited in ascending order with respect to their column IDs
//
// - static row is visited before any clustered row
//
// - for each column in a row only one variant of accept_(static|row)_cell() is called, appropriate
//   for column's kind (atomic or collection).
//
class mutation_partition_visitor {
public:
    virtual void accept_partition_tombstone(tombstone) = 0;

    virtual void accept_static_cell(column_id, atomic_cell_view) = 0;

    virtual void accept_static_cell(column_id, collection_mutation_view) = 0;

    virtual void accept_row_tombstone(const range_tombstone&) = 0;

    virtual void accept_row(position_in_partition_view key, const row_tombstone& deleted_at, const row_marker& rm,
        is_dummy = is_dummy::no, is_continuous = is_continuous::yes) = 0;

    virtual void accept_row_cell(column_id id, atomic_cell_view) = 0;

    virtual void accept_row_cell(column_id id, collection_mutation_view) = 0;
};
