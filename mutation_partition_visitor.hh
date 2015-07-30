/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "atomic_cell.hh"
#include "tombstone.hh"
#include "keys.hh"

class row_marker;

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

    virtual void accept_static_cell(column_id, collection_mutation::view) = 0;

    virtual void accept_row_tombstone(clustering_key_prefix_view, tombstone) = 0;

    virtual void accept_row(clustering_key_view key, tombstone deleted_at, const row_marker& rm) = 0;

    virtual void accept_row_cell(column_id id, atomic_cell_view) = 0;

    virtual void accept_row_cell(column_id id, collection_mutation::view) = 0;
};
