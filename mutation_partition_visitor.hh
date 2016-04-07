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

    virtual void accept_static_cell(column_id, collection_mutation_view) = 0;

    virtual void accept_row_tombstone(clustering_key_prefix_view, tombstone) = 0;

    virtual void accept_row(clustering_key_view key, tombstone deleted_at, const row_marker& rm) = 0;

    virtual void accept_row_cell(column_id id, atomic_cell_view) = 0;

    virtual void accept_row_cell(column_id id, collection_mutation_view) = 0;
};
