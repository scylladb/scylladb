/*
 * Copyright 2015 ScyllaDB
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

#include <vector>
#include "mutation.hh"
#include "schema.hh"
#include "canonical_mutation.hh"

// Commutative representation of table schema
// Equality ignores tombstones.
class schema_mutations {
    mutation _columnfamilies;
    mutation _columns;
    stdx::optional<mutation> _indices;
    stdx::optional<mutation> _dropped_columns;
    stdx::optional<mutation> _scylla_tables;
public:
    schema_mutations(mutation columnfamilies, mutation columns, stdx::optional<mutation> indices, stdx::optional<mutation> dropped_columns,
        stdx::optional<mutation> scylla_tables)
            : _columnfamilies(std::move(columnfamilies))
            , _columns(std::move(columns))
            , _indices(std::move(indices))
            , _dropped_columns(std::move(dropped_columns))
            , _scylla_tables(std::move(scylla_tables))
    { }
    schema_mutations(canonical_mutation columnfamilies,
                     canonical_mutation columns,
                     bool is_view,
                     stdx::optional<canonical_mutation> indices,
                     stdx::optional<canonical_mutation> dropped_columns,
                     stdx::optional<canonical_mutation> scylla_tables);

    schema_mutations(schema_mutations&&) = default;
    schema_mutations& operator=(schema_mutations&&) = default;
    schema_mutations(const schema_mutations&) = default;
    schema_mutations& operator=(const schema_mutations&) = default;

    void copy_to(std::vector<mutation>& dst) const;

    const mutation& columnfamilies_mutation() const {
        return _columnfamilies;
    }

    const mutation& columns_mutation() const {
        return _columns;
    }

    const stdx::optional<mutation>& scylla_tables() const {
        return _scylla_tables;
    }

    const stdx::optional<mutation>& indices_mutation() const {
        return _indices;
    }
    const stdx::optional<mutation>& dropped_columns_mutation() const {
        return _dropped_columns;
    }

    canonical_mutation columnfamilies_canonical_mutation() const {
        return canonical_mutation(_columnfamilies);
    }

    canonical_mutation columns_canonical_mutation() const {
        return canonical_mutation(_columns);
    }

    stdx::optional<canonical_mutation> indices_canonical_mutation() const {
        if (_indices) {
            return canonical_mutation(_indices.value());
        }
        return {};
    }
    stdx::optional<canonical_mutation> dropped_columns_canonical_mutation() const {
        if (_dropped_columns) {
            return canonical_mutation(_dropped_columns.value());
        }
        return {};
    }
    stdx::optional<canonical_mutation> scylla_tables_canonical_mutation() const {
        if (_scylla_tables) {
            return canonical_mutation(_scylla_tables.value());
        }
        return {};
    }

    bool is_view() const;

    table_schema_version digest() const;

    bool operator==(const schema_mutations&) const;
    bool operator!=(const schema_mutations&) const;

    // Returns true iff any mutations contain any live cells
    bool live() const;
};

