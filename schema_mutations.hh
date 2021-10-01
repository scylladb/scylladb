/*
 * Copyright 2015-present ScyllaDB
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
#include "schema_fwd.hh"
#include "canonical_mutation.hh"

class schema_mutations;

struct frozen_schema_mutations {
    canonical_mutation columnfamilies;
    canonical_mutation columns;
    bool is_view;
    std::optional<canonical_mutation> view_virtual_columns;
    std::optional<canonical_mutation> computed_columns;
    std::optional<canonical_mutation> indices;
    std::optional<canonical_mutation> dropped_columns;
    std::optional<canonical_mutation> scylla_tables;

    frozen_schema_mutations(
            canonical_mutation columnfamilies,
            canonical_mutation columns,
            bool is_view,
            std::optional<canonical_mutation> indices,
            std::optional<canonical_mutation> dropped_columns,
            std::optional<canonical_mutation> scylla_tables,
            std::optional<canonical_mutation> view_virtual_columns,
            std::optional<canonical_mutation> computed_columns)
        : columnfamilies(std::move(columnfamilies))
        , columns(std::move(columns))
        , is_view(is_view)
        , view_virtual_columns(std::move(view_virtual_columns))
        , computed_columns(std::move(computed_columns))
        , indices(std::move(indices))
        , dropped_columns(std::move(dropped_columns))
        , scylla_tables(std::move(scylla_tables))
    { }
    frozen_schema_mutations(const schema_mutations& sm);
};

// Commutative representation of table schema
// Equality ignores tombstones.
class schema_mutations {
    mutation _columnfamilies;
    mutation _columns;
    mutation_opt _view_virtual_columns;
    mutation_opt _computed_columns;
    mutation_opt _indices;
    mutation_opt _dropped_columns;
    mutation_opt _scylla_tables;
public:
    schema_mutations(mutation columnfamilies, mutation columns, mutation_opt view_virtual_columns, mutation_opt computed_columns, mutation_opt indices, mutation_opt dropped_columns,
        mutation_opt scylla_tables)
            : _columnfamilies(std::move(columnfamilies))
            , _columns(std::move(columns))
            , _view_virtual_columns(std::move(view_virtual_columns))
            , _computed_columns(std::move(computed_columns))
            , _indices(std::move(indices))
            , _dropped_columns(std::move(dropped_columns))
            , _scylla_tables(std::move(scylla_tables))
    { }
    explicit schema_mutations(frozen_schema_mutations);

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

    const mutation_opt& view_virtual_columns_mutation() const {
        return _view_virtual_columns;
    }

    const mutation_opt& computed_columns_mutation() const {
        return _computed_columns;
    }

    const mutation_opt& scylla_tables() const {
        return _scylla_tables;
    }

    mutation_opt& scylla_tables() {
        return _scylla_tables;
    }

    const mutation_opt& indices_mutation() const {
        return _indices;
    }
    const mutation_opt& dropped_columns_mutation() const {
        return _dropped_columns;
    }

    canonical_mutation columnfamilies_canonical_mutation() const {
        return canonical_mutation(_columnfamilies);
    }

    canonical_mutation columns_canonical_mutation() const {
        return canonical_mutation(_columns);
    }

    std::optional<canonical_mutation> view_virtual_columns_canonical_mutation() const {
        if (_view_virtual_columns) {
            return canonical_mutation(*_view_virtual_columns);
        }
        return {};
    }

    std::optional<canonical_mutation> computed_columns_canonical_mutation() const {
        if (_computed_columns) {
            return canonical_mutation(*_computed_columns);
        }
        return {};
    }

    std::optional<canonical_mutation> indices_canonical_mutation() const {
        if (_indices) {
            return canonical_mutation(*_indices);
        }
        return {};
    }
    std::optional<canonical_mutation> dropped_columns_canonical_mutation() const {
        if (_dropped_columns) {
            return canonical_mutation(*_dropped_columns);
        }
        return {};
    }
    std::optional<canonical_mutation> scylla_tables_canonical_mutation() const {
        if (_scylla_tables) {
            return canonical_mutation(*_scylla_tables);
        }
        return {};
    }

    bool is_view() const;

    table_schema_version digest() const;
    std::optional<sstring> partitioner() const;

    bool operator==(const schema_mutations&) const;
    bool operator!=(const schema_mutations&) const;

    // Returns true iff any mutations contain any live cells
    bool live() const;

    friend std::ostream& operator<<(std::ostream&, const schema_mutations&);
};

