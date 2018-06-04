/*
 * Copyright (C) 2017 ScyllaDB
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

#include "cql3/statements/select_statement.hh"
#include "dht/i_partitioner.hh"
#include "query-request.hh"
#include "schema.hh"

class view_info final {
    const schema& _schema;
    raw_view_info _raw;
    // The following fields are used to select base table rows.
    mutable shared_ptr<cql3::statements::select_statement> _select_statement;
    mutable stdx::optional<query::partition_slice> _partition_slice;
    mutable stdx::optional<dht::partition_range_vector> _partition_ranges;
    // Id of a regular base table column included in the view's PK, if any.
    mutable stdx::optional<column_id> _base_non_pk_column_in_view_pk;
public:
    view_info(const schema& schema, const raw_view_info& raw_view_info);

    const raw_view_info& raw() const {
        return _raw;
    }

    const utils::UUID& base_id() const {
        return _raw.base_id();
    }

    const sstring& base_name() const {
        return _raw.base_name();
    }

    bool include_all_columns() const {
        return _raw.include_all_columns();
    }

    const sstring& where_clause() const {
        return _raw.where_clause();
    }

    bool is_index() const;

    cql3::statements::select_statement& select_statement() const;
    const query::partition_slice& partition_slice() const;
    const dht::partition_range_vector& partition_ranges() const;
    const column_definition* view_column(const schema& base, column_id base_id) const;
    const column_definition* view_column(const column_definition& base_def) const;
    stdx::optional<column_id> base_non_pk_column_in_view_pk() const;
    void initialize_base_dependent_fields(const schema& base);

    friend bool operator==(const view_info& x, const view_info& y) {
        return x._raw == y._raw;
    }
    friend std::ostream& operator<<(std::ostream& os, const view_info& view) {
        return os << view._raw;
    }
};
