/*
 * Copyright (C) 2017-present ScyllaDB
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

#include "dht/i_partitioner.hh"
#include "query-request.hh"
#include "schema_fwd.hh"
#include "db/view/view.hh"

namespace cql3::statements {
class select_statement;
}

class view_info final {
    const schema& _schema;
    raw_view_info _raw;
    // The following fields are used to select base table rows.
    mutable shared_ptr<cql3::statements::select_statement> _select_statement;
    mutable std::optional<query::partition_slice> _partition_slice;
    db::view::base_info_ptr _base_info;
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

    cql3::statements::select_statement& select_statement() const;
    const query::partition_slice& partition_slice() const;
    const column_definition* view_column(const schema& base, column_id base_id) const;
    const column_definition* view_column(const column_definition& base_def) const;
    bool has_base_non_pk_columns_in_view_pk() const;

    /// Returns a pointer to the base_dependent_view_info which matches the current
    /// schema of the base table.
    ///
    /// base_dependent_view_info lives separately from the view schema.
    /// It can change without the view schema changing its value.
    /// This pointer is updated on base table schema changes as long as this view_info
    /// corresponds to the current schema of the view. After that the pointer stops tracking
    /// the base table schema.
    ///
    /// The snapshot of both the view schema and base_dependent_view_info is represented
    /// by view_and_base. See with_base_info_snapshot().
    const db::view::base_info_ptr& base_info() const { return _base_info; }
    void set_base_info(db::view::base_info_ptr);
    db::view::base_info_ptr make_base_dependent_view_info(const schema& base_schema) const;

    friend bool operator==(const view_info& x, const view_info& y) {
        return x._raw == y._raw;
    }
    friend std::ostream& operator<<(std::ostream& os, const view_info& view) {
        return os << view._raw;
    }
};
