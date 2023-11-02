/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "query-request.hh"
#include "schema/schema_fwd.hh"
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
    mutable bool _has_computed_column_depending_on_base_non_primary_key;
public:
    view_info(const schema& schema, const raw_view_info& raw_view_info);

    const raw_view_info& raw() const {
        return _raw;
    }

    const table_id& base_id() const {
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

    cql3::statements::select_statement& select_statement(data_dictionary::database) const;
    const query::partition_slice& partition_slice(data_dictionary::database) const;
    const column_definition* view_column(const schema& base, column_kind kind, column_id base_id) const;
    const column_definition* view_column(const column_definition& base_def) const;
    bool has_base_non_pk_columns_in_view_pk() const;
    bool has_computed_column_depending_on_base_non_primary_key() const {
        return _has_computed_column_depending_on_base_non_primary_key;
    }

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
