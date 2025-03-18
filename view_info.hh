/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
public:
    view_info(const schema& schema, const raw_view_info& raw_view_info, schema_ptr base_schema);
    view_info(const schema& schema, const raw_view_info& raw_view_info, db::view::base_dependent_view_info base_info);

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
    bool has_computed_column_depending_on_base_non_primary_key() const;

    bool is_partition_key_permutation_of_base_partition_key() const;

    /// base_dependent_view_info contains information about the view that depends on the base table,
    /// which isn't dependent on the base schema version
    const db::view::base_info_ptr& base_info() const { return _base_info; }
    void reset_view_info();
    db::view::base_info_ptr make_base_dependent_view_info(const schema& base_schema) const;

    friend bool operator==(const view_info& x, const view_info& y) {
        return x._raw == y._raw;
    }
    friend fmt::formatter<view_info>;
};

template <> struct fmt::formatter<view_info> : fmt::formatter<string_view> {
    auto format(const view_info& view, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}", view._raw);
    }
};
