/*
 * Modified by ScyllaDB
 *
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "column_specification.hh"
#include "operation.hh"

namespace cql3 {

/**
 * Static helper methods and classes for user types.
 */
class user_types {
    user_types() = delete;
public:
    static lw_shared_ptr<column_specification> field_spec_of(const column_specification& column, size_t field);

    class setter : public operation_skip_if_unset {
    public:
        using operation_skip_if_unset::operation_skip_if_unset;

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const cql3::raw_value& value);
    };

    class setter_by_field : public operation_skip_if_unset {
        size_t _field_idx;
    public:
        setter_by_field(const column_definition& column, size_t field_idx, expr::expression e)
            : operation_skip_if_unset(column, std::move(e)), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class deleter_by_field : public operation_no_unset_support {
        size_t _field_idx;
    public:
        deleter_by_field(const column_definition& column, size_t field_idx)
            : operation_no_unset_support(column, std::nullopt), _field_idx(field_idx) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
