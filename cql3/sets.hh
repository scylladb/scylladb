/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "column_specification.hh"
#include "cql3/operation.hh"

namespace cql3 {

/**
 * Static helper methods and classes for sets.
 */
class sets {
    sets() = delete;
public:
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification& column);

    class setter : public operation_skip_if_unset {
    public:
        setter(const column_definition& column, expr::expression e)
                : operation_skip_if_unset(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const cql3::raw_value& value);
    };

    class adder : public operation_skip_if_unset {
    public:
        adder(const column_definition& column, expr::expression e)
            : operation_skip_if_unset(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void do_add(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params,
                const cql3::raw_value& value, const column_definition& column);
    };

    // Note that this is reused for Map subtraction too (we subtract a set from a map)
    class discarder : public operation_skip_if_unset {
    public:
        discarder(const column_definition& column, expr::expression e)
            : operation_skip_if_unset(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };

    class element_discarder : public operation_no_unset_support {
    public:
        element_discarder(const column_definition& column, expr::expression e)
            : operation_no_unset_support(column, std::move(e)) { }
        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
    };
};

}
