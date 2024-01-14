/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "operation.hh"
#include "update_parameters.hh"

namespace cql3 {

/**
 * Static helper methods and classes for maps.
 */
class maps {
private:
    maps() = delete;
public:
    static lw_shared_ptr<column_specification> key_spec_of(const column_specification& column);
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification& column);

    class setter : public operation_skip_if_unset {
    public:
        setter(const column_definition& column, expr::expression e)
                : operation_skip_if_unset(column, std::move(e)) {
        }

        virtual void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& row_key, const update_parameters& params, const column_definition& column, const cql3::raw_value& value);
    };

    class setter_by_key : public operation_skip_if_unset {
        expr::expression _k;
    public:
        setter_by_key(const column_definition& column, expr::expression k, expr::expression e)
            : operation_skip_if_unset(column, std::move(e)), _k(std::move(k)) {
        }
        virtual void fill_prepare_context(prepare_context& ctx) override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class putter : public operation_skip_if_unset {
    public:
        putter(const column_definition& column, expr::expression e)
            : operation_skip_if_unset(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    static void do_put(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params,
            const cql3::raw_value& value, const column_definition& column);

    class discarder_by_key : public operation_no_unset_support {
    public:
        discarder_by_key(const column_definition& column, expr::expression k)
                : operation_no_unset_support(column, std::move(k)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

}
