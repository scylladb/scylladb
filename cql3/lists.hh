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

namespace cql3 {

/**
 * Static helper methods and classes for lists.
 */
class lists {
    lists() = delete;
public:
    static lw_shared_ptr<column_specification> index_spec_of(const column_specification&);
    static lw_shared_ptr<column_specification> value_spec_of(const column_specification&);
    static lw_shared_ptr<column_specification> uuid_index_spec_of(const column_specification&);
public:
    class setter : public operation_skip_if_unset {
    public:
        setter(const column_definition& column, expr::expression e)
                : operation_skip_if_unset(column, std::move(e)) {
        }
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
        static void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params, const column_definition& column, const cql3::raw_value& value);
    };

    class setter_by_index : public operation_skip_if_unset {
    protected:
        expr::expression _idx;
    public:
        setter_by_index(const column_definition& column, expr::expression idx, expr::expression e)
            : operation_skip_if_unset(column, std::move(e)), _idx(std::move(idx)) {
        }
        virtual bool requires_read() const override;
        virtual void fill_prepare_context(prepare_context& ctx) override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class setter_by_uuid : public setter_by_index {
    public:
        setter_by_uuid(const column_definition& column, expr::expression idx, expr::expression e)
            : setter_by_index(column, std::move(idx), std::move(e)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class appender : public operation_skip_if_unset {
    public:
        using operation_skip_if_unset::operation_skip_if_unset;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    static void do_append(const cql3::raw_value& list_value,
            mutation& m,
            const clustering_key_prefix& prefix,
            const column_definition& column,
            const update_parameters& params);

    class prepender : public operation_skip_if_unset {
    public:
        using operation_skip_if_unset::operation_skip_if_unset;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder : public operation_skip_if_unset {
    public:
        discarder(const column_definition& column, expr::expression e)
                : operation_skip_if_unset(column, std::move(e)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };

    class discarder_by_index : public operation_skip_if_unset {
    public:
        discarder_by_index(const column_definition& column, expr::expression idx)
                : operation_skip_if_unset(column, std::move(idx)) {
        }
        virtual bool requires_read() const override;
        virtual void execute(mutation& m, const clustering_key_prefix& prefix, const update_parameters& params) override;
    };
};

}
