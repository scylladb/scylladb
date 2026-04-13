/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/column_specification.hh"
#include "cql3/expr/expression.hh"
#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace cql3 {

namespace statements {

/**
 * Prepared form of SELECT without FROM, e.g. SELECT 1, now(), toTimestamp(now()).
 * Returns exactly one row containing the evaluated expressions.
 */
class select_no_from_statement : public cql_statement {
    uint32_t _bound_terms;
    std::vector<expr::expression> _exprs;
    std::vector<lw_shared_ptr<column_specification>> _column_specs;
    ::shared_ptr<const metadata> _result_metadata;
public:
    select_no_from_statement(
            uint32_t bound_terms,
            std::vector<expr::expression> exprs,
            std::vector<lw_shared_ptr<column_specification>> column_specs);

    virtual uint32_t get_bound_terms() const override;
    virtual ::shared_ptr<const metadata> get_result_metadata() const override;
    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;
    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;
    virtual future<::shared_ptr<cql_transport::messages::result_message>> execute(
            query_processor& qp, service::query_state& state, const query_options& options,
            std::optional<service::group0_guard> guard) const override;
};

}

}
