/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/strongly_consistent_statement.hh"

#include "cql3/query_processor.hh"

namespace cql3 {

namespace statements {

strongly_consistent_statement::strongly_consistent_statement(seastar::shared_ptr<cql_statement> statement)
    : cql_statement_opt_metadata(statement->get_timeout_config_selector()),
      statement(std::move(statement))
{ }


uint32_t strongly_consistent_statement::get_bound_terms() const {
    return statement->get_bound_terms();
}

seastar::future<> strongly_consistent_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return statement->check_access(qp, state);
}

future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_statement::execute(
    query_processor& qp,
    service::query_state& qs,
    const query_options& options,
    std::optional<service::group0_guard> guard) const
{
    if (guard) {
        return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(
            std::runtime_error("Strongly consistent statements must not require group0 guard"));
    }
    return qp.execute_forwarding_statement(*this, qs, options);
}

future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_statement::execute_direct(
    query_processor& qp,
    service::query_state& qs,
    const query_options& options) const
{
    return statement->execute(qp, qs, options, std::nullopt);
}

bool strongly_consistent_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return statement->depends_on(ks_name, cf_name);
}

seastar::shared_ptr<const metadata> strongly_consistent_statement::get_result_metadata() const {
    return statement->get_result_metadata();
}

}

}
