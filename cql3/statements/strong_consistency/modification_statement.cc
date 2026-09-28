/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "modification_statement.hh"

#include "transport/messages/result_message.hh"
#include "cql3/statements/strong_consistency/modification_executor.hh"

namespace cql3::statements::strong_consistency {

modification_statement::modification_statement(shared_ptr<base_statement> statement)
    : cql_statement(&timeout_config::write_timeout)
    , _statement(std::move(statement))
{
}

using result_message = cql_transport::messages::result_message;

future<shared_ptr<result_message>> modification_statement::execute(query_processor& qp, service::query_state& qs, 
    const query_options& options, std::optional<service::group0_guard> guard) const
{
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<result_message>>);
}

future<shared_ptr<result_message>> modification_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    return modification_executor::instance().commit(*_statement, qp, qs, options);
}

future<> modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return _statement->check_access(qp, state);
}

void modification_statement::validate(query_processor& qp, const service::client_state& state) const {
    _statement->validate(qp, state);
}

uint32_t modification_statement::get_bound_terms() const {
    return _statement->get_bound_terms();
}

bool modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _statement->depends_on(ks_name, cf_name);
}
}
