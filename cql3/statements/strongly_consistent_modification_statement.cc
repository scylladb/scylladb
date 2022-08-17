/*
 * Copyright (C) 2022-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */


#include "cql3/statements/strongly_consistent_modification_statement.hh"

#include <boost/range/adaptors.hpp>
#include <optional>

#include <seastar/core/future.hh>

#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "cql3/query_processor.hh"
#include "timeout_config.hh"
#include "service/broadcast_tables/experimental/lang.hh"

namespace cql3 {

namespace statements {

strongly_consistent_modification_statement::strongly_consistent_modification_statement(
    uint32_t bound_terms,
    schema_ptr schema,
    service::broadcast_tables::update_query query)
    : cql_statement_opt_metadata{&timeout_config::write_timeout}
    , _bound_terms{bound_terms}
    , _schema{schema}
    , _query{std::move(query)}
{ }

future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_modification_statement::execute(query_processor& qp, service::query_state& qs, const query_options& options) const {
    return execute_without_checking_exception_message(qp, qs, options)
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<cql_transport::messages::result_message>>);
}
    
future<::shared_ptr<cql_transport::messages::result_message>>
strongly_consistent_modification_statement::execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options) const {
    return service::broadcast_tables::execute(qp.get_group0_client(), { _query })
        .then(make_ready_future<::shared_ptr<cql_transport::messages::result_message>>);
}

uint32_t strongly_consistent_modification_statement::get_bound_terms() const {
    return _bound_terms;
}

future<> strongly_consistent_modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    const data_dictionary::database db = qp.db();
    auto f = state.has_column_family_access(db, _schema->ks_name(), _schema->cf_name(), auth::permission::MODIFY);
    if (_query.value_condition->has_value()) {
        f = f.then([this, &state, db] {
           return state.has_column_family_access(db, _schema->ks_name(), _schema->cf_name(), auth::permission::SELECT);
        });
    }
    return f;
}

void strongly_consistent_modification_statement::validate(query_processor&, const service::client_state& state) const {
    // Nothing to do, all validation has been done by prepare().
}

bool strongly_consistent_modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _schema->ks_name() == ks_name && (!cf_name || _schema->cf_name() == *cf_name);
}

}

}
