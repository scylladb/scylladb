/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/statements/use_statement.hh"
#include "cql3/statements/raw/use_statement.hh"
#include "cql3/query_processor.hh"
#include "transport/messages/result_message.hh"
#include "service/query_state.hh"

namespace cql3 {

namespace statements {

use_statement::use_statement(sstring keyspace)
        : cql_statement_no_metadata(&timeout_config::other_timeout)
        , _keyspace(keyspace)
{
}

uint32_t use_statement::get_bound_terms() const
{
    return 0;
}

namespace raw {

use_statement::use_statement(sstring keyspace)
    : _keyspace(keyspace)
{
}

std::unique_ptr<prepared_statement> use_statement::prepare(data_dictionary::database db, cql_stats& stats)
{
    return std::make_unique<prepared_statement>(::make_shared<cql3::statements::use_statement>(_keyspace));
}

}

bool use_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return false;
}

future<> use_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    state.validate_login();
    return make_ready_future<>();
}

future<::shared_ptr<cql_transport::messages::result_message>>
use_statement::execute(query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const {
    try {
        state.get_client_state().set_keyspace(qp.db().real_database(), _keyspace);
    } catch(...) {
        return make_exception_future<::shared_ptr<cql_transport::messages::result_message>>(std::current_exception());
    }
    auto result =::make_shared<cql_transport::messages::result_message::set_keyspace>(_keyspace);
    return make_ready_future<::shared_ptr<cql_transport::messages::result_message>>(result);
}

}

}
