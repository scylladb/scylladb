/*
 */

/*
 * Copyright (C) 2014-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/statements/truncate_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/cql_statement.hh"
#include "data_dictionary/data_dictionary.hh"
#include "cql3/query_processor.hh"
#include "service/storage_proxy.hh"
#include <optional>

namespace cql3 {

namespace statements {

truncate_statement::truncate_statement(cf_name name)
    : cf_statement{std::move(name)}
    , cql_statement_no_metadata(&timeout_config::truncate_timeout)
{
}

uint32_t truncate_statement::get_bound_terms() const
{
    return 0;
}

std::unique_ptr<prepared_statement> truncate_statement::prepare(data_dictionary::database db,cql_stats& stats)
{
    return std::make_unique<prepared_statement>(::make_shared<truncate_statement>(*this));
}

bool truncate_statement::depends_on_keyspace(const sstring& ks_name) const
{
    return false;
}

bool truncate_statement::depends_on_column_family(const sstring& cf_name) const
{
    return false;
}

future<> truncate_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_column_family_access(qp.db(), keyspace(), column_family(), auth::permission::MODIFY);
}

void truncate_statement::validate(query_processor&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
#if 0
    ThriftValidation.validateColumnFamily(keyspace(), columnFamily());
#endif
}

future<::shared_ptr<cql_transport::messages::result_message>>
truncate_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const
{
    if (qp.db().find_schema(keyspace(), column_family())->is_view()) {
        throw exceptions::invalid_request_exception("Cannot TRUNCATE materialized view directly; must truncate base table instead");
    }
    return qp.proxy().truncate_blocking(keyspace(), column_family()).handle_exception([](auto ep) {
        throw exceptions::truncate_exception(ep);
    }).then([] {
        return ::shared_ptr<cql_transport::messages::result_message>{};
    });
}

}

}
