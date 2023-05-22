/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/drop_keyspace_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_backend.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "transport/event.hh"
#include "mutation/mutation.hh"

namespace cql3 {

namespace statements {

drop_keyspace_statement::drop_keyspace_statement(const sstring& keyspace, bool if_exists)
    : schema_altering_statement(&timeout_config::truncate_timeout)
    , _keyspace{keyspace}
    , _if_exists{if_exists}
{
}

future<> drop_keyspace_statement::check_access(query_backend& qb, const service::client_state& state) const
{
    return state.has_keyspace_access(qb.db(), keyspace(), auth::permission::DROP);
}

void drop_keyspace_statement::validate(query_backend&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
#if 0
    ThriftValidation.validateKeyspaceNotSystem(keyspace);
#endif
}

const sstring& drop_keyspace_statement::keyspace() const
{
    return _keyspace;
}

future<std::pair<::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>>>
drop_keyspace_statement::prepare_schema_mutations(query_backend& qb, api::timestamp_type ts) const {
    std::vector<mutation> m;
    ::shared_ptr<cql_transport::event::schema_change> ret;

    try {
        m = co_await qb.get_migration_manager().prepare_keyspace_drop_announcement(_keyspace, ts);

        using namespace cql_transport;
        ret = ::make_shared<event::schema_change>(
                event::schema_change::change_type::DROPPED,
                event::schema_change::target_type::KEYSPACE,
                keyspace());
    } catch (const exceptions::configuration_exception& e) {
        if (!_if_exists) {
            co_return coroutine::exception(std::current_exception());
        }
    }

    co_return std::make_pair(std::move(ret), std::move(m));
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_keyspace_statement>(*this));
}

}

}
