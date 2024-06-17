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
#include "auth/common.hh"
#include "auth/service.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/query_processor.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
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

future<> drop_keyspace_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return state.has_keyspace_access(keyspace(), auth::permission::DROP);
}

void drop_keyspace_statement::validate(query_processor&, const service::client_state& state) const
{
    warn(unimplemented::cause::VALIDATION);
}

const sstring& drop_keyspace_statement::keyspace() const
{
    return _keyspace;
}

future<std::tuple<::shared_ptr<cql_transport::event::schema_change>, cql3::cql_warnings_vec>> drop_keyspace_statement::prepare_schema_mutations(query_processor& qp, service::query_state& state, const query_options& options, service::group0_batch& mc) const {
    ::shared_ptr<cql_transport::event::schema_change> ret;

    try {
        auto muts = co_await service::prepare_keyspace_drop_announcement(qp.db().real_database(), _keyspace, mc.write_timestamp());
        mc.add_mutations(std::move(muts));

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

    if (!auth::legacy_mode(qp)) {
        const auto& as = *state.get_client_state().get_auth_service();
        co_await auth::revoke_all(as, auth::make_data_resource(_keyspace), mc);
        co_await auth::revoke_all(as, auth::make_functions_resource(_keyspace), mc);
    }

    co_return std::make_tuple(std::move(ret), std::vector<sstring>());
}

std::unique_ptr<cql3::statements::prepared_statement>
drop_keyspace_statement::prepare(data_dictionary::database db, cql_stats& stats) {
    return std::make_unique<prepared_statement>(make_shared<drop_keyspace_statement>(*this));
}

}

}
