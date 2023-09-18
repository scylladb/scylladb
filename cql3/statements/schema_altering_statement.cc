/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include <seastar/core/coroutine.hh>
#include "cql3/statements/schema_altering_statement.hh"
#include "locator/abstract_replication_strategy.hh"
#include "data_dictionary/data_dictionary.hh"
#include "mutation.hh"
#include "cql3/query_processor.hh"
#include "transport/messages/result_message.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/migration_manager.hh"

namespace cql3 {

namespace statements {

static logging::logger mylogger("schema_altering_statement");

schema_altering_statement::schema_altering_statement(timeout_config_selector timeout_selector)
    : cf_statement(cf_name())
    , cql_statement_no_metadata(timeout_selector)
    , _is_column_family_level{false}
{
}

schema_altering_statement::schema_altering_statement(cf_name name, timeout_config_selector timeout_selector)
    : cf_statement{std::move(name)}
    , cql_statement_no_metadata(timeout_selector)
    , _is_column_family_level{true}
{
}

future<> schema_altering_statement::grant_permissions_to_creator(const service::client_state&) const {
    return make_ready_future<>();
}

bool schema_altering_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return false;
}

uint32_t schema_altering_statement::get_bound_terms() const
{
    return 0;
}

void schema_altering_statement::prepare_keyspace(const service::client_state& state)
{
    if (_is_column_family_level) {
        cf_statement::prepare_keyspace(state);
    }
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute0(query_processor& qp, service::query_state& state, const query_options& options) const {
    auto& mm = qp.get_migration_manager();
    ::shared_ptr<cql_transport::event::schema_change> ce;

    if (this_shard_id() != 0) {
        // execute all schema altering statements on a shard zero since this is where raft group 0 is
        co_return ::make_shared<cql_transport::messages::result_message::bounce_to_shard>(0,
                    std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls()));
    }

    auto retries = mm.get_concurrent_ddl_retries();
    while (true) {
        try {
            auto group0_guard = co_await mm.start_group0_operation();

            auto [ret, m] = co_await prepare_schema_mutations(qp, group0_guard.write_timestamp());

            if (!m.empty()) {
                auto description = format("CQL DDL statement: \"{}\"", raw_cql_statement);
                co_await mm.announce(std::move(m), std::move(group0_guard), description);
            }

            ce = std::move(ret);
        } catch (const service::group0_concurrent_modification&) {
            mylogger.warn("Failed to execute DDL statement \"{}\" due to concurrent group 0 modification.{}.",
                    raw_cql_statement, retries ? " Retrying" : " Number of retries exceeded, giving up");
            if (retries--) {
                continue;
            }
            throw;
        }
        break;
    }

    // If an IF [NOT] EXISTS clause was used, this may not result in an actual schema change.  To avoid doing
    // extra work in the drivers to handle schema changes, we return an empty message in this case. (CASSANDRA-7600)
    if (!ce) {
        co_return ::make_shared<messages::result_message::void_message>();
    } else {
        co_return ::make_shared<messages::result_message::schema_change>(ce);
    }
}

future<::shared_ptr<messages::result_message>>
schema_altering_statement::execute(query_processor& qp, service::query_state& state, const query_options& options) const {
    bool internal = state.get_client_state().is_internal();
    if (internal) {
        auto replication_type = locator::replication_strategy_type::everywhere_topology;
        data_dictionary::database db = qp.db();
        if (_cf_name && _cf_name->has_keyspace()) {
           const auto& ks = db.find_keyspace(_cf_name->get_keyspace());
           replication_type = ks.get_replication_strategy().get_type();
        }
        if (replication_type != locator::replication_strategy_type::local) {
            sstring info = _cf_name ? _cf_name->to_string() : "schema";
            throw std::logic_error(format("Attempted to modify {} via internal query: such schema changes are not propagated and thus illegal", info));
        }
    }

    return execute0(qp, state, options).then([this, &state, internal](::shared_ptr<messages::result_message> result) {
        // We don't want to grant the permissions to the supposed creator even if the statement succeeded if it's an internal query
        // or if the query did not actually create the item, i.e. the query is bounced to another shard or it's a IF NOT EXISTS
        // query where the item already exists.
        auto permissions_granted_fut = internal || !result->is_schema_change()
                ? make_ready_future<>()
                : grant_permissions_to_creator(state.get_client_state());
        return permissions_granted_fut.then([result = std::move(result)] {
           return result;
        });
    });
}

}

}
