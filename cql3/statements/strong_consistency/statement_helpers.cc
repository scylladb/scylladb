/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "statement_helpers.hh"

#include "transport/messages/result_message_base.hh"
#include "cql3/query_processor.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"

namespace cql3::statements::strong_consistency {
future<::shared_ptr<cql_transport::messages::result_message>> redirect_statement(query_processor& qp,
        const query_options& options,
        const locator::tablet_replica& target)
{
    const auto my_host_id = qp.db().real_database().get_token_metadata().get_topology().my_host_id();
    if (target.host != my_host_id) {
        throw exceptions::invalid_request_exception(format(
            "Strongly consistent writes can be executed only on the leader node, "
            "leader id {}, current host id {}",
            target.host, my_host_id));
    }
    auto&& func_values_cache = const_cast<cql3::query_options&>(options).take_cached_pk_function_calls();
    co_return qp.bounce_to_shard(target.shard, std::move(func_values_cache));
}

bool is_strongly_consistent(data_dictionary::database db, std::string_view ks_name) {
    const auto* tablet_aware_rs = db.find_keyspace(ks_name).get_replication_strategy().maybe_as_tablet_aware();
    return tablet_aware_rs && tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::eventual;
}

}