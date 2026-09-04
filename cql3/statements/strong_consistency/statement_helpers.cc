/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "statement_helpers.hh"

#include "transport/messages/result_message_base.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/modification_statement.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/coordinator.hh"

namespace cql3::statements::strong_consistency {
future<::shared_ptr<cql_transport::messages::result_message>> redirect_statement(query_processor& qp,
        const query_options& options,
        const locator::tablet_replica& target,
        db::timeout_clock::time_point timeout,
        bool is_write,
        service::strong_consistency::stats& stats,
        locator::host_id_or_exception_callback on_forwarding_finished)
{
    auto&& func_values_cache = const_cast<cql3::query_options&>(options).take_cached_pk_function_calls();
    const auto my_host_id = qp.db().real_database().get_token_metadata().get_topology().my_host_id();
    if (target.host != my_host_id) {
        ++(is_write ? stats.write_node_bounces : stats.read_node_bounces);
        co_return qp.bounce_to_node(target, std::move(func_values_cache), timeout, is_write, std::move(on_forwarding_finished));
    }
    ++(is_write ? stats.write_shard_bounces : stats.read_shard_bounces);
    co_return qp.bounce_to_shard(target.shard, std::move(func_values_cache));
}

bool is_strongly_consistent(data_dictionary::database db, std::string_view ks_name) {
    const auto* tablet_aware_rs = db.find_keyspace(ks_name).get_replication_strategy().maybe_as_tablet_aware();
    return tablet_aware_rs && tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::eventual;
}

void validate_modification_support(const cql3::statements::modification_statement& stmt) {
    if (stmt.has_conditions()) {
        throw exceptions::invalid_request_exception("Strongly consistent updates don't support conditions");
    }
    if (stmt.requires_read()) {
        throw exceptions::invalid_request_exception("Strongly consistent updates don't support data prefetch");
    }
    if (stmt.is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Strongly consistent queries don't support user-provided timestamps");
    }
}

void validate_write_consistency_level(db::consistency_level cl) {
    if (cl != db::consistency_level::QUORUM && cl != db::consistency_level::LOCAL_QUORUM) {
        throw exceptions::invalid_request_exception("Strongly consistent writes must use QUORUM/LOCAL_QUORUM consistency level");
    }
}

}