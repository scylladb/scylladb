/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_storage_proxy.hh"
#include "db/system_keyspace.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_group0_client.hh"
#include "service/raft/strong_consistency/sc_state_machine.hh"
#include "idl/sc_state_machine.dist.hh"
#include "idl/sc_state_machine.dist.impl.hh"

namespace service {

static logging::logger logger("sc_storage_proxy");

sc_storage_proxy::sc_storage_proxy(raft_group_registry& raft_groups, db::system_keyspace& sys_ks)
    : _raft_groups(raft_groups)
    , _sys_ks(sys_ks)
{
}

future<> sc_storage_proxy::mutate(const schema& schema, const dht::token& token, mutatations_gen&& mutatations_gen) {
    if (this_shard_id() != 0) {
        on_internal_error(logger, "mutate() can only be called on zero shard");
    }

    auto erm = schema.table().get_effective_replication_map();

    if (const auto* tablet_aware_rs = erm->get_replication_strategy().maybe_as_tablet_aware();
        !tablet_aware_rs || 
        tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::local)
    {
        on_internal_error(logger, "Unexpected replication strategy");
    }
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(schema.id());
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& group_id = tablet_map.get_tablet_raft_info(tablet_id).group_id;

    const auto prev_state_id = co_await _sys_ks.get_last_group0_state_id(::format("{}", group_id.uuid()));
    const auto new_state_id = raft_group0_client::generate_group0_state_id(prev_state_id);
    const auto ts = utils::UUID_gen::micros_timestamp(new_state_id);

    sc_raft_command command {
        .prev_state_id = prev_state_id,
        .new_state_id = new_state_id
    };
    auto muts = mutatations_gen(ts);
    command.mutations.reserve(muts.size());
    for (const auto& m: muts) {
        command.mutations.emplace_back(m);
    }
    raft::command raft_cmd;
    ser::serialize(raft_cmd, command);

    auto& raft_server = _raft_groups.get_server(group_id);
    co_await raft_server.add_entry(std::move(raft_cmd), raft::wait_type::applied, nullptr);
}
}
