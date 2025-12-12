/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "sc_groups_manager.hh"

#include "seastar/core/when_all.hh"
#include "service/raft/strong_consistency/sc_state_machine.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/storage_proxy.hh"
#include "replica/database.hh"
#include "db/config.hh"


namespace service {

static logging::logger logger("sc_groups_manager");

static raft::server_id to_server_id(locator::host_id host_id) {
    return raft::server_id{host_id.uuid()};
};

class sc_groups_manager::rpc_impl: public service::raft_rpc {
public:
    rpc_impl(raft_state_machine& sm, netw::messaging_service& ms,
             shared_ptr<raft::failure_detector> failure_detector,
             raft::group_id gid, raft::server_id my_id)
        : service::raft_rpc(sm, ms, std::move(failure_detector), gid, my_id)
    {
    }

    void on_configuration_change(raft::server_address_set add, raft::server_address_set del) override {
    }
};

sc_groups_manager::sc_groups_manager(netw::messaging_service& ms,
        raft_group_registry& raft_gr, cql3::query_processor& qp)
    : _ms(ms)
    , _raft_gr(raft_gr)
    , _qp(qp)
{
}

future<> sc_groups_manager::start_raft_server(table_id table_id, locator::tablet_id tablet_id,
    raft::group_id gid, raft::server_id server_id, const locator::tablet_info& tablet_info)
{
    auto state_machine = make_sc_state_machine(table_id, tablet_id, gid, 
        _qp.proxy().local_db(), _qp.proxy().system_keyspace());
    auto rpc = std::make_unique<rpc_impl>(*state_machine, _ms, _raft_gr.failure_detector(), gid, server_id);
    // Keep a reference to a specific RPC class.
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(_qp, gid, server_id);

    // Store the initial configuration if this is the first time we create this group
    // on this node
    const auto snapshot = co_await storage->load_snapshot_descriptor();
    if (!snapshot.id) {
        raft::configuration configuration;
        configuration.current.reserve(tablet_info.replicas.size());
        for (const auto& r: tablet_info.replicas) {
            configuration.current.emplace(raft::server_address{to_server_id(r.host), {}},
                raft::is_voter::yes);
        }
        co_await storage->bootstrap(std::move(configuration), false);
    }

    auto& persistence_ref = *storage;
    auto config = raft::server::configuration {
        .enable_forwarding = false,
        .on_background_error = [table_id, tablet_id, gid](std::exception_ptr e) {
            on_internal_error(logger, ::format("table {}, tablet {} raft group {} background error {}",
                table_id, tablet_id, gid, e));
        }
    };
    auto server = raft::create_server(server_id, std::move(rpc), std::move(state_machine),
            std::move(storage), _raft_gr.failure_detector(), config);

    // initialize the corresponding timer to tick the raft server instance
    auto ticker = std::make_unique<raft_ticker_type>([srv = server.get()] { srv->tick(); });
    co_await _raft_gr.start_server_for_group(raft_server_for_group {
        .gid = gid,
        .server = std::move(server),
        .ticker = std::move(ticker),
        .rpc = rpc_ref,
        .persistence = persistence_ref,
        .default_op_timeout_in_ms = _qp.proxy().get_db().local().get_config().group0_raft_op_timeout_in_ms
    });
};

future<> sc_groups_manager::start_raft_servers(const erms_map& erms, locator::host_id host_id) {
    const auto this_replica = locator::tablet_replica {
        .host = host_id,
        .shard = this_shard_id()
    };

    std::vector<future<>> futures;
    for (const auto& [table_id, erm]: erms) {
        if (!erm->get_replication_strategy().uses_tablets()) {
            continue;
        }
        const auto& tablets_map = erm->get_token_metadata().tablets().get_tablet_map(table_id);
        if (!tablets_map.has_raft_info()) {
            continue;
        }
        for (const auto tid: tablets_map.tablet_ids()) {
            if (!tablets_map.has_replica(tid, this_replica)) {
                continue;
            }

            const auto group_id = tablets_map.get_tablet_raft_info(tid).group_id;
            if (_raft_gr.find_server(group_id)) {
                continue;
            }

            logger.info("Starting tablet raft group server, tablet id {}, group id {}", tid, group_id);
            futures.push_back(start_raft_server(table_id, tid, group_id,
                to_server_id(this_replica.host), tablets_map.get_tablet_info(tid)));
        }
    }

    co_await when_all_succeed(std::move(futures));
}

future<> sc_groups_manager::stop() {
    return seastar::make_ready_future<>();
}
}