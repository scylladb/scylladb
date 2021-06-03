/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
#include "service/raft/raft_services.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_sys_table_storage.hh"
#include "service/raft/schema_raft_state_machine.hh"
#include "service/raft/raft_gossip_failure_detector.hh"

#include "raft/raft.hh"
#include "message/messaging_service.hh"
#include "cql3/query_processor.hh"
#include "gms/gossiper.hh"

#include <seastar/core/smp.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/util/log.hh>
#include <seastar/util/defer.hh>

logging::logger rslog("raft_services");

raft_services::raft_services(netw::messaging_service& ms, gms::gossiper& gs, cql3::query_processor& qp)
    : _ms(ms), _qp(qp), _fd(make_shared<raft_gossip_failure_detector>(gs, *this))
{}

void raft_services::init_rpc_verbs() {
    auto handle_raft_rpc = [this] (
            const rpc::client_info& cinfo,
            const raft::group_id& gid, raft::server_id from, raft::server_id dst, auto handler) {
        return container().invoke_on(shard_for_group(gid),
                [addr = netw::messaging_service::get_source(cinfo).addr, from, dst, handler] (raft_services& self) mutable {
            // Update the address mappings for the rpc module
            // in case the sender is encountered for the first time
            auto& rpc = self.get_rpc(dst);
            // The address learnt from a probably unknown server should
            // eventually expire
            self.update_address_mapping(from, std::move(addr), true);
            // Execute the actual message handling code
            return handler(rpc);
        });
    };

    _ms.register_raft_send_snapshot([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::install_snapshot snp) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, snp = std::move(snp)] (raft_rpc& rpc) mutable {
            return rpc.apply_snapshot(std::move(from), std::move(snp));
        });
    });

    _ms.register_raft_append_entries([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
           raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_request append_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, append_request = std::move(append_request)] (raft_rpc& rpc) mutable {
            rpc.append_entries(std::move(from), std::move(append_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_append_entries_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_reply reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, reply = std::move(reply)] (raft_rpc& rpc) mutable {
            rpc.append_entries_reply(std::move(from), std::move(reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_request([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_request vote_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_request] (raft_rpc& rpc) mutable {
            rpc.request_vote(std::move(from), std::move(vote_request));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_vote_reply([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_reply vote_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_reply] (raft_rpc& rpc) mutable {
            rpc.request_vote_reply(std::move(from), std::move(vote_reply));
            return make_ready_future<>();
        });
    });

    _ms.register_raft_timeout_now([handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::timeout_now timeout_now) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, timeout_now] (raft_rpc& rpc) mutable {
            rpc.timeout_now_request(std::move(from), std::move(timeout_now));
            return make_ready_future<>();
        });
    });
}

future<> raft_services::uninit_rpc_verbs() {
    return when_all_succeed(
        _ms.unregister_raft_send_snapshot(),
        _ms.unregister_raft_append_entries(),
        _ms.unregister_raft_append_entries_reply(),
        _ms.unregister_raft_vote_request(),
        _ms.unregister_raft_vote_reply(),
        _ms.unregister_raft_timeout_now()
    ).discard_result();
}

future<> raft_services::stop_servers() {
    std::vector<future<>> stop_futures;
    stop_futures.reserve(_servers.size());
    for (auto& entry : _servers) {
        stop_futures.emplace_back(entry.second.server->abort());
    }
    co_await when_all_succeed(stop_futures.begin(), stop_futures.end());
}

seastar::future<> raft_services::init() {
    init_rpc_verbs();
    auto uninit_rpc_verbs = defer([this] { this->uninit_rpc_verbs().get(); });
    // schema raft server instance always resides on shard 0
    if (this_shard_id() == 0) {
        // FIXME: Server id will change each time scylla server restarts,
        // need to persist it or find a deterministic way to compute!
        raft::server_id id = {.id = utils::make_random_uuid()};
        co_await add_server(id, create_schema_server(id));
    }
    uninit_rpc_verbs.cancel();
}

seastar::future<> raft_services::uninit() {
    return uninit_rpc_verbs().then([this] {
        return stop_servers();
    });
}

raft_rpc& raft_services::get_rpc(raft::server_id id) {
    auto it = _servers.find(id);
    if (it == _servers.end()) {
        on_internal_error(rslog, format("No raft server found with id = {}", id));
    }
    return *it->second.rpc;
}

raft_services::create_server_result raft_services::create_schema_server(raft::server_id id) {
    auto rpc = std::make_unique<raft_rpc>(_ms, *this, schema_raft_state_machine::gid, id);
    auto& rpc_ref = *rpc;
    auto storage = std::make_unique<raft_sys_table_storage>(_qp, schema_raft_state_machine::gid);
    auto state_machine = std::make_unique<schema_raft_state_machine>();

    return std::pair(raft::create_server(id,
            std::move(rpc),
            std::move(state_machine),
            std::move(storage),
            _fd,
            raft::server::configuration()), // use default raft server configuration
        &rpc_ref);
}

future<> raft_services::add_server(raft::server_id id, create_server_result srv) {
    auto srv_ref = std::ref(*srv.first);
    // initialize the corresponding timer to tick the raft server instance
    ticker_type t([srv_ref] {
        srv_ref.get().tick();
    });
    auto [it, inserted] = _servers.emplace(std::pair(id, servers_value_type{
        .server = std::move(srv.first),
        .rpc = srv.second,
        .ticker = std::move(t)
    }));
    if (!inserted) {
        on_internal_error(rslog, format("Attempt to add the second instance of raft server with the same id={}", id));
    }
    ticker_type& ticker_from_map = it->second.ticker;
    try {
        // start the server instance prior to arming the ticker timer.
        // By the time the tick() is executed the server should already be initialized.
        co_await srv_ref.get().start();
    } catch (...) {
        // remove server from the map to prevent calling `abort()` on a
        // non-started instance when `raft_services::uninit` is called.
        _servers.erase(it);
        on_internal_error(rslog, std::current_exception());
    }

    ticker_from_map.arm_periodic(tick_interval);
}

unsigned raft_services::shard_for_group(const raft::group_id& gid) const {
    if (gid == schema_raft_state_machine::gid) {
        return 0; // schema raft server is always owned by shard 0
    }
    // We haven't settled yet on how to organize and manage (group_id -> shard_id) mapping
    on_internal_error(rslog, format("Could not map raft group id {} to a corresponding shard id", gid));
}

gms::inet_address raft_services::get_inet_address(raft::server_id id) const {
    auto it = _srv_address_mappings.find(id);
    if (!it) {
        on_internal_error(rslog, format("Destination raft server not found with id {}", id));
    }
    return *it;
}

void raft_services::update_address_mapping(raft::server_id id, gms::inet_address addr, bool expiring) {
    _srv_address_mappings.set(id, std::move(addr), expiring);
}

void raft_services::remove_address_mapping(raft::server_id id) {
    _srv_address_mappings.erase(id);
}
