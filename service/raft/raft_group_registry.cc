/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_gossip_failure_detector.hh"
#include "message/messaging_service.hh"
#include "serializer_impl.hh"
#include "idl/raft.dist.hh"

#include <seastar/core/coroutine.hh>

namespace service {

logging::logger rslog("raft_group_registry");

raft_group_registry::raft_group_registry(bool is_enabled, netw::messaging_service& ms, gms::gossiper& gossiper)
    : _is_enabled(is_enabled), _ms(ms), _fd(make_shared<raft_gossip_failure_detector>(gossiper, _srv_address_mappings))
{
}

void raft_group_registry::init_rpc_verbs() {
    auto handle_raft_rpc = [this] (
            const rpc::client_info& cinfo,
            const raft::group_id& gid, raft::server_id from, raft::server_id dst, auto handler) {
        return container().invoke_on(shard_for_group(gid),
                [addr = netw::messaging_service::get_source(cinfo).addr, from, dst, gid, handler] (raft_group_registry& self) mutable {
            // Update the address mappings for the rpc module
            // in case the sender is encountered for the first time
            auto& rpc = self.get_rpc(gid);
            // The address learnt from a probably unknown server should
            // eventually expire
            self._srv_address_mappings.set(from, std::move(addr), true);
            // Execute the actual message handling code
            return handler(rpc);
        });
    };

    ser::raft_rpc_verbs::register_raft_send_snapshot(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::install_snapshot snp) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, snp = std::move(snp)] (raft_rpc& rpc) mutable {
            return rpc.apply_snapshot(std::move(from), std::move(snp));
        });
    });

    ser::raft_rpc_verbs::register_raft_append_entries(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
           raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_request append_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, append_request = std::move(append_request)] (raft_rpc& rpc) mutable {
            rpc.append_entries(std::move(from), std::move(append_request));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_append_entries_reply(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_reply reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, reply = std::move(reply)] (raft_rpc& rpc) mutable {
            rpc.append_entries_reply(std::move(from), std::move(reply));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_vote_request(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_request vote_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_request] (raft_rpc& rpc) mutable {
            rpc.request_vote(std::move(from), std::move(vote_request));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_vote_reply(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_reply vote_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_reply] (raft_rpc& rpc) mutable {
            rpc.request_vote_reply(std::move(from), std::move(vote_reply));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_timeout_now(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::timeout_now timeout_now) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, timeout_now] (raft_rpc& rpc) mutable {
            rpc.timeout_now_request(std::move(from), std::move(timeout_now));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_read_quorum(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum read_quorum) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum] (raft_rpc& rpc) mutable {
            rpc.read_quorum_request(std::move(from), std::move(read_quorum));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_read_quorum_reply(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum_reply read_quorum_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum_reply] (raft_rpc& rpc) mutable {
            rpc.read_quorum_reply(std::move(from), std::move(read_quorum_reply));
            return netw::messaging_service::no_wait();
        });
    });

    ser::raft_rpc_verbs::register_raft_execute_read_barrier_on_leader(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from] (raft_rpc& rpc) mutable {
            return rpc.execute_read_barrier(from);
        });
    });

    ser::raft_rpc_verbs::register_raft_add_entry(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::command cmd) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, cmd = std::move(cmd)] (raft_rpc& rpc) mutable {
            return rpc.execute_add_entry(from, std::move(cmd));
        });
    });

    ser::raft_rpc_verbs::register_raft_modify_config(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst,
            std::vector<raft::server_address> add, std::vector<raft::server_id> del) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst,
            [from, add = std::move(add), del = std::move(del)] (raft_rpc& rpc) mutable {

            return rpc.execute_modify_config(from, std::move(add), std::move(del));
        });
    });
}

future<> raft_group_registry::uninit_rpc_verbs() {
    return when_all_succeed(
        ser::raft_rpc_verbs::unregister_raft_send_snapshot(&_ms),
        ser::raft_rpc_verbs::unregister_raft_append_entries(&_ms),
        ser::raft_rpc_verbs::unregister_raft_append_entries_reply(&_ms),
        ser::raft_rpc_verbs::unregister_raft_vote_request(&_ms),
        ser::raft_rpc_verbs::unregister_raft_vote_reply(&_ms),
        ser::raft_rpc_verbs::unregister_raft_timeout_now(&_ms),
        ser::raft_rpc_verbs::unregister_raft_read_quorum(&_ms),
        ser::raft_rpc_verbs::unregister_raft_read_quorum_reply(&_ms),
        ser::raft_rpc_verbs::unregister_raft_execute_read_barrier_on_leader(&_ms),
        ser::raft_rpc_verbs::unregister_raft_add_entry(&_ms),
        ser::raft_rpc_verbs::unregister_raft_modify_config(&_ms)
    ).discard_result();
}

future<> raft_group_registry::stop_servers() noexcept {
    gate g;
    for (auto it = _servers.begin(); it != _servers.end(); it = _servers.erase(it)) {
        auto abort_server = it->second.server->abort();
        // discarded future is waited via g.close()
        (void)std::move(abort_server).handle_exception([rsfg = std::move(it->second), gh = g.hold()] (std::exception_ptr ex) {
            rslog.warn("Failed to abort raft group server {}: {}", rsfg.gid, ex);
        });
    }
    co_await g.close();
}

seastar::future<> raft_group_registry::start() {
    if (!_is_enabled) {
        co_return;
    }
    // Once a Raft server starts, it soon times out
    // and starts an election, so RPC must be ready by
    // then to send VoteRequest messages.
    co_return init_rpc_verbs();
}

seastar::future<> raft_group_registry::stop() {
    if (!_is_enabled) {
        co_return;
    }
    co_await when_all_succeed(
        uninit_rpc_verbs(),
        stop_servers()
    ).discard_result();
}

raft_server_for_group& raft_group_registry::server_for_group(raft::group_id gid) {
    auto it = _servers.find(gid);
    if (it == _servers.end()) {
        throw raft_group_not_found(gid);
    }
    return it->second;
}

raft_rpc& raft_group_registry::get_rpc(raft::group_id gid) {
    return server_for_group(gid).rpc;
}

raft::server& raft_group_registry::get_server(raft::group_id gid) {
    return *(server_for_group(gid).server);
}

raft::server& raft_group_registry::group0() {
    return *(server_for_group(*_group0_id).server);
}

future<> raft_group_registry::start_server_for_group(raft_server_for_group new_grp) {
    auto gid = new_grp.gid;

    if (_servers.contains(gid)) {
        on_internal_error(rslog, format("Attempt to add the second instance of raft server with the same gid={}", gid));
    }

    try {
        // start the server instance prior to arming the ticker timer.
        // By the time the tick() is executed the server should already be initialized.
        co_await new_grp.server->start();
        new_grp.server->register_metrics();
    } catch (...) {
        on_internal_error(rslog, std::current_exception());
    }

    std::exception_ptr ex;
    raft::server& server = *new_grp.server;

    try {
        auto [it, inserted] = _servers.emplace(std::move(gid), std::move(new_grp));

        if (_servers.size() == 1 && this_shard_id() == 0) {
            _group0_id = gid;
        }

        it->second.ticker->arm_periodic(raft_tick_interval);
    } catch (...) {
        ex = std::current_exception();
    }

    if (ex) {
        co_await server.abort();
        std::rethrow_exception(ex);
    }
}

unsigned raft_group_registry::shard_for_group(const raft::group_id& gid) const {
    return 0; // schema raft server is always owned by shard 0
}

} // end of namespace service
