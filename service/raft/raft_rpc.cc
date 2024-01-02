/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/raft_rpc.hh"
#include <seastar/core/coroutine.hh>
#include "gms/inet_address.hh"
#include "serializer_impl.hh"
#include "message/msg_addr.hh"
#include "message/messaging_service.hh"
#include "db/timeout_clock.hh"
#include "idl/raft.dist.hh"
#include "service/raft/raft_address_map.hh"
#include "service/raft/raft_state_machine.hh"

namespace service {

static seastar::logger rlogger("raft_rpc");

using sloc = seastar::compat::source_location;

raft_ticker_type::time_point timeout() {
    return raft_ticker_type::clock::now() + raft_tick_interval * (raft::ELECTION_TIMEOUT.count() / 2);
}

raft_rpc::raft_rpc(raft_state_machine& sm, netw::messaging_service& ms,
        raft_address_map& address_map, shared_ptr<raft::failure_detector> failure_detector, raft::group_id gid, raft::server_id my_id)
    : _sm(sm), _group_id(std::move(gid)), _my_id(my_id), _messaging(ms)
    , _address_map(address_map), _failure_detector(std::move(failure_detector))
{}


template <raft_rpc::one_way_kind rpc_kind, typename Verb, typename Msg> void
raft_rpc::one_way_rpc(sloc loc, raft::server_id id,
        Verb&& verb, Msg&& msg) {
    (void)with_gate(_shutdown_gate, [this, loc = std::move(loc), id, &verb, &msg] () mutable {
        if (rpc_kind == one_way_kind::request && !_failure_detector->is_alive(id)) {
            rlogger.debug("{}:{}: {} dropping outgoing message to {} - node is not seen as alive by the failure detector",
                loc.file_name(), loc.line(), loc.function_name(), id);
            return make_ready_future<>();
        }
        auto ip_addr = _address_map.find(id);
        if (!ip_addr) {
            rlogger.debug("{}:{}: {} dropping outgoing message to {} - IP address not found",
                loc.file_name(), loc.line(), loc.function_name(), id);
            return make_ready_future<>();
        }
        return verb(&_messaging, netw::msg_addr(*ip_addr), timeout(), _group_id, _my_id, id, std::forward<Msg>(msg))
            .handle_exception([loc = std::move(loc), id] (std::exception_ptr ex) {
                try {
                    std::rethrow_exception(ex);
                } catch (seastar::rpc::timeout_error&) {
                } catch (seastar::rpc::closed_error&) {
                } catch (...) {
                    rlogger.error("Failed to send {} to {}: {}", loc.function_name(), id, std::current_exception());
                }
        });
    });
}

template <typename Verb, typename... Args>
auto
raft_rpc::two_way_rpc(sloc loc, raft::server_id id,
        Verb&& verb, Args&&... args) {
    using Fut = decltype(verb(&_messaging, netw::msg_addr(gms::inet_address()), db::no_timeout, _group_id, _my_id, id, std::forward<Args>(args)...));
    using Ret = typename Fut::value_type;
    if (!_failure_detector->is_alive(id)) {
        return make_exception_future<Ret>(raft::destination_not_alive_error(id, loc));
    }
    auto ip_addr = _address_map.find(id);
    if (!ip_addr) {
        const auto msg = format("Failed to send {} {}: ip address not found", loc.function_name(), id);
        return make_exception_future<Ret>(raft::transport_error(msg));
    }
    return verb(&_messaging, netw::msg_addr(*ip_addr), db::no_timeout, _group_id, _my_id, id, std::forward<Args>(args)...)
        .handle_exception_type([loc= std::move(loc), id] (const seastar::rpc::closed_error& e) {;
            const auto msg = format("Failed to execute {} on leader {}: {}", loc.function_name(), id, e);
            rlogger.trace("{}", msg);
            return make_exception_future<Ret>(raft::transport_error(msg));
    });
}

future<raft::snapshot_reply> raft_rpc::send_snapshot(raft::server_id id, const raft::install_snapshot& snap, seastar::abort_source& as) {
    return two_way_rpc(sloc::current(), id, ser::raft_rpc_verbs::send_raft_send_snapshot, snap);
}

future<> raft_rpc::send_append_entries(raft::server_id id, const raft::append_request& append_request) {
    if (!_failure_detector->is_alive(id)) {
        rlogger.debug("Failed to send append_entires to {}: node is not seen as alive by the failure detector", id);
        co_return;
    }
    auto ip_addr = _address_map.find(id);
    if (!ip_addr) {
        const auto msg = format("Failed to send append_entires to {}: ip address not found", id);
        co_await coroutine::return_exception_ptr(std::make_exception_ptr(raft::transport_error(msg)));
    }
    co_return co_await ser::raft_rpc_verbs::send_raft_append_entries(&_messaging, netw::msg_addr(*ip_addr),
            db::no_timeout, _group_id, _my_id, id, append_request);
}

void raft_rpc::send_append_entries_reply(raft::server_id id, const raft::append_reply& reply) {
    one_way_rpc<one_way_kind::reply>(sloc::current(), id, ser::raft_rpc_verbs::send_raft_append_entries_reply, reply);
}

void raft_rpc::send_vote_request(raft::server_id id, const raft::vote_request& vote_request) {
    one_way_rpc<one_way_kind::request>(sloc::current(), id, ser::raft_rpc_verbs::send_raft_vote_request, vote_request);
}

void raft_rpc::send_vote_reply(raft::server_id id, const raft::vote_reply& vote_reply) {
    one_way_rpc<one_way_kind::reply>(sloc::current(), id, ser::raft_rpc_verbs::send_raft_vote_reply, vote_reply);
}

void raft_rpc::send_timeout_now(raft::server_id id, const raft::timeout_now& timeout_now) {
    one_way_rpc<one_way_kind::request>(sloc::current(), id, ser::raft_rpc_verbs::send_raft_timeout_now, timeout_now);
}

void raft_rpc::send_read_quorum(raft::server_id id, const raft::read_quorum& read_quorum) {
    one_way_rpc<one_way_kind::request>(sloc::current(), id, ser::raft_rpc_verbs::send_raft_read_quorum, read_quorum);
}

void raft_rpc::send_read_quorum_reply(raft::server_id id, const raft::read_quorum_reply& read_quorum_reply) {
    one_way_rpc<one_way_kind::reply>(sloc::current(), id, ser::raft_rpc_verbs::send_raft_read_quorum_reply, read_quorum_reply);
}

future<raft::add_entry_reply> raft_rpc::send_add_entry(raft::server_id id, const raft::command& cmd) {
    return two_way_rpc(sloc::current(), id, ser::raft_rpc_verbs::send_raft_add_entry, cmd);
}

future<raft::add_entry_reply> raft_rpc::send_modify_config(raft::server_id id,
        const std::vector<raft::config_member>& add,
        const std::vector<raft::server_id>& del) {
    return two_way_rpc(sloc::current(), id, ser::raft_rpc_verbs::send_raft_modify_config, add, del);
}

future<raft::read_barrier_reply> raft_rpc::execute_read_barrier_on_leader(raft::server_id id) {
    return two_way_rpc(sloc::current(), id, ser::raft_rpc_verbs::send_raft_execute_read_barrier_on_leader);
}

future<> raft_rpc::abort() {
    return _shutdown_gate.close();
}

void raft_rpc::append_entries(raft::server_id from, raft::append_request append_request) {
    _client->append_entries(from, std::move(append_request));
}

void raft_rpc::append_entries_reply(raft::server_id from, raft::append_reply reply) {
    _client->append_entries_reply(from, std::move(reply));
}

void raft_rpc::request_vote(raft::server_id from, raft::vote_request vote_request) {
    _client->request_vote(from, vote_request);
}

void raft_rpc::request_vote_reply(raft::server_id from, raft::vote_reply vote_reply) {
    _client->request_vote_reply(from, vote_reply);
}

void raft_rpc::timeout_now_request(raft::server_id from, raft::timeout_now timeout_now) {
    _client->timeout_now_request(from, timeout_now);
}

void raft_rpc::read_quorum_request(raft::server_id from, raft::read_quorum check_quorum) {
    _client->read_quorum_request(from, check_quorum);
}

void raft_rpc::read_quorum_reply(raft::server_id from, raft::read_quorum_reply check_quorum_reply) {
    _client->read_quorum_reply(from, check_quorum_reply);
}

// Simple helper that throws `raft::stopped_error` instead of `gate_closed_exception` on shutdown.
template <typename F>
auto raft_with_gate(gate& g, F&& f) -> decltype(f()) {
    if (g.is_closed()) {
        throw raft::stopped_error{};
    }
    return with_gate(g, std::forward<F>(f));
}

future<raft::read_barrier_reply> raft_rpc::execute_read_barrier(raft::server_id from) {
    return raft_with_gate(_shutdown_gate, [&] {
        return _client->execute_read_barrier(from, nullptr);
    });
}

future<raft::snapshot_reply> raft_rpc::apply_snapshot(raft::server_id from, raft::install_snapshot snp) {
    co_await _sm.transfer_snapshot(from, snp.snp);
    co_return co_await raft_with_gate(_shutdown_gate, [&] {
        return _client->apply_snapshot(from, std::move(snp));
    });
}

future<raft::add_entry_reply> raft_rpc::execute_add_entry(raft::server_id from, raft::command cmd) {
    return raft_with_gate(_shutdown_gate, [&] {
        return _client->execute_add_entry(from, std::move(cmd), nullptr);
    });
}

future<raft::add_entry_reply> raft_rpc::execute_modify_config(raft::server_id from,
    std::vector<raft::config_member> add,
    std::vector<raft::server_id> del) {
    return raft_with_gate(_shutdown_gate, [&] {
        return _client->execute_modify_config(from, std::move(add), std::move(del), nullptr);
    });
}

} // end of namespace service
