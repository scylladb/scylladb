/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "service/raft/raft_group_registry.hh"
#include "service/raft/raft_rpc.hh"
#include "service/raft/raft_address_map.hh"
#include "db/system_keyspace.hh"
#include "message/messaging_service.hh"
#include "gms/gossiper.hh"
#include "gms/i_endpoint_state_change_subscriber.hh"
#include "serializer_impl.hh"
#include "idl/raft.dist.hh"
#include "utils/composite_abort_source.hh"

#include <seastar/core/coroutine.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/sleep.hh>

namespace service {

logging::logger rslog("raft_group_registry");

// {{{ direct_fd_proxy

class direct_fd_proxy : public raft::failure_detector, public direct_failure_detector::listener {
    std::unordered_set<raft::server_id> _alive_set;
    raft::server_id _my_id;

public:
    direct_fd_proxy(raft::server_id my_id)
            : _my_id(my_id)
    {
    }

    future<> mark_alive(direct_failure_detector::pinger::endpoint_id id) override {
        static constexpr auto msg = "marking Raft server {} as alive for raft groups";

        auto raft_id = raft::server_id{id};
        _alive_set.insert(raft_id);

        // The listener should be registered on every shard.
        // Write the message on INFO level only on shard 0 so we don't spam the logs.
        if (this_shard_id() == 0) {
            rslog.info(msg, raft_id);
        } else {
            rslog.debug(msg, raft_id);
        }

        co_return;
    }

    future<> mark_dead(direct_failure_detector::pinger::endpoint_id id) override {
        static constexpr auto msg = "marking Raft server {} as dead for raft groups";

        auto raft_id = raft::server_id{id};
        _alive_set.erase(raft_id);

        // As above.
        if (this_shard_id() == 0) {
            rslog.info(msg, raft_id);
        } else {
            rslog.debug(msg, raft_id);
        }

        co_return;
    }

    bool is_alive(raft::server_id srv) override {
        return srv == _my_id || _alive_set.contains(srv);
    }
};
// }}} direct_fd_proxy

raft_group_registry::raft_group_registry(
        raft::server_id my_id,
        raft_address_map& address_map,
        netw::messaging_service& ms, direct_failure_detector::failure_detector& fd)
    : _ms(ms)
    , _address_map{address_map}
    , _direct_fd(fd)
    , _direct_fd_proxy(make_shared<direct_fd_proxy>(my_id))
    , _my_id(my_id)
{
}

void raft_group_registry::init_rpc_verbs() {
    auto handle_raft_rpc = [this] (
            const rpc::client_info& cinfo,
            const raft::group_id& gid, raft::server_id from, raft::server_id dst, auto handler) {
        constexpr bool is_one_way = std::is_void_v<std::invoke_result_t<decltype(handler), raft_rpc&>>;
        if (_my_id != dst) {
            if constexpr (is_one_way) {
                rslog.debug("Got message for server {}, but my id is {}", dst, _my_id);
                return make_ready_future<rpc::no_wait_type>(netw::messaging_service::no_wait());
            } else {
                throw raft_destination_id_not_correct{_my_id, dst};
            }
        }

        return container().invoke_on(shard_for_group(gid),
                [addr = netw::messaging_service::get_source(cinfo).addr, from, gid, handler] (raft_group_registry& self) mutable {
            // Update the address mappings for the rpc module
            // in case the sender is encountered for the first time
            auto& rpc = self.get_rpc(gid);
            // The address learnt from a probably unknown server should
            // eventually expire. Do not use it to update
            // a previously learned gossiper address: otherwise an RPC from
            // a node outside of the config could permanently
            // change the address map of a healthy cluster.
            self._address_map.opt_add_entry(from, std::move(addr));
            // Execute the actual message handling code
            if constexpr (is_one_way) {
                handler(rpc);
                return netw::messaging_service::no_wait();
            } else {
                return handler(rpc);
            }
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
        return handle_raft_rpc(cinfo, gid, from, dst, [from, append_request = std::move(append_request), original_shard_id = this_shard_id()] (raft_rpc& rpc) mutable {
            // lw_shared_ptr (raft::log_entry_ptr) doesn't support cross-shard ref counting (see debug_shared_ptr_counter_type),
            // so we are copying entries to this shard if it isn't equal the original one.
            rpc.append_entries(std::move(from),
                this_shard_id() == original_shard_id ? std::move(append_request) : append_request.copy());
        });
    });

    ser::raft_rpc_verbs::register_raft_append_entries_reply(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::append_reply reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, reply = std::move(reply)] (raft_rpc& rpc) mutable {
            rpc.append_entries_reply(std::move(from), std::move(reply));
        });
    });

    ser::raft_rpc_verbs::register_raft_vote_request(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_request vote_request) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_request] (raft_rpc& rpc) mutable {
            rpc.request_vote(std::move(from), std::move(vote_request));
        });
    });

    ser::raft_rpc_verbs::register_raft_vote_reply(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::vote_reply vote_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, vote_reply] (raft_rpc& rpc) mutable {
            rpc.request_vote_reply(std::move(from), std::move(vote_reply));
        });
    });

    ser::raft_rpc_verbs::register_raft_timeout_now(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::timeout_now timeout_now) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, timeout_now] (raft_rpc& rpc) mutable {
            rpc.timeout_now_request(std::move(from), std::move(timeout_now));
        });
    });

    ser::raft_rpc_verbs::register_raft_read_quorum(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum read_quorum) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum] (raft_rpc& rpc) mutable {
            rpc.read_quorum_request(std::move(from), std::move(read_quorum));
        });
    });

    ser::raft_rpc_verbs::register_raft_read_quorum_reply(&_ms, [handle_raft_rpc] (const rpc::client_info& cinfo, rpc::opt_time_point timeout,
            raft::group_id gid, raft::server_id from, raft::server_id dst, raft::read_quorum_reply read_quorum_reply) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst, [from, read_quorum_reply] (raft_rpc& rpc) mutable {
            rpc.read_quorum_reply(std::move(from), std::move(read_quorum_reply));
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
            std::vector<raft::config_member> add, std::vector<raft::server_id> del) mutable {
        return handle_raft_rpc(cinfo, gid, from, dst,
            [from, add = std::move(add), del = std::move(del)] (raft_rpc& rpc) mutable {

            return rpc.execute_modify_config(from, std::move(add), std::move(del));
        });
    });

    ser::raft_rpc_verbs::register_direct_fd_ping(&_ms,
            [this] (const rpc::client_info&, raft::server_id dst) -> future<direct_fd_ping_reply> {
        // XXX: update address map here as well?

        if (_my_id != dst) {
            return make_ready_future<direct_fd_ping_reply>(direct_fd_ping_reply {
                .result = wrong_destination {
                    .reached_id = _my_id,
                },
            });
        }

        return container().invoke_on(0, [] (raft_group_registry& me) -> future<direct_fd_ping_reply> {
            bool group0_alive = false;
            if (me._group0_id) {
                auto* group0_server = me.find_server(*me._group0_id);
                if (group0_server && group0_server->is_alive()) {
                    group0_alive = true;
                }
            }
            co_return direct_fd_ping_reply {
                .result = service::group_liveness_info{
                    .group0_alive = group0_alive,
                }
            };
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
        ser::raft_rpc_verbs::unregister_raft_modify_config(&_ms),
        ser::raft_rpc_verbs::unregister_direct_fd_ping(&_ms)
    ).discard_result();
}

static void ensure_aborted(raft_server_for_group& server_for_group, sstring reason) {
    if (!server_for_group.aborted) {
        server_for_group.aborted = server_for_group.server->abort(std::move(reason))
            .handle_exception([gid = server_for_group.gid] (std::exception_ptr ex) {
                rslog.warn("Failed to abort raft group server {}: {}", gid, ex);
            });
    }
}

future<> raft_group_registry::stop_server(raft::group_id gid, sstring reason) {
    auto it = _servers.find(gid);

    if (it == _servers.end()) {
        throw raft_group_not_found(gid);
    }

    auto srv = std::move(it->second);
    _servers.erase(it);
    ensure_aborted(srv, std::move(reason));
    co_await std::move(*srv.aborted);
}

future<> raft_group_registry::stop_servers() noexcept {
    gate g;
    for (auto it = _servers.begin(); it != _servers.end(); it = _servers.erase(it)) {
        ensure_aborted(it->second, "raft group registry is stopped");
        auto aborted = std::move(it->second.aborted);
        // discarded future is waited via g.close()
        (void)std::move(aborted)->finally([rsfg = std::move(it->second), gh = g.hold()]{});
    }
    co_await g.close();
}

seastar::future<> raft_group_registry::start() {
    // Once a Raft server starts, it soon times out
    // and starts an election, so RPC must be ready by
    // then to send VoteRequest messages.
    init_rpc_verbs();

    _direct_fd_subscription.emplace(co_await _direct_fd.register_listener(*_direct_fd_proxy,
        direct_fd_clock::base::duration{std::chrono::seconds{1}}.count()));
}

const raft::server_id& raft_group_registry::get_my_raft_id() {
    return _my_id;
}

seastar::future<> raft_group_registry::stop() {
    co_await drain_on_shutdown();
    co_await uninit_rpc_verbs();
    _direct_fd_subscription.reset();
}

seastar::future<> raft_group_registry::drain_on_shutdown() noexcept {
    return stop_servers();
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
    auto ptr = server_for_group(gid).server.get();
    if (!ptr) {
        on_internal_error(rslog, format("get_server(): no server for group {}", gid));
    }
    return *ptr;
}

raft_server_with_timeouts raft_group_registry::get_server_with_timeouts(raft::group_id gid) {
    auto& group_server = server_for_group(gid);
    if (!group_server.server.get()) {
        on_internal_error(rslog, format("get_server(): no server for group {}", gid));
    }
    return raft_server_with_timeouts(group_server, *this);
}

raft::server* raft_group_registry::find_server(raft::group_id gid) {
    auto it = _servers.find(gid);
    if (it == _servers.end()) {
        return nullptr;
    }
    return it->second.server.get();
}

std::vector<raft::group_id> raft_group_registry::all_groups() const {
    std::vector<raft::group_id> result;
    result.reserve(_servers.size());
    for (auto& [gid, _]: _servers) {
        result.push_back(gid);
    }
    return result;
}

raft::server& raft_group_registry::group0() {
    if (!_group0_id) {
        on_internal_error(rslog, "group0(): _group0_id not present");
    }
    return get_server(*_group0_id);
}

raft_server_with_timeouts raft_group_registry::group0_with_timeouts() {
    if (!_group0_id) {
        on_internal_error(rslog, "group0(): _group0_id not present");
    }
    return get_server_with_timeouts(*_group0_id);
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
        on_internal_error(rslog, format("Failed to start a Raft group {}: {}", gid,
                std::current_exception()));
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

void raft_group_registry::abort_server(raft::group_id gid, sstring reason) {
    // abort_server could be called from on_background_error for group0
    // when the server has not yet been added to _servers.
    // In this case we won't find gid in the if below.
    if (const auto it = _servers.find(gid); it != _servers.end()) {
        ensure_aborted(it->second, std::move(reason));
    }
}

unsigned raft_group_registry::shard_for_group(const raft::group_id& gid) const {
    return 0; // schema raft server is always owned by shard 0
}

shared_ptr<raft::failure_detector> raft_group_registry::failure_detector() {
    return _direct_fd_proxy;
}

raft_group_registry::~raft_group_registry() = default;

namespace {
    auto fmt_loc(const seastar::compat::source_location& l) {
        return fmt::format("{}({}:{}) `{}`", l.file_name(), l.line(), l.column(), l.function_name());
    };
}

raft_server_with_timeouts::raft_server_with_timeouts(raft_server_for_group& group_server, raft_group_registry& registry)
    : _group_server(group_server)
    , _registry(registry)
{
}

template <std::invocable<abort_source*> Op>
std::invoke_result_t<Op, abort_source*>
raft_server_with_timeouts::run_with_timeout(Op&& op, const char* op_name,
    seastar::abort_source* as, std::optional<raft_timeout> timeout)
{
    if (!timeout) {
        co_await op(as);
        co_return;
    }
    if (!timeout->value) {
        if (!_group_server.default_op_timeout) {
            on_internal_error(rslog, ::format("raft operation [{}], timeout requested at [{}],"
                                              "but no value for it has been defined",
                op_name, fmt_loc(timeout->loc)));
        }
        timeout->value = lowres_clock::now() + _group_server.default_op_timeout.value();
    }
    utils::composite_abort_source composite_as;

    abort_on_expiry<> expiry{*timeout->value};
    composite_as.add(expiry.abort_source());

    if (as) {
        composite_as.add(*as);
    }

    try {
        co_return co_await op(&composite_as.abort_source());
    } catch (const raft::request_aborted& e) {
        if (!expiry.abort_source().abort_requested() || (as && as->abort_requested())) {
            throw;
        }
        // TODO: improve error message (check for quorum loss etc)
        const auto message = ::format("group [{}] raft operation [{}] timed out", _group_server.gid, op_name);
        static thread_local logger::rate_limit rate_limit{std::chrono::seconds(1)};
        rslog.log(log_level::warn, rate_limit, "{}; timeout requested at [{}], original error {}",
            message, fmt_loc(timeout->loc), std::current_exception());
        throw raft_operation_timeout_error(message.c_str());
    }
}

future<> raft_server_with_timeouts::add_entry(raft::command command, raft::wait_type type,
        seastar::abort_source* as, std::optional<raft_timeout> timeout)
{
    return run_with_timeout([&](abort_source* as) {
            return _group_server.server->add_entry(std::move(command), type, as);
        }, "add_entry", as, timeout);
}

future<> raft_server_with_timeouts::modify_config(std::vector<raft::config_member> add, std::vector<raft::server_id> del,
        seastar::abort_source* as, std::optional<raft_timeout> timeout)
{
    return run_with_timeout([&](abort_source* as) {
            return _group_server.server->modify_config(std::move(add), std::move(del), as);
        }, "modify_config", as, timeout);
}

future<> raft_server_with_timeouts::read_barrier(seastar::abort_source* as, std::optional<raft_timeout> timeout)
{
    return run_with_timeout([&](abort_source* as) {
        return _group_server.server->read_barrier(as);
    }, "read_barrier", as, timeout);
}

future<bool> direct_fd_pinger::ping(direct_failure_detector::pinger::endpoint_id id, abort_source& as) {
    auto dst_id = raft::server_id{std::move(id)};
    auto addr = _address_map.find(dst_id);
    if (!addr) {
        {
            auto& rate_limit = _rate_limits.try_get_recent_entry(id, std::chrono::minutes(5));
            rslog.log(log_level::warn, rate_limit, "Raft server id {} cannot be translated to an IP address.", id);
        }
        _rate_limits.remove_least_recent_entries(std::chrono::minutes(30));
        co_return false;
    }

    try {
        auto reply = co_await ser::raft_rpc_verbs::send_direct_fd_ping(&_ms, netw::msg_addr(*addr), as, dst_id);
        if (auto* wrong_dst = std::get_if<wrong_destination>(&reply.result)) {
            // This may happen e.g. when node B is replacing node A with the same IP.
            // When we ping node A, the pings will reach node B instead.
            // B will detect they were destined for node A and return wrong_destination.
            rslog.trace("ping(id = {}, ip_addr = {}): wrong destination (reached {})",
                        dst_id, *addr, wrong_dst->reached_id);
            co_return false;
        } else if (auto* info = std::get_if<group_liveness_info>(&reply.result)) {
            co_return info->group0_alive;
        }
    } catch (seastar::rpc::closed_error&) {
        co_return false;
    }
    co_return true;
}

direct_failure_detector::clock::timepoint_t direct_fd_clock::now() noexcept {
    return base::now().time_since_epoch().count();
}

future<> direct_fd_clock::sleep_until(direct_failure_detector::clock::timepoint_t tp, abort_source& as) {
    auto t = base::time_point{base::duration{tp}};
    auto n = base::now();
    if (t <= n) {
        return make_ready_future<>();
    }

    return sleep_abortable(t - n, as);
}

} // end of namespace service
