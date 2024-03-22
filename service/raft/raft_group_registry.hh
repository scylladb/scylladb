/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/gate.hh>

#include "message/messaging_service_fwd.hh"
#include "raft/raft.hh"
#include "raft/server.hh"
#include "utils/recent_entries_map.hh"
#include "direct_failure_detector/failure_detector.hh"
#include "service/raft/group0_fwd.hh"

namespace db {
class system_keyspace;
}

namespace service {

class raft_rpc;
class raft_sys_table_storage;
using raft_ticker_type = seastar::timer<lowres_clock>;

struct raft_group_not_found: public raft::error {
    raft::group_id gid;
    raft_group_not_found(raft::group_id gid_arg)
            : raft::error(format("Raft group {} not found", gid_arg)), gid(gid_arg)
    {}
};

struct raft_destination_id_not_correct: public raft::error {
    raft_destination_id_not_correct(raft::server_id my_id, raft::server_id dst_id)
            : raft::error(format("Got message for server {}, but my id is {}", dst_id, my_id))
    {}
};

class raft_group_registry;

// An entry in the group registry
struct raft_server_for_group {
    raft::group_id gid;
    std::unique_ptr<raft::server> server;
    std::unique_ptr<raft_ticker_type> ticker;
    raft_rpc& rpc;
    raft_sys_table_storage& persistence;
    std::optional<seastar::future<>> aborted;
    std::optional<lowres_clock::duration> default_op_timeout;
};

struct raft_timeout {
    seastar::compat::source_location loc = seastar::compat::source_location::current();
    std::optional<lowres_clock::time_point> value;
};

class raft_operation_timeout_error : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

// A wrapper for raft::server that adds timeout support to the main raft methods.
// If an operation doesn't finish in a specified amount of time, raft_operation_timeout_error
// exception is thrown.
// An instance can be obtained through raft_group_registry methods get_server_with_timeouts
// and group0_with_timeouts.
// Passing std::nullopt as a raft_timeout parameter to methods of this class is equivalent
// to calling the original raft methods without timeout.
// Passing raft_timeout{} means 'use default timeout for this group', which is taken
// from raft_server_for_group::default_op_timeout. For group0 default_op_timeout
// is set to 1 minute and can be overridden in tests through the group0-raft-op-timeout-in-ms injection.
// A custom timeout can be passed via raft_timeout::value, it will take precedence over
// the default timeout default_op_timeout.
// If no default_op_timeout is configured for a group and raft_timeout{} is passed without
// the value parameter set, on_internal_error will be triggered.
//
// Use methods of these class if the code is handling a client request
// and the client expects a potential error. Don't use timeouts for background
// fibers, such as topology coordinator, since they wouldn't add much value.
// The only thing the background fiber can do with a timeout is to retry, and
// this will have the same end effect as not having a timeout at all.
class raft_server_with_timeouts {
    raft_server_for_group& _group_server;
    raft_group_registry& _registry;

    template <std::invocable<abort_source*> Op>
    std::invoke_result_t<Op, abort_source*>
    run_with_timeout(Op&& op, const char* op_name, seastar::abort_source* as, std::optional<raft_timeout> timeout);
public:
    raft_server_with_timeouts(raft_server_for_group& group_server, raft_group_registry& registry);
    future<> add_entry(raft::command command, raft::wait_type type, seastar::abort_source* as, std::optional<raft_timeout> timeout);
    future<> modify_config(std::vector<raft::config_member> add, std::vector<raft::server_id> del, seastar::abort_source* as, std::optional<raft_timeout> timeout);
    future<> read_barrier(seastar::abort_source* as, std::optional<raft_timeout> timeout);
};

class direct_fd_pinger;
class direct_fd_proxy;

// This class is responsible for creating, storing and accessing raft servers.
// It also manages the raft rpc verbs initialization.
//
// `peering_sharded_service` inheritance is used to forward requests
// to the owning shard for a given raft group_id.
class raft_group_registry : public seastar::peering_sharded_service<raft_group_registry> {
private:
    netw::messaging_service& _ms;
    // Raft servers along with the corresponding timers to tick each instance.
    // Currently ticking every 100ms.
    std::unordered_map<raft::group_id, raft_server_for_group> _servers;
    // inet_address:es for remote raft servers known to us
    raft_address_map& _address_map;

    direct_failure_detector::failure_detector& _direct_fd;
    // Listens to notifications from direct failure detector.
    // Implements the `raft::failure_detector` interface. Used by all raft groups to check server liveness.
    seastar::shared_ptr<direct_fd_proxy> _direct_fd_proxy;
    // Direct failure detector listener subscription for `_direct_fd_proxy`.
    std::optional<direct_failure_detector::subscription> _direct_fd_subscription;

    void init_rpc_verbs();
    seastar::future<> uninit_rpc_verbs();
    seastar::future<> stop_servers() noexcept;

    raft_server_for_group& server_for_group(raft::group_id id);

    // Group 0 id, valid only on shard 0 after boot/upgrade is over
    std::optional<raft::group_id> _group0_id;

    // My Raft ID. Shared between different Raft groups.
    raft::server_id _my_id;

public:
    raft_group_registry(raft::server_id my_id, raft_address_map&, netw::messaging_service& ms,
            direct_failure_detector::failure_detector& fd);
    ~raft_group_registry();

    // Called manually at start on every shard.
    seastar::future<> start();
    // Called by sharded<>::stop()
    seastar::future<> stop();

    // Stop the server for the given group and remove it from the registry.
    // It differs from abort_server in that it waits for the server to stop
    // and removes it from the registry.
    seastar::future<> stop_server(raft::group_id gid, sstring reason);

    // Must not be called before `start`.
    const raft::server_id& get_my_raft_id();

    // Called by before stopping the database.
    // May be called multiple times.
    seastar::future<> drain_on_shutdown() noexcept;

    raft_rpc& get_rpc(raft::group_id gid);

    // Find server for group by group id. Throws exception if
    // there is no such group.
    raft::server& get_server(raft::group_id gid);

    // Returns a server with timeouts support for group by group id.
    // Throws exception if there is no such group.
    raft_server_with_timeouts get_server_with_timeouts(raft::group_id gid);

    // Find server for the given group.
    // Returns `nullptr` if there is no such group.
    raft::server* find_server(raft::group_id);

    // Returns the list of all Raft groups on this shard by their IDs.
    std::vector<raft::group_id> all_groups() const;

    // Return an instance of group 0. Valid only on shard 0,
    // after boot/upgrade is complete
    raft::server& group0();

    // Return an instance of group 0 server with timeouts support. Valid only on shard 0,
    // after boot/upgrade is complete
    raft_server_with_timeouts group0_with_timeouts();

    // Start raft server instance, store in the map of raft servers and
    // arm the associated timer to tick the server.
    future<> start_server_for_group(raft_server_for_group grp);
    void abort_server(raft::group_id gid, sstring reason = "");
    unsigned shard_for_group(const raft::group_id& gid) const;
    shared_ptr<raft::failure_detector> failure_detector();
    raft_address_map& address_map() { return _address_map; }
    direct_failure_detector::failure_detector& direct_fd() { return _direct_fd; }
};

// Implementation of `direct_failure_detector::pinger` which uses DIRECT_FD_PING verb for pinging.
// Translates `raft::server_id`s to `gms::inet_address`es before pinging.
class direct_fd_pinger : public seastar::peering_sharded_service<direct_fd_pinger>, public direct_failure_detector::pinger {
    netw::messaging_service& _ms;
    raft_address_map& _address_map;

    using rate_limits = utils::recent_entries_map<direct_failure_detector::pinger::endpoint_id, logger::rate_limit>;
    rate_limits _rate_limits;

public:
    direct_fd_pinger(netw::messaging_service& ms, raft_address_map& address_map)
            : _ms(ms), _address_map(address_map) {}

    direct_fd_pinger(const direct_fd_pinger&) = delete;
    direct_fd_pinger(direct_fd_pinger&&) = delete;

    future<bool> ping(direct_failure_detector::pinger::endpoint_id id, abort_source& as) override;
};

// XXX: find a better place to put this?
struct direct_fd_clock : public direct_failure_detector::clock {
    using base = std::chrono::steady_clock;

    direct_failure_detector::clock::timepoint_t now() noexcept override;
    future<> sleep_until(direct_failure_detector::clock::timepoint_t tp, abort_source& as) override;
};

} // end of namespace service
