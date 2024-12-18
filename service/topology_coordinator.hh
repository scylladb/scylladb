/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <chrono>

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "utils/log.hh"
#include "raft/raft.hh"
#include "gms/inet_address.hh"
#include "gms/feature_service.hh"
#include "service/endpoint_lifecycle_subscriber.hh"
#include "service/topology_state_machine.hh"

namespace db {
class system_keyspace;
class system_distributed_keyspace;
}

namespace gms {
class gossiper;
}

namespace netw {
class messaging_service;
}

namespace locator {
class shared_token_metadata;
}

namespace replica {
class database;
}

namespace raft {
class server;
}

namespace service {

class raft_group0;
class tablet_allocator;

extern logging::logger rtlogger;

struct wait_for_ip_timeout : public std::runtime_error {
        wait_for_ip_timeout(raft::server_id id, long timeout) :
                std::runtime_error::runtime_error(format("failed to obtain an IP for {} in {}s", id, timeout)) {}
};

// Wait for the node with provided id to appear in the gossiper
future<> wait_for_gossiper(raft::server_id id, const gms::gossiper& g, seastar::abort_source& as);

using raft_topology_cmd_handler_type = noncopyable_function<future<raft_topology_cmd_result>(
        raft::term_t, uint64_t, const raft_topology_cmd&)>;

future<> run_topology_coordinator(
        seastar::sharded<db::system_distributed_keyspace>& sys_dist_ks, gms::gossiper& gossiper,
        netw::messaging_service& messaging, locator::shared_token_metadata& shared_tm,
        db::system_keyspace& sys_ks, replica::database& db, service::raft_group0& group0,
        service::topology_state_machine& topo_sm, seastar::abort_source& as, raft::server& raft,
        raft_topology_cmd_handler_type raft_topology_cmd_handler,
        tablet_allocator& tablet_allocator,
        std::chrono::milliseconds ring_delay,
        endpoint_lifecycle_notifier& lifecycle_notifier,
        gms::feature_service& feature_service);

}
