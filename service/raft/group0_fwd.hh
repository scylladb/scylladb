/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <iosfwd>
#include "raft/raft.hh"
#include "gms/inet_address.hh"

namespace service {

// Address of a discovery peer
struct discovery_peer {
    raft::server_id id;
    gms::inet_address ip_addr;
    discovery_peer(raft::server_id id_, gms::inet_address ip_addr_)
        : id(id_)
        , ip_addr(ip_addr_)
    {}
};

// Used in a bootstrapped Scylla cluster, provides group  0
// identifier and the current group leader address.
struct group0_info {
    raft::group_id group0_id;
    raft::server_id id;
    gms::inet_address ip_addr;
    bool operator==(const group0_info& rhs) const {
        return rhs.group0_id == group0_id && rhs.id == id && rhs.ip_addr == ip_addr;
    }
};

// If the peer has no cluster discovery running, it returns
// std::monostate, which means the caller needs to retry
// contacting this server after a pause. Otherwise it returns
// its leader data or a list of peers.
struct group0_peer_exchange {
    std::variant<std::monostate, group0_info, std::vector<discovery_peer>> info;
};

struct wrong_destination {
    raft::server_id reached_id;
};

struct group_liveness_info {
    bool group0_alive;
};

struct direct_fd_ping_reply {
    std::variant<std::monostate, wrong_destination, group_liveness_info> result;
};

using raft_ticker_type = seastar::timer<lowres_clock>;
// TODO: should be configurable.
static constexpr raft_ticker_type::duration raft_tick_interval = std::chrono::milliseconds(100);

} // namespace service