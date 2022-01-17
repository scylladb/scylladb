/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "service/raft/raft_gossip_failure_detector.hh"
#include "gms/gossiper.hh"

namespace service {

raft_gossip_failure_detector::raft_gossip_failure_detector(gms::gossiper& gs, raft_address_map<>& address_map)
    : _gossip(gs), _address_map(address_map)
{}

bool raft_gossip_failure_detector::is_alive(raft::server_id server) {
    return _gossip.is_alive(_address_map.get_inet_address(server));
}

} // end of namespace service
