
/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include "raft_address_map.hh"

namespace gms {
class gossiper;
}

namespace service {

// Scylla-specific implementation of raft failure detector module.
//
// Currently uses gossiper as underlying implementation to test for `is_alive(gms::inet_address)`.
// Gets the mapping from server id to gms::inet_address from RPC module.
class raft_gossip_failure_detector : public raft::failure_detector {
    gms::gossiper& _gossip;
    raft_address_map<>& _address_map;

public:
    raft_gossip_failure_detector(gms::gossiper& gs, raft_address_map<>& address_map);

    bool is_alive(raft::server_id server) override;
};

} // end of namespace service
