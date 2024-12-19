/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#include "service/raft/raft_group0.hh"
#include "service/topology_state_machine.hh"

namespace replica {
class database;
}

namespace service {

future<> run_view_building_coordinator(abort_source& as, replica::database& db, raft_group0& group0, db::system_keyspace& sys_ks, const topology_state_machine& topo_sm);

}
