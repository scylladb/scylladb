/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include "raft/raft.hh"
#include "service/topology_state_machine.hh"
#include "utils/UUID.hh"
#include <seastar/core/future.hh>
#include <seastar/core/timer.hh>

namespace gms {
class gossiper;
}

namespace replica {
class database;
}

namespace raft {
class server;
}


namespace service {

class raft_group_registry;

class group0_state_id_handler {

    topology_state_machine& _topo_sm;
    replica::database& _local_db;
    gms::gossiper& _gossiper;
    lowres_clock::duration _refresh_interval;

    timer<> _timer;

    utils::UUID _state_id_last_advertised;
    utils::UUID _state_id_last_reconcile;

    static lowres_clock::duration get_refresh_interval(const replica::database& db);

    void refresh();

public:
    group0_state_id_handler(topology_state_machine& topo_sm, replica::database& local_db, gms::gossiper& gossiper);

    void run();

    future<> advertise_state_id(utils::UUID state_id);
};

} // namespace service
