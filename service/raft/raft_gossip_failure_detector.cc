/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "service/raft/raft_gossip_failure_detector.hh"
#include "service/raft/raft_services.hh"
#include "gms/gossiper.hh"

raft_gossip_failure_detector::raft_gossip_failure_detector(gms::gossiper& gs, raft_services& raft_srvs)
    : _gossip(gs), _raft_services(raft_srvs)
{}

bool raft_gossip_failure_detector::is_alive(raft::server_id server) {
    return _gossip.is_alive(_raft_services.get_inet_address(server));
}