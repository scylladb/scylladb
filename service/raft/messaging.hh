/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once
#include "raft/raft.hh"
#include "gms/inet_address.hh"

namespace service {

gms::inet_address raft_addr_to_inet_addr(const raft::server_info&);
gms::inet_address raft_addr_to_inet_addr(const raft::server_address&);
raft::server_info inet_addr_to_raft_addr(const gms::inet_address&);

/////////////////////////////////////////
} // end of namespace service

