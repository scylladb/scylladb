/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "idl/raft_storage.idl.hh"
#include "gms/inet_address_serializer.hh"

namespace service {

struct discovery_peer {
    raft::server_id id;
    gms::inet_address ip_addr;
};

struct group0_info {
    raft::group_id group0_id;
    raft::server_id id;
    gms::inet_address ip_addr;
};

struct group0_peer_exchange {
    std::variant<std::monostate, service::group0_info, std::vector<service::discovery_peer>> info;
};

verb [[with_client_info, with_timeout, ip]] group0_peer_exchange (std::vector<service::discovery_peer> peers) -> service::group0_peer_exchange;
verb [[with_client_info, with_timeout]] group0_modify_config (raft::group_id gid, std::vector<raft::config_member> add, std::vector<raft::server_id> del);

} // namespace raft
