/*
 * Copyright 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace service {

struct group0_info {
    raft::group_id group0_id;
    raft::server_address addr;
};

struct group0_peer_exchange {
    std::variant<std::monostate, service::group0_info, std::vector<raft::server_address>> info;
};

} // namespace raft
