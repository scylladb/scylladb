/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "group0_voter_registry.hh"

#include "seastar/util/log.hh"

namespace service {

seastar::logger rvlogger("group0_voter_registry");

future<> group0_voter_registry::insert_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    std::unordered_set<raft::server_id> new_voters;
    new_voters.reserve(nodes.size());

    for (const auto& node : nodes) {
        if (new_voters.size() >= _max_voters) {
            rvlogger.debug("Reached the maximum number of voters: {}", _max_voters);
            break;
        }
#if 0
        const auto& server_info = _server_info_accessor.find(node);
#endif

        new_voters.insert(node);
    }

    return _voter_client.set_voters_status(new_voters, can_vote::yes, as);
}

future<> group0_voter_registry::remove_nodes(const std::unordered_set<raft::server_id>& nodes, abort_source& as) {
    return _voter_client.set_voters_status(nodes, can_vote::no, as);
}

} // namespace service
