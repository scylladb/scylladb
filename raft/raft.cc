/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "raft.hh"
#include "to_string.hh"

namespace raft {

seastar::logger logger("raft");

std::ostream& operator<<(std::ostream& os, const raft::server_address& addr) {
    return os << format("{{.id={}}}", addr.id);
}

std::ostream& operator<<(std::ostream& os, const raft::config_member& s) {
    return os << format("{{.id={} .can_vote={}}}", s.addr.id, s.can_vote);
}

std::ostream& operator<<(std::ostream& os, const raft::configuration& cfg) {
    if (cfg.previous.empty()) {
        return os << cfg.current;
    }
    return os << format("{}->{}", cfg.previous, cfg.current);
}

} // end of namespace raft
