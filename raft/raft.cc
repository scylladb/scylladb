/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include "raft.hh"
#include <fmt/ranges.h>

namespace raft {

seastar::logger logger("raft");

} // end of namespace raft

auto fmt::formatter<raft::server_address>::format(const raft::server_address& addr,
                                                fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{.id={}}}", addr.id);
}

auto fmt::formatter<raft::config_member>::format(const raft::config_member& s,
                                                fmt::format_context& ctx) const -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{.id={} .can_vote={}}}", s.addr.id, s.can_vote);
}

auto fmt::formatter<raft::configuration>::format(const raft::configuration& cfg,
                                                fmt::format_context& ctx) const -> decltype(ctx.out()) {
    if (cfg.previous.empty()) {
        return fmt::format_to(ctx.out(), "{}", cfg.current);
    }
    return fmt::format_to(ctx.out(), "{}->{}", cfg.previous, cfg.current);
}
