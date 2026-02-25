/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include "raft.hh"
#include <fmt/ranges.h>

namespace raft {

seastar::logger logger("raft");

size_t log_entry::get_size() const {
    struct overloaded {
        size_t operator()(const command& c) {
            return c.size();
        }
        size_t operator()(const configuration& c) {
            size_t size = 0;
            for (auto& s : c.current) {
                size += sizeof(s.addr.id);
                size += s.addr.info.size();
                size += sizeof(s.can_vote);
            }
            return size;
        }
        size_t operator()(const log_entry::dummy& d) {
            return 0;
        }
    };
    return std::visit(overloaded{}, this->data) + sizeof(*this);
}

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
