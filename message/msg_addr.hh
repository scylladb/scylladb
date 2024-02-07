/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "gms/inet_address.hh"
#include <cstdint>

namespace netw {

struct msg_addr {
    gms::inet_address addr;
    uint32_t cpu_id;
    friend bool operator==(const msg_addr& x, const msg_addr& y) noexcept;
    friend bool operator<(const msg_addr& x, const msg_addr& y) noexcept;
    struct hash {
        size_t operator()(const msg_addr& id) const noexcept;
    };
    explicit msg_addr(gms::inet_address ip) noexcept : addr(ip), cpu_id(0) { }
    explicit msg_addr(const sstring& addr) noexcept : addr(addr), cpu_id(0) { }
    msg_addr(gms::inet_address ip, uint32_t cpu) noexcept : addr(ip), cpu_id(cpu) { }
};

}

template <>
struct fmt::formatter<netw::msg_addr> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    template <typename FormatContext>
    auto format(const netw::msg_addr& addr, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}:{}", addr.addr, addr.cpu_id);
    }
};
