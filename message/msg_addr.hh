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
    friend std::ostream& operator<<(std::ostream& os, const msg_addr& x);
    struct hash {
        size_t operator()(const msg_addr& id) const noexcept;
    };
    explicit msg_addr(gms::inet_address ip) noexcept : addr(ip), cpu_id(0) { }
    msg_addr(gms::inet_address ip, uint32_t cpu) noexcept : addr(ip), cpu_id(cpu) { }
};

std::ostream& operator<<(std::ostream& os, const netw::msg_addr& x);

} // namespace netw

namespace fmt {

template <>
struct formatter<netw::msg_addr> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const netw::msg_addr& id, FormatContext& ctx) const {
        return format_to(ctx.out(), "{}#{}", id.addr, id.cpu_id);
    }
};

} // namespace fmt
