/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <iostream>

#include <fmt/format.h>

#include "locator/host_id.hh"
#include "gms/inet_address.hh"
#include "message/msg_addr.hh"

namespace gms {

struct endpoint_id {
    locator::host_id host_id;
    gms::inet_address addr;

    endpoint_id() noexcept = default;
    endpoint_id(const locator::host_id& host_id, const gms::inet_address& addr) noexcept : host_id(host_id), addr(addr) {}

    bool operator==(const endpoint_id&) const = default;
    auto operator<=>(const endpoint_id& x) const = default;

    explicit operator netw::msg_addr() const noexcept {
        return netw::msg_addr(addr);
    }
};

} // namespace gms

namespace fmt {
template <>
struct formatter<gms::endpoint_id> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const gms::endpoint_id& epid, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{}/{}", epid.host_id, epid.addr);
    }
};

} // namespace fmt

namespace std {

template<>
struct hash<gms::endpoint_id> {
    size_t operator()(const gms::endpoint_id& id) const {
        return std::hash<locator::host_id>()(id.host_id) ^ std::hash<gms::inet_address>()(id.addr);
    }
};

inline std::ostream& operator<<(std::ostream& os, const gms::endpoint_id& epid) {
    fmt::print(os, "{}", epid);
    return os;
}

} // namespace std
