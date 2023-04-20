/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "gms/inet_address.hh"
#include <cstdint>
#include "locator/host_id.hh"

namespace netw {

struct msg_addr {
    gms::inet_address addr;
    uint32_t cpu_id;
    locator::host_id host_id;

    explicit msg_addr(gms::inet_address ip) noexcept
        : addr(ip)
        , cpu_id(0)
        , host_id(locator::host_id::create_null_id())
    {}

    msg_addr(gms::inet_address ip, uint32_t cpu, locator::host_id host_id = locator::host_id::create_null_id()) noexcept
        : addr(ip)
        , cpu_id(cpu)
        , host_id(host_id)
    {}

    msg_addr(gms::inet_address ip, locator::host_id host_id) noexcept : msg_addr(ip, 0, std::move(host_id)) {}

    bool operator==(const msg_addr& x) const noexcept {
        // Ignore cpu id for now since we do not really support shard to shard connections
        return addr == x.addr && host_id == x.host_id;
    }

    auto operator<=>(const msg_addr& x) const noexcept {
        // Ignore cpu id for now since we do not really support shard to shard connections
        auto ret = addr <=> x.addr;
        if (ret != 0) {
            return ret;
        }
        return host_id <=> x.host_id;
    }

    friend std::ostream& operator<<(std::ostream& os, const msg_addr& x);

    struct legacy_equal_to {
        bool operator()(const msg_addr& x, const msg_addr& y) const noexcept;
    };
};

std::ostream& operator<<(std::ostream& os, const netw::msg_addr& x);

} // namespace netw

namespace fmt {

template <>
struct formatter<netw::msg_addr> : formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const netw::msg_addr& id, FormatContext& ctx) const {
        return format_to(ctx.out(), "{}/{}#{}", id.host_id, id.addr, id.cpu_id);
    }
};

} // namespace fmt

namespace std {

template <>
struct hash<netw::msg_addr> {
    size_t operator()(const netw::msg_addr& id) const noexcept {
        // Ignore cpu id for now since we do not really support // shard to shard connections
        // Ignore host_id for hashing since msg_addr with same address but different host_id should be rare.
        return std::hash<bytes_view>()(id.addr.bytes());
    }
};

} // namespace std
