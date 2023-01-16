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
    friend bool operator==(const msg_addr& x, const msg_addr& y) noexcept;
    friend bool operator<(const msg_addr& x, const msg_addr& y) noexcept;
    msg_addr(gms::inet_address ip, uint32_t cpu = 0, std::optional<locator::host_id> opt_id = std::nullopt) noexcept
        : addr(ip)
        , cpu_id(cpu)
        , host_id(opt_id.value_or(locator::host_id::create_null_id()))
    { }
};

} // namespace netw

namespace std {

template<>
struct hash<netw::msg_addr> {
    size_t operator()(const netw::msg_addr& id) const noexcept {
        return std::hash<bytes_view>()(id.addr.bytes());
    }
};

inline std::ostream& operator<<(std::ostream& os, const netw::msg_addr& id) {
    return os << id.host_id << '/' << id.addr << ':' << id.cpu_id;
}

} // namespace std
