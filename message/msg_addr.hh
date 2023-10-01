/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "gms/inet_address.hh"
#include <cstdint>
#include <fmt/ostream.h>

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

}

template <> struct fmt::formatter<netw::msg_addr> : fmt::ostream_formatter {};
