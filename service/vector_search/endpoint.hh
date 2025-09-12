/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <cstdint>

namespace service::vector_search {

struct endpoint {
    seastar::sstring host;
    std::uint16_t port;
    seastar::net::inet_address ip;
};

} // namespace service::vector_search
