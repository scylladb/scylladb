/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "seastarx.hh"
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

namespace db {
class config;
}

namespace service {

/// An interface with the vector store service.
class vector_store final {
public:
    using config = db::config;
    using host_name = sstring;
    using port_number = std::uint16_t;

private:
    host_name _host;                                 ///< The host name for the vector-store service.
    port_number _port{};                             ///< The port number for the vector-store service.

public:
    explicit vector_store(config const& cfg);
    ~vector_store();

    /// Stop the service.
    auto stop() -> future<>;

    /// Check if the vector store service is disabled.
    auto is_disabled() const -> bool {
        return _host.empty();
    }

    [[nodiscard]] auto host() const -> host_name const& {
        return _host;
    }
    [[nodiscard]] auto port() const {
        return _port;
    }
};

} // namespace service

