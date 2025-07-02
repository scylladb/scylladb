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
#include <expected>

namespace db {
class config;
}

namespace service {

/// A client with the vector-store service.
class vector_store_client final {
    struct impl;
    std::unique_ptr<impl> _impl;

public:
    using config = db::config;
    using host_name = sstring;
    using port_number = std::uint16_t;

    /// The vector_store_client service is disabled.
    struct disabled {};

    explicit vector_store_client(config const& cfg);
    ~vector_store_client();

    /// Start background tasks.
    void start_background_tasks();

    /// Stop the service.
    auto stop() -> future<>;

    /// Check if the vector_store_client is disabled.
    auto is_disabled() const {
        return !bool{_impl};
    }

    /// Get the current host name.
    [[nodiscard]] auto host() const -> std::expected<host_name, disabled>;

    /// Get the current port number.
    [[nodiscard]] auto port() const -> std::expected<port_number, disabled>;

private:
    friend struct vector_store_client_tester;
};

/// A tester for the vector_store_client, used for testing purposes.
struct vector_store_client_tester {};

} // namespace service

