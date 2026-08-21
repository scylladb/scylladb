/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "auth/default_authorizer.hh"
#include "auth/permission.hh"

namespace auth {

// maintenance_socket_authorizer is used for clients connecting to the
// maintenance socket. It grants all permissions unconditionally (like
// AllowAllAuthorizer) while still supporting grant/revoke operations
// (delegated to the underlying CassandraAuthorizer / default_authorizer).
class maintenance_socket_authorizer : public default_authorizer {
public:
    using default_authorizer::default_authorizer;

    ~maintenance_socket_authorizer() override = default;

    future<> start() override {
        return make_ready_future<>();
    }

    future<permission_set> authorize(const role_or_anonymous&, const resource&) const override {
        return make_ready_future<permission_set>(permissions::ALL);
    }
};

} // namespace auth
