/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "auth/maintenance_socket_authenticator.hh"

#include <string_view>

#include "auth/authenticated_user.hh"
#include "auth/common.hh"
#include "service/migration_manager.hh"
#include "utils/class_registrator.hh"
#include "cql3/query_processor.hh"

namespace auth {

constexpr std::string_view maintenance_socket_authenticator_name("org.apache.cassandra.auth.MaintenanceSocketAuthenticator");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authenticator,
        maintenance_socket_authenticator,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&,
        utils::alien_worker&> maintenance_socket_auth_reg("org.apache.cassandra.auth.MaintenanceSocketAuthenticator");

maintenance_socket_authenticator::~maintenance_socket_authenticator() {
}

future<> maintenance_socket_authenticator::start() {
    return once_among_shards([] {
        return make_ready_future<>();
    });
}

bool maintenance_socket_authenticator::require_authentication() const {
    return false;
}

future<authenticated_user> maintenance_socket_authenticator::authenticate(const credentials_map& credentials) const {
    return make_ready_future<authenticated_user>(anonymous_user());
}

} // namespace auth
