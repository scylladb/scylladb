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
#include "cql3/query_processor.hh"

namespace auth {

maintenance_socket_authenticator::~maintenance_socket_authenticator() {
}

future<> maintenance_socket_authenticator::start() {
    return make_ready_future<>();
}

future<> maintenance_socket_authenticator::ensure_superuser_is_created() const {
    return make_ready_future<>();
}

bool maintenance_socket_authenticator::require_authentication() const {
    return true;
}

future<authenticated_user> maintenance_socket_authenticator::authenticate(const credentials_map& credentials) const {
    return make_ready_future<authenticated_user>(maintenance_user());
}

future<std::optional<authenticated_user>> maintenance_socket_authenticator::authenticate(session_dn_func fn) const {
    return make_ready_future<std::optional<authenticated_user>>(maintenance_user());
}

} // namespace auth
