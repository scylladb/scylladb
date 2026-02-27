/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "auth/maintenance_socket_authenticator.hh"


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
    return false;
}

} // namespace auth
