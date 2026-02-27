/*
 * Copyright (C) 2026-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/shared_future.hh>

#include "password_authenticator.hh"

namespace auth {

// maintenance_socket_authenticator is used for clients connecting to the
// maintenance socket. It does not require authentication,
// while still allowing the managing of roles and their credentials.
class maintenance_socket_authenticator : public password_authenticator {
public:
    using password_authenticator::password_authenticator;

    virtual ~maintenance_socket_authenticator();

    virtual future<> start() override;

    virtual future<> ensure_superuser_is_created() const override;

    bool require_authentication() const override;
};

} // namespace auth

