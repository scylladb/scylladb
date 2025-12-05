/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/shared_future.hh>

#include "db/consistency_level_type.hh"
#include "auth/authenticator.hh"
#include "auth/passwords.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/alien_worker.hh"

#include "password_authenticator.hh"

namespace auth {

extern const std::string_view maintenance_socket_authenticator_name;

class maintenance_socket_authenticator : public password_authenticator {
public:
    using password_authenticator::password_authenticator;

    virtual ~maintenance_socket_authenticator();

    virtual future<> start() override;

    bool require_authentication() const override;

    future<authenticated_user> authenticate(const credentials_map& credentials) const override;
};

} // namespace auth

