/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "auth/authenticator.hh"

#include "auth/authenticated_user.hh"

const sstring auth::authenticator::USERNAME_KEY("username");
const sstring auth::authenticator::PASSWORD_KEY("password");
const sstring auth::authenticator::SERVICE_KEY("service");
const sstring auth::authenticator::REALM_KEY("realm");

future<std::optional<auth::authenticated_user>> auth::authenticator::authenticate(session_dn_func) const {
    return make_ready_future<std::optional<auth::authenticated_user>>(std::nullopt);
}
