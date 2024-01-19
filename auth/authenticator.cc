/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/authenticator.hh"

#include "auth/authenticated_user.hh"

const sstring auth::authenticator::USERNAME_KEY("username");
const sstring auth::authenticator::PASSWORD_KEY("password");

future<std::optional<auth::authenticated_user>> auth::authenticator::authenticate(session_dn_func) const {
    return make_ready_future<std::optional<auth::authenticated_user>>(std::nullopt);
}
