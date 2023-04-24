/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/authenticated_user.hh"

namespace auth {

authenticated_user::authenticated_user(std::string_view name)
        : name(sstring(name)) {
}

static const authenticated_user the_anonymous_user{};

const authenticated_user& anonymous_user() noexcept {
    return the_anonymous_user;
}

}
