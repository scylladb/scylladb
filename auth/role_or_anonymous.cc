/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/role_or_anonymous.hh"

#include <iostream>

namespace auth {

std::ostream& operator<<(std::ostream& os, const role_or_anonymous& mr) {
    os << mr.name.value_or("<anonymous>");
    return os;
}

bool is_anonymous(const role_or_anonymous& mr) noexcept {
    return !mr.name.has_value();
}

}
