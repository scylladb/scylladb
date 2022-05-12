/*
 * Copyright (C) 2017-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/role_name.hh"

#include <algorithm>

namespace cql3 {

role_name::role_name(sstring name, preserve_role_case p) : _name(std::move(name)) {
    if (p == preserve_role_case::no) {
        std::transform(_name.begin(), _name.end(), _name.begin(), &::tolower);
    }
}

std::ostream& operator<<(std::ostream& os, const role_name& r) {
    os << r.to_string();
    return os;
}

}
