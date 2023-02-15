/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/to_string.hh"

namespace std {

std::ostream& operator<<(std::ostream& os, const std::strong_ordering& order) {
    if (order > 0) {
        os << "gt";
    } else if (order < 0) {
        os << "lt";
    } else {
        os << "eq";
    }
    return os;
}

} // namespace std
