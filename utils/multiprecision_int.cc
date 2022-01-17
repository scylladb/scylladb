/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "multiprecision_int.hh"
#include <iostream>

namespace utils {

std::string multiprecision_int::str() const {
    return _v.str();
}

std::ostream& operator<<(std::ostream& os, const multiprecision_int& x) {
    return os << x._v;
}

}

