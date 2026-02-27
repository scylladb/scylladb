/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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

