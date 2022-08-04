/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/UUID.hh"

namespace utils {
class UUID final {
    int64_t get_most_significant_bits();
    int64_t get_least_significant_bits();
};
}
