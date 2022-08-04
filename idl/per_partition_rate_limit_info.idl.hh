/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/per_partition_rate_limit_info.hh"

namespace db {

namespace per_partition_rate_limit {

struct account_only {};

struct account_and_enforce {
    uint32_t random_variable;
};

// using info = std::variant<std::monostate, account_only, account_and_enforce>;

} // namespace per_partition_rate_limit

} // namespace db
