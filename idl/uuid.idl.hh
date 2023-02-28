/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/UUID.hh"
#include "schema/schema_fwd.hh"
#include "query_id.hh"
#include "locator/host_id.hh"
#include "tasks/types.hh"

namespace utils {
class UUID final {
    int64_t get_most_significant_bits();
    int64_t get_least_significant_bits();
};
}

class tasks::task_id final {
    utils::UUID uuid();
};

class table_id final {
    utils::UUID uuid();
};

class table_schema_version final {
    utils::UUID uuid();
};

class query_id final {
    utils::UUID uuid();
};

namespace locator {

class host_id final {
    utils::UUID uuid();
};

} // namespace locator

