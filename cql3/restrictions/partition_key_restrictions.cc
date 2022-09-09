/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "partition_key_restrictions.hh"

namespace cql3 {
namespace restrictions {
partition_key_restrictions::partition_key_restrictions(expr::expression partition_restrictions, schema_ptr table_schema)
    : _table_schema(std::move(table_schema)), _partition_restrictions(std::move(partition_restrictions)) {
    // TODO
}
}  // namespace restrictions
}  // namespace cql3
