/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "pattern_search.hh"

#include "types/types.hh"

namespace cql3::statements::pattern_search {

sstring query_pattern(const cql3::raw_value& value) {
    return value_cast<sstring>(utf8_type->deserialize(cql3::raw_value(value).to_bytes()));
}

} // namespace cql3::statements::pattern_search
