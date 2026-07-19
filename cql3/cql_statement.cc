/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/cql_statement.hh"
#include "cql3/memory_usage.hh"

namespace cql3 {

size_t cql_statement::external_memory_usage() const {
    // raw_cql_statement is a chunked_string backed by managed_bytes.
    size_t s = raw_cql_statement.data().external_memory_usage();
    if (_audit_info) {
        s += sizeof(audit::audit_info);
        s += sstring_external_memory_usage(_audit_info->keyspace());
        s += sstring_external_memory_usage(_audit_info->table());
        s += sstring_external_memory_usage(_audit_info->query());
    }
    return s;
}

} // namespace cql3
