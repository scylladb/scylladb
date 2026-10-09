/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/cql_statement.hh"
#include "cql3/memory_usage.hh"
#include "cql3/result_set.hh"

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
    if (_metadata) {
        // Built by build_cas_result_set_metadata(): only the synthetic
        // "[applied]" column (names[0]) is freshly allocated; the rest are
        // schema-owned column_specifications already accounted for elsewhere.
        s += sizeof(metadata);
        s += sizeof(metadata::column_info);
        const auto& names = _metadata->get_names();
        s += vector_external_memory_usage(names);
        if (!names.empty() && names[0]) {
            s += column_specification_external_memory_usage(*names[0]);
        }
    }
    return s;
}

} // namespace cql3
