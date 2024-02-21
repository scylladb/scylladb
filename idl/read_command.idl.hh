/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "query-request.hh"

#include "idl/keys.idl.hh"
#include "idl/range.idl.hh"
#include "idl/uuid.idl.hh"

class cql_serialization_format final {
    uint8_t protocol_version();
};

namespace query {

class specific_ranges {
    partition_key pk();
    std::vector<interval<clustering_key_prefix>> ranges();
};

// COMPATIBILITY NOTE: the partition-slice for reverse queries has two different
// format:
// * legacy format
// * native format
// The wire format uses the legacy format. See docs/dev/reverse-reads.md
// for more details on the formats.
class partition_slice {
    std::vector<interval<clustering_key_prefix>> default_row_ranges();
    utils::small_vector<uint32_t, 8> static_columns;
    utils::small_vector<uint32_t, 8> regular_columns;
    query::partition_slice::option_set options;
    std::unique_ptr<query::specific_ranges> get_specific_ranges();
    cql_serialization_format cql_format();
    uint32_t partition_row_limit_low_bits() [[version 1.3]] = std::numeric_limits<uint32_t>::max();
    uint32_t partition_row_limit_high_bits() [[version 4.3]] = 0;
};

struct max_result_size {
    uint64_t soft_limit;
    uint64_t hard_limit;
    uint64_t page_size [[version 4.7]] = 0;
}

class read_command {
    table_id cf_id;
    table_schema_version schema_version;
    query::partition_slice slice;
    uint32_t row_limit_low_bits;
    std::chrono::time_point<gc_clock, gc_clock::duration> timestamp;
    std::optional<tracing::trace_info> trace_info [[version 1.3]];
    uint32_t partition_limit [[version 1.3]] = std::numeric_limits<uint32_t>::max();
    query_id query_uuid [[version 2.2]] = query_id::create_null_id();
    query::is_first_page is_first_page [[version 2.2]] = query::is_first_page::no;
    std::optional<query::max_result_size> max_result_size [[version 4.3]] = std::nullopt;
    uint32_t row_limit_high_bits [[version 4.3]] = 0;
    uint64_t tombstone_limit [[version 5.2]] = query::max_tombstones;
};

}
