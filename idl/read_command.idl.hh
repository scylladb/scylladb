/*
 * Copyright 2016 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

class cql_serialization_format final {
    uint8_t protocol_version();
};

namespace query {

class specific_ranges {
    partition_key pk();
    std::vector<nonwrapping_range<clustering_key_prefix>> ranges();
};

class partition_slice {
    std::vector<nonwrapping_range<clustering_key_prefix>> default_row_ranges();
    std::vector<uint32_t> static_columns;
    std::vector<uint32_t> regular_columns;
    query::partition_slice::option_set options;
    std::unique_ptr<query::specific_ranges> get_specific_ranges();
    cql_serialization_format cql_format();
    uint32_t partition_row_limit() [[version 1.3]] = std::numeric_limits<uint32_t>::max();
};

class read_command {
    utils::UUID cf_id;
    utils::UUID schema_version;
    query::partition_slice slice;
    uint32_t row_limit;
    std::chrono::time_point<gc_clock, gc_clock::duration> timestamp;
    std::experimental::optional<tracing::trace_info> trace_info [[version 1.3]];
    uint32_t partition_limit [[version 1.3]] = std::numeric_limits<uint32_t>::max();
    utils::UUID query_uuid [[version 2.2]] = utils::UUID();
    bool is_first_page [[version 2.2]] = false;
};

}
