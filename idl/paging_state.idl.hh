/*
 * Copyright 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "dht/i_partitioner_fwd.hh"
#include "service/pager/paging_state.hh"

#include "idl/range.idl.hh"
#include "idl/token.idl.hh"
#include "idl/keys.idl.hh"
#include "idl/uuid.idl.hh"

namespace db {
enum class read_repair_decision : uint8_t {
  NONE,
  GLOBAL,
  DC_LOCAL,
};
}

enum class bound_weight : int8_t {
    before_all_prefixed = -1,
    equal = 0,
    after_all_prefixed = 1,
}

enum class partition_region : uint8_t {
    partition_start,
    static_row,
    clustered,
    partition_end,
};

namespace service {
namespace pager {
class paging_state {
    partition_key get_partition_key();
    std::optional<clustering_key> get_clustering_key();
    uint32_t get_remaining_low_bits();
    query_id get_query_uuid() [[version 2.2]] = query_id::create_null_id();
    std::unordered_map<dht::token_range, std::vector<locator::host_id>> get_last_replicas() [[version 2.2]] = std::unordered_map<dht::token_range, std::vector<locator::host_id>>();
    std::optional<db::read_repair_decision> get_query_read_repair_decision() [[version 2.3]] = std::nullopt;
    uint32_t get_rows_fetched_for_last_partition_low_bits() [[version 3.1]] = 0;
    uint32_t get_remaining_high_bits() [[version 4.3]] = 0;
    uint32_t get_rows_fetched_for_last_partition_high_bits() [[version 4.3]] = 0;
    bound_weight get_clustering_key_weight() [[version 5.1]] = bound_weight::equal;
    partition_region get_partition_region() [[version 5.1]] = partition_region::clustered;
};
}
}
