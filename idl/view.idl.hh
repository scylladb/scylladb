/*
 * Copyright 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "dht/i_partitioner_fwd.hh"

namespace db {
namespace view {
class update_backlog {
    size_t get_current_bytes();
    size_t get_max_bytes();
};

verb [[cancellable]] build_views_range(table_id base_id, unsigned shard, dht::token_range range, std::vector<table_id> views, raft::term_t term) -> std::vector<table_id>;
verb abort_view_building_work(unsigned shard, raft::term_t term);
verb [[cancellable]] register_staging_sstables(table_id base_id, unsigned shard, dht::token_range_vector ranges);
}
}
