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

verb [[cancellable]] build_views_request(table_id base_id, unsigned shard, dht::token_range range, std::vector<table_id> views);
verb [[one_way]] abort_vbc_work();
}
}
