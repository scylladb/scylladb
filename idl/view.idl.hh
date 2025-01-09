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

verb [[cancellable]] build_view_request(sstring ks_name, sstring view_name, unsigned shard, dht::token_range range) -> dht::token_range;
}
}
