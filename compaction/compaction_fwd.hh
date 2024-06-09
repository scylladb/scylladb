/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "dht/i_partitioner.hh"

namespace compaction {

class table_state;
class strategy_control;
struct compaction_state;

using owned_ranges_ptr = lw_shared_ptr<const dht::token_range_vector>;

inline owned_ranges_ptr make_owned_ranges_ptr(dht::token_range_vector&& ranges) {
    return make_lw_shared<const dht::token_range_vector>(std::move(ranges));
}

} // namespace compaction
