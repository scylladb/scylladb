/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "dht/i_partitioner_fwd.hh"

namespace compaction {

class table_state;
class strategy_control;
struct compaction_state;

using owned_ranges_ptr = seastar::lw_shared_ptr<const dht::token_range_vector>;

} // namespace compaction
