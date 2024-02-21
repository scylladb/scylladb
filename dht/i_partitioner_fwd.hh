/*
 * Modified by ScyllaDB
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once
#include <vector>
#include "interval.hh"

namespace sstables {

class key_view;
class decorated_key_view;

}

namespace dht {

class decorated_key;
class ring_position;
class token;
class sharder;

using partition_range = interval<ring_position>;
using token_range = interval<token>;

using partition_range_vector = std::vector<partition_range>;
using token_range_vector = std::vector<token_range>;

class decorated_key;

using decorated_key_opt = std::optional<decorated_key>;
}
