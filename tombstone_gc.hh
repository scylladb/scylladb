/*
 * Copyright (C) 2021-present ScyllaDB
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

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "gc_clock.hh"
#include "dht/token.hh"
#include "schema_fwd.hh"

namespace dht {

class decorated_key;

using token_range = nonwrapping_range<token>;

}

struct get_gc_before_for_range_result {
    gc_clock::time_point min_gc_before;
    gc_clock::time_point max_gc_before;
    bool knows_entire_range;
};

void drop_repair_history_map_for_table(const utils::UUID& id);

get_gc_before_for_range_result get_gc_before_for_range(schema_ptr s, const dht::token_range& range, const gc_clock::time_point& query_time);

gc_clock::time_point get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, const gc_clock::time_point& query_time);

void update_repair_time(schema_ptr s, const dht::token_range& range, gc_clock::time_point repair_time);

void validate_tombstone_gc_options(const tombstone_gc_options* options, const replica::database& db, sstring ks_name);
