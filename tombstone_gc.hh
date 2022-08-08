/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "gc_clock.hh"
#include "dht/token.hh"
#include "schema_fwd.hh"
#include "range.hh"
#include "interval.hh"

namespace dht {

class decorated_key;

using token_range = nonwrapping_interval<token>;

}

namespace data_dictionary {

class database;

}

class tombstone_gc_options;

struct get_gc_before_for_range_result {
    gc_clock::time_point min_gc_before;
    gc_clock::time_point max_gc_before;
    bool knows_entire_range;
};

void drop_repair_history_map_for_table(const table_id& id);

get_gc_before_for_range_result get_gc_before_for_range(schema_ptr s, const dht::token_range& range, const gc_clock::time_point& query_time);

gc_clock::time_point get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, const gc_clock::time_point& query_time);

void update_repair_time(table_id id, const dht::token_range& range, gc_clock::time_point repair_time);

void validate_tombstone_gc_options(const tombstone_gc_options* options, data_dictionary::database db, sstring ks_name);
