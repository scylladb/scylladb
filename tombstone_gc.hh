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
#include "schema/schema_fwd.hh"
#include "interval.hh"

namespace dht {

class decorated_key;

using token_range = interval<token>;

}

namespace data_dictionary {

class database;

}

class repair_history_map;
using per_table_history_maps = std::unordered_map<table_id, seastar::lw_shared_ptr<repair_history_map>>;

class tombstone_gc_options;

using gc_time_min_source = std::function<gc_clock::time_point(const table_id&)>;

class tombstone_gc_state {
    gc_time_min_source _gc_min_source;
    per_table_history_maps* _repair_history_maps;
    gc_clock::time_point check_min(schema_ptr, gc_clock::time_point) const;
public:
    tombstone_gc_state() = delete;
    tombstone_gc_state(per_table_history_maps* maps) noexcept : _repair_history_maps(maps) {}

    explicit operator bool() const noexcept {
        return _repair_history_maps != nullptr;
    }

    void set_gc_time_min_source(gc_time_min_source src) {
        _gc_min_source = std::move(src);
    }

    // Returns true if it's cheap to retrieve gc_before, e.g. the mode will not require accessing a system table.
    bool cheap_to_get_gc_before(const schema& s) const noexcept;

    seastar::lw_shared_ptr<repair_history_map> get_repair_history_map_for_table(const table_id& id) const;
    seastar::lw_shared_ptr<repair_history_map> get_or_create_repair_history_map_for_table(const table_id& id);
    void drop_repair_history_map_for_table(const table_id& id);

    struct get_gc_before_for_range_result {
        gc_clock::time_point min_gc_before;
        gc_clock::time_point max_gc_before;
        bool knows_entire_range;
    };

    get_gc_before_for_range_result get_gc_before_for_range(schema_ptr s, const dht::token_range& range, const gc_clock::time_point& query_time) const;

    gc_clock::time_point get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, const gc_clock::time_point& query_time) const;

    void update_repair_time(table_id id, const dht::token_range& range, gc_clock::time_point repair_time);
};

std::map<sstring, sstring> get_default_tombstonesonte_gc_mode(data_dictionary::database db, sstring ks_name);
void validate_tombstone_gc_options(const tombstone_gc_options* options, data_dictionary::database db, sstring ks_name);
