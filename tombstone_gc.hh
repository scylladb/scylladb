/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include "gc_clock.hh"
#include "dht/token.hh"
#include "schema/schema_fwd.hh"
#include "interval.hh"
#include "utils/chunked_vector.hh"

namespace replica {
class database;
}

namespace dht {

class decorated_key;

using token_range = interval<token>;

}

namespace data_dictionary {

class database;

}

// The map stores the repair time for each token range in the table.
// The repair time is the time of the last "repair" operation on the table together with the token range.
//
// The map is used to determine the time when the tombstones can be safely removed from the table (for the tables with
// the "repair" tombstone GC mode).
class repair_history_map_ptr;

class per_table_history_maps {
public:
    std::unordered_map<table_id, repair_history_map_ptr> _repair_maps;

    // Separating the group0 GC time - it is not kept per table, but for the whole group0:
    // - the state_id of the last mutation applies to all group0 tables wrt. the tombstone GC
    // - we also always use the full token range for the group0 tables (so we don't need to store the token ranges)
    seastar::lw_shared_ptr<gc_clock::time_point> _group0_gc_time;
};

class tombstone_gc_options;

using gc_time_min_source = std::function<gc_clock::time_point(const table_id&)>;

struct range_repair_time {
    dht::token_range range;
    gc_clock::time_point time;
    shard_id shard;
};

class tombstone_gc_state {
    gc_time_min_source _gc_min_source;
    per_table_history_maps* _reconcile_history_maps;
    [[nodiscard]] gc_clock::time_point check_min(schema_ptr, gc_clock::time_point) const;

    [[nodiscard]] repair_history_map_ptr get_repair_history_for_table(const table_id& id) const;
    [[nodiscard]] repair_history_map_ptr get_or_create_repair_history_for_table(const table_id& id);

    [[nodiscard]] seastar::lw_shared_ptr<gc_clock::time_point> get_group0_gc_time() const;
    [[nodiscard]] seastar::lw_shared_ptr<gc_clock::time_point> get_or_create_group0_gc_time();

    [[nodiscard]] gc_clock::time_point get_gc_before_for_group0(schema_ptr s) const;

private:
    std::unordered_map<table_id, utils::chunked_vector<range_repair_time>> _pending_updates;

public:
    tombstone_gc_state() = delete;
    explicit tombstone_gc_state(per_table_history_maps* maps) noexcept : _reconcile_history_maps(maps) {}

    explicit operator bool() const noexcept {
        return _reconcile_history_maps != nullptr;
    }

    void set_gc_time_min_source(gc_time_min_source src) {
        _gc_min_source = std::move(src);
    }

    // Returns true if it's cheap to retrieve gc_before, e.g. the mode will not require accessing a system table.
    [[nodiscard]] bool cheap_to_get_gc_before(const schema& s) const noexcept;

    void drop_repair_history_for_table(const table_id& id);

    struct get_gc_before_for_range_result {
        gc_clock::time_point min_gc_before;
        gc_clock::time_point max_gc_before;
        bool knows_entire_range{};
    };

    [[nodiscard]] get_gc_before_for_range_result get_gc_before_for_range(schema_ptr s, const dht::token_range& range, const gc_clock::time_point& query_time) const;

    [[nodiscard]] gc_clock::time_point get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, const gc_clock::time_point& query_time) const;

    void update_repair_time(table_id id, const dht::token_range& range, gc_clock::time_point repair_time);
    void update_group0_refresh_time(gc_clock::time_point refresh_time);

    // returns a tombstone_gc_state copy with the commitlog check disabled (i.e.) without _gc_min_source.
    [[nodiscard]] tombstone_gc_state with_commitlog_check_disabled() const { return tombstone_gc_state(_reconcile_history_maps); }

    void insert_pending_repair_time_update(table_id id, const dht::token_range& range, gc_clock::time_point repair_time, shard_id shard);
    future<> flush_pending_repair_time_update(replica::database& db);
};

std::map<sstring, sstring> get_default_tombstonesonte_gc_mode(data_dictionary::database db, sstring ks_name);
void validate_tombstone_gc_options(const tombstone_gc_options* options, data_dictionary::database db, sstring ks_name);
