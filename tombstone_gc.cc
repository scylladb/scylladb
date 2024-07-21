/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <chrono>
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_map.hpp>
#include "schema/schema.hh"
#include "gc_clock.hh"
#include "tombstone_gc.hh"
#include "locator/token_metadata.hh"
#include "exceptions/exceptions.hh"
#include "locator/abstract_replication_strategy.hh"
#include "replica/database.hh"
#include "compaction/compaction_manager.hh"
#include "data_dictionary/data_dictionary.hh"
#include "gms/feature_service.hh"

extern logging::logger dblog;

seastar::lw_shared_ptr<repair_history_map> tombstone_gc_state::get_or_create_repair_history_map_for_table(const table_id& id) {
    if (!_repair_history_maps) {
        return {};
    }
    auto& repair_history_maps = *_repair_history_maps;
    auto it = repair_history_maps.find(id);
    if (it != repair_history_maps.end()) {
        return it->second;
    } else {
        repair_history_maps[id] = seastar::make_lw_shared<repair_history_map>();
        return repair_history_maps[id];
    }
}

seastar::lw_shared_ptr<repair_history_map> tombstone_gc_state::get_repair_history_map_for_table(const table_id& id) const {
    if (!_repair_history_maps) {
        return {};
    }
    auto& repair_history_maps = *_repair_history_maps;
    auto it = repair_history_maps.find(id);
    if (it != repair_history_maps.end()) {
        return it->second;
    } else {
        return {};
    }
}

void tombstone_gc_state::drop_repair_history_map_for_table(const table_id& id) {
    _repair_history_maps->erase(id);
}

// This is useful for a sstable to query a gc_before for a range. The range is
// defined by the first and last key in the sstable.
//
// The min_gc_before and max_gc_before returned are the min and max gc_before for all the keys in the range.
//
// The knows_entire_range is set to true:
// 1) if the tombstone_gc_mode is not repair, since we have the same value for all the keys in the ranges.
// 2) if the tombstone_gc_mode is repair, and the range is a sub range of a range in the repair history map.
tombstone_gc_state::get_gc_before_for_range_result tombstone_gc_state::get_gc_before_for_range(schema_ptr s, const dht::token_range& range, const gc_clock::time_point& query_time) const {
    bool knows_entire_range = true;
    const auto& options = s->tombstone_gc_options();
    switch (options.mode()) {
    case tombstone_gc_mode::timeout: {
        dblog.trace("Get gc_before for ks={}, table={}, range={}, mode=timeout", s->ks_name(), s->cf_name(), range);
        auto gc_before = check_min(s, saturating_subtract(query_time, s->gc_grace_seconds()));
        return {gc_before, gc_before, knows_entire_range};
    }
    case tombstone_gc_mode::disabled: {
        dblog.trace("Get gc_before for ks={}, table={}, range={}, mode=disabled", s->ks_name(), s->cf_name(), range);
        return {gc_clock::time_point::min(), gc_clock::time_point::min(), knows_entire_range};
    }
    case tombstone_gc_mode::immediate: {
        dblog.trace("Get gc_before for ks={}, table={}, range={}, mode=immediate", s->ks_name(), s->cf_name(), range);
        auto t = check_min(s, query_time);
        return {t, t, knows_entire_range};
    }
    case tombstone_gc_mode::repair: {
        const std::chrono::seconds& propagation_delay = options.propagation_delay_in_seconds();
        auto min_gc_before = gc_clock::time_point::min();
        auto max_gc_before = gc_clock::time_point::min();
        auto min_repair_timestamp = gc_clock::time_point::min();
        auto max_repair_timestamp = gc_clock::time_point::min();
        int hits = 0;
        knows_entire_range = false;
        auto m = get_repair_history_map_for_table(s->id());
        if (m) {
            auto interval = locator::token_metadata::range_to_interval(range);
            auto min = gc_clock::time_point::max();
            auto max = gc_clock::time_point::min();
            bool contains_all = false;
            for (auto& x : boost::make_iterator_range(m->map.equal_range(interval))) {
                auto r = locator::token_metadata::interval_to_range(x.first);
                min = std::min(x.second, min);
                max = std::max(x.second, max);
                if (++hits == 1 && r.contains(range, dht::token_comparator{})) {
                    contains_all = true;
                }
            }
            if (hits == 0) {
                min_repair_timestamp = gc_clock::time_point::min();
                max_repair_timestamp = gc_clock::time_point::min();
            } else {
                knows_entire_range = hits == 1 && contains_all;
                min_repair_timestamp = min;
                max_repair_timestamp = max;
            }
            min_gc_before = check_min(s, saturating_subtract(min_repair_timestamp, propagation_delay));
            max_gc_before = check_min(s, saturating_subtract(max_repair_timestamp, propagation_delay));
        };
        dblog.trace("Get gc_before for ks={}, table={}, range={}, mode=repair, min_repair_timestamp={}, max_repair_timestamp={}, propagation_delay={}, min_gc_before={}, max_gc_before={}, hits={}, knows_entire_range={}",
                s->ks_name(), s->cf_name(), range, min_repair_timestamp, max_repair_timestamp, propagation_delay.count(), min_gc_before, max_gc_before, hits, knows_entire_range);
        return {min_gc_before, max_gc_before, knows_entire_range};
    }
    }
    std::abort();
}

bool tombstone_gc_state::cheap_to_get_gc_before(const schema& s) const noexcept {
    return s.tombstone_gc_options().mode() != tombstone_gc_mode::repair;
}

gc_clock::time_point tombstone_gc_state::check_min(schema_ptr s, gc_clock::time_point t) const {
    if (_gc_min_source && t != gc_clock::time_point::min()) {
        return std::min(t, _gc_min_source(s->id()));
    }
    return t;
}

gc_clock::time_point tombstone_gc_state::get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, const gc_clock::time_point& query_time) const {
    // if mode = timeout    // default option, if user does not specify tombstone_gc options
    // if mode = disabled   // never gc tombstone
    // if mode = immediate  // can gc tombstone immediately
    // if mode = repair     // gc after repair
    const auto& options = s->tombstone_gc_options();
    switch (options.mode()) {
    case tombstone_gc_mode::timeout:
        dblog.trace("Get gc_before for ks={}, table={}, dk={}, mode=timeout", s->ks_name(), s->cf_name(), dk);
        return check_min(s, saturating_subtract(query_time, s->gc_grace_seconds()));
    case tombstone_gc_mode::disabled:
        dblog.trace("Get gc_before for ks={}, table={}, dk={}, mode=disabled", s->ks_name(), s->cf_name(), dk);
        return gc_clock::time_point::min();
    case tombstone_gc_mode::immediate:
        dblog.trace("Get gc_before for ks={}, table={}, dk={}, mode=immediate", s->ks_name(), s->cf_name(), dk);
        return check_min(s, query_time);
    case tombstone_gc_mode::repair:
        const std::chrono::seconds& propagation_delay = options.propagation_delay_in_seconds();
        auto gc_before = gc_clock::time_point::min();
        auto repair_timestamp = gc_clock::time_point::min();
        auto m = get_repair_history_map_for_table(s->id());
        if (m) {
            const auto it = m->map.find(dk.token());
            if (it == m->map.end()) {
                gc_before = gc_clock::time_point::min();
            } else {
                repair_timestamp = it->second;
                gc_before = saturating_subtract(repair_timestamp, propagation_delay);
            }
        }
        gc_before = check_min(s, gc_before);
        dblog.trace("Get gc_before for ks={}, table={}, dk={}, mode=repair, repair_timestamp={}, propagation_delay={}, gc_before={}",
                s->ks_name(), s->cf_name(), dk, repair_timestamp, propagation_delay.count(), gc_before);
        return gc_before;
    }
    std::abort();
}

void tombstone_gc_state::update_repair_time(table_id id, const dht::token_range& range, gc_clock::time_point repair_time) {
    auto m = get_or_create_repair_history_map_for_table(id);
    m->map += std::make_pair(locator::token_metadata::range_to_interval(range), repair_time);
}

static bool needs_repair_before_gc(const replica::database& db, sstring ks_name) {
    // If a table uses local replication strategy or rf one, there is no
    // need to run repair even if tombstone_gc mode = repair.
    auto& ks = db.find_keyspace(ks_name);
    auto& rs = ks.get_replication_strategy();
    bool needs_repair = rs.get_type() != locator::replication_strategy_type::local
            && rs.get_replication_factor(db.get_token_metadata()) != 1;
    return needs_repair;
}

static bool requires_repair_before_gc(data_dictionary::database db, sstring ks_name) {
    auto real_db_ptr = db.real_database_ptr();
    if (!real_db_ptr) {
        return false;
    }

    const auto& rs = db.find_keyspace(ks_name).get_replication_strategy();
    return rs.uses_tablets() && needs_repair_before_gc(*real_db_ptr, ks_name);
}

std::map<sstring, sstring> get_default_tombstonesonte_gc_mode(data_dictionary::database db, sstring ks_name) {
    return {{"mode", requires_repair_before_gc(db, ks_name) ? "repair" : "timeout"}};
}

void validate_tombstone_gc_options(const tombstone_gc_options* options, data_dictionary::database db, sstring ks_name) {
    if (!options) {
        return;
    }
    if (!db.features().tombstone_gc_options) {
        throw exceptions::configuration_exception("tombstone_gc option not supported by the cluster");
    }

    if (options->mode() == tombstone_gc_mode::repair && !needs_repair_before_gc(db.real_database(), ks_name)) {
        throw exceptions::configuration_exception("tombstone_gc option with mode = repair not supported for table with RF one or local replication strategy");
    }
}
