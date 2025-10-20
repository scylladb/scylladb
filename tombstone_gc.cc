/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <chrono>
#include <boost/icl/interval.hpp>
#include "schema/schema.hh"
#include "gc_clock.hh"
#include "tombstone_gc.hh"
#include "tombstone_gc-internals.hh"
#include "locator/token_metadata.hh"
#include "exceptions/exceptions.hh"
#include "locator/abstract_replication_strategy.hh"
#include "replica/database.hh"
#include "data_dictionary/data_dictionary.hh"
#include "gms/feature_service.hh"
#include "compaction/compaction_manager.hh"
#include <seastar/coroutine/maybe_yield.hh>

extern logging::logger dblog;

repair_history_map_ptr tombstone_gc_state::get_repair_history_for_table(const table_id& id) const {
    if (!_shared_state) {
        return {};
    }
    auto& reconcile_history_maps = _shared_state->get_reconcile_history_maps()._repair_maps;
    auto it = reconcile_history_maps.find(id);
    if (it != reconcile_history_maps.end()) {
        return it->second;
    }
    return {};
}

gc_clock::time_point tombstone_gc_state::get_gc_before_for_group0(schema_ptr s) const {
    // use the reconcile mode for group0 tables with 0 propagation delay
    if (!_shared_state) {
        return gc_clock::time_point::min();
    }
    return check_min(s, _shared_state->get_reconcile_history_maps()._group0_gc_time);
}

void shared_tombstone_gc_state::drop_repair_history_for_table(const table_id& id) {
    mutate_repair_history([id] (per_table_history_maps& maps) {
        maps._repair_maps.erase(id);
    });
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
    switch (_mode) {
        case mode::no_gc: return get_gc_before_for_range_result{gc_clock::time_point::min(), gc_clock::time_point::min(), true};
        case mode::gc_all: return get_gc_before_for_range_result{gc_clock::time_point::max(), gc_clock::time_point::max(), true};
        case mode::gc_expired: break;
    }

    bool knows_entire_range = true;

    if (s->static_props().is_group0_table) {
        const auto gc_before = get_gc_before_for_group0(s);
        dblog.trace("Get gc_before for ks={}, table={}, range={}, mode=reconcile, gc_before={}", s->ks_name(), s->cf_name(), range, gc_before);
        return {gc_before, gc_before, knows_entire_range};
    }

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
        auto m = get_repair_history_for_table(s->id());
        if (m) {
            auto interval = locator::token_metadata::range_to_interval(range);
            auto min = gc_clock::time_point::max();
            auto max = gc_clock::time_point::min();
            bool contains_all = false;
            for (const auto& [i, s] = m->equal_range(interval); auto& x : std::ranges::subrange(i, s)) {
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
    if (_mode != mode::gc_expired) {
        return true;
    }
    return s.tombstone_gc_options().mode() != tombstone_gc_mode::repair;
}

gc_clock::time_point tombstone_gc_state::check_min(schema_ptr s, gc_clock::time_point t) const {
    if (_check_commitlog && _shared_state && t != gc_clock::time_point::min()) {
        return std::min(t, _shared_state->get_gc_min_time(s->id()));
    }
    return t;
}

gc_clock::time_point tombstone_gc_state::get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, const gc_clock::time_point& query_time) const {
    switch (_mode) {
        case mode::no_gc: return gc_clock::time_point::min();
        case mode::gc_all: return gc_clock::time_point::max();
        case mode::gc_expired: break;
    }

    if (s->static_props().is_group0_table) {
        const auto gc_before = get_gc_before_for_group0(s);
        dblog.trace("Get gc_before for ks={}, table={}, dk={}, mode=reconcile, gc_before={}", s->ks_name(), s->cf_name(), dk, gc_before);
        return gc_before;
    }

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
        auto m = get_repair_history_for_table(s->id());
        if (m) {
            const auto it = m->find(dk.token());
            if (it == m->end()) {
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

void shared_tombstone_gc_state::mutate_repair_history(std::function<void(per_table_history_maps&)> mutator) {
    auto new_maps = make_lw_shared<per_table_history_maps>(*_reconcile_history_maps);
    mutator(*new_maps);
    _reconcile_history_maps = std::move(new_maps);
}

shared_tombstone_gc_state::shared_tombstone_gc_state()
    : _reconcile_history_maps(make_lw_shared<const per_table_history_maps>())
 { }

shared_tombstone_gc_state::shared_tombstone_gc_state(gc_time_min_source gc_min_source, lw_shared_ptr<const per_table_history_maps> reconcile_history_maps)
    : _gc_min_source(std::move(gc_min_source))
    , _reconcile_history_maps(std::move(reconcile_history_maps))
{ }

shared_tombstone_gc_state::shared_tombstone_gc_state(shared_tombstone_gc_state&&) = default;

shared_tombstone_gc_state::~shared_tombstone_gc_state() { }

static void do_update_repair_time(per_table_history_maps& maps, table_id id, const dht::token_range& range, gc_clock::time_point repair_time) {
    auto& reconcile_history_maps = maps._repair_maps;
    auto [it, inserted] = reconcile_history_maps.try_emplace(id, lw_shared_ptr<repair_history_map>(nullptr));
    if (inserted || !it->second) { // check for failed past update, leaving behind nullptr
        it->second = seastar::make_lw_shared<repair_history_map>();
    } else {
        it->second = seastar::make_lw_shared<repair_history_map>(*it->second);
    }
    *it->second += std::make_pair(locator::token_metadata::range_to_interval(range), repair_time);
}

void shared_tombstone_gc_state::update_repair_time(table_id id, const dht::token_range& range, gc_clock::time_point repair_time) {
    mutate_repair_history([id, &range, repair_time] (per_table_history_maps& maps) {
        do_update_repair_time(maps, id, range, repair_time);
    });
}

void shared_tombstone_gc_state::insert_pending_repair_time_update(table_id id,
        const dht::token_range& range, gc_clock::time_point repair_time, shard_id shard) {
    _pending_updates[id].push_back(range_repair_time{range, repair_time, shard});
}

future<> shared_tombstone_gc_state::flush_pending_repair_time_update(replica::database& db) {
    auto pending_updates = std::exchange(_pending_updates, {});

    co_await db.container().invoke_on_all([&pending_updates] (replica::database &localdb) -> future<> {
        auto& shared_gc_state = localdb.get_compaction_manager().get_shared_tombstone_gc_state();
        auto shard_maps = make_lw_shared<per_table_history_maps>(shared_gc_state.get_reconcile_history_maps());
        for (auto& x : pending_updates) {
            auto& table = x.first;
            for (auto& update : x.second) {
                co_await coroutine::maybe_yield();
                if (update.shard == this_shard_id()) {
                    do_update_repair_time(*shard_maps, table, update.range, update.time);
                    dblog.debug("Flush pending repair time for tombstone gc: table={} range={} repair_time={}",
                            table, update.range, update.time);
                }
            }
        }
        shared_gc_state._reconcile_history_maps = std::move(shard_maps);
    });
};

void shared_tombstone_gc_state::update_group0_refresh_time(gc_clock::time_point refresh_time) {
    mutate_repair_history([refresh_time] (per_table_history_maps& maps) {
        maps._group0_gc_time = refresh_time;
    });
}

tombstone_gc_state_snapshot shared_tombstone_gc_state::snapshot() const noexcept {
    return tombstone_gc_state_snapshot(shared_tombstone_gc_state(_gc_min_source, _reconcile_history_maps));
}

tombstone_gc_state_snapshot::tombstone_gc_state_snapshot(shared_tombstone_gc_state&& shared_state)
    : _shared_state(std::move(shared_state)), _query_time(gc_clock::now())
{ }

gc_clock::time_point tombstone_gc_state_snapshot::get_gc_before_for_key(schema_ptr s, const dht::decorated_key& dk, bool check_commitlog) const {
    return tombstone_gc_state(_shared_state, check_commitlog).get_gc_before_for_key(s, dk, _query_time);
}

static bool needs_repair_before_gc(const locator::abstract_replication_strategy& rs, const locator::token_metadata& tm) {
    // If a table uses local replication strategy or rf one, there is no
    // need to run repair even if tombstone_gc mode = repair.
    bool needs_repair = !rs.is_local() && rs.get_replication_factor(tm) != 1;
    return needs_repair;
}

static bool requires_repair_before_gc(const locator::abstract_replication_strategy& rs, const locator::token_metadata& tm) {
    return rs.uses_tablets() && needs_repair_before_gc(rs, tm);
}

std::map<sstring, sstring> get_default_tombstone_gc_mode(const locator::abstract_replication_strategy& rs, const locator::token_metadata& tm) {
    return {{"mode", requires_repair_before_gc(rs, tm) ? "repair" : "timeout"}};
}

std::map<sstring, sstring> get_default_tombstone_gc_mode(data_dictionary::database db, sstring ks_name) {
    auto real_db_ptr = db.real_database_ptr();
    if (!real_db_ptr) {
        return {{"mode", "timeout"}};
    }

    const replica::keyspace& ks = real_db_ptr->find_keyspace(ks_name);
    const locator::token_metadata& tm = real_db_ptr->get_token_metadata();

    return get_default_tombstone_gc_mode(ks.get_replication_strategy(), tm);
}

void validate_tombstone_gc_options(const tombstone_gc_options* options, data_dictionary::database db, sstring ks_name) {
    if (!options) {
        return;
    }
    if (!db.features().tombstone_gc_options) {
        throw exceptions::configuration_exception("tombstone_gc option not supported by the cluster");
    }

    auto real_db_ptr = db.real_database_ptr();
    if (!real_db_ptr) {
        return;
    }

    const replica::keyspace& ks = db.real_database().find_keyspace(ks_name);
    if (options->mode() == tombstone_gc_mode::repair && !needs_repair_before_gc(ks.get_replication_strategy(), real_db_ptr->get_token_metadata())) {
        throw exceptions::configuration_exception("tombstone_gc option with mode = repair not supported for table with RF one or local replication strategy");
    }
}
