/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "locator/network_topology_strategy.hh"
#include "locator/tablet_replication_strategy.hh"
#include "locator/tablets.hh"
#include "locator/tablet_metadata_guard.hh"
#include "locator/tablet_sharder.hh"
#include "locator/token_range_splitter.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"
#include "utils/stall_free.hh"
#include "utils/rjson.hh"
#include "gms/feature_service.hh"

#include <algorithm>
#include <iterator>
#include <chrono>

#include <fmt/ranges.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <type_traits>

namespace locator {

seastar::logger tablet_logger("tablets");

std::optional<std::pair<tablet_id, tablet_id>> tablet_map::sibling_tablets(tablet_id t) const {
    if (tablet_count() == 1) {
        return std::nullopt;
    }
    auto first_sibling = tablet_id(t.value() & ~0x1);
    return std::make_pair(first_sibling, *next_tablet(first_sibling));
}


static
write_replica_set_selector get_selector_for_writes(tablet_transition_stage stage) {
    switch (stage) {
        case tablet_transition_stage::allow_write_both_read_old:
            return write_replica_set_selector::previous;
        case tablet_transition_stage::write_both_read_old:
            return write_replica_set_selector::both;
        case tablet_transition_stage::streaming:
            return write_replica_set_selector::both;
        case tablet_transition_stage::rebuild_repair:
            return write_replica_set_selector::both;
        case tablet_transition_stage::repair:
            return write_replica_set_selector::previous;
        case tablet_transition_stage::end_repair:
            return write_replica_set_selector::previous;
        case tablet_transition_stage::write_both_read_new:
            return write_replica_set_selector::both;
        case tablet_transition_stage::use_new:
            return write_replica_set_selector::next;
        case tablet_transition_stage::cleanup:
            return write_replica_set_selector::next;
        case tablet_transition_stage::cleanup_target:
            return write_replica_set_selector::previous;
        case tablet_transition_stage::revert_migration:
            return write_replica_set_selector::previous;
        case tablet_transition_stage::end_migration:
            return write_replica_set_selector::next;
    }
    on_internal_error(tablet_logger, format("Invalid tablet transition stage: {}", static_cast<int>(stage)));
}

static
read_replica_set_selector get_selector_for_reads(tablet_transition_stage stage) {
    switch (stage) {
        case tablet_transition_stage::allow_write_both_read_old:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::write_both_read_old:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::streaming:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::rebuild_repair:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::repair:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::end_repair:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::write_both_read_new:
            return read_replica_set_selector::next;
        case tablet_transition_stage::use_new:
            return read_replica_set_selector::next;
        case tablet_transition_stage::cleanup:
            return read_replica_set_selector::next;
        case tablet_transition_stage::cleanup_target:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::revert_migration:
            return read_replica_set_selector::previous;
        case tablet_transition_stage::end_migration:
            return read_replica_set_selector::next;
    }
    on_internal_error(tablet_logger, format("Invalid tablet transition stage: {}", static_cast<int>(stage)));
}

tablet_transition_info::tablet_transition_info(tablet_transition_stage stage,
                                               tablet_transition_kind transition,
                                               tablet_replica_set next,
                                               std::optional<tablet_replica> pending_replica,
                                               service::session_id session_id)
    : stage(stage)
    , transition(transition)
    , next(std::move(next))
    , pending_replica(std::move(pending_replica))
    , session_id(session_id)
    , writes(get_selector_for_writes(stage))
    , reads(get_selector_for_reads(stage))
{ }

tablet_migration_streaming_info get_migration_streaming_info(const locator::topology& topo, const tablet_info& tinfo, const tablet_migration_info& trinfo) {
    return get_migration_streaming_info(topo, tinfo, migration_to_transition_info(tinfo, trinfo));
}

tablet_migration_streaming_info get_migration_streaming_info(const locator::topology& topo, const tablet_info& tinfo, const tablet_transition_info& trinfo) {
    tablet_migration_streaming_info result;
    switch (trinfo.transition) {
        case tablet_transition_kind::intranode_migration:
            [[fallthrough]];
        case tablet_transition_kind::migration:
            result.read_from = substract_sets(tinfo.replicas, trinfo.next);
            result.written_to = substract_sets(trinfo.next, tinfo.replicas);
            return result;
        case tablet_transition_kind::rebuild:
            if (!trinfo.pending_replica.has_value()) {
                return result; // No nodes to stream to -> no nodes to stream from
            }

            result.written_to.insert(*trinfo.pending_replica);
            result.read_from = std::unordered_set<tablet_replica>(trinfo.next.begin(), trinfo.next.end());
            result.read_from.erase(*trinfo.pending_replica);

            erase_if(result.read_from, [&] (const tablet_replica& r) {
                auto* n = topo.find_node(r.host);
                return !n || n->is_excluded();
            });

            return result;
        case tablet_transition_kind::rebuild_v2: {
            if (!trinfo.pending_replica.has_value()) {
                return result; // No nodes to stream to -> no nodes to stream from
            }

            auto s = std::unordered_set<tablet_replica>(tinfo.replicas.begin(), tinfo.replicas.end());
            erase_if(s, [&] (const tablet_replica& r) {
                auto* n = topo.find_node(r.host);
                return !n || n->is_excluded();
            });
            result.stream_weight = locator::tablet_migration_stream_weight_repair;
            result.read_from = s;
            result.written_to = std::move(s);
            result.written_to.insert(*trinfo.pending_replica);

            return result;
        }
        case tablet_transition_kind::repair:
            auto s = std::unordered_set<tablet_replica>(tinfo.replicas.begin(), tinfo.replicas.end());
            result.stream_weight = locator::tablet_migration_stream_weight_repair;
            result.read_from = s;
            result.written_to = std::move(s);
            return result;
    }
    on_internal_error(tablet_logger, format("Invalid tablet transition kind: {}", static_cast<int>(trinfo.transition)));
}

bool tablet_has_excluded_node(const locator::topology& topo, const tablet_info& tinfo) {
    for (const auto& r : tinfo.replicas) {
        auto* n = topo.find_node(r.host);
        if (!n || n->is_excluded()) {
            return true;
        }
    }
    return false;
}

tablet_info::tablet_info(tablet_replica_set replicas, db_clock::time_point repair_time, tablet_task_info repair_task_info, tablet_task_info migration_task_info, int64_t sstables_repaired_at)
    : replicas(std::move(replicas))
    , repair_time(repair_time)
    , repair_task_info(std::move(repair_task_info))
    , migration_task_info(std::move(migration_task_info))
    , sstables_repaired_at(sstables_repaired_at)
{}

tablet_info::tablet_info(tablet_replica_set replicas)
    : tablet_info(std::move(replicas), db_clock::time_point{}, tablet_task_info{}, tablet_task_info{}, int64_t(0))
{}

std::optional<tablet_info> merge_tablet_info(tablet_info a, tablet_info b) {
    if (a.repair_task_info.is_valid() || b.repair_task_info.is_valid()) {
        return {};
    }

    auto sorted = [] (tablet_replica_set rs) {
        std::ranges::sort(rs, std::less<tablet_replica>());
        return rs;
    };
    if (sorted(a.replicas) != sorted(b.replicas)) {
        return {};
    }

    auto repair_time = std::max(a.repair_time, b.repair_time);
    int64_t sstables_repaired_at = std::max(a.sstables_repaired_at, b.sstables_repaired_at);
    auto info = tablet_info(std::move(a.replicas), repair_time, a.repair_task_info, a.migration_task_info, sstables_repaired_at);
    return info;
}

std::optional<tablet_replica> get_leaving_replica(const tablet_info& tinfo, const tablet_transition_info& trinfo) {
    auto leaving = substract_sets(tinfo.replicas, trinfo.next);
    if (leaving.empty()) {
        return {};
    }
    if (leaving.size() > 1) {
        throw std::runtime_error(format("More than one leaving replica"));
    }
    return *leaving.begin();
}

bool is_post_cleanup(tablet_replica replica, const tablet_info& tinfo, const tablet_transition_info& trinfo) {
    if (replica == locator::get_leaving_replica(tinfo, trinfo)) {
        // we do tablet cleanup on the leaving replica in the `cleanup` stage, after which there is only the `end_migration` stage.
        return trinfo.stage == locator::tablet_transition_stage::end_migration;
    }
    if (replica == trinfo.pending_replica) {
        // we do tablet cleanup on the pending replica in the `cleanup_target` stage, after which there is only the `revert_migration` stage.
        return trinfo.stage == locator::tablet_transition_stage::revert_migration;
    }
    return false;
}

tablet_replica_set get_new_replicas(const tablet_info& tinfo, const tablet_migration_info& mig) {
    return replace_replica(tinfo.replicas, mig.src, mig.dst);
}

tablet_replica_set get_primary_replicas(const tablet_info& info, const tablet_transition_info* transition) {
    auto write_selector = [&] {
        if (!transition) {
            return write_replica_set_selector::previous;
        }
        return transition->writes;
    };
    auto primary = [] (tablet_replica_set set) -> tablet_replica {
        return set.front();
    };
    auto add = [] (tablet_replica r1, tablet_replica r2) -> tablet_replica_set {
        // if primary replica is not the one leaving, then only primary will be streamed to.
        return (r1 == r2) ? tablet_replica_set{r1} : tablet_replica_set{r1, r2};
    };

    switch (write_selector()) {
        case write_replica_set_selector::previous: return {primary(info.replicas)};
        case write_replica_set_selector::both: return add(primary(info.replicas), primary(transition->next));
        case write_replica_set_selector::next: return {primary(transition->next)};
    }
}

tablet_transition_info migration_to_transition_info(const tablet_info& ti, const tablet_migration_info& mig) {
    return tablet_transition_info {
            tablet_transition_stage::allow_write_both_read_old,
            mig.kind,
            get_new_replicas(ti, mig),
            mig.dst
    };
}

no_such_tablet_map::no_such_tablet_map(const table_id& id)
        : runtime_error{fmt::format("Tablet map not found for table {}", id)}
{
}

const tablet_map& tablet_metadata::get_tablet_map(table_id id) const {
    try {
        return *_tablets.at(id);
    } catch (const std::out_of_range&) {
        throw_with_backtrace<no_such_tablet_map>(id);
    }
}

bool tablet_metadata::has_tablet_map(table_id id) const {
    return _tablets.contains(id);
}

table_id tablet_metadata::get_base_table(table_id id) const {
    if (auto it = _base_table.find(id); it != _base_table.end()) {
        return it->second;
    } else {
        return id;
    }
}

bool tablet_metadata::is_base_table(table_id id) const {
    return !_base_table.contains(id);
}

future<> tablet_metadata::mutate_tablet_map_async(table_id id, noncopyable_function<future<>(tablet_map&)> func) {
    auto it = _tablets.find(id);
    if (it == _tablets.end()) {
        throw no_such_tablet_map(id);
    }
    auto tablet_map_copy = make_lw_shared<tablet_map>(co_await it->second->clone_gently());
    co_await func(*tablet_map_copy);
    auto new_map_ptr = lw_shared_ptr<const tablet_map>(std::move(tablet_map_copy));
    // share the tablet map with all co-located tables
    for (auto colocated_id : _table_groups.at(id)) {
        if (colocated_id != id) {
            _tablets[colocated_id] = make_foreign(new_map_ptr);
        }
    }
    it->second = make_foreign(std::move(new_map_ptr));
}

future<tablet_metadata> tablet_metadata::copy() const {
    tablet_metadata copy;
    for (const auto& e : _tablets) {
        copy._tablets.emplace(e.first, co_await e.second.copy());
    }

    copy._table_groups = _table_groups;
    copy._base_table = _base_table;

    copy._balancing_enabled = _balancing_enabled;

    co_return copy;
}

void tablet_metadata::set_tablet_map(table_id id, tablet_map map) {
    auto map_ptr = make_lw_shared<const tablet_map>(std::move(map));
    if (auto it = _table_groups.find(id); it == _table_groups.end()) {
        _table_groups[id] = {id};
    } else {
        for (auto colocated_id : it->second) {
            if (colocated_id != id) {
                _tablets[colocated_id] = map_ptr;
            }
        }
    }
    auto it = _tablets.find(id);
    if (it == _tablets.end()) {
        _tablets.emplace(id, std::move(map_ptr));
    } else {
        it->second = std::move(map_ptr);
    }
}

future<> tablet_metadata::set_colocated_table(table_id id, table_id base_id) {
    if (auto it = _table_groups.find(id); it != _table_groups.end()) {
        // Allow changing a base table to be a co-located table of another base table, if it doesn't have any other co-located tables.
        // This shouldn't be used normally except for unit tests.
        tablet_logger.warn("Changing base table {} to be a co-located table of another base table {}. This should be used only in tests.", id, base_id);
        if (it->second.size() > 1) {
            on_internal_error(tablet_logger, format("Table {} is already a base table for {} and cannot be set as a co-located table of another base table.", id, it->second));
        }
        _table_groups.erase(it);
    }

    if (auto it = _base_table.find(id); it == _base_table.end()) {
        _base_table[id] = base_id;
        _table_groups[base_id].push_back(id);

        if (!_tablets.contains(base_id)) {
            on_internal_error(tablet_logger, format("Base table {} of co-located table {} does not have a tablet map", base_id, id));
        }
        auto map_ptr = co_await _tablets.at(base_id).copy();
        _tablets[id] = std::move(map_ptr);
    } else if (it->second != base_id) {
        on_internal_error(tablet_logger, format("Cannot set base table {} for table {} because it already has base table {}", base_id, id, it->second));
    }
}

void tablet_metadata::drop_tablet_map(table_id id) {
    if (auto it = _base_table.find(id); it != _base_table.end()) {
        // it's a co-located table. We need to remove it from the base table's colocated tables list.
        auto base_id = it->second;
        if (auto group_it = _table_groups.find(base_id); group_it != _table_groups.end()) {
            auto& tables = group_it->second;
            tables.erase(std::remove(tables.begin(), tables.end(), id), tables.end());
            if (tables.empty()) {
                _table_groups.erase(group_it);
            }
        }
        _base_table.erase(it);
    }

    _table_groups.erase(id);

    _tablets.erase(id);
}

future<> tablet_metadata::clear_gently() {
    tablet_logger.debug("tablet_metadata::clear_gently {}", fmt::ptr(this));
    // First, Sort the tablet maps per shard to avoid destruction of all foreign tablet map ptrs
    // on this shard. We don't use sharded<> here since it will require a similar 
    // submit_to to each shard owner per tablet-map.
    std::vector<std::vector<tablet_map_ptr>> tablet_maps_per_shard;
    tablet_maps_per_shard.resize(smp::count);
    for (auto& [_, map_ptr] : _tablets) {
        tablet_maps_per_shard[map_ptr.get_owner_shard()].emplace_back(std::move(map_ptr));
    }
    _tablets.clear();

    // Now destroy the foreign tablet map pointers on each shard.
    co_await smp::invoke_on_all([&] -> future<> {
        for (auto& map_ptr : tablet_maps_per_shard[this_shard_id()]) {
            auto map = map_ptr.release();
            co_await utils::clear_gently(map);
        }
    });

    co_await utils::clear_gently(_table_groups);
    co_await utils::clear_gently(_base_table);

    co_return;
}

bool tablet_metadata::operator==(const tablet_metadata& o) const {
    if (_tablets.size() != o._tablets.size()) {
        return false;
    }
    for (const auto& [k, v] : _tablets) {
        const auto it = o._tablets.find(k);
        if (it == o._tablets.end() || *v != *it->second) {
            return false;
        }
    }
    return true;
}

tablet_map::tablet_map(size_t tablet_count)
        : _log2_tablets(log2ceil(tablet_count)) {
    if (tablet_count != 1ul << _log2_tablets) {
        on_internal_error(tablet_logger, format("Tablet count not a power of 2: {}", tablet_count));
    }
    _tablets.resize(tablet_count);
}

tablet_map tablet_map::clone() const {
    return tablet_map(_tablets, _log2_tablets, _transitions, _resize_decision, _resize_task_info, _repair_scheduler_config);
}

future<tablet_map> tablet_map::clone_gently() const {
    tablet_container tablets;
    tablets.reserve(_tablets.size());
    for (const auto& t : _tablets) {
        tablets.emplace_back(t);
        co_await coroutine::maybe_yield();
    }

    transitions_map transitions;
    transitions.reserve(_transitions.size());
    for (const auto& [id, trans] : _transitions) {
        transitions.emplace(id, trans);
        co_await coroutine::maybe_yield();
    }

    co_return tablet_map(std::move(tablets), _log2_tablets, std::move(transitions), _resize_decision, _resize_task_info, _repair_scheduler_config);
}

void tablet_map::check_tablet_id(tablet_id id) const {
    if (size_t(id) >= tablet_count()) {
        throw std::logic_error(format("Invalid tablet id: {} >= {}", id, tablet_count()));
    }
}

const tablet_info& tablet_map::get_tablet_info(tablet_id id) const {
    check_tablet_id(id);
    return _tablets[size_t(id)];
}

tablet_id tablet_map::get_tablet_id(token t) const {
    return tablet_id(dht::compaction_group_of(_log2_tablets, t));
}

std::pair<tablet_id, tablet_range_side> tablet_map::get_tablet_id_and_range_side(token t) const {
    auto id_after_split = dht::compaction_group_of(_log2_tablets + 1, t);
    auto current_id = id_after_split >> 1;
    return {tablet_id(current_id), tablet_range_side(id_after_split & 0x1)};
}

dht::token tablet_map::get_last_token(tablet_id id, size_t log2_tablets) const {
    return dht::last_token_of_compaction_group(log2_tablets, size_t(id));
}

dht::token tablet_map::get_last_token(tablet_id id) const {
    check_tablet_id(id);
    return get_last_token(id, _log2_tablets);
}

dht::token tablet_map::get_first_token(tablet_id id) const {
    if (id == first_tablet()) {
        return dht::first_token();
    } else {
        return dht::next_token(get_last_token(tablet_id(size_t(id) - 1)));
    }
}

dht::token_range tablet_map::get_token_range(tablet_id id, size_t log2_tablets) const {
    if (id == first_tablet()) {
        return dht::token_range::make({dht::minimum_token(), false}, {get_last_token(id, log2_tablets), true});
    } else {
        return dht::token_range::make({get_last_token(tablet_id(size_t(id) - 1), log2_tablets), false}, {get_last_token(id, log2_tablets), true});
    }
}

dht::token_range tablet_map::get_token_range(tablet_id id) const {
    check_tablet_id(id);
    return get_token_range(id, _log2_tablets);
}

dht::token_range tablet_map::get_token_range_after_split(const token& t) const noexcept {
    // when the tablets are split, the tablet count doubles, (i.e.) _log2_tablets increases by 1
    const auto log2_tablets_after_split = _log2_tablets + 1;
    auto id_after_split = tablet_id(dht::compaction_group_of(log2_tablets_after_split, t));
    return get_token_range(id_after_split, log2_tablets_after_split);
}

std::optional<tablet_replica> maybe_get_primary_replica(tablet_id id, const tablet_replica_set& replica_set, std::function<bool(const tablet_replica&)> filter) {
    const auto replicas = replica_set | std::views::filter(std::move(filter)) | std::ranges::to<tablet_replica_set>();
    return !replicas.empty() ? std::make_optional(replicas.at(size_t(id) % replicas.size())) : std::nullopt;
}

tablet_replica tablet_map::get_primary_replica(tablet_id id) const {
    const auto& replicas = get_tablet_info(id).replicas;
    return replicas.at(size_t(id) % replicas.size());
}

tablet_replica tablet_map::get_secondary_replica(tablet_id id) const {
    if (get_tablet_info(id).replicas.size() < 2) {
        throw std::runtime_error(format("No secondary replica for tablet id {}", id));
    }
    const auto& replicas = get_tablet_info(id).replicas;
    return replicas.at((size_t(id)+1) % replicas.size());
}

std::optional<tablet_replica> tablet_map::maybe_get_selected_replica(tablet_id id, const topology& topo, const tablet_task_info& tablet_task_info) const {
    return maybe_get_primary_replica(id, get_tablet_info(id).replicas, [&] (const auto& tr) {
        return tablet_task_info.selected_by_filters(tr, topo);
    });
}

future<utils::chunked_vector<token>> tablet_map::get_sorted_tokens() const {
    utils::chunked_vector<token> tokens;
    tokens.reserve(tablet_count());

    for (auto id : tablet_ids()) {
        tokens.push_back(get_last_token(id));
        co_await coroutine::maybe_yield();
    }

    co_return tokens;
}

void tablet_map::set_tablet(tablet_id id, tablet_info info) {
    check_tablet_id(id);
    _tablets[size_t(id)] = std::move(info);
}

void tablet_map::set_tablet_transition_info(tablet_id id, tablet_transition_info info) {
    check_tablet_id(id);
    _transitions.insert_or_assign(id, std::move(info));
}

void tablet_map::set_resize_decision(locator::resize_decision decision) {
    _resize_decision = std::move(decision);
}

void tablet_map::set_resize_task_info(tablet_task_info task_info) {
    _resize_task_info = std::move(task_info);
}

void tablet_map::set_repair_scheduler_config(locator::repair_scheduler_config config) {
    _repair_scheduler_config = std::move(config);
}

void tablet_map::clear_tablet_transition_info(tablet_id id) {
    check_tablet_id(id);
    _transitions.erase(id);
}

future<> tablet_map::for_each_tablet(seastar::noncopyable_function<future<>(tablet_id, const tablet_info&)> func) const {
    std::optional<tablet_id> tid = first_tablet();
    for (const tablet_info& ti : tablets()) {
        co_await func(*tid, ti);
        tid = next_tablet(*tid);
    }
}

future<> tablet_map::for_each_sibling_tablets(seastar::noncopyable_function<future<>(tablet_desc, std::optional<tablet_desc>)> func) const {
    auto make_desc = [this] (tablet_id tid) {
        return tablet_desc{tid, &get_tablet_info(tid), get_tablet_transition_info(tid)};
    };
    if (_tablets.size() == 1) {
        co_return co_await func(make_desc(first_tablet()), std::nullopt);
    }
    for (std::optional<tablet_id> tid = first_tablet(); tid; tid = next_tablet(*tid)) {
        auto tid1 = tid;
        auto tid2 = tid = next_tablet(*tid);
        if (!tid2) {
            // Cannot happen with power-of-two invariant.
            throw std::logic_error(format("Cannot retrieve sibling tablet with tablet count {}", tablet_count()));
        }
        co_await func(make_desc(*tid1), make_desc(*tid2));
    }
}

void tablet_map::clear_transitions() {
    _transitions.clear();
}

bool tablet_map::has_replica(tablet_id tid, tablet_replica r) const {
    auto& tinfo = get_tablet_info(tid);
    if (contains(tinfo.replicas, r)) {
        return true;
    }
    auto* trinfo = get_tablet_transition_info(tid);
    if (trinfo && contains(trinfo->next, r)) {
        return true;
    }
    return false;
}

future<> tablet_map::clear_gently() {
    return utils::clear_gently(_tablets);
}

const tablet_transition_info* tablet_map::get_tablet_transition_info(tablet_id id) const {
    auto i = _transitions.find(id);
    if (i == _transitions.end()) {
        return nullptr;
    }
    return &i->second;
}

// The names are persisted in system tables so should not be changed.
static const std::unordered_map<tablet_transition_stage, sstring> tablet_transition_stage_to_name = {
    {tablet_transition_stage::allow_write_both_read_old, "allow_write_both_read_old"},
    {tablet_transition_stage::write_both_read_old, "write_both_read_old"},
    {tablet_transition_stage::write_both_read_new, "write_both_read_new"},
    {tablet_transition_stage::streaming, "streaming"},
    {tablet_transition_stage::rebuild_repair, "rebuild_repair"},
    {tablet_transition_stage::repair, "repair"},
    {tablet_transition_stage::end_repair, "end_repair"},
    {tablet_transition_stage::use_new, "use_new"},
    {tablet_transition_stage::cleanup, "cleanup"},
    {tablet_transition_stage::cleanup_target, "cleanup_target"},
    {tablet_transition_stage::revert_migration, "revert_migration"},
    {tablet_transition_stage::end_migration, "end_migration"},
};

static const std::unordered_map<sstring, tablet_transition_stage> tablet_transition_stage_from_name = std::invoke([] {
    std::unordered_map<sstring, tablet_transition_stage> result;
    for (auto&& [v, s] : tablet_transition_stage_to_name) {
        result.emplace(s, v);
    }
    return result;
});

tablet_transition_kind choose_rebuild_transition_kind(const gms::feature_service& features) {
    return features.repair_based_tablet_rebuild ? tablet_transition_kind::rebuild_v2 : tablet_transition_kind::rebuild;
}

sstring tablet_transition_stage_to_string(tablet_transition_stage stage) {
    auto i = tablet_transition_stage_to_name.find(stage);
    if (i == tablet_transition_stage_to_name.end()) {
        on_internal_error(tablet_logger, format("Invalid tablet transition stage: {}", static_cast<int>(stage)));
    }
    return i->second;
}

tablet_transition_stage tablet_transition_stage_from_string(const sstring& name) {
    return tablet_transition_stage_from_name.at(name);
}

// The names are persisted in system tables so should not be changed.
static const std::unordered_map<tablet_transition_kind, sstring> tablet_transition_kind_to_name = {
        {tablet_transition_kind::migration, "migration"},
        {tablet_transition_kind::intranode_migration, "intranode_migration"},
        {tablet_transition_kind::rebuild, "rebuild"},
        {tablet_transition_kind::rebuild_v2, "rebuild_v2"},
        {tablet_transition_kind::repair, "repair"},
};

static const std::unordered_map<sstring, tablet_transition_kind> tablet_transition_kind_from_name = std::invoke([] {
    std::unordered_map<sstring, tablet_transition_kind> result;
    for (auto&& [v, s] : tablet_transition_kind_to_name) {
        result.emplace(s, v);
    }
    return result;
});

sstring tablet_transition_kind_to_string(tablet_transition_kind kind) {
    auto i = tablet_transition_kind_to_name.find(kind);
    if (i == tablet_transition_kind_to_name.end()) {
        on_internal_error(tablet_logger, format("Invalid tablet transition kind: {}", static_cast<int>(kind)));
    }
    return i->second;
}

tablet_transition_kind tablet_transition_kind_from_string(const sstring& name) {
    return tablet_transition_kind_from_name.at(name);
}

// The names are persisted in system tables so should not be changed.
static const std::unordered_map<tablet_task_type, sstring> tablet_task_type_to_name = {
    {locator::tablet_task_type::none, "none"},
    {locator::tablet_task_type::user_repair, "user_repair"},
    {locator::tablet_task_type::auto_repair, "auto_repair"},
    {locator::tablet_task_type::migration, "migration"},
    {locator::tablet_task_type::intranode_migration, "intranode_migration"},
    {locator::tablet_task_type::split, "split"},
    {locator::tablet_task_type::merge, "merge"},
};

static const std::unordered_map<sstring, tablet_task_type> tablet_task_type_from_name = std::invoke([] {
    std::unordered_map<sstring, tablet_task_type> result;
    for (auto&& [v, s] : tablet_task_type_to_name) {
        result.emplace(s, v);
    }
    return result;
});

sstring tablet_task_type_to_string(tablet_task_type kind) {
    auto i = tablet_task_type_to_name.find(kind);
    if (i == tablet_task_type_to_name.end()) {
        on_internal_error(tablet_logger, format("Invalid tablet task type: {}", static_cast<int>(kind)));
    }
    return i->second;
}

tablet_task_type tablet_task_type_from_string(const sstring& name) {
    return tablet_task_type_from_name.at(name);
}

size_t tablet_map::external_memory_usage() const {
    size_t result = _tablets.external_memory_usage();
    for (auto&& tablet : _tablets) {
        result += tablet.replicas.external_memory_usage();
    }
    return result;
}

bool resize_decision::operator==(const resize_decision& o) const {
    return way.index() == o.way.index() && sequence_number == o.sequence_number;
}

bool tablet_map::needs_split() const {
    return std::holds_alternative<resize_decision::split>(_resize_decision.way);
}

bool tablet_map::needs_merge() const {
    return std::holds_alternative<resize_decision::merge>(_resize_decision.way);
}

const locator::resize_decision& tablet_map::resize_decision() const {
    return _resize_decision;
}

const tablet_task_info& tablet_map::resize_task_info() const {
    return _resize_task_info;
}

const locator::repair_scheduler_config& tablet_map::repair_scheduler_config() const {
    return _repair_scheduler_config;
}

static auto to_resize_type(sstring decision) {
    static const std::unordered_map<sstring, decltype(resize_decision::way)> string_to_type = {
        {"none", resize_decision::none{}},
        {"split", resize_decision::split{}},
        {"merge", resize_decision::merge{}},
    };
    return string_to_type.at(decision);
}

resize_decision::resize_decision(sstring decision, uint64_t seq_number)
    : way(to_resize_type(decision))
    , sequence_number(seq_number) {
}

sstring resize_decision::type_name() const {
    return fmt::format("{}", way);
}

resize_decision::seq_number_t resize_decision::next_sequence_number() const {
    // Doubt we'll ever wrap around, but just in case.
    // Even if sequence number is bumped every second, it would take 292471208677 years
    // for it to happen, about 21x the age of the universe, or ~11x according to the new
    // prediction after james webb.
    return (sequence_number == std::numeric_limits<seq_number_t>::max()) ? 0 : sequence_number + 1;
}

table_load_stats& table_load_stats::operator+=(const table_load_stats& s) noexcept {
    size_in_bytes = size_in_bytes + s.size_in_bytes;
    split_ready_seq_number = std::min(split_ready_seq_number, s.split_ready_seq_number);
    return *this;
}

load_stats load_stats::from_v1(load_stats_v1&& stats) {
    return { .tables = std::move(stats.tables) };
}

load_stats& load_stats::operator+=(const load_stats& s) {
    for (auto& [id, stats] : s.tables) {
        tables[id] += stats;
    }
    for (auto& [host, cap] : s.capacity) {
        capacity[host] = cap;
    }
    return *this;
}

tablet_range_splitter::tablet_range_splitter(schema_ptr schema, const tablet_map& tablets, host_id host, const dht::partition_range_vector& ranges)
    : _schema(std::move(schema))
    , _ranges(ranges)
    , _ranges_it(_ranges.begin())
{
    // Filter all tablets and save only those that have a replica on the specified host.
    for (auto tid = std::optional(tablets.first_tablet()); tid; tid = tablets.next_tablet(*tid)) {
        const auto& tablet_info = tablets.get_tablet_info(*tid);

        auto replica_it = std::ranges::find_if(tablet_info.replicas, [&] (auto&& r) { return r.host == host; });
        if (replica_it == tablet_info.replicas.end()) {
            continue;
        }

        _tablet_ranges.emplace_back(range_split_result{replica_it->shard, dht::to_partition_range(tablets.get_token_range(*tid))});
    }
    _tablet_ranges_it = _tablet_ranges.begin();
}

std::optional<tablet_range_splitter::range_split_result> tablet_range_splitter::operator()() {
    if (_ranges_it == _ranges.end() || _tablet_ranges_it == _tablet_ranges.end()) {
        return {};
    }

    dht::ring_position_comparator cmp(*_schema);

    while (_ranges_it != _ranges.end()) {
        // First, skip all tablet-ranges that are completely before the current range.
        while (_ranges_it->other_is_before(_tablet_ranges_it->range, cmp)) {
            ++_tablet_ranges_it;
        }
        // Generate intersections with all tablet-ranges that overlap with the current range.
        if (auto intersection = _ranges_it->intersection(_tablet_ranges_it->range, cmp)) {
            const auto shard = _tablet_ranges_it->shard;
            if (_ranges_it->end() && cmp(_ranges_it->end()->value(), _tablet_ranges_it->range.end()->value()) < 0) {
                // The current tablet range extends beyond the current range,
                // move to the next range.
                ++_ranges_it;
            } else {
                // The current range extends beyond the current tablet range,
                // move to the next tablet range.
                ++_tablet_ranges_it;
            }
            return range_split_result{shard, std::move(*intersection)};
        }
        // Current tablet-range is completely after the current range, move to the next range.
        ++_ranges_it;
    }

    return {};
}

// Estimates the external memory usage of std::unordered_map<>.
// Does not include external memory usage of elements.
template <typename K, typename V>
static size_t estimate_external_memory_usage(const std::unordered_map<K, V>& map) {
    return map.bucket_count() * sizeof(void*) + map.size() * (sizeof(std::pair<const K, V>) + 8);
}

size_t tablet_metadata::external_memory_usage() const {
    size_t result = estimate_external_memory_usage(_tablets);
    for (auto&& [id, map] : _tablets) {
        result += map->external_memory_usage();
    }
    return result;
}

bool tablet_metadata::has_replica_on(host_id host) const {
    for (auto&& [id, map] : _tablets) {
        for (auto&& tablet : map->tablet_ids()) {
            auto& tinfo = map->get_tablet_info(tablet);
            for (auto&& r : tinfo.replicas) {
                if (r.host == host) {
                    return true;
                }
            }
            auto* trinfo = map->get_tablet_transition_info(tablet);
            if (trinfo && trinfo->pending_replica && trinfo->pending_replica->host == host) {
                return true;
            }
        }
    }
    return false;
}

future<bool> check_tablet_replica_shards(const tablet_metadata& tm, host_id this_host) {
    bool valid = true;
    for (const auto& [table, tmap] : tm.all_tables_ungrouped()) {
        co_await tmap->for_each_tablet([this_host, &valid] (locator::tablet_id tid, const tablet_info& tinfo) -> future<> {
            for (const auto& replica : tinfo.replicas) {
                if (replica.host == this_host) {
                    valid &= replica.shard < smp::count;
                }
            }
            return make_ready_future<>();
        });
        if (!valid) {
            break;
        }
    }
    co_return valid;
}

class tablet_effective_replication_map : public effective_replication_map {
    table_id _table;
    tablet_sharder _sharder;
    mutable const tablet_map* _tmap = nullptr;
private:
    host_id_vector_replica_set to_host_set(const tablet_replica_set& replicas) const {
        host_id_vector_replica_set result;
        result.reserve(replicas.size());
        for (auto&& replica : replicas) {
            result.emplace_back(replica.host);
        }
        return result;
    }

    const tablet_map& get_tablet_map() const {
        if (!_tmap) {
            _tmap = &_tmptr->tablets().get_tablet_map(_table);
        }
        return *_tmap;
    }

    const tablet_replica_set& get_replicas_for_write(dht::token search_token) const {
        auto&& tablets = get_tablet_map();
        auto tablet = tablets.get_tablet_id(search_token);
        auto* info = tablets.get_tablet_transition_info(tablet);
        auto&& replicas = std::invoke([&] () -> const tablet_replica_set& {
            if (!info) {
                return tablets.get_tablet_info(tablet).replicas;
            }
            switch (info->writes) {
                case write_replica_set_selector::previous:
                    [[fallthrough]];
                case write_replica_set_selector::both:
                    return tablets.get_tablet_info(tablet).replicas;
                case write_replica_set_selector::next: {
                    return info->next;
                }
            }
            on_internal_error(tablet_logger, format("Invalid replica selector", static_cast<int>(info->writes)));
        });
        tablet_logger.trace("get_replicas_for_write({}): table={}, tablet={}, replicas={}", search_token, _table, tablet, replicas);
        return replicas;
    }

    host_id_vector_topology_change get_pending_helper(const token& search_token) const {
        auto&& tablets = get_tablet_map();
        auto tablet = tablets.get_tablet_id(search_token);
        auto&& info = tablets.get_tablet_transition_info(tablet);
        if (!info || info->transition == tablet_transition_kind::intranode_migration) {
            return {};
        }
        switch (info->writes) {
            case write_replica_set_selector::previous:
                return {};
            case write_replica_set_selector::both: {
                if (!info->pending_replica) {
                    return {};
                }
                tablet_logger.trace("get_pending_endpoints({}): table={}, tablet={}, replica={}",
                                    search_token, _table, tablet, *info->pending_replica);
                return {info->pending_replica->host};
            }
            case write_replica_set_selector::next:
                return {};
        }
        on_internal_error(tablet_logger, format("Invalid replica selector", static_cast<int>(info->writes)));
    }

    host_id_vector_replica_set get_for_reading_helper(const token& search_token) const {
        auto&& tablets = get_tablet_map();
        auto tablet = tablets.get_tablet_id(search_token);
        auto&& info = tablets.get_tablet_transition_info(tablet);
        auto&& replicas = std::invoke([&] () -> const tablet_replica_set& {
            if (!info) {
                return tablets.get_tablet_info(tablet).replicas;
            }
            switch (info->reads) {
                case read_replica_set_selector::previous:
                    return tablets.get_tablet_info(tablet).replicas;
                case read_replica_set_selector::next: {
                    return info->next;
                }
            }
            on_internal_error(tablet_logger, format("Invalid replica selector", static_cast<int>(info->reads)));
        });
        tablet_logger.trace("get_endpoints_for_reading({}): table={}, tablet={}, replicas={}", search_token, _table, tablet, replicas);
        return to_host_set(replicas);
    }


public:
    tablet_effective_replication_map(table_id table,
                                     replication_strategy_ptr rs,
                                     token_metadata_ptr tmptr,
                                     size_t replication_factor)
            : effective_replication_map(std::move(rs), std::move(tmptr), replication_factor)
            , _table(table)
            , _sharder(*_tmptr, table)
    { }

    virtual ~tablet_effective_replication_map() = default;

    virtual host_id_vector_replica_set get_replicas(const token& search_token, bool is_vnode = false) const override {
        return to_host_set(get_replicas_for_write(search_token));
    }

    virtual host_id_vector_replica_set get_natural_replicas(const token& search_token, bool is_vnode = false) const override {
        return to_host_set(get_replicas_for_write(search_token));
    }

    virtual future<dht::token_range_vector> get_ranges(host_id ep) const override {
        dht::token_range_vector ret;

        auto& tablet_map = get_tablet_map();
        for (auto tablet_id : tablet_map.tablet_ids()) {
            auto endpoints = get_natural_replicas(tablet_map.get_last_token(tablet_id));
            auto should_add_range = std::find(std::begin(endpoints), std::end(endpoints), ep) != std::end(endpoints);

            if (should_add_range) {
                ret.push_back(tablet_map.get_token_range(tablet_id));
            }
            co_await coroutine::maybe_yield();
        }

        co_return ret;
    }

    virtual host_id_vector_topology_change get_pending_replicas(const token& search_token) const override {
        return get_pending_helper(search_token);
    }

    virtual host_id_vector_replica_set get_replicas_for_reading(const token& search_token, bool is_vnode = false) const override {
        return get_for_reading_helper(search_token);
    }

    std::optional<tablet_routing_info> check_locality(const token& search_token) const override {
        auto&& tablets = get_tablet_map();
        auto tid = tablets.get_tablet_id(search_token);
        auto&& info = tablets.get_tablet_info(tid);
        auto host = get_token_metadata().get_my_id();
        auto shard = this_shard_id();

        auto make_tablet_routing_info = [&] {
            dht::token first_token;
            if (tid == tablets.first_tablet()) {
                first_token = dht::minimum_token();
            } else {
                first_token = tablets.get_last_token(tablet_id(size_t(tid) - 1));
            }
            auto token_range = std::make_pair(first_token, tablets.get_last_token(tid));
            return tablet_routing_info{info.replicas, token_range};
        };

        for (auto&& r : info.replicas) {
            if (r.host == host) {
                if (r.shard == shard) {
                    return std::nullopt; // routed correctly
                } else {
                    return make_tablet_routing_info();
                }
            }
        }

        auto tinfo = tablets.get_tablet_transition_info(tid);
        if (tinfo && tinfo->pending_replica && tinfo->pending_replica->host == host && tinfo->pending_replica->shard == shard) {
            return std::nullopt; // routed correctly
        }

        return make_tablet_routing_info();
    }

    virtual bool has_pending_ranges(locator::host_id host_id) const override {
        for (const auto& [id, transition_info]: get_tablet_map().transitions()) {
            if (transition_info.pending_replica && transition_info.pending_replica->host == host_id) {
                return true;
            }
        }
        return false;
    }

    virtual std::unique_ptr<token_range_splitter> make_splitter() const override {
        class splitter : public token_range_splitter {
            token_metadata_ptr _tmptr; // To keep the tablet map alive.
            const tablet_map& _tmap;
            std::optional<tablet_id> _next;
        public:
            splitter(token_metadata_ptr tmptr, const tablet_map& tmap)
                : _tmptr(std::move(tmptr))
                , _tmap(tmap)
            { }

            void reset(dht::ring_position_view pos) override {
                _next = _tmap.get_tablet_id(pos.token());
            }

            std::optional<dht::token> next_token() override {
                if (!_next) {
                    return std::nullopt;
                }
                auto t = _tmap.get_last_token(*_next);
                _next = _tmap.next_tablet(*_next);
                return t;
            }
        };
        return std::make_unique<splitter>(_tmptr, get_tablet_map());
    }

    const dht::sharder& get_sharder(const schema& s) const override {
        return _sharder;
    }
};

void tablet_aware_replication_strategy::validate_tablet_options(const abstract_replication_strategy& ars,
                                                                const gms::feature_service& fs,
                                                                const replication_strategy_config_options& opts) const {
    if (ars._uses_tablets && !fs.tablets) {
        throw exceptions::configuration_exception("Tablet replication is not enabled");
    }
}

void tablet_aware_replication_strategy::process_tablet_options(abstract_replication_strategy& ars,
                                                               replication_strategy_config_options& opts,
                                                               replication_strategy_params params) {
    if (ars._uses_tablets) {
        _initial_tablets = params.initial_tablets.value_or(0);
        mark_as_per_table(ars);
    }
}

effective_replication_map_ptr tablet_aware_replication_strategy::do_make_replication_map(
        table_id table, replication_strategy_ptr rs, token_metadata_ptr tm, size_t replication_factor) const {
    return seastar::make_shared<tablet_effective_replication_map>(table, std::move(rs), std::move(tm), replication_factor);
}

void tablet_metadata_guard::check() noexcept {
    auto erm = _table->get_effective_replication_map();
    auto& tmap = erm->get_token_metadata_ptr()->tablets().get_tablet_map(_tablet.table);
    auto* trinfo = tmap.get_tablet_transition_info(_tablet.tablet);
    if (bool(_stage) != bool(trinfo) || (_stage && _stage != trinfo->stage)) {
        _abort_source.request_abort();
    } else {
        _erm = std::move(erm);
        subscribe();
    }
}

tablet_metadata_guard::tablet_metadata_guard(replica::table& table, global_tablet_id tablet)
    : _table(table.shared_from_this())
    , _tablet(tablet)
    , _erm(table.get_effective_replication_map())
{
    subscribe();
    if (auto* trinfo = get_tablet_map().get_tablet_transition_info(tablet.tablet)) {
        _stage = trinfo->stage;
    }
}

tablet_metadata_guard::~tablet_metadata_guard() = default;

void tablet_metadata_guard::subscribe() {
    _callback = _erm->get_validity_abort_source().subscribe([this] () noexcept {
        check();
    });
}

token_metadata_guard::token_metadata_guard(replica::table& table, dht::token token)
    : _guard(std::invoke([&] -> guard_type {
        auto erm = table.get_effective_replication_map();
        if (!table.uses_tablets()) {
            return std::move(erm);
        }
        const auto table_id = table.schema()->id();
        const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(table_id);
        return make_lw_shared<tablet_metadata_guard>(table, global_tablet_id {
            .table = table_id,
            .tablet = tablet_map.get_tablet_id(token)
        });
    }))
{
}

const effective_replication_map_ptr& token_metadata_guard::get_erm() const {
    const auto* g = get_if<lw_shared_ptr<tablet_metadata_guard>>(&_guard);
    return g ? (**g).get_erm() : get<effective_replication_map_ptr>(_guard);
}

void assert_rf_rack_valid_keyspace(std::string_view ks, const token_metadata_ptr tmptr, const abstract_replication_strategy& ars) {
    tablet_logger.debug("[assert_rf_rack_valid_keyspace]: Starting verifying that keyspace '{}' is RF-rack-valid", ks);

    // Any keyspace that does NOT use tablets is RF-rack-valid.
    if (!ars.uses_tablets()) {
        tablet_logger.debug("[assert_rf_rack_valid_keyspace]: Keyspace '{}' has been verified to be RF-rack-valid (no tablets)", ks);
        return;
    }

    // Tablets can only be used with NetworkTopologyStrategy.
    SCYLLA_ASSERT(ars.get_type() == replication_strategy_type::network_topology);
    const auto& nts = *static_cast<const network_topology_strategy*>(std::addressof(ars));

    const auto& dc_rack_map = tmptr->get_topology().get_datacenter_racks();

    for (const auto& dc : nts.get_datacenters()) {
        if (!dc_rack_map.contains(dc)) {
            on_internal_error(tablet_logger, seastar::format(
                    "Precondition violated: DC '{}' is part of the passed replication strategy, but it is not "
                    "known by the passed locator::token_metadata_ptr.", dc));
        }
    }

    for (const auto& [dc, rack_map] : dc_rack_map) {
        tablet_logger.debug("[assert_rf_rack_valid_keyspace]: Verifying for '{}' / '{}'", ks, dc);

        size_t normal_rack_count = 0;
        for (const auto& [_, rack_nodes] : rack_map) {
            // We must ignore zero-token nodes because they don't take part in replication.
            // Verify that this rack has at least one normal node.
            const bool normal_rack = std::ranges::any_of(rack_nodes, [tmptr] (host_id host_id) {
                return tmptr->is_normal_token_owner(host_id);
            });
            if (normal_rack) {
                ++normal_rack_count;
            }
        }

        const size_t rf = nts.get_replication_factor(dc);

        // We must not allow for a keyspace to become RF-rack-invalid. Any attempt at that must be rejected.
        // For more context, see: scylladb/scylladb#23276.
        const bool invalid_rf = rf != normal_rack_count && rf != 1 && rf != 0;
        // Edge case: the DC in question is an arbiter DC and does NOT take part in replication.
        // Any positive RF for that DC is invalid.
        const bool invalid_arbiter_dc = normal_rack_count == 0 && rf > 0;

        if (invalid_rf || invalid_arbiter_dc) {
            throw std::invalid_argument(std::format(
                    "The option `rf_rack_valid_keyspaces` is enabled. It requires that all keyspaces are RF-rack-valid. "
                    "That condition is violated: keyspace '{}' doesn't satisfy it for DC '{}': RF={} vs. rack count={}.",
                    ks, std::string_view(dc), rf, normal_rack_count));
        }
    }

    tablet_logger.debug("[assert_rf_rack_valid_keyspace]: Keyspace '{}' has been verified to be RF-rack-valid", ks);
}

}

auto fmt::formatter<locator::resize_decision_way>::format(const locator::resize_decision_way& way, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    static const std::array<sstring, 3> index_to_string = {
        "none",
        "split",
        "merge",
    };
    static_assert(std::variant_size_v<locator::resize_decision_way> == index_to_string.size());
    return fmt::format_to(ctx.out(), "{}", index_to_string[way.index()]);
}

auto fmt::formatter<locator::global_tablet_id>::format(const locator::global_tablet_id& id, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}:{}", id.table, id.tablet);
}

auto fmt::formatter<locator::tablet_transition_stage>::format(const locator::tablet_transition_stage& stage, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", locator::tablet_transition_stage_to_string(stage));
}

auto fmt::formatter<locator::tablet_transition_kind>::format(const locator::tablet_transition_kind& kind, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", locator::tablet_transition_kind_to_string(kind));
}

auto fmt::formatter<locator::tablet_task_type>::format(const locator::tablet_task_type& kind, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", locator::tablet_task_type_to_string(kind));
}

auto fmt::formatter<locator::tablet_map>::format(const locator::tablet_map& r, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    if (r.tablet_count() == 0) {
        return fmt::format_to(out, "{{}}");
    }
    out = fmt::format_to(out, "{{");
    bool first = true;
    locator::tablet_id tid = r.first_tablet();
    for (auto&& tablet : r._tablets) {
        if (!first) {
            out = fmt::format_to(out, ",");
        }
        out = fmt::format_to(out, "\n    [{}]: last_token={}, replicas={}", tid, r.get_last_token(tid), tablet.replicas);
        if (auto tr = r.get_tablet_transition_info(tid)) {
            out = fmt::format_to(out, ", stage={}, new_replicas={}, pending={}", tr->stage, tr->next, tr->pending_replica);
            if (tr->session_id) {
                out = fmt::format_to(out, ", session={}", tr->session_id);
            }
        }
        first = false;
        tid = *r.next_tablet(tid);
    }
    return fmt::format_to(out, "}}");
}

auto fmt::formatter<locator::tablet_metadata>::format(const locator::tablet_metadata& tm, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{");
    bool first = true;
    for (auto&& [id, map] : tm._tablets) {
        if (!first) {
            out = fmt::format_to(out, ",");
        }
        out = fmt::format_to(out, "\n  {}: {}", id, *map);
        first = false;
    }
    return fmt::format_to(out, "\n}}");
}

auto fmt::formatter<locator::tablet_metadata_change_hint>::format(const locator::tablet_metadata_change_hint& hint, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    auto out = ctx.out();
    out = fmt::format_to(out, "{{");
    bool first = true;
    for (auto&& [table_id, table_hint] : hint.tables) {
        if (!first) {
            out = fmt::format_to(out, ",");
        }
        out = fmt::format_to(out, "\n  [{}]: {}", table_id, table_hint.tokens);
        first = false;
    }
    return fmt::format_to(out, "\n}}");
}

auto fmt::formatter<locator::repair_scheduler_config>::format(const locator::repair_scheduler_config& config, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::map<sstring, sstring> ret{
        {"auto_repair_enabled", config.auto_repair_enabled ? "true" : "false"},
        {"auto_repair_threshold", std::to_string(config.auto_repair_threshold.count())},
    };
    return fmt::format_to(ctx.out(), "{}", rjson::print(rjson::from_string_map(ret)));
};

auto fmt::formatter<locator::tablet_task_info>::format(const locator::tablet_task_info& info, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::map<sstring, sstring> ret{
        {"request_type", fmt::to_string(info.request_type)},
        {"tablet_task_id", fmt::to_string(info.tablet_task_id)},
        {"request_time", fmt::to_string(db_clock::to_time_t(info.request_time))},
        {"sched_nr", fmt::to_string(info.sched_nr)},
        {"sched_time", fmt::to_string(db_clock::to_time_t(info.sched_time))},
        {"repair_hosts_filter", locator::tablet_task_info::serialize_repair_hosts_filter(info.repair_hosts_filter)},
        {"repair_dcs_filter", locator::tablet_task_info::serialize_repair_dcs_filter(info.repair_dcs_filter)},
    };
    return fmt::format_to(ctx.out(), "{}", rjson::print(rjson::from_string_map(ret)));
};

bool locator::tablet_task_info::is_valid() const {
    return request_type != locator::tablet_task_type::none;
}

bool locator::tablet_task_info::is_user_repair_request() const {
    return request_type == locator::tablet_task_type::user_repair;
}

bool locator::tablet_task_info::selected_by_filters(const tablet_replica& replica, const topology& topo) const {
    if (!repair_hosts_filter.empty() && !repair_hosts_filter.contains(replica.host)) {
        return false;
    }
    auto dc = topo.get_datacenter(replica.host);
    if (!repair_dcs_filter.empty() && !repair_dcs_filter.contains(dc)) {
        return false;
    }
    return true;
}

locator::tablet_task_info locator::tablet_task_info::make_auto_repair_request(std::unordered_set<locator::host_id> hosts_filter, std::unordered_set<sstring> dcs_filter) {
    long sched_nr = 0;
    auto tablet_task_id = locator::tablet_task_id(utils::UUID_gen::get_time_UUID());
    return locator::tablet_task_info{locator::tablet_task_type::auto_repair, tablet_task_id, db_clock::now(), sched_nr, db_clock::time_point(), hosts_filter, dcs_filter};
}

locator::tablet_task_info locator::tablet_task_info::make_user_repair_request(std::unordered_set<locator::host_id> hosts_filter, std::unordered_set<sstring> dcs_filter) {
    long sched_nr = 0;
    auto tablet_task_id = locator::tablet_task_id(utils::UUID_gen::get_time_UUID());
    return locator::tablet_task_info{locator::tablet_task_type::user_repair, tablet_task_id, db_clock::now(), sched_nr, db_clock::time_point(), hosts_filter, dcs_filter};
}

sstring locator::tablet_task_info::serialize_repair_hosts_filter(std::unordered_set<locator::host_id> filter) {
    sstring res = "";
    bool first = true;
    for (const auto& host : filter) {
        if (!std::exchange(first, false)) {
            res += ",";
        }
        res += host.to_sstring();
    }
    return res;
}

sstring locator::tablet_task_info::serialize_repair_dcs_filter(std::unordered_set<sstring> filter) {
    sstring res = "";
    bool first = true;
    for (const auto& dc : filter) {
        if (!std::exchange(first, false)) {
            res += ",";
        }
        res += dc;
    }
    return res;
}

std::unordered_set<locator::host_id> locator::tablet_task_info::deserialize_repair_hosts_filter(sstring filter) {
    if (filter.empty()) {
        return {};
    }
    sstring delim = ",";
    return std::ranges::views::split(filter, delim) | std::views::transform([](auto&& h) {
        return locator::host_id(utils::UUID(std::string_view{h}));
    }) | std::ranges::to<std::unordered_set>();
}

std::unordered_set<sstring> locator::tablet_task_info::deserialize_repair_dcs_filter(sstring filter) {
    if (filter.empty()) {
        return {};
    }
    sstring delim = ",";
    return std::ranges::views::split(filter, delim) | std::views::transform([](auto&& h) {
        return sstring{std::string_view{h}};
    }) | std::ranges::to<std::unordered_set>();
}

locator::tablet_task_info locator::tablet_task_info::make_migration_request() {
    long sched_nr = 0;
    auto tablet_task_id = locator::tablet_task_id(utils::UUID_gen::get_time_UUID());
    return locator::tablet_task_info{locator::tablet_task_type::migration, tablet_task_id, db_clock::now(), sched_nr, db_clock::time_point()};
}

locator::tablet_task_info locator::tablet_task_info::make_intranode_migration_request() {
    long sched_nr = 0;
    auto tablet_task_id = locator::tablet_task_id(utils::UUID_gen::get_time_UUID());
    return locator::tablet_task_info{locator::tablet_task_type::intranode_migration, tablet_task_id, db_clock::now(), sched_nr, db_clock::time_point()};
}

locator::tablet_task_info locator::tablet_task_info::make_split_request() {
    long sched_nr = 0;
    auto tablet_task_id = locator::tablet_task_id(utils::UUID_gen::get_time_UUID());
    return locator::tablet_task_info{locator::tablet_task_type::split, tablet_task_id, db_clock::now(), sched_nr, db_clock::time_point()};
}

locator::tablet_task_info locator::tablet_task_info::make_merge_request() {
    long sched_nr = 0;
    auto tablet_task_id = locator::tablet_task_id(utils::UUID_gen::get_time_UUID());
    return locator::tablet_task_info{locator::tablet_task_type::merge, tablet_task_id, db_clock::now(), sched_nr, db_clock::time_point()};
}
