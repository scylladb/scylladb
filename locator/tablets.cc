/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/tablet_replication_strategy.hh"
#include "locator/tablets.hh"
#include "locator/tablet_metadata_guard.hh"
#include "locator/tablet_sharder.hh"
#include "locator/token_range_splitter.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"
#include "utils/stall_free.hh"
#include "gms/feature_service.hh"

#include <algorithm>
#include <iterator>

#include <fmt/ranges.h>

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>

namespace locator {

seastar::logger tablet_logger("tablets");


static
write_replica_set_selector get_selector_for_writes(tablet_transition_stage stage) {
    switch (stage) {
        case tablet_transition_stage::allow_write_both_read_old:
            return write_replica_set_selector::previous;
        case tablet_transition_stage::write_both_read_old:
            return write_replica_set_selector::both;
        case tablet_transition_stage::streaming:
            return write_replica_set_selector::both;
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
    }
    on_internal_error(tablet_logger, format("Invalid tablet transition kind: {}", static_cast<int>(trinfo.transition)));
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

const tablet_map& tablet_metadata::get_tablet_map(table_id id) const {
    try {
        return *_tablets.at(id);
    } catch (const std::out_of_range&) {
        throw_with_backtrace<std::runtime_error>(format("Tablet map not found for table {}", id));
    }
}

void tablet_metadata::mutate_tablet_map(table_id id, noncopyable_function<void(tablet_map&)> func) {
    auto it = _tablets.find(id);
    if (it == _tablets.end()) {
        throw std::runtime_error(format("Tablet map not found for table {}", id));
    }
    auto tablet_map_copy = make_lw_shared<tablet_map>(*it->second);
    func(*tablet_map_copy);
    it->second = make_foreign(lw_shared_ptr<const tablet_map>(std::move(tablet_map_copy)));
}

future<> tablet_metadata::mutate_tablet_map_async(table_id id, noncopyable_function<future<>(tablet_map&)> func) {
    auto it = _tablets.find(id);
    if (it == _tablets.end()) {
        throw std::runtime_error(format("Tablet map not found for table {}", id));
    }
    auto tablet_map_copy = make_lw_shared<tablet_map>(*it->second);
    co_await func(*tablet_map_copy);
    it->second = make_foreign(lw_shared_ptr<const tablet_map>(std::move(tablet_map_copy)));
}

future<tablet_metadata> tablet_metadata::copy() const {
    if (_tablets.empty()) {
        co_return tablet_metadata{};
    }
    tablet_metadata copy;
    for (const auto& e : _tablets) {
        copy._tablets.emplace(e.first, co_await e.second.copy());
    }

    copy._balancing_enabled = _balancing_enabled;

    co_return copy;
}

void tablet_metadata::set_tablet_map(table_id id, tablet_map map) {
    auto map_ptr = make_lw_shared<const tablet_map>(std::move(map));
    auto it = _tablets.find(id);
    if (it == _tablets.end()) {
        _tablets.emplace(id, std::move(map_ptr));
    } else {
        it->second = std::move(map_ptr);
    }
}

void tablet_metadata::drop_tablet_map(table_id id) {
    auto it = _tablets.find(id);
    if (it == _tablets.end()) {
        return;
    }
    _tablets.erase(it);
}

future<> tablet_metadata::clear_gently() {
    for (auto&& [id, map] : _tablets) {
        const auto shard = map.get_owner_shard();
        co_await smp::submit_to(shard, [map = std::move(map)] () mutable {
            auto map_ptr = map.release();
            // Others copies exist, we simply drop ours, no need to clear anything.
            if (map_ptr.use_count() > 1) {
                return make_ready_future<>();
            }
            return const_cast<tablet_map&>(*map_ptr).clear_gently().finally([map_ptr = std::move(map_ptr)] { });
        });
    }
    _tablets.clear();
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

dht::token tablet_map::get_last_token(tablet_id id) const {
    check_tablet_id(id);
    return dht::last_token_of_compaction_group(_log2_tablets, size_t(id));
}

dht::token tablet_map::get_first_token(tablet_id id) const {
    if (id == first_tablet()) {
        return dht::first_token();
    } else {
        return dht::next_token(get_last_token(tablet_id(size_t(id) - 1)));
    }
}

dht::token_range tablet_map::get_token_range(tablet_id id) const {
    if (id == first_tablet()) {
        return dht::token_range::make({dht::minimum_token(), false}, {get_last_token(id), true});
    } else {
        return dht::token_range::make({get_last_token(tablet_id(size_t(id) - 1)), false}, {get_last_token(id), true});
    }
}

tablet_replica tablet_map::get_primary_replica(tablet_id id) const {
    const auto& replicas = get_tablet_info(id).replicas;
    return replicas.at(size_t(id) % replicas.size());
}

tablet_replica tablet_map::get_primary_replica_within_dc(tablet_id id, const topology& topo, sstring dc) const {
    const auto replicas = boost::copy_range<tablet_replica_set>(get_tablet_info(id).replicas | boost::adaptors::filtered([&] (const auto& tr) {
        const auto& node = topo.get_node(tr.host);
        return node.dc_rack().dc == dc;
    }));
    return replicas.at(size_t(id) % replicas.size());
}

future<std::vector<token>> tablet_map::get_sorted_tokens() const {
    std::vector<token> tokens;
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

const locator::resize_decision& tablet_map::resize_decision() const {
    return _resize_decision;
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
    static const std::array<sstring, 3> index_to_string = {
        "none",
        "split",
        "merge",
    };
    static_assert(std::variant_size_v<decltype(way)> == index_to_string.size());
    return index_to_string[way.index()];
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

load_stats& load_stats::operator+=(const load_stats& s) {
    for (auto& [id, stats] : s.tables) {
        tables[id] += stats;
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
    for (const auto& [table_id, tmap] : tm.all_tables()) {
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
    inet_address_vector_replica_set to_replica_set(const tablet_replica_set& replicas) const {
        inet_address_vector_replica_set result;
        result.reserve(replicas.size());
        auto& topo = _tmptr->get_topology();
        for (auto&& replica : replicas) {
            auto* node = topo.find_node(replica.host);
            if (node && !node->left()) {
                result.emplace_back(node->endpoint());
            }
        }
        return result;
    }

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

    virtual host_id_vector_replica_set get_replicas(const token& search_token) const override {
        return to_host_set(get_replicas_for_write(search_token));
    }

    virtual inet_address_vector_replica_set get_natural_endpoints(const token& search_token) const override {
        return to_replica_set(get_replicas_for_write(search_token));
    }

    virtual inet_address_vector_replica_set get_natural_endpoints_without_node_being_replaced(const token& search_token) const override {
        auto result = get_natural_endpoints(search_token);
        maybe_remove_node_being_replaced(*_tmptr, *_rs, result);
        return result;
    }

    virtual future<dht::token_range_vector> get_ranges(inet_address ep) const override {
        dht::token_range_vector ret;

        auto& tablet_map = get_tablet_map();
        for (auto tablet_id : tablet_map.tablet_ids()) {
            auto endpoints = get_natural_endpoints(tablet_map.get_last_token(tablet_id));
            auto should_add_range = std::find(std::begin(endpoints), std::end(endpoints), ep) != std::end(endpoints);

            if (should_add_range) {
                ret.push_back(tablet_map.get_token_range(tablet_id));
            }
            co_await coroutine::maybe_yield();
        }

        co_return ret;
    }

    virtual inet_address_vector_topology_change get_pending_endpoints(const token& search_token) const override {
        auto&& tablets = get_tablet_map();
        auto tablet = tablets.get_tablet_id(search_token);
        auto&& info = tablets.get_tablet_transition_info(tablet);
        if (!info || info->transition == tablet_transition_kind::intranode_migration) {
            return {};
        }
        switch (info->writes) {
            case write_replica_set_selector::previous:
                return {};
            case write_replica_set_selector::both:
                if (!info->pending_replica) {
                    return {};
                }
                tablet_logger.trace("get_pending_endpoints({}): table={}, tablet={}, replica={}",
                                    search_token, _table, tablet, *info->pending_replica);
                return {_tmptr->get_endpoint_for_host_id(info->pending_replica->host)};
            case write_replica_set_selector::next:
                return {};
        }
        on_internal_error(tablet_logger, format("Invalid replica selector", static_cast<int>(info->writes)));
    }

    virtual inet_address_vector_replica_set get_endpoints_for_reading(const token& search_token) const override {
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
        auto result = to_replica_set(replicas);
        maybe_remove_node_being_replaced(*_tmptr, *_rs, result);
        return result;
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
    if (params.initial_tablets.has_value()) {
        _initial_tablets = *params.initial_tablets;
        ars._uses_tablets = true;
        mark_as_per_table(ars);
    }
}

std::unordered_set<sstring> tablet_aware_replication_strategy::recognized_tablet_options() const {
    std::unordered_set<sstring> opts;
    return opts;
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

void tablet_metadata_guard::subscribe() {
    _callback = _erm->get_validity_abort_source().subscribe([this] () noexcept {
        check();
    });
}

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
