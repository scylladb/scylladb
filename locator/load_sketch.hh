/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "service/tablet_allocator_fwd.hh"
#include "locator/topology.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"
#include "utils/stall_free.hh"
#include "utils/extremum_tracking.hh"
#include "utils/div_ceil.hh"
#include "utils/pretty_printers.hh"

#include <absl/container/btree_set.h>
#include <seastar/util/defer.hh>

#include <optional>
#include <vector>

namespace locator {

struct disk_usage {
    using load_type = double; // Disk usage factor (0.0 to 1.0)

    uint64_t capacity = 0;
    uint64_t used = 0;

    load_type get_load() const {
        if (capacity == 0) {
            return 0;
        }
        return load_type(used) / capacity;
    }
};

/// A data structure which keeps track of load associated with data ownership
/// on shards of the whole cluster.
class load_sketch {
    using shard_id = seastar::shard_id;
    using load_type = disk_usage::load_type;

    struct shard_load {
        shard_id id;
        disk_usage du;
        size_t tablet_count = 0;

        // Returns storage utilization for the shard
        load_type get_load() const {
            return du.get_load();
        }
    };

    // Less-comparator which orders by load first (ascending), and then by shard id (ascending).
    struct shard_load_cmp {
        bool operator()(const shard_load& shard_a, const shard_load& shard_b) const {
            auto load_a = shard_a.get_load();
            auto load_b = shard_b.get_load();
            return load_a == load_b ? shard_a.id < shard_b.id : load_a < load_b;
        }
    };

    struct node_load {
        std::vector<shard_load> _shards;
        absl::btree_set<shard_load, shard_load_cmp> _shards_by_load;
        disk_usage _du;
        size_t _tablet_count = 0;

        // These can be false only when _load_stats != nullptr
        bool _has_valid_disk_capacity = true;
        bool _has_all_tablet_sizes = true;

        node_load(size_t shard_count, uint64_t capacity)
                : _shards(shard_count) {
            _du.capacity = capacity;
            uint64_t shard_capacity = capacity / shard_count;
            for (shard_id i = 0; i < shard_count; ++i) {
                _shards[i].id = i;
                _shards[i].du.capacity = shard_capacity;
            }
        }

        void update_shard_load(shard_id shard, ssize_t tablet_count_delta, int64_t tablet_size_delta) {
            _shards_by_load.erase(_shards[shard]);
            _shards[shard].tablet_count += tablet_count_delta;
            _shards[shard].du.used += tablet_size_delta;
            _shards_by_load.insert(_shards[shard]);
            _du.used += tablet_size_delta;
            _tablet_count += tablet_count_delta;
        }

        void populate_shards_by_load() {
            _shards_by_load.clear();
            _shards_by_load.insert(_shards.begin(), _shards.end());
        }

        // Returns storage utilization for the node
        load_type get_load() const noexcept {
            return _du.get_load();
        }
    };
    std::unordered_map<host_id, node_load> _nodes;
    token_metadata_ptr _tm;
    load_stats_ptr _load_stats;
    uint64_t _default_tablet_size = service::default_target_tablet_size;
    uint64_t _minimal_tablet_size = 0;

    // When set to true, it will use gross disk capacity instead of effective_capacity and
    // treat all tablet as having the same size: _default_tablet_size
    bool _force_capacity_based_load = false;

private:
    tablet_replica_set get_replicas_for_tablet_load(const tablet_info& ti, const tablet_transition_info* trinfo) const {
        // We reflect migrations in the load as if they already happened,
        // optimistically assuming that they will succeed.
        return trinfo ? trinfo->next : ti.replicas;
    }

    std::optional<uint64_t> get_disk_capacity_for_node(host_id node) {
        if (_load_stats) {
            if (_load_stats->tablet_stats.contains(node) && !_force_capacity_based_load) {
                return _load_stats->tablet_stats.at(node).effective_capacity;
            } else if (_load_stats->capacity.contains(node)) {
                return _load_stats->capacity.at(node);
            }
        }
        return std::nullopt;
    }

    std::optional<uint64_t> get_tablet_size(host_id host, const range_based_tablet_id& rb_tid, const tablet_info& ti, const tablet_transition_info* trinfo) const {
        if (_force_capacity_based_load) {
            return _default_tablet_size;
        }

        std::optional<uint64_t> tablet_size_opt;
        if (_load_stats) {
            tablet_size_opt = _load_stats->get_tablet_size_in_transition(host, rb_tid, ti, trinfo);
        }
        return tablet_size_opt;
    }

    future<> populate_table(table_id table, const tablet_map& tmap, std::optional<host_id> host, std::optional<sstring> only_dc) {
        const topology& topo = _tm->get_topology();
        co_await tmap.for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
            auto trinfo = tmap.get_tablet_transition_info(tid);
            for (auto&& replica : get_replicas_for_tablet_load(ti, trinfo)) {
                if (host && *host != replica.host) {
                    continue;
                }
                if (!_nodes.contains(replica.host)) {
                    auto node = topo.find_node(replica.host);
                    if (only_dc && node->dc_rack().dc != *only_dc) {
                        continue;
                    }
                    auto disk_capacity_opt = get_disk_capacity_for_node(replica.host);
                    auto [i, _] = _nodes.emplace(replica.host, node_load{node->get_shard_count(), disk_capacity_opt.value_or(_default_tablet_size)});
                    if (!disk_capacity_opt && _load_stats) {
                        i->second._has_valid_disk_capacity = false;
                    }
                }
                node_load& n = _nodes.at(replica.host);
                if (replica.shard < n._shards.size()) {
                    const range_based_tablet_id rb_tid {table, tmap.get_token_range(tid)};
                    auto tablet_size_opt = get_tablet_size(replica.host, rb_tid, ti, trinfo);
                    if (!tablet_size_opt && _load_stats) {
                        n._has_all_tablet_sizes = false;
                    }
                    const uint64_t tablet_size = std::max(tablet_size_opt.value_or(_default_tablet_size), _minimal_tablet_size);
                    n._du.used += tablet_size;
                    n._tablet_count++;
                    n._shards[replica.shard].du.used += tablet_size;
                    n._shards[replica.shard].tablet_count++;
                    // Note: as an optimization, _shards_by_load is populated later in populate_shards_by_load()
                }
            }
            return make_ready_future<>();
        });
    }

    void throw_on_incomplete_data(host_id host, bool only_check_disk_capacity = false) const {
        if (!has_complete_data(host, only_check_disk_capacity)) {
            throw std::runtime_error(format("Can't provide accurate load computation with incomplete load_stats for host: {}", host));
        }
    }
public:
    load_sketch(token_metadata_ptr tm, load_stats_ptr load_stats = {}, uint64_t default_tablet_size = service::default_target_tablet_size)
        : _tm(std::move(tm))
        , _load_stats(std::move(load_stats))
        , _default_tablet_size(default_tablet_size) {
    }

    future<> populate(std::optional<host_id> host = std::nullopt,
                      std::optional<table_id> only_table = std::nullopt,
                      std::optional<sstring> only_dc = std::nullopt) {
        co_await utils::clear_gently(_nodes);

        if (host) {
            ensure_node(*host);
        } else {
            _tm->for_each_token_owner([&] (const node& n) {
                if (!only_dc || *only_dc == n.dc_rack().dc) {
                    ensure_node(n.host_id());
                }
            });
        }

        if (only_table) {
            if (_tm->tablets().has_tablet_map(*only_table)) {
                auto& tmap = _tm->tablets().get_tablet_map(*only_table);
                co_await populate_table(*only_table, tmap, host, only_dc);
            }
        } else {
            for (const auto& [table, tmap] : _tm->tablets().all_tables_ungrouped()) {
                co_await populate_table(table, *tmap, host, only_dc);
            }
        }

        for (auto&& [id, n] : _nodes) {
            n.populate_shards_by_load();
        }
    }

    future<> populate_dc(const sstring& dc) {
        return populate(std::nullopt, std::nullopt, dc);
    }

    shard_id next_shard(host_id node, size_t tablet_count, uint64_t tablet_size_sum) {
        auto shard = get_least_loaded_shard(node);
        pick(node, shard, tablet_count, tablet_size_sum);
        return shard;
    }

    bool has_complete_data(host_id node, bool only_check_disk_capacity = false) const {
        if (!_nodes.contains(node)) {
            return false;
        }
        auto& n = _nodes.at(node);
        return n._has_valid_disk_capacity && (only_check_disk_capacity || n._has_all_tablet_sizes);
    }

    void ignore_incomplete_data(host_id node) {
        if (!_nodes.contains(node)) {
            return;
        }
        auto& n = _nodes.at(node);
        n._has_valid_disk_capacity = true;
        n._has_all_tablet_sizes = true;
    }

    void set_minimal_tablet_size(uint64_t min_ts) {
        _minimal_tablet_size = min_ts;
    }

    void set_force_capacity_based_load(bool force_capacity_based_load) {
        _force_capacity_based_load = force_capacity_based_load;
    }

    node_load& ensure_node(host_id node) {
        if (!_nodes.contains(node)) {
            const topology& topo = _tm->get_topology();
            auto shard_count = topo.find_node(node)->get_shard_count();
            if (shard_count == 0) {
                throw std::runtime_error(format("Shard count not known for node {}", node));
            }
            auto disk_capacity_opt = get_disk_capacity_for_node(node);
            auto [i, _] = _nodes.emplace(node, node_load{shard_count, disk_capacity_opt.value_or(_default_tablet_size)});
            i->second.populate_shards_by_load();
            if (!disk_capacity_opt && _load_stats) {
                i->second._has_valid_disk_capacity = false;
            }
        }
        return _nodes.at(node);
    }

    shard_id get_least_loaded_shard(host_id node) {
        auto& n = ensure_node(node);
        throw_on_incomplete_data(node);
        return n._shards_by_load.begin()->id;
    }

    shard_id get_most_loaded_shard(host_id node) {
        auto& n = ensure_node(node);
        throw_on_incomplete_data(node);
        return std::prev(n._shards_by_load.end())->id;
    }

    void unload(host_id node, shard_id shard, size_t tablet_count_delta, uint64_t tablet_sizes_delta) {
        throw_on_incomplete_data(node);
        auto& n = _nodes.at(node);
        n.update_shard_load(shard, -ssize_t(tablet_count_delta), -int64_t(tablet_sizes_delta));
    }

    void pick(host_id node, shard_id shard, size_t tablet_count_delta, uint64_t tablet_sizes_delta) {
        throw_on_incomplete_data(node);
        auto& n = _nodes.at(node);
        n.update_shard_load(shard, tablet_count_delta, tablet_sizes_delta);
    }

    load_type get_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        throw_on_incomplete_data(node);
        return _nodes.at(node).get_load();
    }

    uint64_t get_tablet_count(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        return _nodes.at(node)._tablet_count;
    }

    uint64_t get_avg_tablet_count(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        auto& n = _nodes.at(node);
        return div_ceil(n._tablet_count, n._shards.size());
    }

    double get_real_avg_tablet_count(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        auto& n = _nodes.at(node);
        return double(n._tablet_count) / n._shards.size();
    }

    uint64_t get_disk_used(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        throw_on_incomplete_data(node);
        return _nodes.at(node)._du.used;
    }

    uint64_t get_capacity(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        throw_on_incomplete_data(node, true);
        return _nodes.at(node)._du.capacity;
    }

    shard_id get_shard_count(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        return _nodes.at(node)._shards.size();
    }

    // Returns the difference in tablet count between highest-loaded shard and lowest-loaded shard.
    // Returns 0 when shards are perfectly balanced.
    // Returns 1 when shards are imbalanced, but it's not possible to balance them.
    size_t get_shard_tablet_count_imbalance(host_id node) const {
        auto minmax = get_shard_minmax_tablet_count(node);
        return minmax.max() - minmax.min();
    }

    min_max_tracker<load_type> get_shard_minmax(host_id node) const {
        min_max_tracker<load_type> minmax;
        if (_nodes.contains(node)) {
            throw_on_incomplete_data(node);
            auto& n = _nodes.at(node);
            for (auto&& shard: n._shards) {
                minmax.update(shard.get_load());
            }
        } else {
            minmax.update(0);
        }
        return minmax;
    }

    min_max_tracker<size_t> get_shard_minmax_tablet_count(host_id node) const {
        min_max_tracker<size_t> minmax;
        if (_nodes.contains(node)) {
            auto& n = _nodes.at(node);
            for (auto&& shard: n._shards) {
                minmax.update(shard.tablet_count);
            }
        } else {
            minmax.update(0);
        }
        return minmax;
    }

    // Returns nullopt if node is not known, or we don't have valid disk capacity.
    std::optional<load_type> get_allocated_utilization(host_id node) const {
        if (!_nodes.contains(node) || !has_complete_data(node, true)) {
            return std::nullopt;
        }
        const node_load& n = _nodes.at(node);
        return load_type(n._tablet_count * _default_tablet_size) / n._du.capacity;
    }

    // Returns nullopt if node is not known, or we don't have tablet sizes or valid disk capacity.
    std::optional<load_type> get_storage_utilization(host_id node) const {
        if (!_nodes.contains(node) || !has_complete_data(node)) {
            return std::nullopt;
        }
        return _nodes.at(node).get_load();
    }
};

} // namespace locator

template<>
struct fmt::formatter<locator::disk_usage> : fmt::formatter<string_view> {
    template <typename FormatContext>
    auto format(const locator::disk_usage& du, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "cap: {:i} used: {:i} load: {}",
                              utils::pretty_printed_data_size(du.capacity), utils::pretty_printed_data_size(du.used), du.get_load());
    }
};
