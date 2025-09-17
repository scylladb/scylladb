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

        load_type get_load() const {
            return du.get_load();
        }
    };

    // Less-comparator which orders by load first (ascending), and then by shard id (ascending).
    struct shard_load_cmp {
        const std::vector<shard_load>& shards;
        shard_load_cmp(const std::vector<shard_load>& sl)
            : shards(sl) {
        }

        bool operator()(shard_id aid, shard_id bid) const {
            auto load_a = shards[aid].get_load();
            auto load_b = shards[bid].get_load();
            return load_a == load_b ? aid < bid : load_a < load_b;
        }
    };

    struct node_load {
        std::vector<shard_load> _shards;
        absl::btree_set<shard_id, shard_load_cmp> _shards_by_load;
        disk_usage _du;
        size_t _tablet_count = 0;

        node_load(const node_load& c)
                : _shards(c._shards)
                , _shards_by_load(shard_load_cmp(_shards))
                , _du(c._du) {
        }

        node_load(node_load&& m)
                : _shards(std::move(m._shards))
                , _shards_by_load(shard_load_cmp(_shards))
                , _du(std::move(m._du)) {
        }

        node_load(size_t shard_count, uint64_t capacity)
                : _shards(shard_count)
                , _shards_by_load(shard_load_cmp(_shards))
                , _du({capacity, 0}) {
            uint64_t shard_capacity = capacity / shard_count;
            for (shard_id i = 0; i < shard_count; ++i) {
                _shards[i].du.capacity = shard_capacity;
            }
        }

        node_load& operator=(node_load&& m) {
            _shards = std::move(m._shards);
            _du = std::move(m._du);
            return *this;
        }

        node_load& operator=(const node_load& m) {
            _shards = m._shards;
            _du = m._du;
            return *this;
        }

        void update_shard_load(shard_id shard, int count_delta, uint64_t tablet_size_delta) {
            _shards_by_load.erase(shard);
            _shards[shard].tablet_count += count_delta;
            if (count_delta > 0) {
                _shards[shard].du.used += tablet_size_delta;
            } else {
                _shards[shard].du.used -= tablet_size_delta;
            }
            _shards_by_load.insert(shard);
            _du.used += tablet_size_delta;
            _tablet_count += count_delta;
        }

        void populate_shards_by_load() {
            _shards_by_load.clear();
            for (shard_id i = 0; i < _shards.size(); ++i) {
                _shards_by_load.insert(i);
            }
        }

        load_type get_load() const noexcept {
            return _du.get_load();
        }
    };
    std::unordered_map<host_id, node_load> _nodes;
    token_metadata_ptr _tm;
    load_stats_ptr _load_stats;
    uint64_t _default_tablet_size = service::default_target_tablet_size;

private:
    tablet_replica_set get_replicas_for_tablet_load(const tablet_info& ti, const tablet_transition_info* trinfo) const {
        // We reflect migrations in the load as if they already happened,
        // optimistically assuming that they will succeed.
        return trinfo ? trinfo->next : ti.replicas;
    }

    uint64_t get_disk_capacity_for_node(host_id node) {
        if (_load_stats) {
            if (_load_stats->tablet_stats.contains(node)) {
                return _load_stats->tablet_stats.at(node).effective_capacity;
            } else if (_load_stats->capacity.contains(node)) {
                return _load_stats->capacity.at(node);
            }
        }
        return service::default_target_tablet_size;
    }

    uint64_t get_tablet_size(host_id host, const range_based_tablet_id& rb_tid) const {
        if (!_load_stats) {
            return _default_tablet_size;
        }
        uint64_t tablet_size = _load_stats->get_tablet_size(host, rb_tid).value_or(_default_tablet_size);
        return std::max(tablet_size, uint64_t(1));
    }

    future<> populate_table(table_id table, const tablet_map& tmap, std::optional<host_id> host, std::optional<sstring> only_dc) {
        const topology& topo = _tm->get_topology();
        co_await tmap.for_each_tablet([&] (tablet_id tid, const tablet_info& ti) -> future<> {
            for (auto&& replica : get_replicas_for_tablet_load(ti, tmap.get_tablet_transition_info(tid))) {
                if (host && *host != replica.host) {
                    continue;
                }
                if (!_nodes.contains(replica.host)) {
                    auto node = topo.find_node(replica.host);
                    if (only_dc && node->dc_rack().dc != *only_dc) {
                        continue;
                    }
                    _nodes.emplace(replica.host, node_load{node->get_shard_count(), get_disk_capacity_for_node(replica.host)});
                }
                node_load& n = _nodes.at(replica.host);
                if (replica.shard < n._shards.size()) {
                    const range_based_tablet_id rb_tid {table, tmap.get_token_range(tid)};
                    auto tablet_size = get_tablet_size(replica.host, rb_tid);
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

    shard_id next_shard(host_id node) {
        auto shard = get_least_loaded_shard(node);
        pick(node, shard);
        return shard;
    }

    node_load& ensure_node(host_id node) {
        if (!_nodes.contains(node)) {
            const topology& topo = _tm->get_topology();
            auto shard_count = topo.find_node(node)->get_shard_count();
            if (shard_count == 0) {
                throw std::runtime_error(format("Shard count not known for node {}", node));
            }
            auto [i, _] = _nodes.emplace(node, node_load{shard_count, get_disk_capacity_for_node(node)});
            i->second.populate_shards_by_load();
        }
        return _nodes.at(node);
    }

    shard_id get_least_loaded_shard(host_id node) {
        auto& n = ensure_node(node);
        return *n._shards_by_load.begin();
    }

    shard_id get_most_loaded_shard(host_id node) {
        auto& n = ensure_node(node);
        return *std::prev(n._shards_by_load.end());
    }

    void unload(host_id node, shard_id shard, std::optional<size_t> tablet_count = std::nullopt, std::optional<uint64_t> tablet_sizes = std::nullopt) {
        auto& n = _nodes.at(node);
        int count_delta = -int(tablet_count.value_or(1));
        n.update_shard_load(shard, count_delta, tablet_sizes.value_or(_default_tablet_size));
    }

    void pick(host_id node, shard_id shard, std::optional<size_t> tablet_count = std::nullopt, std::optional<uint64_t> tablet_sizes = std::nullopt) {
        auto& n = _nodes.at(node);
        int count_delta = int(tablet_count.value_or(1));
        n.update_shard_load(shard, count_delta, tablet_sizes.value_or(_default_tablet_size));
    }

    load_type get_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
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

    uint64_t get_disk_used(host_id node) {
        if (!_nodes.contains(node)) {
            return 0;
        }
        return _nodes.at(node)._du.used;
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

    // Returns nullopt if node is not known.
    std::optional<load_type> get_allocated_utilization(host_id node) const {
        if (!_nodes.contains(node)) {
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
