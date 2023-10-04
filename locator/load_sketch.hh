/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "locator/topology.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"
#include "utils/stall_free.hh"
#include "utils/extremum_tracking.hh"
#include "utils/div_ceil.hh"

#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <absl/container/btree_set.h>

#include <optional>
#include <vector>

namespace locator {

/// A data structure which keeps track of load associated with data ownership
/// on shards of the whole cluster.
class load_sketch {
    using shard_id = seastar::shard_id;
    using load_type = ssize_t; // In tablets.

    struct shard_load {
        shard_id id;
        load_type load;
    };

    // Less-comparator which orders by load first (ascending), and then by shard id (ascending).
    struct shard_load_cmp {
        bool operator()(const shard_load& a, const shard_load& b) const {
            return a.load == b.load ? a.id < b.id : a.load < b.load;
        }
    };

    struct node_load {
        absl::btree_set<shard_load, shard_load_cmp> _shards_by_load;
        std::vector<load_type> _shards;
        load_type _load = 0;

        node_load(size_t shard_count) : _shards(shard_count) {
            for (shard_id i = 0; i < shard_count; ++i) {
                _shards[i] = 0;
            }
        }

        void update_shard_load(shard_id shard, load_type load_delta) {
            _load += load_delta;

            auto old_load = _shards[shard];
            auto new_load = old_load + load_delta;
            _shards_by_load.erase(shard_load{shard, old_load});
            _shards[shard] = new_load;
            _shards_by_load.insert(shard_load{shard, new_load});
        }

        void populate_shards_by_load() {
            _shards_by_load.clear();
            for (shard_id i = 0; i < _shards.size(); ++i) {
                _shards_by_load.insert(shard_load{i, _shards[i]});
            }
        }

        load_type& load() noexcept {
            return _load;
        }

        const load_type& load() const noexcept {
            return _load;
        }
    };
    std::unordered_map<host_id, node_load> _nodes;
    token_metadata_ptr _tm;
private:
    tablet_replica_set get_replicas_for_tablet_load(const tablet_info& ti, const tablet_transition_info* trinfo) const {
        // We reflect migrations in the load as if they already happened,
        // optimistically assuming that they will succeed.
        return trinfo ? trinfo->next : ti.replicas;
    }

    future<> populate_table(const tablet_map& tmap, std::optional<host_id> host, std::optional<sstring> only_dc) {
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
                    _nodes.emplace(replica.host, node_load{node->get_shard_count()});
                }
                node_load& n = _nodes.at(replica.host);
                if (replica.shard < n._shards.size()) {
                    n.load() += 1;
                    n._shards[replica.shard] += 1;
                    // Note: as an optimization, _shards_by_load is populated later in populate_shards_by_load()
                }
            }
            return make_ready_future<>();
        });
    }
public:
    load_sketch(token_metadata_ptr tm)
        : _tm(std::move(tm)) {
    }

    future<> populate(std::optional<host_id> host = std::nullopt,
                      std::optional<table_id> only_table = std::nullopt,
                      std::optional<sstring> only_dc = std::nullopt) {
        co_await utils::clear_gently(_nodes);

        if (only_table) {
            auto& tmap = _tm->tablets().get_tablet_map(*only_table);
            co_await populate_table(tmap, host, only_dc);
        } else {
            for (auto&& [table, tmap]: _tm->tablets().all_tables()) {
                co_await populate_table(*tmap, host, only_dc);
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
            auto [i, _] = _nodes.emplace(node, node_load{shard_count});
            i->second.populate_shards_by_load();
        }
        return _nodes.at(node);
    }

    shard_id get_least_loaded_shard(host_id node) {
        auto& n = ensure_node(node);
        const shard_load& s = *n._shards_by_load.begin();
        return s.id;
    }

    shard_id get_most_loaded_shard(host_id node) {
        auto& n = ensure_node(node);
        const shard_load& s = *std::prev(n._shards_by_load.end());
        return s.id;
    }

    void unload(host_id node, shard_id shard) {
        auto& n = _nodes.at(node);
        n.update_shard_load(shard, -1);
    }

    void pick(host_id node, shard_id shard) {
        auto& n = _nodes.at(node);
        n.update_shard_load(shard, 1);
    }

    load_type get_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        return _nodes.at(node).load();
    }

    load_type total_load() const {
        load_type total = 0;
        for (auto&& n : _nodes) {
            total += n.second.load();
        }
        return total;
    }

    load_type get_avg_shard_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        auto& n = _nodes.at(node);
        return div_ceil(n.load(), n._shards.size());
    }

    double get_real_avg_shard_load(host_id node) const {
        if (!_nodes.contains(node)) {
            return 0;
        }
        auto& n = _nodes.at(node);
        return double(n.load()) / n._shards.size();
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
    load_type get_shard_imbalance(host_id node) const {
        auto minmax = get_shard_minmax(node);
        return minmax.max() - minmax.max();
    }

    min_max_tracker<load_type> get_shard_minmax(host_id node) const {
        min_max_tracker<load_type> minmax;
        if (_nodes.contains(node)) {
            auto& n = _nodes.at(node);
            for (auto&& load: n._shards) {
                minmax.update(load);
            }
        } else {
            minmax.update(0);
        }
        return minmax;
    }
};

} // namespace locator
