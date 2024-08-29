/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/tablets.hh"
#include "replica/tablets.hh"
#include "locator/tablet_replication_strategy.hh"
#include "replica/database.hh"
#include "service/migration_listener.hh"
#include "service/tablet_allocator.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/stall_free.hh"
#include "db/config.hh"
#include "locator/load_sketch.hh"
#include <utility>
#include <fmt/ranges.h>
#include <absl/container/flat_hash_map.h>

using namespace locator;
using namespace replica;

namespace service {

seastar::logger lblogger("load_balancer");

void load_balancer_stats_manager::setup_metrics(const dc_name& dc, load_balancer_dc_stats& stats) {
    namespace sm = seastar::metrics;
    auto dc_lb = dc_label(dc);
    _metrics.add_group(group_name, {
        sm::make_counter("calls", sm::description("number of calls to the load balancer"),
                         stats.calls)(dc_lb),
        sm::make_counter("migrations_produced", sm::description("number of migrations produced by the load balancer"),
                         stats.migrations_produced)(dc_lb),
        sm::make_counter("migrations_skipped", sm::description("number of migrations skipped by the load balancer due to load limits"),
                         stats.migrations_skipped)(dc_lb),
    });
}

void load_balancer_stats_manager::setup_metrics(const dc_name& dc, host_id node, load_balancer_node_stats& stats) {
    namespace sm = seastar::metrics;
    auto dc_lb = dc_label(dc);
    auto node_lb = node_label(node);
    _metrics.add_group(group_name, {
        sm::make_gauge("load", sm::description("node load during last load balancing"),
                       stats.load)(dc_lb)(node_lb)
    });
}

void load_balancer_stats_manager::setup_metrics(load_balancer_cluster_stats& stats) {
    namespace sm = seastar::metrics;
    // FIXME: we can probably improve it by making it per resize type (split, merge or none).
    _metrics.add_group(group_name, {
        sm::make_counter("resizes_emitted", sm::description("number of resizes produced by the load balancer"),
            stats.resizes_emitted),
        sm::make_counter("resizes_revoked", sm::description("number of resizes revoked by the load balancer"),
            stats.resizes_revoked),
        sm::make_counter("resizes_finalized", sm::description("number of resizes finalized by the load balancer"),
            stats.resizes_finalized)
    });
}

load_balancer_stats_manager::load_balancer_stats_manager(sstring group_name):
    group_name(std::move(group_name))
{
    setup_metrics(_cluster_stats);
}

load_balancer_dc_stats& load_balancer_stats_manager::for_dc(const dc_name& dc) {
    auto it = _dc_stats.find(dc);
    if (it == _dc_stats.end()) {
        auto stats = std::make_unique<load_balancer_dc_stats>();
        setup_metrics(dc, *stats);
        it = _dc_stats.emplace(dc, std::move(stats)).first;
    }
    return *it->second;
}

load_balancer_node_stats& load_balancer_stats_manager::for_node(const dc_name& dc, host_id node) {
    auto it = _node_stats.find(node);
    if (it == _node_stats.end()) {
        auto stats = std::make_unique<load_balancer_node_stats>();
        setup_metrics(dc, node, *stats);
        it = _node_stats.emplace(node, std::move(stats)).first;
    }
    return *it->second;
}

load_balancer_cluster_stats& load_balancer_stats_manager::for_cluster() {
    return _cluster_stats;
}

void load_balancer_stats_manager::unregister() {
    _metrics.clear();
}

// Used to compare different migration choices in regard to impact on load imbalance.
// There is a total order on migration_badness such that better migrations are ordered before worse ones.
struct migration_badness {
    double src_shard_badness = 0;
    double src_node_badness = 0;
    double dst_shard_badness = 0;
    double dst_node_badness = 0;
    bool bad;

    migration_badness()
        : bad(false)
    {}

    migration_badness(double src_shard_badness, double src_node_badness, double dst_shard_badness, double dst_node_badness)
        : src_shard_badness(src_shard_badness)
        , src_node_badness(src_node_badness)
        , dst_shard_badness(dst_shard_badness)
        , dst_node_badness(dst_node_badness)
        , bad(src_shard_badness > 0 || src_node_badness > 0 || dst_shard_badness > 0 || dst_node_badness > 0)
    {}

    double node_badness() const {
        return std::max(src_node_badness, dst_node_badness);
    }

    double shard_badness() const {
        return std::max(src_shard_badness, dst_shard_badness);
    }

    bool is_bad() const {
        return bad;
    }

    bool operator<(const migration_badness& other) const {
        // Prefer candidates with no across-node badness to those with across-node badness.
        // Then, prefer those with lowest shard badness.
        // We want to balance nodes first as balancing nodes internally between shards is cheap.
        if (node_badness() == other.node_badness()) {
            return shard_badness() < other.shard_badness();
        }
        if (node_badness() > 0 || other.node_badness() > 0) {
            return node_badness() < other.node_badness();
        }
        return shard_badness() < other.shard_badness();
    }

    bool operator<=>(const migration_badness& other) const = default;
};

struct migration_candidate {
    global_tablet_id tablet;
    tablet_replica src;
    tablet_replica dst;
    migration_badness badness;
};

}

template<>
struct fmt::formatter<service::migration_badness> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const service::migration_badness& badness, FormatContext& ctx) const {
        return fmt::format_to(ctx.out(), "{{s: {:.4f}, n: {:.4f}}}", badness.shard_badness(), badness.node_badness());
    }
};

template<>
struct fmt::formatter<service::migration_candidate> : fmt::formatter<std::string_view> {
    template <typename FormatContext>
    auto format(const service::migration_candidate& candidate, FormatContext& ctx) const {
        fmt::format_to(ctx.out(), "{{tablet: {}, {} -> {}, badness: {}", candidate.tablet, candidate.src,
                       candidate.dst, candidate.badness);
        if (candidate.badness.is_bad()) {
            fmt::format_to(ctx.out(), " (bad!)");
        }
        fmt::format_to(ctx.out(), "}}");
        return ctx.out();
    }
};

namespace service {

/// The algorithm aims to equalize tablet count on each shard.
/// This goal is based on the assumption that every shard has similar processing power and space capacity,
/// and that each tablet has equal consumption of those resources. So by equalizing tablet count per shard we
/// equalize resource utilization.
///
/// The algorithm produces a migration plan which is a set of instructions about which tablets to move
/// where. The plan is a small increment, not a complete plan. To achieve balance, the algorithm should
/// be invoked iteratively until an empty plan is returned.
///
/// The algorithm keeps track of load at two levels, per node and per shard. The reason for this is that
/// we want to equalize the per-node score first, by moving tablets across nodes. Tablets are moved away
/// from the most loaded node first. We also track load per shard, so that we move tablets from the most
/// loaded shard on a given node first.
///
/// The metric for node load is (number of tablets / shard count) which is the average
/// per-shard load. If we achieve balance according to this metric, and then rebalance the nodes internally,
/// we will achieve global balance on all shards in the cluster.
///
/// The reason why we focus on nodes first before rebalancing them internally is that this results
/// in less tablet movements than looking at shards only.
///
/// It would be still beneficial to rebalance tablet-receiving nodes internally before moving tablets
/// to them so that we can distribute load equally without overloading shards which are out of balance,
/// but this is not implemented yet.
///
/// The outline of the inter-node balancing algorithm is as follows:
///
///   1. Determine the set of nodes whose load should be balanced.
///   2. Divide the nodes into two sets, sources and destinations.
///      Tablets are only moved from sources to destinations.
///      When nodes are drained (e.g. on decommission), the drained nodes are sources and all other
///      nodes are destinations.
///      During free load balancing, we pick a single destination node which is the least loaded node
///      and all other nodes are sources.
///   3. Move tablets from sources to destinations until load order between nodes would get inverted after the movement:
///      3.1. Pick the most-loaded source node (src.host)
///           3.1.1 Pick the most-loaded shard (src.shard) on src.host
///      3.2. Pick the least-loaded destination node (dst.host)
///      3.3. Pick the least-loaded shard (dst.shard) on dst.host
///      3.4. If candidate is not chosen, pick the best candidate tablet on src to move to dst.
///      3.5. If movement impact is bad:
///           3.5.1. Consider moving from other shards on src.host and to other destination hosts and shards.
///                  Picks the best candidate according to the impact of the movement on load imbalance.
///      3.6. Evaluate collocation constraints for tablet replicas
///           3.6.1. If met, schedule migration
///           3.6.2. If not, add the tablet to the list of skipped tablets on src.host
///
///
/// Even though the algorithm focuses on a single target, the fact the the produced plan is just an increment
/// means that many under-loaded nodes can be driven forward to balance concurrently because the load balancer
/// will alternate between them across make_plan() calls.
///
/// The algorithm behaves differently when there are decommissioning nodes which have tablet replicas.
/// In this case, we move those tablets away first. The balancing works in the opposite direction.
/// Rather than picking a single least-loaded target and moving tablets into it from many sources,
/// we have a single source and move tablets to multiple targets. This process necessarily disregards
/// convergence checks, and the stop condition is that the source is drained. We still take target
/// load into consideration and pick least-loaded targets first. When draining is not possible
/// because there is no viable new replica for a tablet, load balancing will throw an exception.
///
/// After scheduling inter-node migrations, the algorithm schedules intra-node migrations.
/// This means that across-node migrations can proceed in parallel with intra-node migrations
/// if there is free capacity to carry them out, but across-node migrations have higher priority.
///
/// Intra-node migrations are scheduled for each node independently with the aim to equalize
/// per-shard tablet count on each node.
///
/// If the algorithm is called with active tablet migrations in tablet metadata, those are treated
/// by load balancer as if they were already completed. This allows the algorithm to incrementally
/// make decision which when executed with active migrations will produce the desired result.
/// Overload of shards which still contain migrated-away tablets is limited by the fact
/// that the algorithm tracks streaming concurrency on both source and target shards of active
/// migrations and takes concurrency limit into account when producing new migrations.
///
/// The cost of make_plan() is relatively heavy in terms of preparing data structures, so the current
/// implementation is not efficient if the scheduler would like to call make_plan() multiple times
/// to parallelize execution. This will be addressed in the future by keeping the data structures
/// valid across calls and only recalculating them when starting a new round with a new token metadata version.
///
class load_balancer {
    using global_shard_id = tablet_replica;
    using shard_id = seastar::shard_id;

    // Represents metric for per-node load which we want to equalize between nodes.
    // It's an average per-shard load in terms of tablet count.
    using load_type = double;

    struct shard_load {
        size_t tablet_count = 0;

        absl::flat_hash_map<table_id, size_t> tablet_count_per_table;

        // Number of tablets which are streamed from this shard.
        size_t streaming_read_load = 0;

        // Number of tablets which are streamed to this shard.
        size_t streaming_write_load = 0;

        // Tablets which still have a replica on this shard which are candidates for migrating away from this shard.
        // Grouped by table. Used when _use_table_aware_balancing == true.
        // The set of candidates per table may be empty.
        std::unordered_map<table_id, std::unordered_set<global_tablet_id>> candidates;
        // For all tables. Used when _use_table_aware_balancing == false.
        std::unordered_set<global_tablet_id> candidates_all_tables;

        future<> clear_gently() {
            co_await utils::clear_gently(candidates);
            co_await utils::clear_gently(candidates_all_tables);
        }

        bool has_candidates() const {
            for (const auto& [table, tablets] : candidates) {
                if (!tablets.empty()) {
                    return true;
                }
            }
            return !candidates_all_tables.empty();
        }

        size_t candidate_count() const {
            size_t result = 0;
            for (const auto& [table, tablets] : candidates) {
                result += tablets.size();
            }
            return result + candidates_all_tables.size();
        }
    };

    struct skipped_candidate {
        tablet_replica replica;
        global_tablet_id tablet;
        std::unordered_set<host_id> viable_targets;
    };

    struct node_load {
        host_id id;
        uint64_t shard_count = 0;
        uint64_t tablet_count = 0;
        bool drained = false;
        const locator::node* node; // never nullptr

        // The average shard load on this node.
        load_type avg_load = 0;

        absl::flat_hash_map<table_id, size_t> tablet_count_per_table;

        // heap which tracks most-loaded shards using shards_by_load_cmp().
        // Valid during intra-node plan-making for nodes which are in the source node set.
        std::vector<shard_id> shards_by_load;

        std::vector<shard_load> shards; // Indexed by shard_id to which a given shard_load corresponds.

        utils::chunked_vector<skipped_candidate> skipped_candidates;

        const sstring& dc() const {
            return node->dc_rack().dc;
        }

        const sstring& rack() const {
            return node->dc_rack().rack;
        }

        locator::node::state state() const {
            return node->get_state();
        }

        // Call when tablet_count changes.
        void update() {
            avg_load = get_avg_load(tablet_count);
        }

        load_type get_avg_load(uint64_t tablets) const {
            return double(tablets) / shard_count;
        }

        auto shards_by_load_cmp() {
            return [this] (const auto& a, const auto& b) {
                return shards[a].tablet_count < shards[b].tablet_count;
            };
        }

        future<> clear_gently() {
            co_await utils::clear_gently(shards);
            co_await utils::clear_gently(skipped_candidates);
        }
    };

    // Data structure used for making load-balancing decisions over a set of nodes.
    using node_load_map = std::unordered_map<host_id, node_load>;

    // Less-comparator which orders nodes by load.
    struct nodes_by_load_cmp {
        node_load_map& nodes;

        bool operator()(host_id a, host_id b) const {
            return nodes[a].avg_load < nodes[b].avg_load;
        }
    };

    // We have split and merge thresholds, which work respectively as (target) upper and lower
    // bound for average size of tablets.
    //
    // The merge threshold is 50% of target tablet size (a midpoint between split and merge),
    // such that after a merge, the average size is equally far from split and merge.
    // The same applies to split. It's 100% of target size, so after split, the average is
    // close to the target size (assuming small variations during the operation).
    //
    // It might happen that during a resize decision, average size changes drastically, and
    // split or merge might get cancelled. E.g. after deleting a large partition or lots of
    // data becoming suddenly expired.
    // If we're splitting, we will only cancel it, if the average size dropped below the
    // target size. That's because a merge would be required right after split completes,
    // due to the average size dropping below the merge threshold, as tablet count doubles.
    const uint64_t _target_tablet_size = default_target_tablet_size;

    static constexpr uint64_t target_max_tablet_size(uint64_t target_tablet_size) {
        return target_tablet_size * 2;
    }
    static constexpr uint64_t target_min_tablet_size(uint64_t max_tablet_size) {
        return double(max_tablet_size / 2) * 0.5;
    }

    struct table_size_desc {
        uint64_t target_max_tablet_size;
        uint64_t avg_tablet_size;
        locator::resize_decision resize_decision;
        size_t tablet_count;
        size_t shard_count;

        uint64_t target_min_tablet_size() const noexcept {
            return load_balancer::target_min_tablet_size(target_max_tablet_size);
        }
    };

    struct cluster_resize_load {
        using table_id_and_size_desc = std::pair<table_id, table_size_desc>;
        std::vector<table_id_and_size_desc> tables_need_resize;
        std::vector<table_id_and_size_desc> tables_being_resized;

        static bool table_needs_merge(const table_size_desc& d) {
            // FIXME: ignore merge request if tablet_count == initial_tablets.
            return d.tablet_count > 1 && d.avg_tablet_size < d.target_min_tablet_size();
        }
        static bool table_needs_split(const table_size_desc& d) {
            return d.avg_tablet_size > d.target_max_tablet_size;
        }

        bool table_needs_resize(const table_size_desc& d) const {
            return table_needs_merge(d) || table_needs_split(d);
        }

        // Resize cancellation will account for possible oscillations caused by compaction, etc.
        // We shouldn't rush into cancelling an ongoing resize. That will only happen if the
        // average size is past the point it would be if either split or merge had completed.
        // If we cancel a split, that's because average size dropped so much a merge would be
        // required post completion, and vice-versa.
        bool table_needs_resize_cancellation(const table_size_desc& d) const {
            auto& way = d.resize_decision.way;
            if (std::holds_alternative<locator::resize_decision::split>(way)) {
                return d.avg_tablet_size < d.target_max_tablet_size / 2;
            } else if (std::holds_alternative<locator::resize_decision::merge>(way)) {
                return d.avg_tablet_size > d.target_min_tablet_size() * 2;
            }
            return false;
        }

        void update(table_id id, table_size_desc d) {
            bool table_undergoing_resize = d.resize_decision.split_or_merge();

            // Resizing tables that no longer need resize will have the resize decision revoked,
            // therefore they must be listed as being resized.
            if (!table_needs_resize(d) && !table_undergoing_resize) {
                return;
            }

            auto entry = std::make_pair(id, std::move(d));
            if (table_undergoing_resize) {
                tables_being_resized.push_back(entry);
            } else {
                tables_need_resize.push_back(entry);
            }
        }

        // Comparator that measures the weight of the need for resizing.
        auto resize_urgency_cmp() const {
            return [] (const table_id_and_size_desc& a, const table_id_and_size_desc& b) {
                auto urgency = [] (const table_size_desc& d) -> double {
                    // FIXME: only takes into account split today.
                    return double(d.avg_tablet_size) / d.target_max_tablet_size;
                };
                return urgency(a.second) < urgency(b.second);
            };
        }

        static locator::resize_decision to_resize_decision(const table_size_desc& d) {
            locator::resize_decision decision;
            if (table_needs_split(d)) {
                decision.way = locator::resize_decision::split{};
            } else if (table_needs_merge(d)) {
                decision.way = locator::resize_decision::merge{};
            }
            return decision;
        }

        // Resize decisions can be revoked with an empty (none) decision, so replicas
        // will know they're no longer required to prepare storage for the execution of
        // topology changes.
        static locator::resize_decision revoke_resize_decision() {
            return locator::resize_decision{};
        }
    };

    // Per-shard limits for active tablet streaming sessions.
    //
    // There is no hard reason for these values being what they are other than
    // the guidelines below.
    //
    // We want to limit concurrency of active streaming for several reasons.
    // One is that we want to prevent over-utilization of memory required to carry out streaming,
    // as that may lead to OOM or excessive cache eviction.
    //
    // There is no network scheduler yet, so we want to avoid over-utilization of network bandwidth.
    // Limiting per-shard concurrency is a lame way to achieve that, but it's better than nothing.
    //
    // Scheduling groups should limit impact of streaming on other kinds of processes on the same node,
    // so this aspect is not the reason for limiting concurrency.
    //
    // We don't want too much parallelism because it means that we have plenty of migrations
    // which progress slowly. It's better to have fewer which complete faster because
    // less user requests suffer from double-quorum overhead, and under-loaded nodes can take
    // the load sooner. At the same time, we want to have enough concurrency to fully utilize resources.
    //
    // Streaming speed is supposed to be I/O bound and writes are more expensive in terms of IO than reads,
    // so we allow more read concurrency.
    //
    // We allow at least two sessions per shard so that there is less chance for idling until load balancer
    // makes the next decision after streaming is finished.
    const size_t max_write_streaming_load = 2;
    const size_t max_read_streaming_load = 4;

    token_metadata_ptr _tm;
    std::optional<locator::load_sketch> _load_sketch;
    absl::flat_hash_map<table_id, size_t> _tablet_count_per_table;
    dc_name _dc;
    size_t _total_capacity_shards; // Total number of non-drained shards in the balanced node set.
    size_t _total_capacity_nodes; // Total number of non-drained nodes in the balanced node set.
    locator::load_stats_ptr _table_load_stats;
    load_balancer_stats_manager& _stats;
    std::unordered_set<host_id> _skiplist;
    bool _use_table_aware_balancing = true;
private:
    tablet_replica_set get_replicas_for_tablet_load(const tablet_info& ti, const tablet_transition_info* trinfo) const {
        // We reflect migrations in the load as if they already happened,
        // optimistically assuming that they will succeed.
        return trinfo ? trinfo->next : ti.replicas;
    }

    // Whether to count the tablet as putting streaming load on the system.
    // Tablets which are streaming or are yet-to-stream are counted.
    bool is_streaming(const tablet_transition_info* trinfo) {
        if (!trinfo) {
            return false;
        }
        switch (trinfo->stage) {
            case tablet_transition_stage::allow_write_both_read_old:
                return true;
            case tablet_transition_stage::write_both_read_old:
                return true;
            case tablet_transition_stage::streaming:
                return true;
            case tablet_transition_stage::write_both_read_new:
                return false;
            case tablet_transition_stage::use_new:
                return false;
            case tablet_transition_stage::cleanup:
                return false;
            case tablet_transition_stage::cleanup_target:
                return false;
            case tablet_transition_stage::revert_migration:
                return false;
            case tablet_transition_stage::end_migration:
                return false;
        }
        on_internal_error(lblogger, format("Invalid transition stage: {}", static_cast<int>(trinfo->stage)));
    }

public:
    load_balancer(token_metadata_ptr tm, locator::load_stats_ptr table_load_stats, load_balancer_stats_manager& stats, uint64_t target_tablet_size, std::unordered_set<host_id> skiplist)
        : _target_tablet_size(target_tablet_size)
        , _tm(std::move(tm))
        , _table_load_stats(std::move(table_load_stats))
        , _stats(stats)
        , _skiplist(std::move(skiplist))
    { }

    future<migration_plan> make_plan() {
        const locator::topology& topo = _tm->get_topology();
        migration_plan plan;

        // Prepare plans for each DC separately and combine them to be executed in parallel.
        for (auto&& dc : topo.get_datacenters()) {
            auto dc_plan = co_await make_plan(dc);
            lblogger.info("Prepared {} migrations in DC {}", dc_plan.size(), dc);
            plan.merge(std::move(dc_plan));
        }
        plan.set_resize_plan(co_await make_resize_plan());

        lblogger.info("Prepared {} migration plans, out of which there were {} tablet migration(s) and {} resize decision(s)",
                      plan.size(), plan.tablet_migration_count(), plan.resize_decision_count());
        co_return std::move(plan);
    }

    void set_use_table_aware_balancing(bool use_table_aware_balancing) {
        _use_table_aware_balancing = use_table_aware_balancing;
    }

    const locator::table_load_stats* load_stats_for_table(table_id id) const {
        if (!_table_load_stats) {
            return nullptr;
        }
        auto it = _table_load_stats->tables.find(id);
        return (it != _table_load_stats->tables.end()) ? &it->second : nullptr;
    }

    future<table_resize_plan> make_resize_plan() {
        table_resize_plan resize_plan;

        if (!_tm->tablets().balancing_enabled()) {
            co_return std::move(resize_plan);
        }

        cluster_resize_load resize_load;

        for (auto&& [table, tmap_] : _tm->tablets().all_tables()) {
            auto& tmap = *tmap_;

            const auto* table_stats = load_stats_for_table(table);
            if (!table_stats) {
                continue;
            }

            auto avg_tablet_size = table_stats->size_in_bytes / std::max(tmap.tablet_count(), size_t(1));
            // shard presence of a table across the cluster
            size_t shard_count = std::accumulate(tmap.tablets().begin(), tmap.tablets().end(), size_t(0),
                [] (size_t shard_count, const locator::tablet_info& info) {
                    return shard_count + info.replicas.size();
                });

            table_size_desc size_desc {
                .target_max_tablet_size = target_max_tablet_size(_target_tablet_size),
                .avg_tablet_size = avg_tablet_size,
                .resize_decision = tmap.resize_decision(),
                .tablet_count = tmap.tablet_count(),
                .shard_count = shard_count
            };

            resize_load.update(table, std::move(size_desc));
            lblogger.info("Table {} with tablet_count={} has an average tablet size of {}", table, tmap.tablet_count(), avg_tablet_size);
            co_await coroutine::maybe_yield();
        }

        // Emit new resize decisions

        // The limit of resize requests is determined by the shard presence (count) of tables involved.
        // If tables still have a low tablet count, the concurrency must be high in order to saturate the cluster.
        // If a table covers the entire cluster, and needs split, concurrency will be reduced to 1.

        size_t total_shard_count = std::invoke([this] {
            size_t shard_count = 0;
            _tm->for_each_token_owner([&] (const locator::node& node) {
                shard_count += node.get_shard_count();
            });
            return shard_count;
        });
        size_t resizing_shard_count = std::accumulate(resize_load.tables_being_resized.begin(), resize_load.tables_being_resized.end(), size_t(0),
             [] (size_t shard_count, const auto& table_desc) {
                 return shard_count + table_desc.second.shard_count;
             });
        // Limits the amount of new resize requests to be generated in a single round, as each one is a mutation to group0.
        constexpr size_t max_new_resize_requests = 10;

        auto available_shards = std::max(ssize_t(total_shard_count) - ssize_t(resizing_shard_count), ssize_t(0));

        std::make_heap(resize_load.tables_need_resize.begin(), resize_load.tables_need_resize.end(), resize_load.resize_urgency_cmp());
        while (resize_load.tables_need_resize.size() && resize_plan.size() < max_new_resize_requests) {
            const auto& [table, size_desc] = resize_load.tables_need_resize.front();

            if (resize_plan.size() > 0 && std::cmp_less(available_shards, size_desc.shard_count)) {
                break;
            }

            auto resize_decision = cluster_resize_load::to_resize_decision(size_desc);
            lblogger.info("Emitting resize decision of type {} for table {} due to avg tablet size of {}",
                          resize_decision.type_name(), table, size_desc.avg_tablet_size);
            resize_plan.resize[table] = std::move(resize_decision);
            _stats.for_cluster().resizes_emitted++;

            std::pop_heap(resize_load.tables_need_resize.begin(), resize_load.tables_need_resize.end(), resize_load.resize_urgency_cmp());
            resize_load.tables_need_resize.pop_back();

            available_shards -= size_desc.shard_count;
        }

        // Revoke resize decision if any table no longer needs it
        // Also communicate coordinator if any table is ready for finalizing resizing

        for (const auto& [table, size_desc] : resize_load.tables_being_resized) {
            if (resize_load.table_needs_resize_cancellation(size_desc)) {
                resize_plan.resize[table] = cluster_resize_load::revoke_resize_decision();
                _stats.for_cluster().resizes_revoked++;
                lblogger.info("Revoking resize decision for table {} due to avg tablet size of {}", table, size_desc.avg_tablet_size);
                continue;
            }

            auto& tmap = _tm->tablets().get_tablet_map(table);

            const auto* table_stats = load_stats_for_table(table);
            if (!table_stats) {
                continue;
            }

            // If all replicas have completed split work for the current sequence number, it means that
            // load balancer can emit finalize decision, for split to be completed.
            if (table_stats->split_ready_seq_number == tmap.resize_decision().sequence_number) {
                _stats.for_cluster().resizes_finalized++;
                resize_plan.finalize_resize.insert(table);
                lblogger.info("Finalizing resize decision for table {} as all replicas agree on sequence number {}",
                              table, table_stats->split_ready_seq_number);
            }
        }

        co_return std::move(resize_plan);
    }

    void apply_load(node_load_map& nodes, const tablet_migration_streaming_info& info) {
        for (auto&& replica : info.read_from) {
            if (nodes.contains(replica.host)) {
                nodes[replica.host].shards[replica.shard].streaming_read_load += 1;
            }
        }
        for (auto&& replica : info.written_to) {
            if (nodes.contains(replica.host)) {
                nodes[replica.host].shards[replica.shard].streaming_write_load += 1;
            }
        }
    }

    bool can_accept_load(node_load_map& nodes, const tablet_migration_streaming_info& info) {
        for (auto r : info.read_from) {
            if (!nodes.contains(r.host)) {
                continue;
            }
            auto load = nodes[r.host].shards[r.shard].streaming_read_load;
            if (load >= max_read_streaming_load) {
                lblogger.debug("Migration skipped because of read load limit on {} ({})", r, load);
                return false;
            }
        }
        for (auto r : info.written_to) {
            if (!nodes.contains(r.host)) {
                continue;
            }
            auto load = nodes[r.host].shards[r.shard].streaming_write_load;
            if (load >= max_write_streaming_load) {
                lblogger.debug("Migration skipped because of write load limit on {} ({})", r, load);
                return false;
            }
        }
        return true;
    }

    bool in_shuffle_mode() const {
        return utils::get_local_injector().enter("tablet_allocator_shuffle");
    }

    size_t rand_int() const {
        static thread_local std::default_random_engine re{std::random_device{}()};
        static thread_local std::uniform_int_distribution<size_t> dist;
        return dist(re);
    }

    shard_id rand_shard(shard_id shard_count) const {
        return rand_int() % shard_count;
    }

    table_id pick_table(const std::unordered_map<table_id, std::unordered_set<global_tablet_id>>& candidates) {
        if (!_use_table_aware_balancing) {
            on_internal_error(lblogger, "pick_table() called when table-aware balancing is disabled");
        }
        size_t total = 0;
        for (auto&& [table, tablets] : candidates) {
            total += tablets.size();
        }
        ssize_t candidate_index = rand_int() % total;
        for (auto&& [table, tablets] : candidates) {
            candidate_index -= tablets.size();
            if (candidate_index <= 0 && !tablets.empty()) {
                return table;
            }
        }
        on_internal_error(lblogger, "No candidate table");
    }

    global_tablet_id peek_candidate(shard_load& shard_info) {
        if (_use_table_aware_balancing) {
            auto table = pick_table(shard_info.candidates);
            return *shard_info.candidates[table].begin();
        }

        return *shard_info.candidates_all_tables.begin();
    }

    // Evaluates impact on load balance of migrating a single tablet of a given table to dst.
    migration_badness evaluate_dst_badness(node_load_map& nodes, table_id table, tablet_replica dst) {
        _stats.for_dc(_dc).candidates_evaluated++;

        auto& node_info = nodes[dst.host];
        size_t total_load = _tablet_count_per_table[table];
        size_t total_shard_count = _total_capacity_shards;
        size_t node_count = _total_capacity_nodes;

        // max number of tablets per shard to keep perfect distribution.
        // Rounded up because we don't want to consider movement which is within the best possible
        // per-shard distribution as bad.
        double shard_balance_threshold = div_ceil(total_load, total_shard_count);
        auto new_shard_load = node_info.shards[dst.shard].tablet_count_per_table[table] + 1;
        auto dst_shard_badness = (new_shard_load - shard_balance_threshold) / total_load;
        lblogger.trace("Table {} @{} shard balance threshold: {}, dst: {} ({:.4f})", table, dst,
                       shard_balance_threshold, new_shard_load, dst_shard_badness);

        // max number of tablets per node to keep perfect distribution.
        double node_balance_threshold = div_ceil(total_load, node_count);
        size_t new_node_load = node_info.tablet_count_per_table[table] + 1;
        auto dst_node_badness = (new_node_load - node_balance_threshold) / total_load;
        lblogger.trace("Table {} @{} node balance threshold: {}, dst: {} ({:.4f})", table, dst,
                       node_balance_threshold, new_node_load, dst_node_badness);

        return migration_badness{0, 0, dst_shard_badness, dst_node_badness};
    }

    // Evaluates impact on load balance of migrating a single tablet of a given table from src.
    migration_badness evaluate_src_badness(node_load_map& nodes, table_id table, tablet_replica src) {
        _stats.for_dc(_dc).candidates_evaluated++;

        auto& node_info = nodes[src.host];
        size_t total_load = _tablet_count_per_table[table];
        size_t total_shard_count = _total_capacity_shards;
        size_t node_count = _total_capacity_nodes;

        // For determining impact on leaving, round down, because we don't want to consider movement which is within
        // the best possible per-shard distribution as bad.
        double leaving_shard_balance_threshold = total_load / total_shard_count;
        auto new_shard_load = node_info.shards[src.shard].tablet_count_per_table[table] - 1;

        auto src_shard_badness = node_info.drained
                ? 0 // Moving a tablet away from a drained node is always good.
                : (leaving_shard_balance_threshold - new_shard_load) / total_load;

        lblogger.trace("Table {} @{} shard balance threshold: {}, src: {} ({:.4f})", table, src,
                       leaving_shard_balance_threshold, new_shard_load, src_shard_badness);

        // max number of tablets per node to keep perfect distribution.
        double leaving_node_balance_threshold = total_load / node_count;
        size_t new_node_load = node_info.tablet_count_per_table[table] - 1;

        auto src_node_badness = node_info.drained
                ? 0 // Moving a tablet away from a drained node is always good.
                : (leaving_node_balance_threshold - new_node_load) / total_load;

        lblogger.trace("Table {} @{} node balance threshold: {}, src: {} ({:.4f})", table, src,
                       leaving_node_balance_threshold, new_node_load, src_node_badness);

        return migration_badness{src_shard_badness, src_node_badness, 0, 0};
    }

    // Evaluates impact on load balance of migrating a single tablet of a given table from src to dst.
    migration_badness evaluate_candidate(node_load_map& nodes, table_id table, tablet_replica src, tablet_replica dst) {
        auto src_badness = evaluate_src_badness(nodes, table, src);
        auto dst_badness = evaluate_dst_badness(nodes, table, dst);

        if (src.host == dst.host) {
            src_badness.src_node_badness = 0;
            dst_badness.dst_node_badness = 0;
        }

        return {
            src_badness.shard_badness(),
            src_badness.node_badness(),
            dst_badness.shard_badness(),
            dst_badness.node_badness()
        };
    }

    future<migration_candidate> peek_candidate(node_load_map& nodes, shard_load& shard_info, tablet_replica src, tablet_replica dst) {
        if (!_use_table_aware_balancing) {
            co_return migration_candidate{peek_candidate(shard_info), src, dst, migration_badness{}};
        }

        if (shard_info.candidates.empty()) {
            on_internal_error(lblogger, format("No candidates for migration on {}", src));
        }

        std::optional<migration_candidate> best_candidate;

        for (auto&& [table, tablets] : shard_info.candidates) {
            if (!tablets.empty()) {
                auto badness = evaluate_candidate(nodes, table, src, dst);
                auto candidate = migration_candidate{*tablets.begin(), src, dst, badness};
                lblogger.trace("Candidate: {}", candidate);
                if (!best_candidate || candidate.badness < best_candidate->badness) {
                    best_candidate = candidate;
                }
            }
        }

        if (!best_candidate) {
            on_internal_error(lblogger, format("No candidates for migration on {}", src));
        }

        lblogger.trace("Best candidate: {}", *best_candidate);
        co_return *best_candidate;
    }

    void erase_candidate(shard_load& shard_info, global_tablet_id tablet) {
        if (_use_table_aware_balancing) {
            shard_info.candidates[tablet.table].erase(tablet);
            if (shard_info.candidates[tablet.table].empty()) {
                shard_info.candidates.erase(tablet.table);
            }
        } else {
            shard_info.candidates_all_tables.erase(tablet);
        }
    }

    void add_candidate(shard_load& shard_info, global_tablet_id tablet) {
        if (_use_table_aware_balancing) {
            shard_info.candidates[tablet.table].insert(tablet);
        } else {
            shard_info.candidates_all_tables.insert(tablet);
        }
    }

    // Checks whether moving a tablet from src_info to target_info would go against convergence.
    // Returns false if the tablet should not be moved, and true if it may be moved.
    //
    // Moving tablets only when this method returns true ensures that balancing nodes will reach convergence.
    // Otherwise, oscillations of tablet load between nodes across different plan making rounds could happen,
    // where tablets are moved back and forth between nodes and convergence is never reached.
    //
    // The assumption is that the algorithm moves tablets from more loaded nodes to less loaded nodes,
    // so convergence is reached where the node we picked as source has lower load, or will have lower
    // load post-movement, than the node we picked as the destination.
    bool check_convergence(node_load& src_info, node_load& dst_info) {
        // Allow migrating only from candidate nodes which have higher load than the target.
        if (src_info.avg_load <= dst_info.avg_load) {
            lblogger.trace("Load inversion: src={} (avg_load={}), dst={} (avg_load={})",
                           src_info.id, src_info.avg_load, dst_info.id, dst_info.avg_load);
            return false;
        }

        // Prevent load inversion post-movement which can lead to oscillations.
        if (src_info.get_avg_load(src_info.tablet_count - 1) <
            dst_info.get_avg_load(dst_info.tablet_count + 1)) {
            lblogger.trace("Load inversion post-movement: src={} (avg_load={}), dst={} (avg_load={})",
                           src_info.id, src_info.avg_load, dst_info.id, dst_info.avg_load);
            return false;
        }

        return true;
    }

    future<migration_plan> make_node_plan(node_load_map& nodes, host_id host, node_load& node_load) {
        migration_plan plan;
        const tablet_metadata& tmeta = _tm->tablets();
        bool shuffle = in_shuffle_mode();

        if (node_load.shard_count <= 1) {
            lblogger.debug("Node {} is balanced", host);
            co_return plan;
        }

        auto& sketch = *_load_sketch;

        // Keeps candidate source shards in a heap which yields highest-loaded shard first.
        std::vector<shard_id> src_shards;
        src_shards.reserve(node_load.shard_count);
        for (shard_id shard = 0; shard < node_load.shard_count; shard++) {
            src_shards.push_back(shard);
        }
        std::make_heap(src_shards.begin(), src_shards.end(), node_load.shards_by_load_cmp());

        size_t max_load = 0; // Tracks max load among shards which ran out of candidates.

        while (true) {
            co_await coroutine::maybe_yield();

            if (src_shards.empty()) {
                lblogger.debug("Unable to balance node {}: ran out of candidates, max load: {}, avg load: {}",
                               host, max_load, node_load.avg_load);
                break;
            }

            shard_id src, dst;

            // Post-conditions:
            // 1) src and dst are chosen.
            // 2) src_shards.back() == src.
            if (shuffle) {
                src = src_shards[rand_shard(src_shards.size())];
                std::swap(src_shards.back(), src_shards[src]);
                do {
                    dst = rand_shard(node_load.shard_count);
                } while (src == dst); // There are at least two shards here so this converges.
            } else {
                std::pop_heap(src_shards.begin(), src_shards.end(), node_load.shards_by_load_cmp());
                src = src_shards.back();
                dst = sketch.get_least_loaded_shard(host);
            }

            auto push_back = seastar::defer([&] {
                // When shuffling, src_shards is not a heap.
                if (!shuffle) {
                    std::push_heap(src_shards.begin(), src_shards.end(), node_load.shards_by_load_cmp());
                }
            });

            auto& src_info = node_load.shards[src];
            auto& dst_info = node_load.shards[dst];

            // Convergence check

            // When in shuffle mode, exit condition is guaranteed by running out of candidates or by load limit.
            if (!shuffle && (src == dst || src_info.tablet_count <= dst_info.tablet_count + 1)) {
                lblogger.debug("Node {} is balanced", host);
                break;
            }

            if (!src_info.has_candidates()) {
                lblogger.debug("No more candidates on shard {} of {}", src, host);
                max_load = std::max(max_load, src_info.tablet_count);
                src_shards.pop_back();
                push_back.cancel();
                continue;
            }

            auto candidate = co_await peek_candidate(nodes, src_info, tablet_replica{host, src}, tablet_replica{host, dst});
            auto tablet = candidate.tablet;

            // Emit migration.

            auto mig = tablet_migration_info {tablet_transition_kind::intranode_migration, tablet,
                                              tablet_replica{host, src}, tablet_replica{host, dst}};
            auto& tmap = tmeta.get_tablet_map(tablet.table);
            auto& src_tinfo = tmap.get_tablet_info(tablet.tablet);
            auto mig_streaming_info = get_migration_streaming_info(_tm->get_topology(), src_tinfo, mig);

            if (!can_accept_load(nodes, mig_streaming_info)) {
                _stats.for_dc(node_load.dc()).migrations_skipped++;
                lblogger.debug("Unable to balance {}: load limit reached", host);
                break;
            }

            apply_load(nodes, mig_streaming_info);
            lblogger.debug("Adding migration: {}", mig);
            _stats.for_dc(node_load.dc()).migrations_produced++;
            _stats.for_dc(node_load.dc()).intranode_migrations_produced++;
            plan.add(std::move(mig));

            for (auto&& r : src_tinfo.replicas) {
                if (nodes.contains(r.host)) {
                    erase_candidate(nodes[r.host].shards[r.shard], tablet);
                }
            }

            dst_info.tablet_count++;
            src_info.tablet_count--;
            dst_info.tablet_count_per_table[tablet.table]++;
            src_info.tablet_count_per_table[tablet.table]--;
            sketch.pick(host, dst);
            sketch.unload(host, src);
        }

        co_return plan;
    }

    future<migration_plan> make_intranode_plan(node_load_map& nodes, const std::unordered_set<host_id>& skip_nodes) {
        migration_plan plan;

        for (auto&& [host, node_load] : nodes) {
            if (skip_nodes.contains(host)) {
                lblogger.debug("Skipped balancing of node {}", host);
                continue;
            }

            plan.merge(co_await make_node_plan(nodes, host, node_load));
        }

        co_return plan;
    }

    struct skip_info {
        std::unordered_set<host_id> viable_targets;
    };

    // Verifies if moving a given tablet from src_info.id to dst_info.id would not violate
    // replication constraints (no increase in replica co-location on nodes, racks).
    // Returns std::nullopt if it does not and the movement is allowed.
    std::optional<skip_info> check_constraints(node_load_map& nodes,
                                               const locator::tablet_map& tmap,
                                               node_load& src_info,
                                               node_load& dst_info,
                                               global_tablet_id tablet,
                                               bool need_viable_targets)
    {
        int max_rack_load;
        std::unordered_map<sstring, int> rack_load;

        auto get_viable_targets = [&] () {
            std::unordered_set<host_id> viable_targets;

            for (auto&& [id, node] : nodes) {
                if (node.dc() != src_info.dc() || node.drained) {
                    continue;
                }
                viable_targets.emplace(id);
            }

            for (auto&& r : tmap.get_tablet_info(tablet.tablet).replicas) {
                viable_targets.erase(r.host);
                auto i = nodes.find(r.host);
                if (i != nodes.end()) {
                    auto& node = i->second;
                    if (node.dc() == src_info.dc()) {
                        rack_load[node.rack()] += 1;
                    }
                }
            }

            // Drop targets which would increase max rack load.

            max_rack_load = std::max_element(rack_load.begin(), rack_load.end(),
                                                  [] (auto& a, auto& b) { return a.second < b.second; })->second;

            for (auto i = viable_targets.begin(); i != viable_targets.end(); ) {
                auto target = *i;
                auto& t_info = nodes[target];
                auto old_i = i++;
                if (src_info.rack() != t_info.rack()) {
                    auto new_rack_load = rack_load[t_info.rack()] + 1;
                    if (new_rack_load > max_rack_load) {
                        viable_targets.erase(old_i);
                    }
                }
            }

            return viable_targets;
        };

        if (dst_info.rack() != src_info.rack()) {
            auto targets = get_viable_targets();
            if (!targets.contains(dst_info.id)) {
                auto new_rack_load = rack_load[dst_info.rack()] + 1;
                lblogger.debug("candidate tablet {} skipped because it would increase load on rack {} to {}, max={}",
                               tablet, dst_info.rack(), new_rack_load, max_rack_load);
                _stats.for_dc(src_info.dc()).tablets_skipped_rack++;
                return skip_info{std::move(targets)};
            }
        }

        for (auto&& r : tmap.get_tablet_info(tablet.tablet).replicas) {
            if (r.host == dst_info.id) {
                _stats.for_dc(src_info.dc()).tablets_skipped_node++;
                lblogger.debug("candidate tablet {} skipped because it has a replica on target node", tablet);
                if (need_viable_targets) {
                    return skip_info{get_viable_targets()};
                }
                return skip_info{};
            }
        }

        return std::nullopt;
    }

    // Picks best tablet replica to move and its new destination.
    // The destination host is picked among nodes_by_load_dst, with dst being the preferred destination.
    //
    // If drain_skipped is false, the replica is picked among tablets on src.host,
    // with src.shard as the preferred source shard.
    //
    // If drain_skipped is true, the chosen replica is src_node_info.skipped_candidates.back()
    // and src must match its location.
    //
    // Pre-conditions:
    //
    //   src_node_info.id == src.host
    //   target_info.id == dst.host
    //   src_node_info.shard_by_load.back() == src.shard
    //   nodes_by_load_dst.back().id == dst.host
    //
    //   if drain_skipped == true:
    //     src_node_info.skipped_candidates.back().replica = src
    //
    //   if drain_skipped == false:
    //     src_node_info.shards_by_load
    //
    // Invariants:
    //
    //   nodes_by_load_dst[:-1] is a valid heap
    //   src_node_info.shard_by_load[:-1] is a valid heap
    //
    // Post-conditions:
    //
    //   src_node_info.shard_by_load.back() == result.src.shard
    //   nodes_by_load_dst.back().id == result.dst.host
    //   result.tablet is removed from candidate lists in src_node_info.
    //
    future<migration_candidate> pick_candidate(node_load_map& nodes,
                                               node_load& src_node_info,
                                               node_load& target_info,
                                               tablet_replica src,
                                               tablet_replica dst,
                                               std::vector<host_id>& nodes_by_load_dst,
                                               bool drain_skipped)
    {
        auto get_candidate = [this, drain_skipped, &nodes, &src_node_info] (tablet_replica src, tablet_replica dst)
                -> future<migration_candidate> {
            if (drain_skipped) {
                auto source_tablet = src_node_info.skipped_candidates.back().tablet;
                auto badness = evaluate_candidate(nodes, source_tablet.table, src, dst);
                co_return migration_candidate{source_tablet, src, dst, badness};
            } else {
                auto&& src_shard_info = src_node_info.shards[src.shard];
                co_return co_await peek_candidate(nodes, src_shard_info, src, dst);
            }
        };

        migration_candidate min_candidate = co_await get_candidate(src, dst);

        // Given src as the source replica, evaluate all destinations.
        // Updates min_candidate with the best candidate, if better is found.
        auto evaluate_targets = [&] (global_tablet_id tablet, tablet_replica src, migration_badness src_badness) -> future<> {
            migration_badness min_dst_badness;
            std::optional<host_id> min_dst_host;
            std::vector<host_id> best_hosts;

            // First, find the best target nodes in terms of node badness.
            for (auto& new_target : nodes_by_load_dst) {
                co_await coroutine::maybe_yield();
                auto& new_target_info = nodes[new_target];

                // Skip movements which may harm convergence.
                if (!src_node_info.drained && !check_convergence(src_node_info, new_target_info)) {
                    continue;
                }

                auto badness = evaluate_dst_badness(nodes, tablet.table, tablet_replica{new_target, 0});
                if (!min_dst_host || badness.dst_node_badness < min_dst_badness.dst_node_badness) {
                    min_dst_badness = badness;
                    min_dst_host = new_target;
                    best_hosts.clear();
                }
                if (badness.dst_node_badness == min_dst_badness.dst_node_badness) {
                    best_hosts.push_back(new_target);
                }
            }

            if (!min_dst_host) {
                lblogger.debug("No viable targets for src node {}", src.host);
                co_return;
            }

            std::optional<tablet_replica> min_dst;

            // Find the best shards on best targets.

            for (auto host : best_hosts) {
                for (shard_id new_dst_shard = 0; new_dst_shard < nodes[host].shard_count; new_dst_shard++) {
                    co_await coroutine::maybe_yield();
                    auto new_dst = tablet_replica{host, new_dst_shard};
                    auto badness = evaluate_dst_badness(nodes, tablet.table, new_dst);
                    if (!min_dst || badness < min_dst_badness) {
                        min_dst_badness = badness;
                        min_dst = new_dst;
                    }
                }
                if (min_dst && !min_dst_badness.is_bad()) {
                    break;
                }
            }

            if (!min_dst) {
                on_internal_error(lblogger, format("No destination shards on {}", best_hosts));
            }

            auto candidate = migration_candidate{
                    tablet, src, *min_dst,
                    migration_badness{src_badness.shard_badness(),
                                      src_badness.node_badness(),
                                      min_dst_badness.shard_badness(),
                                      min_dst_badness.node_badness()}
            };

            lblogger.trace("candidate: {}", candidate);

            if (candidate.badness < min_candidate.badness) {
                min_candidate = candidate;
            }
        };

        if (min_candidate.badness.is_bad() && _use_table_aware_balancing) {
            _stats.for_dc(_dc).bad_first_candidates++;

            // Consider better alternatives.
            if (drain_skipped) {
                auto source_tablet = src_node_info.skipped_candidates.back().tablet;
                auto badness = evaluate_src_badness(nodes, source_tablet.table, src);
                co_await evaluate_targets(source_tablet, src, badness);
            } else {
                // Find a better candidate.
                // Consider different tables. For each table, first find the best source shard.
                // Then find the best target node. Then find the best shard on the target node.
                for (auto [table, load] : src_node_info.tablet_count_per_table) {
                    migration_badness min_src_badness;
                    std::optional<tablet_replica> min_src;

                    if (load == 0) {
                        lblogger.trace("No src candidates for table {} on node {}", table, src.host);
                        continue;
                    }

                    for (auto new_src_shard: src_node_info.shards_by_load) {
                        auto new_src = tablet_replica{src.host, new_src_shard};
                        if (src_node_info.shards[new_src_shard].candidates[table].empty()) {
                            lblogger.trace("No src candidates for table {} on shard {}", table, new_src);
                            continue;
                        }
                        auto badness = evaluate_src_badness(nodes, table, new_src);
                        if (!min_src || badness < min_src_badness) {
                            min_src_badness = badness;
                            min_src = new_src;
                        }
                    }

                    if (!min_src) {
                        lblogger.debug("No candidates for table {} on {}", table, src.host);
                        continue;
                    }

                    auto tablet = *src_node_info.shards[min_src->shard].candidates[table].begin();
                    co_await evaluate_targets(tablet, *min_src, min_src_badness);
                    if (!min_candidate.badness.is_bad()) {
                        break;
                    }
                }
            }
        }

        lblogger.trace("best candidate: {}", min_candidate);

        if (drain_skipped) {
            src_node_info.skipped_candidates.pop_back();
        } else {
            erase_candidate(src_node_info.shards[min_candidate.src.shard], min_candidate.tablet);
        }

        // Restore invariants.

        if (min_candidate.dst != dst) {
            lblogger.trace("dst changed.");

            if (min_candidate.dst.host != dst.host) {
                auto i = std::find(nodes_by_load_dst.begin(), nodes_by_load_dst.end(), min_candidate.dst.host);
                std::swap(*i, nodes_by_load_dst.back());

                auto nodes_dst_cmp = [cmp = nodes_by_load_cmp(nodes)] (const host_id& a, const host_id& b) {
                    return cmp(b, a);
                };
                std::make_heap(nodes_by_load_dst.begin(), std::prev(nodes_by_load_dst.end()), nodes_dst_cmp);
            }

            if (min_candidate.src.shard != src.shard) {
                lblogger.trace("src changed.");
                auto i = std::find(src_node_info.shards_by_load.begin(), src_node_info.shards_by_load.end(), min_candidate.src.shard);
                std::swap(src_node_info.shards_by_load.back(), *i);
                std::make_heap(src_node_info.shards_by_load.begin(), std::prev(src_node_info.shards_by_load.end()),
                               src_node_info.shards_by_load_cmp());
            }
        }

        co_return min_candidate;
    }

    future<> log_table_load(node_load_map& nodes, table_id table) {
        size_t total_load = 0;
        size_t shard_count = 0;
        size_t max_shard_load = 0;

        for (auto&& [host, node] : nodes) {
            if (node.drained) {
                continue;
            }
            shard_count += node.shard_count;
            size_t this_node_max_shard_load = 0;
            size_t node_load = 0;
            for (shard_id shard = 0; shard < node.shard_count; shard++) {
                co_await coroutine::maybe_yield();
                auto load = node.shards[shard].tablet_count_per_table[table];
                total_load += load;
                node_load += load;
                max_shard_load = std::max(max_shard_load, load);
                this_node_max_shard_load = std::max(this_node_max_shard_load, load);
            }
            lblogger.debug("Load on host {} for table {}: total={}, max={}", host, table, node_load, this_node_max_shard_load);
        }
        auto avg_load = double(total_load) / shard_count;
        auto overcommit = max_shard_load / avg_load;
        lblogger.debug("Table {} shard overcommit: {}", table, overcommit);
    }

    future<migration_plan> make_internode_plan(const dc_name& dc, node_load_map& nodes,
                                               const std::unordered_set<host_id>& nodes_to_drain,
                                               host_id target) {
        migration_plan plan;

        // Prepare candidate nodes and shards for heap-based balancing.

        // Any given node is either in nodes_by_load or nodes_by_load_dst, but not both.
        // This means that either of the heap needs to be updated when the node's load changes, not both.

        // heap which tracks most-loaded nodes in terms of avg_load.
        // It is used to find source tablet candidates.
        std::vector<host_id> nodes_by_load;
        nodes_by_load.reserve(nodes.size());

        // heap which tracks least-loaded nodes in terms of avg_load.
        // Used to find candidates for target nodes.
        std::vector<host_id> nodes_by_load_dst;
        nodes_by_load_dst.reserve(nodes.size());

        auto nodes_cmp = nodes_by_load_cmp(nodes);
        auto nodes_dst_cmp = [&] (const host_id& a, const host_id& b) {
            return nodes_cmp(b, a);
        };

        for (auto&& [host, node_load] : nodes) {
            if (lblogger.is_enabled(seastar::log_level::debug)) {
                shard_id shard = 0;
                for (auto&& shard_load : node_load.shards) {
                    lblogger.debug("shard {}: all tablets: {}, candidates: {}, tables: {}", tablet_replica {host, shard},
                                   shard_load.tablet_count, shard_load.candidate_count(), shard_load.tablet_count_per_table);
                    shard++;
                }
            }

            if (host != target && (nodes_to_drain.empty() || node_load.drained)) {
                nodes_by_load.push_back(host);
                std::make_heap(node_load.shards_by_load.begin(), node_load.shards_by_load.end(),
                               node_load.shards_by_load_cmp());
            } else {
                nodes_by_load_dst.push_back(host);
            }
        }

        std::make_heap(nodes_by_load.begin(), nodes_by_load.end(), nodes_cmp);
        std::make_heap(nodes_by_load_dst.begin(), nodes_by_load_dst.end(), nodes_dst_cmp);

        const tablet_metadata& tmeta = _tm->tablets();
        const locator::topology& topo = _tm->get_topology();
        load_type max_off_candidate_load = 0; // max load among nodes which ran out of candidates.
        auto batch_size = nodes[target].shard_count;
        const size_t max_skipped_migrations = nodes[target].shards.size() * 2;
        size_t skipped_migrations = 0;
        auto shuffle = in_shuffle_mode();
        while (plan.size() < batch_size) {
            co_await coroutine::maybe_yield();

            if (nodes_by_load.empty()) {
                lblogger.debug("No more candidate nodes");
                _stats.for_dc(dc).stop_no_candidates++;
                break;
            }

            // Pick source node.

            std::pop_heap(nodes_by_load.begin(), nodes_by_load.end(), nodes_cmp);
            auto src_host = nodes_by_load.back();
            auto& src_node_info = nodes[src_host];

            bool drain_skipped = src_node_info.shards_by_load.empty() && src_node_info.drained
                    && !src_node_info.skipped_candidates.empty();

            lblogger.debug("source node: {}, avg_load={:.2f}, skipped={}, drain_skipped={}", src_host,
                           src_node_info.avg_load, src_node_info.skipped_candidates.size(), drain_skipped);

            if (src_node_info.shards_by_load.empty() && !drain_skipped) {
                lblogger.debug("candidate node {} ran out of candidate shards with {} tablets remaining",
                               src_host, src_node_info.tablet_count);
                max_off_candidate_load = std::max(max_off_candidate_load, src_node_info.avg_load);
                nodes_by_load.pop_back();
                continue;
            }

            auto push_back_node_candidate = seastar::defer([&] {
                std::push_heap(nodes_by_load.begin(), nodes_by_load.end(), nodes_cmp);
            });

            tablet_replica src;

            auto push_back_shard_candidate = seastar::defer([&] {
                std::push_heap(src_node_info.shards_by_load.begin(), src_node_info.shards_by_load.end(), src_node_info.shards_by_load_cmp());
            });

            if (drain_skipped) {
                push_back_shard_candidate.cancel();
                auto& candidate = src_node_info.skipped_candidates.back();
                src = candidate.replica;
                lblogger.debug("Skipped candidate: tablet={}, replica={}, targets={}", candidate.tablet, src, candidate.viable_targets);

                // When draining, need to narrow down targets to viable targets before choosing the best target.
                nodes_by_load_dst.clear();
                for (auto&& h : candidate.viable_targets) {
                    nodes_by_load_dst.push_back(h);
                }
                std::make_heap(nodes_by_load_dst.begin(), nodes_by_load_dst.end(), nodes_dst_cmp);
            } else {
                // Pick best source shard.

                std::pop_heap(src_node_info.shards_by_load.begin(), src_node_info.shards_by_load.end(),
                              src_node_info.shards_by_load_cmp());
                auto src_shard = src_node_info.shards_by_load.back();
                src = tablet_replica {src_host, src_shard};
                auto&& src_shard_info = src_node_info.shards[src_shard];
                if (!src_shard_info.has_candidates()) {
                    lblogger.debug("shard {} ran out of candidates with {} tablets remaining.", src,
                                   src_shard_info.tablet_count);
                    src_node_info.shards_by_load.pop_back();
                    push_back_shard_candidate.cancel();
                    if (src_node_info.shards_by_load.empty()) {
                        lblogger.debug("candidate node {} ran out of candidate shards with {} tablets remaining, {} skipped.",
                                       src_host, src_node_info.tablet_count, src_node_info.skipped_candidates.size());
                    }
                    continue;
                }
            }

            // Pick best target node.

            if (nodes_by_load_dst.empty()) {
                lblogger.debug("No more target nodes");
                _stats.for_dc(dc).stop_no_candidates++;
                break;
            }

            std::pop_heap(nodes_by_load_dst.begin(), nodes_by_load_dst.end(), nodes_dst_cmp);
            target = nodes_by_load_dst.back();
            auto& target_info = nodes[target];
            auto push_back_target_node = seastar::defer([&] {
                std::push_heap(nodes_by_load_dst.begin(), nodes_by_load_dst.end(), nodes_dst_cmp);
            });

            lblogger.debug("target node: {}, avg_load={}", target, target_info.avg_load);

            // Check convergence conditions.

            // When draining nodes, disable convergence checks so that all tablets are migrated away.
            if (!shuffle && nodes_to_drain.empty()) {
                // Check if all nodes reached the same avg_load. There are three sets of nodes: target, candidates (nodes_by_load)
                // and off-candidates (removed from nodes_by_load). At any time, the avg_load for target is not greater than
                // that of any candidate, and avg_load of any candidate is not greater than that of any in the off-candidates set.
                // This is ensured by the fact that we remove candidates in the order of avg_load from the heap, and
                // because we prevent load inversion between candidate and target in the next check.
                // So the max avg_load of candidates is that of the current src_node_info, and max avg_load of off-candidates
                // is tracked in max_off_candidate_load. If max_off_candidate_load is equal to target's avg_load,
                // it means that all nodes have equal avg_load. We take the maximum with the current candidate in src_node_info
                // to handle the case of off-candidates being empty. In that case, max_off_candidate_load is 0.
                if (std::max(max_off_candidate_load, src_node_info.avg_load) == target_info.avg_load) {
                    lblogger.debug("Balance achieved.");
                    _stats.for_dc(dc).stop_balance++;
                    break;
                }

                if (!check_convergence(src_node_info, target_info)) {
                    lblogger.debug("No more candidates. Load would be inverted.");
                    _stats.for_dc(dc).stop_load_inversion++;
                    break;
                }
            }

            // Pick best target shard.

            auto dst = global_shard_id {target, _load_sketch->get_least_loaded_shard(target)};
            lblogger.trace("target shard: {}, load={}", dst.shard, target_info.shards[dst.shard].tablet_count);

            if (lblogger.is_enabled(seastar::log_level::trace)) {
                shard_id shard = 0;
                for (auto&& shard_load : target_info.shards) {
                    lblogger.trace("shard {}: all tablets: {}, candidates: {}, tables: {}", tablet_replica {dst.host, shard},
                                   shard_load.tablet_count, shard_load.candidate_count(), shard_load.tablet_count_per_table);
                    shard++;
                }
            }

            // Pick tablet movement.

            // May choose a different source shard than src.shard or different destination host/shard than dst.
            auto candidate = co_await pick_candidate(nodes, src_node_info, target_info, src, dst, nodes_by_load_dst,
                                                     drain_skipped);
            auto source_tablet = candidate.tablet;
            src = candidate.src;
            dst = candidate.dst;

            auto& tmap = tmeta.get_tablet_map(source_tablet.table);

            // Check replication strategy constraints.

            // When drain_skipped is true, we already picked movement to a viable target.
            if (!drain_skipped) {
                auto skip = check_constraints(nodes, tmap, src_node_info, nodes[dst.host], source_tablet, src_node_info.drained);
                if (skip) {
                    if (src_node_info.drained && skip->viable_targets.empty()) {
                        auto replicas = tmap.get_tablet_info(source_tablet.tablet).replicas;
                        throw std::runtime_error(format("Unable to find new replica for tablet {} on {} when draining {} (nodes {}, replicas {})",
                                                        source_tablet, src, nodes_to_drain, nodes_by_load_dst, replicas));
                    }
                    src_node_info.skipped_candidates.emplace_back(src, source_tablet, std::move(skip->viable_targets));
                    continue;
                }
            }

            if (candidate.badness.is_bad()) {
                _stats.for_dc(_dc).bad_migrations++;
            }

            if (drain_skipped) {
                _stats.for_dc(_dc).migrations_from_skiplist++;
            }

            tablet_transition_kind kind = (src_node_info.state() == locator::node::state::being_removed
                                           || src_node_info.state() == locator::node::state::left)
                       ? tablet_transition_kind::rebuild : tablet_transition_kind::migration;
            auto mig = tablet_migration_info {kind, source_tablet, src, dst};
            auto& src_tinfo = tmap.get_tablet_info(source_tablet.tablet);
            auto mig_streaming_info = get_migration_streaming_info(topo, src_tinfo, mig);

            _load_sketch->pick(dst.host, dst.shard);

            if (can_accept_load(nodes, mig_streaming_info)) {
                apply_load(nodes, mig_streaming_info);
                lblogger.debug("Adding migration: {}", mig);
                _stats.for_dc(dc).migrations_produced++;
                plan.add(std::move(mig));
            } else {
                // Shards are overloaded with streaming. Do not include the migration in the plan, but
                // continue as if it was in the hope that we will find a migration which can be executed without
                // violating the load. Next make_plan() invocation will notice that the migration was not executed.
                // We should not just stop here because that can lead to underutilization of the cluster.
                // Just because the next migration is blocked doesn't mean we could not proceed with migrations
                // for other shards which are produced by the planner subsequently.
                skipped_migrations++;
                _stats.for_dc(dc).migrations_skipped++;
                if (skipped_migrations >= max_skipped_migrations) {
                    lblogger.debug("Too many migrations skipped, aborting balancing");
                    _stats.for_dc(dc).stop_skip_limit++;
                    break;
                }
            }

            for (auto&& r : src_tinfo.replicas) {
                if (nodes.contains(r.host)) {
                    erase_candidate(nodes[r.host].shards[r.shard], source_tablet);
                }
            }

            {
                auto& target_info = nodes[dst.host];
                target_info.shards[dst.shard].tablet_count++;
                target_info.shards[dst.shard].tablet_count_per_table[source_tablet.table]++;
                target_info.tablet_count_per_table[source_tablet.table]++;
                target_info.tablet_count += 1;
                target_info.update();
            }

            auto& src_shard_info = src_node_info.shards[src.shard];
            src_shard_info.tablet_count -= 1;
            src_shard_info.tablet_count_per_table[source_tablet.table]--;
            src_node_info.tablet_count_per_table[source_tablet.table]--;

            src_node_info.tablet_count -= 1;
            src_node_info.update();
            if (src_node_info.tablet_count == 0) {
                push_back_node_candidate.cancel();
                nodes_by_load.pop_back();
            }

            if (lblogger.is_enabled(seastar::log_level::debug)) {
                co_await log_table_load(nodes, source_tablet.table);
            }
        }

        if (plan.size() == batch_size) {
            _stats.for_dc(dc).stop_batch_size++;
        }

        if (plan.empty()) {
            // Due to replica collocation constraints, it may not be possible to balance the cluster evenly.
            // For example, if nodes have different number of shards. Nodes which have more shards will be
            // replicas for more tablets which rules out more candidates on other nodes with a higher per-shard load.
            //
            // Example:
            //
            //   node1: 1 shard
            //   node2: 1 shard
            //   node3: 7 shard
            //
            // If there are 7 tablets and RF=3, each node must have 1 tablet replica.
            // So node3 will have average load of 1, and node1 and node2 will have
            // average shard load of 7.
            lblogger.info("Not possible to achieve balance.");
        }

        co_return std::move(plan);
    }

    future<migration_plan> make_plan(dc_name dc) {
        migration_plan plan;

        _dc = dc;

        // Causes load balancer to move some tablet even though load is balanced.
        auto shuffle = in_shuffle_mode();

        _stats.for_dc(dc).calls++;
        lblogger.info("Examining DC {} (shuffle={}, balancing={})", dc, shuffle, _tm->tablets().balancing_enabled());

        const locator::topology& topo = _tm->get_topology();

        // Select subset of nodes to balance.

        node_load_map nodes;
        std::unordered_set<host_id> nodes_to_drain;

        auto ensure_node = [&] (host_id host) {
            if (nodes.contains(host)) {
                return;
            }
            auto* node = topo.find_node(host);
            if (!node) {
                on_internal_error(lblogger, format("Node {} not found in topology", host));
            }
            node_load& load = nodes[host];
            load.id = host;
            load.node = node;
            load.shard_count = node->get_shard_count();
            load.shards.resize(load.shard_count);
            if (!load.shard_count) {
                throw std::runtime_error(format("Shard count of {} not found in topology", host));
            }
        };

        _tm->for_each_token_owner([&] (const locator::node& node) {
            if (node.dc_rack().dc != dc) {
                return;
            }
            bool is_drained = node.get_state() == locator::node::state::being_decommissioned
                              || node.get_state() == locator::node::state::being_removed;
            if (node.get_state() == locator::node::state::normal || is_drained) {
                if (is_drained) {
                    ensure_node(node.host_id());
                    lblogger.info("Will drain node {} ({}) from DC {}", node.host_id(), node.get_state(), dc);
                    nodes_to_drain.emplace(node.host_id());
                    nodes[node.host_id()].drained = true;
                } else if (node.is_excluded() || _skiplist.contains(node.host_id())) {
                    // Excluded nodes should not be chosen as targets for migration.
                    lblogger.debug("Ignoring excluded or dead node {}: state={}", node.host_id(), node.get_state());
                } else {
                    ensure_node(node.host_id());
                }
            }
        });

        // Compute tablet load on nodes.

        for (auto&& [table, tmap_] : _tm->tablets().all_tables()) {
            auto& tmap = *tmap_;

            co_await tmap.for_each_tablet([&, table = table] (tablet_id tid, const tablet_info& ti) -> future<> {
                auto trinfo = tmap.get_tablet_transition_info(tid);

                // Check if any replica is on a node which has left.
                // When node is replaced we don't rebuild as part of topology request.
                for (auto&& r : ti.replicas) {
                    auto* node = topo.find_node(r.host);
                    if (!node) {
                        on_internal_error(lblogger, format("Replica {} of tablet {} not found in topology",
                                                           r, global_tablet_id{table, tid}));
                    }
                    if (node->left() && node->dc_rack().dc == dc) {
                        ensure_node(r.host);
                        nodes_to_drain.insert(r.host);
                        nodes[r.host].drained = true;
                    }
                }

                // We reflect migrations in the load as if they already happened,
                // optimistically assuming that they will succeed.
                for (auto&& replica : get_replicas_for_tablet_load(ti, trinfo)) {
                    if (nodes.contains(replica.host)) {
                        nodes[replica.host].tablet_count += 1;
                        // This invariant is assumed later.
                        if (replica.shard >= nodes[replica.host].shard_count) {
                            auto gtid = global_tablet_id{table, tid};
                            on_internal_error(lblogger, format("Tablet {} replica {} targets non-existent shard", gtid, replica));
                        }
                    }
                }

                return make_ready_future<>();
            });
        }

        if (nodes.empty()) {
            lblogger.debug("No nodes to balance.");
            _stats.for_dc(dc).stop_balance++;
            co_return plan;
        }

        // Detect finished drain.

        for (auto i = nodes_to_drain.begin(); i != nodes_to_drain.end();) {
            if (nodes[*i].tablet_count == 0) {
                lblogger.info("Node {} is already drained, ignoring", *i);
                nodes.erase(*i);
                i = nodes_to_drain.erase(i);
            } else {
                ++i;
            }
        }

        plan.set_has_nodes_to_drain(!nodes_to_drain.empty());

        // Compute load imbalance.

        _total_capacity_shards = 0;
        _total_capacity_nodes = 0;
        load_type max_load = 0;
        load_type min_load = 0;
        std::optional<host_id> min_load_node = std::nullopt;
        for (auto&& [host, load] : nodes) {
            load.update();
            _stats.for_node(dc, host).load = load.avg_load;

            if (!load.drained) {
                if (!min_load_node || load.avg_load < min_load) {
                    min_load = load.avg_load;
                    min_load_node = host;
                }
                if (load.avg_load > max_load) {
                    max_load = load.avg_load;
                }
                _total_capacity_shards += load.shard_count;
                _total_capacity_nodes++;
            }
        }

        for (auto&& [host, load] : nodes) {
            lblogger.info("Node {}: rack={} avg_load={}, tablets={}, shards={}, state={}",
                          host, load.rack(), load.avg_load, load.tablet_count, load.shard_count, load.state());
        }

        if (!min_load_node) {
            lblogger.debug("No candidate nodes");
            _stats.for_dc(dc).stop_no_candidates++;
            co_return plan;
        }

        // We want to saturate the target node so we migrate several tablets in parallel, one for each shard
        // on the target node. This assumes that the target node is well-balanced and that tablet migrations
        // complete at the same time. Both assumptions are not generally true in practice, which we currently ignore.
        // But they will be true typically, because we fill shards starting from least-loaded shards,
        // so we naturally strive towards balance between shards.
        //
        // If target node is not balanced across shards, we will overload some shards. Streaming concurrency
        // will suffer because more loaded shards will not participate, which will under-utilize the node.
        // FIXME: To handle the above, we should rebalance the target node before migrating tablets from other nodes.

        // Compute per-shard load and candidate tablets.

        _load_sketch = locator::load_sketch(_tm);
        co_await _load_sketch->populate_dc(dc);
        _tablet_count_per_table.clear();

        for (auto&& [table, tmap_] : _tm->tablets().all_tables()) {
            auto& tmap = *tmap_;
            uint64_t total_load = 0;
            co_await tmap.for_each_tablet([&, table = table] (tablet_id tid, const tablet_info& ti) -> future<> {
                auto trinfo = tmap.get_tablet_transition_info(tid);

                if (is_streaming(trinfo)) {
                    apply_load(nodes, get_migration_streaming_info(topo, ti, *trinfo));
                }

                for (auto&& replica : get_replicas_for_tablet_load(ti, trinfo)) {
                    if (!nodes.contains(replica.host)) {
                        continue;
                    }
                    auto& node_load_info = nodes[replica.host];
                    shard_load& shard_load_info = node_load_info.shards[replica.shard];
                    if (shard_load_info.tablet_count == 0) {
                        node_load_info.shards_by_load.push_back(replica.shard);
                    }
                    shard_load_info.tablet_count += 1;
                    shard_load_info.tablet_count_per_table[table]++;
                    node_load_info.tablet_count_per_table[table]++;
                    total_load++;
                    if (!trinfo) { // migrating tablets are not candidates
                        add_candidate(shard_load_info, global_tablet_id {table, tid});
                    }
                }

                return make_ready_future<>();
            });
            _tablet_count_per_table[table] = total_load;
        }

        if (!nodes_to_drain.empty() || (_tm->tablets().balancing_enabled() && (shuffle || max_load != min_load))) {
            host_id target = *min_load_node;
            lblogger.info("target node: {}, avg_load: {}, max: {}", target, min_load, max_load);
            plan.merge(co_await make_internode_plan(dc, nodes, nodes_to_drain, target));
        } else {
            _stats.for_dc(dc).stop_balance++;
        }

        if (_tm->tablets().balancing_enabled()) {
            plan.merge(co_await make_intranode_plan(nodes, nodes_to_drain));
        }

        co_await utils::clear_gently(nodes);
        co_return std::move(plan);
    }
};

class tablet_allocator_impl : public tablet_allocator::impl
                            , public service::migration_listener::empty_listener {
    const tablet_allocator::config _config;
    service::migration_notifier& _migration_notifier;
    replica::database& _db;
    load_balancer_stats_manager _load_balancer_stats;
    bool _stopped = false;
    bool _use_tablet_aware_balancing = true;
public:
    tablet_allocator_impl(tablet_allocator::config cfg, service::migration_notifier& mn, replica::database& db)
            : _config(std::move(cfg))
            , _migration_notifier(mn)
            , _db(db)
            , _load_balancer_stats("load_balancer") {
        if (_config.initial_tablets_scale == 0) {
            throw std::runtime_error("Initial tablets scale must be positive");
        }
        if (db.get_config().enable_tablets()) {
            _migration_notifier.register_listener(this);
        }
    }

    tablet_allocator_impl(tablet_allocator_impl&&) = delete; // "this" captured.

    ~tablet_allocator_impl() {
        SCYLLA_ASSERT(_stopped);
    }

    future<> stop() {
        co_await _migration_notifier.unregister_listener(this);
        _stopped = true;
    }

    future<migration_plan> balance_tablets(token_metadata_ptr tm, locator::load_stats_ptr table_load_stats, std::unordered_set<host_id> skiplist) {
        load_balancer lb(tm, std::move(table_load_stats), _load_balancer_stats, _db.get_config().target_tablet_size_in_bytes(), std::move(skiplist));
        lb.set_use_table_aware_balancing(_use_tablet_aware_balancing);
        co_return co_await lb.make_plan();
    }

    void set_use_tablet_aware_balancing(bool use_tablet_aware_balancing) {
        _use_tablet_aware_balancing = use_tablet_aware_balancing;
    }

    void on_before_create_column_family(const keyspace_metadata& ksm, const schema& s, std::vector<mutation>& muts, api::timestamp_type ts) override {
        locator::replication_strategy_params params(ksm.strategy_options(), ksm.initial_tablets());
        auto rs = abstract_replication_strategy::create_replication_strategy(ksm.strategy_name(), params);
        if (auto&& tablet_rs = rs->maybe_as_tablet_aware()) {
            auto tm = _db.get_shared_token_metadata().get();
            lblogger.debug("Creating tablets for {}.{} id={}", s.ks_name(), s.cf_name(), s.id());
            auto map = tablet_rs->allocate_tablets_for_new_table(s.shared_from_this(), tm, _config.initial_tablets_scale).get();
            muts.emplace_back(tablet_map_to_mutation(map, s.id(), s.keypace_name(), s.cf_name(), ts).get());
        }
    }

    void on_before_drop_column_family(const schema& s, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(s.ks_name());
        auto&& rs = ks.get_replication_strategy();
        if (rs.uses_tablets()) {
            auto tm = _db.get_shared_token_metadata().get();
            lblogger.debug("Dropping tablets for {}.{} id={}", s.ks_name(), s.cf_name(), s.id());
            muts.emplace_back(make_drop_tablet_map_mutation(s.id(), ts));
        }
    }

    void on_before_drop_keyspace(const sstring& keyspace_name, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(keyspace_name);
        auto&& rs = ks.get_replication_strategy();
        if (rs.uses_tablets()) {
            lblogger.debug("Dropping tablets for keyspace {}", keyspace_name);
            auto tm = _db.get_shared_token_metadata().get();
            for (auto&& [name, s] : ks.metadata()->cf_meta_data()) {
                muts.emplace_back(make_drop_tablet_map_mutation(s->id(), ts));
            }
        }
    }

    void on_leadership_lost() {
        _load_balancer_stats.unregister();
    }

    load_balancer_stats_manager& stats() {
        return _load_balancer_stats;
    }

    // The splitting of tablets today is completely based on the power-of-two constraint.
    // A tablet of id X is split into 2 new tablets, which new ids are (x << 1) and
    // (x << 1) + 1.
    // So a tablet of id 0 is remapped into ids 0 and 1. Another of id 1 is remapped
    // into ids 2 and 3, and so on.
    future<tablet_map> split_tablets(token_metadata_ptr tm, table_id table) {
        auto& tablets = tm->tablets().get_tablet_map(table);

        tablet_map new_tablets(tablets.tablet_count() * 2);

        for (tablet_id tid : tablets.tablet_ids()) {
            co_await coroutine::maybe_yield();

            tablet_id new_left_tid = tablet_id(tid.value() << 1);
            tablet_id new_right_tid = tablet_id(new_left_tid.value() + 1);

            auto& tablet_info = tablets.get_tablet_info(tid);

            new_tablets.set_tablet(new_left_tid, tablet_info);
            new_tablets.set_tablet(new_right_tid, tablet_info);
        }

        lblogger.info("Split tablets for table {}, increasing tablet count from {} to {}",
                      table, tablets.tablet_count(), new_tablets.tablet_count());
        co_return std::move(new_tablets);
    }

    // FIXME: Handle materialized views.
};

tablet_allocator::tablet_allocator(config cfg, service::migration_notifier& mn, replica::database& db)
    : _impl(std::make_unique<tablet_allocator_impl>(std::move(cfg), mn, db)) {
}

future<> tablet_allocator::stop() {
    return impl().stop();
}

future<migration_plan> tablet_allocator::balance_tablets(locator::token_metadata_ptr tm, locator::load_stats_ptr load_stats, std::unordered_set<host_id> skiplist) {
    return impl().balance_tablets(std::move(tm), std::move(load_stats), std::move(skiplist));
}

void tablet_allocator::set_use_table_aware_balancing(bool use_tablet_aware_balancing) {
    impl().set_use_tablet_aware_balancing(use_tablet_aware_balancing);
}

future<locator::tablet_map> tablet_allocator::split_tablets(locator::token_metadata_ptr tm, table_id table) {
    return impl().split_tablets(std::move(tm), table);
}

tablet_allocator_impl& tablet_allocator::impl() {
    return static_cast<tablet_allocator_impl&>(*_impl);
}

void tablet_allocator::on_leadership_lost() {
    impl().on_leadership_lost();
}

load_balancer_stats_manager& tablet_allocator::stats() {
    return impl().stats();
}

}

auto fmt::formatter<service::tablet_migration_info>::format(const service::tablet_migration_info& mig, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{tablet: {}, src: {}, dst: {}}}", mig.tablet, mig.src, mig.dst);
}
