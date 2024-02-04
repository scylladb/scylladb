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
#include "utils/error_injection.hh"
#include "utils/stall_free.hh"
#include "db/config.hh"
#include "locator/load_sketch.hh"
#include <utility>

using namespace locator;
using namespace replica;

namespace service {

seastar::logger lblogger("load_balancer");

struct load_balancer_dc_stats {
    uint64_t calls = 0;
    uint64_t migrations_produced = 0;
    uint64_t migrations_skipped = 0;
    uint64_t tablets_skipped_node = 0;
    uint64_t tablets_skipped_rack = 0;
    uint64_t stop_balance = 0;
    uint64_t stop_load_inversion = 0;
    uint64_t stop_no_candidates = 0;
    uint64_t stop_skip_limit = 0;
    uint64_t stop_batch_size = 0;
};

struct load_balancer_node_stats {
    double load = 0;
};

struct load_balancer_cluster_stats {
    uint64_t resizes_emitted = 0;
    uint64_t resizes_revoked = 0;
    uint64_t resizes_finalized = 0;
};

using dc_name = sstring;

class load_balancer_stats_manager {
    std::unordered_map<dc_name, std::unique_ptr<load_balancer_dc_stats>> _dc_stats;
    std::unordered_map<host_id, std::unique_ptr<load_balancer_node_stats>> _node_stats;
    load_balancer_cluster_stats _cluster_stats;
    seastar::metrics::label dc_label{"target_dc"};
    seastar::metrics::label node_label{"target_node"};
    seastar::metrics::metric_groups _metrics;

    void setup_metrics(const dc_name& dc, load_balancer_dc_stats& stats) {
        namespace sm = seastar::metrics;
        auto dc_lb = dc_label(dc);
        _metrics.add_group("load_balancer", {
            sm::make_counter("calls", sm::description("number of calls to the load balancer"),
                             stats.calls)(dc_lb),
            sm::make_counter("migrations_produced", sm::description("number of migrations produced by the load balancer"),
                             stats.migrations_produced)(dc_lb),
            sm::make_counter("migrations_skipped", sm::description("number of migrations skipped by the load balancer due to load limits"),
                             stats.migrations_skipped)(dc_lb),
        });
    }

    void setup_metrics(const dc_name& dc, host_id node, load_balancer_node_stats& stats) {
        namespace sm = seastar::metrics;
        auto dc_lb = dc_label(dc);
        auto node_lb = node_label(node);
        _metrics.add_group("load_balancer", {
            sm::make_gauge("load", sm::description("node load during last load balancing"),
                           stats.load)(dc_lb)(node_lb)
        });
    }

    void setup_metrics(load_balancer_cluster_stats& stats) {
        namespace sm = seastar::metrics;
        // FIXME: we can probably improve it by making it per resize type (split, merge or none).
        _metrics.add_group("load_balancer", {
            sm::make_counter("resizes_emitted", sm::description("number of resizes produced by the load balancer"),
                stats.resizes_emitted),
            sm::make_counter("resizes_revoked", sm::description("number of resizes revoked by the load balancer"),
                stats.resizes_revoked),
            sm::make_counter("resizes_finalized", sm::description("number of resizes finalized by the load balancer"),
                stats.resizes_finalized)
        });
    }
public:
    load_balancer_stats_manager() {
        setup_metrics(_cluster_stats);
    }

    load_balancer_dc_stats& for_dc(const dc_name& dc) {
        auto it = _dc_stats.find(dc);
        if (it == _dc_stats.end()) {
            auto stats = std::make_unique<load_balancer_dc_stats>();
            setup_metrics(dc, *stats);
            it = _dc_stats.emplace(dc, std::move(stats)).first;
        }
        return *it->second;
    }

    load_balancer_node_stats& for_node(const dc_name& dc, host_id node) {
        auto it = _node_stats.find(node);
        if (it == _node_stats.end()) {
            auto stats = std::make_unique<load_balancer_node_stats>();
            setup_metrics(dc, node, *stats);
            it = _node_stats.emplace(node, std::move(stats)).first;
        }
        return *it->second;
    }

    load_balancer_cluster_stats& for_cluster() {
        return _cluster_stats;
    }

    void unregister() {
        _metrics.clear();
    }
};

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
/// The outline of the algorithm is as follows:
///
///   1. Determine the set of nodes whose load should be balanced.
///   2. Pick the least-loaded node (target)
///   3. Keep moving tablets to the target until balance is achieved with the highest-loaded node,
///              or we hit a limit for plan size:
///      3.1. Pick the most-loaded node (source)
///      3.2. Pick the most-loaded shard on the source
///      3.3. Pick one of the candidate tablets on the source shard
///      3.4. Evaluate collocation constraints for tablet replicas, if they pass:
///           3.4.1 Pick the least-loaded shard on the target
///           3.4.2 Generate a migration for the candidate tablet from the source shard to the target shard
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

        // Number of tablets which are streamed from this shard.
        size_t streaming_read_load = 0;

        // Number of tablets which are streamed to this shard.
        size_t streaming_write_load = 0;

        // Tablets which still have a replica on this shard which are candidates for migrating away from this shard.
        std::unordered_set<global_tablet_id> candidates;

        future<> clear_gently() {
            return utils::clear_gently(candidates);
        }
    };

    struct node_load {
        host_id id;
        uint64_t shard_count = 0;
        uint64_t tablet_count = 0;

        // The average shard load on this node.
        load_type avg_load = 0;

        std::vector<shard_id> shards_by_load; // heap which tracks most-loaded shards using shards_by_load_cmp().
        std::vector<shard_load> shards; // Indexed by shard_id to which a given shard_load corresponds.

        std::optional<locator::load_sketch> target_load_sketch;

        future<load_sketch&> get_load_sketch(const token_metadata_ptr& tm) {
            if (!target_load_sketch) {
                target_load_sketch.emplace(tm);
                co_await target_load_sketch->populate(id);
            }
            co_return *target_load_sketch;
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
            return utils::clear_gently(shards);
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
    locator::load_stats_ptr _table_load_stats;
    load_balancer_stats_manager& _stats;
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
    load_balancer(token_metadata_ptr tm, locator::load_stats_ptr table_load_stats, load_balancer_stats_manager& stats, uint64_t target_tablet_size)
        : _target_tablet_size(target_tablet_size)
        , _tm(std::move(tm))
        , _table_load_stats(std::move(table_load_stats))
        , _stats(stats)
    { }

    future<migration_plan> make_plan() {
        const locator::topology& topo = _tm->get_topology();
        migration_plan plan;

        // Prepare plans for each DC separately and combine them to be executed in parallel.
        for (auto&& dc : topo.get_datacenter_names()) {
            auto dc_plan = co_await make_plan(dc);
            lblogger.info("Prepared {} migrations in DC {}", dc_plan.size(), dc);
            plan.merge(std::move(dc_plan));
        }
        plan.set_resize_plan(co_await make_resize_plan());

        lblogger.info("Prepared {} migration plans, out of which there were {} tablet migration(s) and {} resize decision(s)",
                      plan.size(), plan.tablet_migration_count(), plan.resize_decision_count());
        co_return std::move(plan);
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
            auto& tmap = tmap_;

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

        size_t total_shard_count = std::invoke([&topo = _tm->get_topology()] {
            size_t shard_count = 0;
            topo.for_each_node([&] (const locator::node* node_ptr) {
                shard_count += node_ptr->get_shard_count();
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
                resize_plan.resize[table] = cluster_resize_load::revoke_resize_decision();
                _stats.for_cluster().resizes_finalized++;
                resize_plan.finalize_resize.insert(table);
                lblogger.info("Finalizing resize decision for table {} as all replicas agree on sequence number {}",
                              table, table_stats->split_ready_seq_number);
            }
        }

        co_return std::move(resize_plan);
    }

    future<migration_plan> make_plan(dc_name dc) {
        migration_plan plan;

        _stats.for_dc(dc).calls++;
        lblogger.info("Examining DC {}", dc);

        // Causes load balancer to move some tablet even though load is balanced.
        auto shuffle = utils::get_local_injector().enter("tablet_allocator_shuffle");
        if (shuffle) {
            lblogger.warn("Running without convergence checks");
        }

        const locator::topology& topo = _tm->get_topology();

        // Select subset of nodes to balance.

        std::unordered_map<host_id, node_load> nodes;
        std::unordered_set<host_id> nodes_to_drain;
        topo.for_each_node([&] (const locator::node* node_ptr) {
            if (node_ptr->dc_rack().dc != dc) {
                return;
            }
            bool is_drained = node_ptr->get_state() == locator::node::state::being_decommissioned
                              || node_ptr->get_state() == locator::node::state::being_removed
                              || node_ptr->get_state() == locator::node::state::being_replaced;
            if (node_ptr->get_state() == locator::node::state::normal || is_drained) {
                node_load& load = nodes[node_ptr->host_id()];
                load.id = node_ptr->host_id();
                load.shard_count = node_ptr->get_shard_count();
                load.shards.resize(load.shard_count);
                if (!load.shard_count) {
                    throw std::runtime_error(format("Shard count of {} not found in topology", node_ptr->host_id()));
                }
                if (is_drained) {
                    lblogger.info("Will drain node {} ({}) from DC {}", node_ptr->host_id(), node_ptr->get_state(), dc);
                    nodes_to_drain.emplace(node_ptr->host_id());
                } else if (node_ptr->is_excluded()) {
                    // Excluded nodes should not be chosen as targets for migration.
                    lblogger.debug("Ignoring excluded node {}: state={}", node_ptr->host_id(), node_ptr->get_state());
                    nodes.erase(node_ptr->host_id());
                }
            }
        });

        if (nodes.empty()) {
            lblogger.debug("No nodes to balance.");
            _stats.for_dc(dc).stop_balance++;
            co_return plan;
        }

        // Compute tablet load on nodes.

        for (auto&& [table, tmap_] : _tm->tablets().all_tables()) {
            auto& tmap = tmap_;

            co_await tmap.for_each_tablet([&, table = table] (tablet_id tid, const tablet_info& ti) -> future<> {
                auto trinfo = tmap.get_tablet_transition_info(tid);

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

        load_type max_load = 0;
        load_type min_load = 0;
        std::optional<host_id> min_load_node = std::nullopt;
        for (auto&& [host, load] : nodes) {
            load.update();
            _stats.for_node(dc, host).load = load.avg_load;

            if (!nodes_to_drain.contains(host)) {
                if (!min_load_node || load.avg_load < min_load) {
                    min_load = load.avg_load;
                    min_load_node = host;
                }
                if (load.avg_load > max_load) {
                    max_load = load.avg_load;
                }
            }
        }

        for (auto&& [host, load] : nodes) {
            auto& node = topo.get_node(host);
            lblogger.info("Node {}: rack={} avg_load={}, tablets={}, shards={}, state={}",
                          host, node.dc_rack().rack, load.avg_load, load.tablet_count, load.shard_count, node.get_state());
        }

        if (!min_load_node) {
            lblogger.debug("No candidate nodes");
            _stats.for_dc(dc).stop_no_candidates++;
            co_return plan;
        }

        if (nodes_to_drain.empty()) {
            if (!shuffle && (max_load == min_load || !_tm->tablets().balancing_enabled())) {
                // load is balanced.
                // TODO: Evaluate and fix intra-node balance.
                _stats.for_dc(dc).stop_balance++;
                co_return plan;
            }
            lblogger.info("target node: {}, avg_load: {}, max: {}", *min_load_node, min_load, max_load);
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

        auto target = *min_load_node;
        auto batch_size = nodes[target].shard_count;

        // Compute per-shard load and candidate tablets.

        auto apply_load = [&] (const tablet_migration_streaming_info& info) {
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
        };

        auto can_accept_load = [&] (const tablet_migration_streaming_info& info) {
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
        };

        for (auto&& [table, tmap_] : _tm->tablets().all_tables()) {
            auto& tmap = tmap_;
            co_await tmap.for_each_tablet([&, table = table] (tablet_id tid, const tablet_info& ti) -> future<> {
                auto trinfo = tmap.get_tablet_transition_info(tid);

                if (is_streaming(trinfo)) {
                    apply_load(get_migration_streaming_info(topo, ti, *trinfo));
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
                    if (!trinfo) { // migrating tablets are not candidates
                        shard_load_info.candidates.emplace(global_tablet_id {table, tid});
                    }
                }

                return make_ready_future<>();
            });
        }

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

        auto nodes_cmp = [&] (const host_id& a, const host_id& b) {
            return nodes[a].avg_load < nodes[b].avg_load;
        };
        auto nodes_dst_cmp = [&] (const host_id& a, const host_id& b) {
            return nodes_cmp(b, a);
        };

        for (auto&& [host, node_load] : nodes) {
            if (lblogger.is_enabled(seastar::log_level::debug)) {
                shard_id shard = 0;
                for (auto&& shard_load : node_load.shards) {
                    lblogger.debug("shard {}: all tablets: {}, candidates: {}", tablet_replica{host, shard},
                                   shard_load.tablet_count, shard_load.candidates.size());
                    shard++;
                }
            }

            if (host != target && (nodes_to_drain.empty() || nodes_to_drain.contains(host))) {
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
        load_type max_off_candidate_load = 0; // max load among nodes which ran out of candidates.
        const size_t max_skipped_migrations = nodes[target].shards.size() * 2;
        size_t skipped_migrations = 0;
        while (plan.size() < batch_size) {
            co_await coroutine::maybe_yield();

            // Pick a source tablet.

            if (nodes_by_load.empty()) {
                lblogger.debug("No more candidate nodes");
                _stats.for_dc(dc).stop_no_candidates++;
                break;
            }

            std::pop_heap(nodes_by_load.begin(), nodes_by_load.end(), nodes_cmp);
            auto src_host = nodes_by_load.back();
            auto& src_node_info = nodes[src_host];

            if (src_node_info.shards_by_load.empty()) {
                lblogger.debug("candidate node {} ran out of candidate shards with {} tablets remaining.",
                               src_host, src_node_info.tablet_count);
                max_off_candidate_load = std::max(max_off_candidate_load, src_node_info.avg_load);
                nodes_by_load.pop_back();
                continue;
            }
            auto push_back_node_candidate = seastar::defer([&] {
                std::push_heap(nodes_by_load.begin(), nodes_by_load.end(), nodes_cmp);
            });

            std::pop_heap(src_node_info.shards_by_load.begin(), src_node_info.shards_by_load.end(), src_node_info.shards_by_load_cmp());
            auto src_shard = src_node_info.shards_by_load.back();
            auto src = tablet_replica{src_host, src_shard};
            auto&& src_shard_info = src_node_info.shards[src_shard];
            if (src_shard_info.candidates.empty()) {
                lblogger.debug("shard {} ran out of candidates with {} tablets remaining.", src, src_shard_info.tablet_count);
                src_node_info.shards_by_load.pop_back();
                continue;
            }
            auto push_back_shard_candidate = seastar::defer([&] {
                std::push_heap(src_node_info.shards_by_load.begin(), src_node_info.shards_by_load.end(), src_node_info.shards_by_load_cmp());
            });

            auto source_tablet = *src_shard_info.candidates.begin();
            src_shard_info.candidates.erase(source_tablet);
            auto& tmap = tmeta.get_tablet_map(source_tablet.table);

            // Pick a target node.

            if (nodes_by_load_dst.empty()) {
                lblogger.debug("No more target nodes");
                _stats.for_dc(dc).stop_no_candidates++;
                break;
            }

            // The post-condition of this block is that nodes_by_load_dst.back() is a viable target node
            // for the source tablet.
            if (nodes_to_drain.empty()) {
                std::pop_heap(nodes_by_load_dst.begin(), nodes_by_load_dst.end(), nodes_dst_cmp);
            } else {
                std::unordered_set<host_id> replicas;
                std::unordered_map<sstring, int> rack_load;
                int max_rack_load = 0;
                for (auto&& r : tmap.get_tablet_info(source_tablet.tablet).replicas) {
                    replicas.insert(r.host);
                    if (nodes.contains(r.host)) {
                        const locator::node& node = topo.get_node(r.host);
                        rack_load[node.dc_rack().rack] += 1;
                        max_rack_load = std::max(max_rack_load, rack_load[node.dc_rack().rack]);
                    }
                }

                auto end = nodes_by_load_dst.end();
                while (true) {
                    if (nodes_by_load_dst.begin() == end) {
                        throw std::runtime_error(format("Unable to find new replica for tablet {} on {} when draining {}",
                                                        source_tablet, src, nodes_to_drain));
                    }

                    pop_heap(nodes_by_load_dst.begin(), end, nodes_dst_cmp);
                    --end;
                    auto new_target = *end;

                    if (replicas.contains(new_target)) {
                        lblogger.debug("next best target {} (avg_load={}) skipped because it is already a replica for {}",
                                       new_target, nodes[new_target].avg_load, source_tablet);
                        continue;
                    }

                    const locator::node& target_node = topo.get_node(new_target);
                    const locator::node& source_node = topo.get_node(src_host);
                    if (target_node.dc_rack().rack != source_node.dc_rack().rack
                            && (rack_load[target_node.dc_rack().rack] + 1 > max_rack_load)) {
                        lblogger.debug("next best target {} (avg_load={}) skipped because it would overload rack {} "
                                       "with {} replicas of {}, current max is {}",
                                       new_target, nodes[new_target].avg_load, target_node.dc_rack().rack,
                                       rack_load[target_node.dc_rack().rack] + 1, source_tablet, max_rack_load);
                        continue;
                    }

                    // Found a viable target, restore the heap
                    std::swap(*end, nodes_by_load_dst.back());
                    while (end != std::prev(nodes_by_load_dst.end())) {
                        ++end;
                        push_heap(nodes_by_load_dst.begin(), end, nodes_dst_cmp);
                    }
                    break;
                }
            }

            target = nodes_by_load_dst.back();
            auto& target_info = nodes[target];
            const locator::node& target_node = topo.get_node(target);
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

                // If balance is not achieved, still consider migrating from candidate nodes which have higher load than the target.
                // max_off_candidate_load may be higher than the load of current candidate.
                if (src_node_info.avg_load <= target_info.avg_load) {
                    lblogger.debug("No more candidate nodes. Next candidate is {} with avg_load={}, target's avg_load={}",
                            src_host, src_node_info.avg_load, target_info.avg_load);
                    _stats.for_dc(dc).stop_no_candidates++;
                    break;
                }

                // Prevent load inversion which can lead to oscillations.
                if (src_node_info.get_avg_load(nodes[src_host].tablet_count - 1) <
                        target_info.get_avg_load(target_info.tablet_count + 1)) {
                    lblogger.debug("No more candidate nodes, load would be inverted. Next candidate is {} with "
                                   "avg_load={}, target's avg_load={}",
                            src_host, src_node_info.avg_load, target_info.avg_load);
                    _stats.for_dc(dc).stop_load_inversion++;
                    break;
                }
            }

            // Check replication strategy constraints.

            bool check_rack_load = false;
            bool has_replica_on_target = false;
            std::unordered_map<sstring, int> rack_load; // Will be built if check_rack_load

            if (nodes_to_drain.empty()) {
                check_rack_load = target_node.dc_rack().rack != topo.get_node(src.host).dc_rack().rack;
                for (auto&& r: tmap.get_tablet_info(source_tablet.tablet).replicas) {
                    if (r.host == target) {
                        has_replica_on_target = true;
                        break;
                    }
                    if (check_rack_load) {
                        const locator::node& node = topo.get_node(r.host);
                        if (node.dc_rack().dc == dc) {
                            rack_load[node.dc_rack().rack] += 1;
                        }
                    }
                }
            }

            if (has_replica_on_target) {
                _stats.for_dc(dc).tablets_skipped_node++;
                lblogger.debug("candidate tablet {} skipped because it has a replica on target node", source_tablet);
                continue;
            }

            // Make sure we don't increase level of duplication of racks in the replica list.
            if (check_rack_load) {
                auto max_rack_load = std::max_element(rack_load.begin(), rack_load.end(),
                                                 [] (auto& a, auto& b) { return a.second < b.second; })->second;
                auto new_rack_load = rack_load[target_node.dc_rack().rack] + 1;
                if (new_rack_load > max_rack_load) {
                    lblogger.debug("candidate tablet {} skipped because it would increase load on rack {} to {}, max={}",
                                   source_tablet, target_node.dc_rack().rack, new_rack_load, max_rack_load);
                    _stats.for_dc(dc).tablets_skipped_rack++;
                    continue;
                }
            }

            auto& target_load_sketch = co_await target_info.get_load_sketch(_tm);
            auto dst = global_shard_id {target, target_load_sketch.next_shard(target)};

            const locator::node& src_node = topo.get_node(src.host);
            tablet_transition_kind kind = (src_node.get_state() == locator::node::state::being_removed
                                           || src_node.get_state() == locator::node::state::being_replaced)
                       ? tablet_transition_kind::rebuild : tablet_transition_kind::migration;
            auto mig = tablet_migration_info {kind, source_tablet, src, dst};
            auto mig_streaming_info = get_migration_streaming_info(topo, tmap.get_tablet_info(source_tablet.tablet), mig);

            if (can_accept_load(mig_streaming_info)) {
                apply_load(mig_streaming_info);
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

            target_info.tablet_count += 1;
            target_info.update();

            src_shard_info.tablet_count -= 1;
            if (src_shard_info.tablet_count == 0) {
                push_back_shard_candidate.cancel();
                src_node_info.shards_by_load.pop_back();
            }

            src_node_info.tablet_count -= 1;
            src_node_info.update();
            if (src_node_info.tablet_count == 0) {
                push_back_node_candidate.cancel();
                nodes_by_load.pop_back();
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
public:
    tablet_allocator_impl(tablet_allocator::config cfg, service::migration_notifier& mn, replica::database& db)
            : _config(std::move(cfg))
            , _migration_notifier(mn)
            , _db(db) {
        if (_config.initial_tablets_scale == 0) {
            throw std::runtime_error("Initial tablets scale must be positive");
        }
        if (db.get_config().check_experimental(db::experimental_features_t::feature::TABLETS)) {
            _migration_notifier.register_listener(this);
        }
    }

    tablet_allocator_impl(tablet_allocator_impl&&) = delete; // "this" captured.

    ~tablet_allocator_impl() {
        assert(_stopped);
    }

    future<> stop() {
        co_await _migration_notifier.unregister_listener(this);
        _stopped = true;
    }

    future<migration_plan> balance_tablets(token_metadata_ptr tm, locator::load_stats_ptr table_load_stats) {
        load_balancer lb(tm, std::move(table_load_stats), _load_balancer_stats, _db.get_config().target_tablet_size_in_bytes());
        co_return co_await lb.make_plan();
    }

    void on_before_create_column_family(const keyspace_metadata& ksm, const schema& s, std::vector<mutation>& muts, api::timestamp_type ts) override {
        locator::replication_strategy_params params(ksm.strategy_options(), ksm.initial_tablets());
        auto rs = abstract_replication_strategy::create_replication_strategy(ksm.strategy_name(), params);
        if (auto&& tablet_rs = rs->maybe_as_tablet_aware()) {
            auto tm = _db.get_shared_token_metadata().get();
            auto map = tablet_rs->allocate_tablets_for_new_table(s.shared_from_this(), tm, _config.initial_tablets_scale).get();
            muts.emplace_back(tablet_map_to_mutation(map, s.id(), s.keypace_name(), s.cf_name(), ts).get());
        }
    }

    void on_before_drop_column_family(const schema& s, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(s.ks_name());
        auto&& rs = ks.get_replication_strategy();
        std::vector<mutation> result;
        if (rs.uses_tablets()) {
            auto tm = _db.get_shared_token_metadata().get();
            muts.emplace_back(make_drop_tablet_map_mutation(s.id(), ts));
        }
    }

    void on_before_drop_keyspace(const sstring& keyspace_name, std::vector<mutation>& muts, api::timestamp_type ts) override {
        keyspace& ks = _db.find_keyspace(keyspace_name);
        auto&& rs = ks.get_replication_strategy();
        if (rs.uses_tablets()) {
            auto tm = _db.get_shared_token_metadata().get();
            for (auto&& [name, s] : ks.metadata()->cf_meta_data()) {
                muts.emplace_back(make_drop_tablet_map_mutation(s->id(), ts));
            }
        }
    }

    void on_leadership_lost() {
        _load_balancer_stats.unregister();
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

future<migration_plan> tablet_allocator::balance_tablets(locator::token_metadata_ptr tm, locator::load_stats_ptr load_stats) {
    return impl().balance_tablets(std::move(tm), std::move(load_stats));
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

}

auto fmt::formatter<service::tablet_migration_info>::format(const service::tablet_migration_info& mig, fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{{tablet: {}, src: {}, dst: {}}}", mig.tablet, mig.src, mig.dst);
}
