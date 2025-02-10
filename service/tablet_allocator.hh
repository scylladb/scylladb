/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "replica/database_fwd.hh"
#include "locator/tablets.hh"
#include "tablet_allocator_fwd.hh"
#include "locator/token_metadata_fwd.hh"
#include <seastar/core/metrics.hh>

namespace service {

struct load_balancer_dc_stats {
    uint64_t calls = 0;
    uint64_t migrations_produced = 0;
    uint64_t migrations_from_skiplist = 0;
    uint64_t candidates_evaluated = 0;
    uint64_t bad_first_candidates = 0;
    uint64_t bad_migrations = 0;
    uint64_t intranode_migrations_produced = 0;
    uint64_t migrations_skipped = 0;
    uint64_t tablets_skipped_node = 0;
    uint64_t tablets_skipped_rack = 0;
    uint64_t stop_balance = 0;
    uint64_t stop_load_inversion = 0;
    uint64_t stop_no_candidates = 0;
    uint64_t stop_skip_limit = 0;
    uint64_t stop_batch_size = 0;

    load_balancer_dc_stats operator-(const load_balancer_dc_stats& other) const {
        return {
            calls - other.calls,
            migrations_produced - other.migrations_produced,
            migrations_from_skiplist - other.migrations_from_skiplist,
            candidates_evaluated - other.candidates_evaluated,
            bad_first_candidates - other.bad_first_candidates,
            bad_migrations - other.bad_migrations,
            intranode_migrations_produced - other.intranode_migrations_produced,
            migrations_skipped - other.migrations_skipped,
            tablets_skipped_node - other.tablets_skipped_node,
            tablets_skipped_rack - other.tablets_skipped_rack,
            stop_balance - other.stop_balance,
            stop_load_inversion - other.stop_load_inversion,
            stop_no_candidates - other.stop_no_candidates,
            stop_skip_limit - other.stop_skip_limit,
            stop_batch_size - other.stop_batch_size,
        };
    }
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
    using host_id = locator::host_id;

    sstring group_name;
    std::unordered_map<dc_name, std::unique_ptr<load_balancer_dc_stats>> _dc_stats;
    std::unordered_map<host_id, std::unique_ptr<load_balancer_node_stats>> _node_stats;
    load_balancer_cluster_stats _cluster_stats;
    seastar::metrics::label dc_label{"target_dc"};
    seastar::metrics::label node_label{"target_node"};
    seastar::metrics::metric_groups _metrics;

    void setup_metrics(const dc_name& dc, load_balancer_dc_stats& stats);
    void setup_metrics(const dc_name& dc, host_id node, load_balancer_node_stats& stats);
    void setup_metrics(load_balancer_cluster_stats& stats);
public:
    load_balancer_stats_manager(sstring group_name);

    load_balancer_dc_stats& for_dc(const dc_name& dc);
    load_balancer_node_stats& for_node(const dc_name& dc, host_id node);
    load_balancer_cluster_stats& for_cluster();

    void unregister();
};

using tablet_migration_info = locator::tablet_migration_info;

/// Represents intention to emit resize (split or merge) request for a
/// table, and finalize or revoke the request previously initiated.
struct table_resize_plan {
    std::unordered_map<table_id, locator::resize_decision> resize;
    std::unordered_set<table_id> finalize_resize;

    size_t size() const { return resize.size() + finalize_resize.size(); }

    void merge(table_resize_plan&& other) {
        for (auto&& [id, other_resize] : other.resize) {
            if (!resize.contains(id) || other_resize.sequence_number > resize[id].sequence_number) {
                resize[id] = std::move(other_resize);
            }
        }

        finalize_resize.merge(std::move(other.finalize_resize));
    }
};

struct tablet_repair_plan {
    std::unordered_set<locator::global_tablet_id> _repairs;

    const std::unordered_set<locator::global_tablet_id>& repairs() const {
        return _repairs;
    }

    size_t size() const { return _repairs.size(); };

    void merge(tablet_repair_plan&& other) {
        for (auto& r : other._repairs) {
            _repairs.insert(r);
        }
    }

    void add(const locator::global_tablet_id& gid) {
        _repairs.insert(gid);
    }
};

class migration_plan {
public:
    using migrations_vector = utils::chunked_vector<tablet_migration_info>;
private:
    migrations_vector _migrations;
    table_resize_plan _resize_plan;
    tablet_repair_plan _repair_plan;
    bool _has_nodes_to_drain = false;
public:
    /// Returns true iff there are decommissioning nodes which own some tablet replicas.
    bool has_nodes_to_drain() const { return _has_nodes_to_drain; }

    const migrations_vector& migrations() const { return _migrations; }
    bool empty() const { return _migrations.empty() && !_resize_plan.size() && !_repair_plan.size();}
    size_t size() const { return _migrations.size() + _resize_plan.size() + _repair_plan.size(); }
    size_t tablet_migration_count() const { return _migrations.size(); }
    size_t resize_decision_count() const { return _resize_plan.size(); }
    size_t tablet_repair_count() const { return _repair_plan.size(); }

    void add(tablet_migration_info info) {
        _migrations.emplace_back(std::move(info));
    }

    void add(migrations_vector migrations) {
        for (auto&& mig : migrations) {
            add(std::move(mig));
        }
    }

    void merge(migration_plan&& other) {
        std::move(other._migrations.begin(), other._migrations.end(), std::back_inserter(_migrations));
        _has_nodes_to_drain |= other._has_nodes_to_drain;
        _resize_plan.merge(std::move(other._resize_plan));
        _repair_plan.merge(std::move(other._repair_plan));
    }

    void set_has_nodes_to_drain(bool b) {
        _has_nodes_to_drain = b;
    }

    const table_resize_plan& resize_plan() const { return _resize_plan; }

    void merge_resize_plan(table_resize_plan resize_plan) {
        _resize_plan.merge(std::move(resize_plan));
    }

    const tablet_repair_plan& repair_plan() const { return _repair_plan; }

    void set_repair_plan(tablet_repair_plan repair) {
        _repair_plan = std::move(repair);
    }

    future<std::unordered_set<locator::global_tablet_id>> get_migration_tablet_ids() const;
};

class migration_notifier;

class tablet_allocator {
public:
    struct config {
    };
    class impl {
    public:
        virtual ~impl() = default;
    };
private:
    std::unique_ptr<impl> _impl;
    tablet_allocator_impl& impl();
public:
    tablet_allocator(config cfg, service::migration_notifier& mn, replica::database& db);
public:
    future<> stop();

    /// Returns a tablet migration plan that aims to achieve better load balance in the whole cluster.
    /// The plan is computed based on information in the given token_metadata snapshot
    /// and thus should be executed and reflected, at least as pending tablet transitions, in token_metadata
    /// before this is called again.
    ///
    /// For any given global_tablet_id there is at most one tablet_migration_info in the returned plan.
    ///
    /// To achieve full balance, do:
    ///
    ///    while (true) {
    ///        auto plan = co_await balance_tablets(get_token_metadata());
    ///        if (plan.empty()) {
    ///            break;
    ///        }
    ///        co_await execute(plan);
    ///    }
    ///
    /// It is ok to invoke the algorithm with already active tablet migrations. The algorithm will take them into account
    /// when balancing the load as if they already succeeded. This means that applying a series of migration plans
    /// produced by this function will give the same result regardless of whether applying means they are fully executed or
    /// only initiated by creating corresponding transitions in tablet metadata.
    ///
    /// The algorithm takes care of limiting the streaming load on the system, also by taking active migrations into account.
    ///
    future<migration_plan> balance_tablets(locator::token_metadata_ptr, locator::load_stats_ptr = {}, std::unordered_set<locator::host_id> = {});

    load_balancer_stats_manager& stats();

    void set_use_table_aware_balancing(bool);

    future<locator::tablet_map> resize_tablets(locator::token_metadata_ptr, table_id);

    /// Should be called when the node is no longer a leader.
    void on_leadership_lost();
};

}

template <>
struct fmt::formatter<service::tablet_migration_info> : fmt::formatter<string_view> {
    auto format(const service::tablet_migration_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
