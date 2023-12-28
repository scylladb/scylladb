/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "replica/database.hh"
#include "locator/tablets.hh"

namespace service {

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

class migration_plan {
public:
    using migrations_vector = utils::chunked_vector<tablet_migration_info>;
private:
    migrations_vector _migrations;
    bool _has_nodes_to_drain = false;
public:
    /// Returns true iff there are decommissioning nodes which own some tablet replicas.
    bool has_nodes_to_drain() const { return _has_nodes_to_drain; }

    const migrations_vector& migrations() const { return _migrations; }
    bool empty() const { return _migrations.empty(); }
    size_t size() const { return _migrations.size(); }

    void add(tablet_migration_info info) {
        _migrations.emplace_back(std::move(info));
    }

    void merge(migration_plan&& other) {
        std::move(other._migrations.begin(), other._migrations.end(), std::back_inserter(_migrations));
        _has_nodes_to_drain |= other._has_nodes_to_drain;
    }

    void set_has_nodes_to_drain(bool b) {
        _has_nodes_to_drain = b;
    }
};

class tablet_allocator_impl;

class tablet_allocator {
public:
    struct config {
        unsigned initial_tablets_scale = 1;
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
    future<migration_plan> balance_tablets(locator::token_metadata_ptr, locator::load_stats_ptr = {});

    /// Should be called when the node is no longer a leader.
    void on_leadership_lost();
};

}

template <>
struct fmt::formatter<service::tablet_migration_info> : fmt::formatter<std::string_view> {
    auto format(const service::tablet_migration_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
