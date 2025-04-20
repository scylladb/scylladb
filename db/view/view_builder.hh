/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "query-request.hh"
#include "service/migration_listener.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/serialized_action.hh"
#include "utils/cross-shard-barrier.hh"
#include "replica/database.hh"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/shared_ptr.hh>

#include <optional>
#include <unordered_map>
#include <vector>

namespace db {

class system_keyspace;
class system_distributed_keyspace;

}

namespace db {

using system_keyspace_view_name = std::pair<sstring, sstring>;
class system_keyspace_view_build_progress;

}

namespace service {
    class migration_manager;
} // namespace service

namespace replica {
class database;
}

class exponential_backoff_retry;

namespace db::view {

class view_update_generator;

/**
 * The view_builder is a sharded service responsible for building all defined materialized views.
 * This process entails walking over the existing data in a given base table, and using it to
 * calculate and insert the respective entries for one or more views.
 *
 * We employ a mutation_reader for each base table for which we're building views.
 *
 * We aim to be resource-conscious. On a given shard, at any given moment, we consume at most
 * from one reader. We also strive for fairness, in that each build step inserts entries for
 * the views of a different base. Each build step reads and generates updates for batch_size rows.
 *
 * We lack a controller, which could potentially allow us to go faster (to execute multiple steps at
 * the same time, or consume more rows per batch), and also which would apply backpressure, so we
 * could, for example, delay executing a build step.
 *
 * View building is necessarily a sharded process. That means that on restart, if the number of shards
 * has changed, we need to calculate the most conservative token range that has been built, and build
 * the remainder.
 *
 * Interaction with the system tables:
 *   - When we start building a view, we add an entry to the scylla_views_builds_in_progress
 *     system table. If the node restarts at this point, we'll consider these newly inserted
 *     views as having made no progress, and we'll treat them as new views;
 *   - When we finish a build step, we update the progress of the views that we built during
 *     this step by writing the next token to the scylla_views_builds_in_progress table. If
 *     the node restarts here, we'll start building the views at the token in the next_token column.
 *   - When we finish building a view, we mark it as completed in the built views system table, and
 *     remove it from the in-progress system table. Under failure, the following can happen:
 *          * When we fail to mark the view as built, we'll redo the last step upon node reboot;
 *          * When we fail to delete the in-progress record, upon reboot we'll remove this record.
 *     A view is marked as completed only when all shards have finished their share of the work, that is,
 *     if a view is not built, then all shards will still have an entry in the in-progress system table,
 *   - A view that a shard finished building, but not all other shards, remains in the in-progress system
 *     table, with first_token == next_token.
 * Interaction with the distributed system table (view_build_status):
 *   - When we start building a view, we mark the view build as being in-progress;
 *   - When we finish building a view, we mark the view as being built. Upon failure,
 *     we ensure that if the view is in the in-progress system table, then it may not
 *     have been written to this table. We don't load the built views from this table
 *     when starting. When starting, the following happens:
 *          * If the view is in the system.built_views table and not the in-progress
 *            system table, then it will be in view_build_status;
 *          * If the view is in the system.built_views table and not in this one, it
 *            will still be in the in-progress system table - we detect this and mark
 *            it as built in this table too, keeping the invariant;
 *          * If the view is in this table but not in system.built_views, then it will
 *            also be in the in-progress system table - we don't detect this and will
 *            redo the missing step, for simplicity.
 */
class view_builder final : public service::migration_listener::only_view_notifications, public seastar::peering_sharded_service<view_builder> {
    /**
     * Keeps track of the build progress for a particular view.
     * When the view is built, next_token == first_token.
     */
    struct view_build_status final {
        view_ptr view;
        dht::token first_token;
        std::optional<dht::token> next_token;
    };

    struct stats {
        uint64_t steps_performed = 0;
        uint64_t steps_failed = 0;
    };

    /**
     * Keeps track of the build progress for all the views of a particular
     * base table. Each execution of the build step comprises a query of
     * the base table for the selected range.
     *
     * We pin the set of sstables that potentially contain data that should be added to a
     * view (they are pinned by the mutation_reader). Adding a view v' overwrites the
     * set of pinned sstables, regardless of there being another view v'' being built. The
     * new set will potentially contain new data already in v'', written as part of the write
     * path. We assume this case is rare and optimize for fewer disk space in detriment of
     * network bandwidth.
     */
    struct build_step final {
        // Ensure we pin the column_family. It may happen that all views are removed,
        // and that the base table is too before we can detect it.
        lw_shared_ptr<replica::column_family> base;
        query::partition_slice pslice;
        dht::partition_range prange;
        mutation_reader reader{nullptr};
        dht::decorated_key current_key{dht::minimum_token(), partition_key::make_empty()};
        std::vector<view_build_status> build_status;

        const dht::token& current_token() const {
            return current_key.token();
        }
    };

    using base_to_build_step_type = std::unordered_map<table_id, build_step>;

    replica::database& _db;
    db::system_keyspace& _sys_ks;
    db::system_distributed_keyspace& _sys_dist_ks;
    service::raft_group0_client& _group0_client;
    cql3::query_processor& _qp;
    service::migration_notifier& _mnotifier;
    view_update_generator&  _vug;
    reader_permit _permit;
    base_to_build_step_type _base_to_build_step;
    base_to_build_step_type::iterator _current_step = _base_to_build_step.end();
    serialized_action _build_step{std::bind(&view_builder::do_build_step, this)};
    // Ensures bookkeeping operations are serialized, meaning that while we execute
    // a build step we don't consider newly added or removed views. This simplifies
    // the algorithms. Also synchronizes an operation wrt. a call to stop().
    seastar::named_semaphore _sem{1, named_semaphore_exception_factory{"view builder"}};
    seastar::abort_source _as;
    future<> _started = make_ready_future<>();
    // Used to coordinate between shards the conclusion of the build process for a particular view.
    std::unordered_set<table_id> _built_views;
    // Used for testing.
    std::unordered_map<std::pair<sstring, sstring>, seastar::shared_promise<>, utils::tuple_hash> _build_notifiers;
    stats _stats;
    metrics::metric_groups _metrics;

    enum class view_build_status_location { sys_dist_ks, group0, both };

    view_build_status_location _view_build_status_on = view_build_status_location::sys_dist_ks;
    bool _init_virtual_table_on_upgrade = false;
    utils::phased_barrier _upgrade_phaser;

    struct view_builder_init_state {
        std::vector<future<>> bookkeeping_ops;
        std::vector<std::vector<view_build_status>> status_per_shard;
        std::unordered_set<table_id> built_views;
    };

    future<> start_in_background(service::migration_manager&, utils::cross_shard_barrier b);

public:
    // The view builder processes the base table in steps of batch_size rows.
    // However, if the individual rows are large, there is no real need to
    // collect batch_size of them in memory at once. Rather, as soon as we've
    // collected batch_memory_max bytes, we can process the rows read so far.
    static constexpr size_t batch_size = 128;
    static constexpr size_t batch_memory_max = 1024*1024;

    replica::database& get_db() noexcept { return _db; }

public:
    view_builder(replica::database&, db::system_keyspace&, db::system_distributed_keyspace&, service::migration_notifier&, view_update_generator& vug,
            service::raft_group0_client& group0_client, cql3::query_processor& qp);
    view_builder(view_builder&&) = delete;

    /**
     * Loads the state stored in the system tables to resume building the existing views.
     * Requires that all views have been loaded from the system tables and are accessible
     * through the database, and that the commitlog has been replayed.
     */
    future<> start(service::migration_manager&, utils::cross_shard_barrier b = utils::cross_shard_barrier(utils::cross_shard_barrier::solo{}));

    /**
     * Drains view building in order to prepare it for shutdown.
     */
    future<> drain();

    /**
     * Stops the view building process.
     */
    future<> stop();

    static future<> generate_mutations_on_node_left(replica::database& db, db::system_keyspace& sys_ks, api::timestamp_type timestamp, locator::host_id host_id, std::vector<canonical_mutation>& muts);

    static future<> migrate_to_v1_5(locator::token_metadata_ptr tmptr, db::system_keyspace& sys_ks, cql3::query_processor& qp, service::raft_group0_client& group0_client, abort_source& as, service::group0_guard guard);
    static future<> migrate_to_v2(locator::token_metadata_ptr tmptr, db::system_keyspace& sys_ks, cql3::query_processor& qp, service::raft_group0_client& group0_client, abort_source& as, service::group0_guard guard);

    future<> upgrade_to_v1_5();
    future<> upgrade_to_v2();

    void init_virtual_table();

    virtual void on_create_view(const sstring& ks_name, const sstring& view_name) override;
    virtual void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override;
    virtual void on_drop_view(const sstring& ks_name, const sstring& view_name) override;

    // For tests
    future<> wait_until_built(const sstring& ks_name, const sstring& view_name);

    future<std::unordered_map<sstring, sstring>> view_build_statuses(sstring keyspace, sstring view_name, const gms::gossiper& g) const;

    // Can only be called on shard-0
    future<> mark_existing_views_as_built();
    future<bool> check_view_build_ongoing(const locator::token_metadata& tm, const sstring& ks_name, const sstring& cf_name);
    future<> register_staging_sstable(sstables::shared_sstable sst, lw_shared_ptr<replica::table> table);

private:
    build_step& get_or_create_build_step(table_id);
    future<> initialize_reader_at_current_token(build_step&);
    void load_view_status(view_build_status, std::unordered_set<table_id>&);
    void reshard(std::vector<std::vector<view_build_status>>, std::unordered_set<table_id>&);
    void setup_shard_build_step(view_builder_init_state& vbi, std::vector<system_keyspace_view_name>, std::vector<system_keyspace_view_build_progress>);
    future<> calculate_shard_build_step(view_builder_init_state& vbi);
    future<> add_new_view(view_ptr, build_step&);
    future<> do_build_step();
    void execute(build_step&, exponential_backoff_retry);
    future<> maybe_mark_view_as_built(view_ptr, dht::token);
    future<> mark_as_built(view_ptr);
    void setup_metrics();

    template <typename Func1, typename Func2>
    future<> write_view_build_status(Func1&& fn_group0, Func2&& fn_sys_dist) {
        auto op = _upgrade_phaser.start();

        // read locally so it doesn't change between async calls
        const auto v = _view_build_status_on;

        if (v == view_build_status_location::group0 || v == view_build_status_location::both) {
            co_await fn_group0();
        }

        if (v == view_build_status_location::sys_dist_ks || v == view_build_status_location::both) {
            co_await fn_sys_dist();
        }
    }

    future<> mark_view_build_started(sstring ks_name, sstring view_name);
    future<> mark_view_build_success(sstring ks_name, sstring view_name);
    future<> remove_view_build_status(sstring ks_name, sstring view_name);
    future<std::unordered_map<locator::host_id, sstring>> view_status(sstring ks_name, sstring view_name) const;

    struct consumer;
};

}
