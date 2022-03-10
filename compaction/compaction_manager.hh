/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <vector>
#include <list>
#include <functional>
#include <algorithm>
#include "compaction.hh"
#include "compaction_weight_registration.hh"
#include "compaction_backlog_manager.hh"
#include "strategy_control.hh"
#include "backlog_controller.hh"
#include "seastarx.hh"
#include "sstables/exceptions.hh"

namespace replica {
class table;
}

class compacting_sstable_registration;

// Compaction manager provides facilities to submit and track compaction jobs on
// behalf of existing tables.
class compaction_manager {
public:
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
        uint64_t active_tasks = 0; // Number of compaction going on.
        int64_t errors = 0;
    };
    struct compaction_scheduling_group {
        seastar::scheduling_group cpu;
        const ::io_priority_class& io;
    };
    struct maintenance_scheduling_group {
        seastar::scheduling_group cpu;
        const ::io_priority_class& io;
    };
private:
    struct compaction_state {
        // Used both by compaction tasks that refer to the compaction_state
        // and by any function running under run_with_compaction_disabled().
        seastar::gate gate;

        // Prevents table from running major and minor compaction at the same time.
        rwlock lock;

        // Raised by any function running under run_with_compaction_disabled();
        long compaction_disabled_counter = 0;

        bool compaction_disabled() const noexcept {
            return compaction_disabled_counter > 0;
        }
    };

public:
    class task {
    protected:
        compaction_manager& _cm;
        replica::table* _compacting_table = nullptr;
        compaction_state& _compaction_state;
        sstables::compaction_data _compaction_data;

    private:
        shared_future<> _compaction_done = make_ready_future<>();
        exponential_backoff_retry _compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
        sstables::compaction_type _type;
        bool _compaction_running = false;
        utils::UUID _output_run_identifier;
        gate::holder _gate_holder;
        sstring _description;

        // FIXME: for now
        friend class compaction_manager;
    public:
        explicit task(compaction_manager& mgr, replica::table* t, sstables::compaction_type type, sstring desc)
            : _cm(mgr)
            , _compacting_table(t)
            , _compaction_state(_cm.get_compaction_state(t))
            , _type(type)
            , _gate_holder(_compaction_state.gate.hold())
            , _description(std::move(desc))
        {}

        task(task&&) = delete;
        task(const task&) = delete;

        virtual ~task() = default;

    protected:
        // Return true if the task isn't stopped
        // and the compaction manager allows proceeding.
        inline bool can_proceed() const;
        void setup_new_compaction(utils::UUID output_run_id = utils::null_uuid());
        void finish_compaction() noexcept;

        // Compaction manager stop itself if it finds an storage I/O error which results in
        // stop of transportation services. It cannot make progress anyway.
        // Returns exception if error is judged fatal, and compaction task must be stopped,
        // otherwise, returns stop_iteration::no after sleep for exponential retry.
        future<stop_iteration> maybe_retry(std::exception_ptr err);

    public:
        const replica::table* compacting_table() const noexcept {
            return _compacting_table;
        }

        sstables::compaction_type type() const noexcept {
            return _type;
        }

        bool compaction_running() const noexcept {
            return _compaction_running;
        }

        const sstables::compaction_data& compaction_data() const noexcept {
            return _compaction_data;
        }

        sstables::compaction_data& compaction_data() noexcept {
            return _compaction_data;
        }

        const compaction_manager::compaction_state& compaction_state() const noexcept {
            return _compaction_state;
        }

        compaction_manager::compaction_state& compaction_state() noexcept {
            return _compaction_state;
        }

        bool generating_output_run() const noexcept {
            return _compaction_running && _output_run_identifier;
        }
        const utils::UUID& output_run_id() const noexcept {
            return _output_run_identifier;
        }

        future<> compaction_done() noexcept {
            return _compaction_done.get_future();
        }

        bool stopping() const noexcept {
            return _compaction_data.abort.abort_requested();
        }

        void stop(sstring reason) noexcept;

        sstables::compaction_stopped_exception make_compaction_stopped_exception() const;

        std::string describe() const;
    };

    class major_compaction_task;
    class custom_compaction_task;
    class regular_compaction_task;
    class offstrategy_compaction_task;
    class rewrite_sstables_compaction_task;
    class compaction_manager_test_task;

private:
    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::list<shared_ptr<task>> _tasks;

    // Possible states in which the compaction manager can be found.
    //
    // none: started, but not yet enabled. Once the compaction manager moves out of "none", it can
    //       never legally move back
    // stopped: stop() was called. The compaction_manager will never be enabled or disabled again
    //          and can no longer be used (although it is possible to still grab metrics, stats,
    //          etc)
    // enabled: accepting compactions
    // disabled: not accepting compactions
    //
    // Moving the compaction manager to and from enabled and disable states is legal, as many times
    // as necessary.
    enum class state { none, stopped, disabled, enabled };
    state _state = state::none;

    std::optional<future<>> _stop_future;

    stats _stats;
    seastar::metrics::metric_groups _metrics;
    double _last_backlog = 0.0f;

    // Store sstables that are being compacted at the moment. That's needed to prevent
    // a sstable from being compacted twice.
    std::unordered_set<sstables::shared_sstable> _compacting_sstables;

    future<> _waiting_reevalution = make_ready_future<>();
    condition_variable _postponed_reevaluation;
    // tables that wait for compaction but had its submission postponed due to ongoing compaction.
    std::unordered_set<replica::table*> _postponed;
    // tracks taken weights of ongoing compactions, only one compaction per weight is allowed.
    // weight is value assigned to a compaction job that is log base N of total size of all input sstables.
    std::unordered_set<int> _weight_tracker;

    std::unordered_map<replica::table*, compaction_state> _compaction_state;

    // Purpose is to serialize all maintenance (non regular) compaction activity to reduce aggressiveness and space requirement.
    // If the operation must be serialized with regular, then the per-table write lock must be taken.
    seastar::named_semaphore _maintenance_ops_sem = {1, named_semaphore_exception_factory{"maintenance operation"}};

    std::function<void()> compaction_submission_callback();
    // all registered tables are reevaluated at a constant interval.
    // Submission is a NO-OP when there's nothing to do, so it's fine to call it regularly.
    timer<lowres_clock> _compaction_submission_timer = timer<lowres_clock>(compaction_submission_callback());
    static constexpr std::chrono::seconds periodic_compaction_submission_interval() { return std::chrono::seconds(3600); }

    compaction_controller _compaction_controller;
    compaction_backlog_manager _backlog_manager;
    maintenance_scheduling_group _maintenance_sg;
    size_t _available_memory;
    optimized_optional<abort_source::subscription> _early_abort_subscription;

    class strategy_control;
    std::unique_ptr<strategy_control> _strategy_control;
private:
    shared_ptr<task> register_task(shared_ptr<task>);

    future<> stop_tasks(std::vector<shared_ptr<task>> tasks, sstring reason);

    // Return the largest fan-in of currently running compactions
    unsigned current_compaction_fan_in_threshold() const;

    // Return true if compaction can be initiated
    bool can_register_compaction(replica::table* t, int weight, unsigned fan_in) const;
    // Register weight for a table. Do that only if can_register_weight()
    // returned true.
    void register_weight(int weight);
    // Deregister weight for a table.
    void deregister_weight(int weight);

    // Get candidates for compaction strategy, which are all sstables but the ones being compacted.
    std::vector<sstables::shared_sstable> get_candidates(const replica::table& t);

    template <typename Iterator, typename Sentinel>
    requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
    void register_compacting_sstables(Iterator first, Sentinel last);

    template <typename Iterator, typename Sentinel>
    requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
    void deregister_compacting_sstables(Iterator first, Sentinel last);

    // gets the table's compaction state
    // throws std::out_of_range exception if not found.
    compaction_state& get_compaction_state(replica::table* t);

    // Return true if compaction manager is enabled and
    // table still exists and compaction is not disabled for the table.
    inline bool can_proceed(replica::table* t) const;

    void postponed_compactions_reevaluation();
    void reevaluate_postponed_compactions() noexcept;
    // Postpone compaction for a table that couldn't be executed due to ongoing
    // similar-sized compaction.
    void postpone_compaction_for_table(replica::table* t);

    future<> perform_sstable_scrub_validate_mode(replica::table* t);

    using get_candidates_func = std::function<future<std::vector<sstables::shared_sstable>>()>;
    class can_purge_tombstones_tag;
    using can_purge_tombstones = bool_class<can_purge_tombstones_tag>;

    future<> rewrite_sstables(replica::table* t, sstables::compaction_type_options options, get_candidates_func, can_purge_tombstones can_purge = can_purge_tombstones::yes);
public:
    compaction_manager(compaction_scheduling_group csg, maintenance_scheduling_group msg, size_t available_memory, abort_source& as);
    compaction_manager(compaction_scheduling_group csg, maintenance_scheduling_group msg, size_t available_memory, uint64_t shares, abort_source& as);
    compaction_manager();
    ~compaction_manager();

    void register_metrics();

    // enable the compaction manager.
    void enable();

    // Stop all fibers. Ongoing compactions will be waited. Should only be called
    // once, from main teardown path.
    future<> stop();

    // cancels all running compactions and moves the compaction manager into disabled state.
    // The compaction manager is still alive after drain but it will not accept new compactions
    // unless it is moved back to enabled state.
    future<> drain();

    // Stop all fibers, without waiting. Safe to be called multiple times.
    void do_stop() noexcept;
    void really_do_stop();

    // Submit a table to be compacted.
    void submit(replica::table* t);

    // Submit a table to be off-strategy compacted.
    future<> perform_offstrategy(replica::table* t);

    // Submit a table to be cleaned up and wait for its termination.
    //
    // Performs a cleanup on each sstable of the table, excluding
    // those ones that are irrelevant to this node or being compacted.
    // Cleanup is about discarding keys that are no longer relevant for a
    // given sstable, e.g. after node loses part of its token range because
    // of a newly added node.
    future<> perform_cleanup(replica::database& db, replica::table* t);

    // Submit a table to be upgraded and wait for its termination.
    future<> perform_sstable_upgrade(replica::database& db, replica::table* t, bool exclude_current_version);

    // Submit a table to be scrubbed and wait for its termination.
    future<> perform_sstable_scrub(replica::table* t, sstables::compaction_type_options::scrub opts);

    // Submit a table for major compaction.
    future<> perform_major_compaction(replica::table* t);


    // Run a custom job for a given table, defined by a function
    // it completes when future returned by job is ready or returns immediately
    // if manager was asked to stop.
    //
    // parameter type is the compaction type the operation can most closely be
    //      associated with, use compaction_type::Compaction, if none apply.
    // parameter job is a function that will carry the operation
    future<> run_custom_job(replica::table* t, sstables::compaction_type type, const char *desc, noncopyable_function<future<>(sstables::compaction_data&)> job);

    // Run a function with compaction temporarily disabled for a table T.
    future<> run_with_compaction_disabled(replica::table* t, std::function<future<> ()> func);

    // Adds a table to the compaction manager.
    // Creates a compaction_state structure that can be used for submitting
    // compaction jobs of all types.
    void add(replica::table* t);

    // Remove a table from the compaction manager.
    // Cancel requests on table and wait for possible ongoing compactions.
    future<> remove(replica::table* t);

    const stats& get_stats() const {
        return _stats;
    }

    const std::vector<sstables::compaction_info> get_compactions(replica::table* t = nullptr) const;

    // Returns true if table has an ongoing compaction, running on its behalf
    bool has_table_ongoing_compaction(const replica::table* t) const {
        return std::any_of(_tasks.begin(), _tasks.end(), [t] (const shared_ptr<task>& task) {
            return task->compacting_table() == t && task->compaction_running();
        });
    };

    bool compaction_disabled(replica::table* t) const {
        return _compaction_state.contains(t) && _compaction_state.at(t).compaction_disabled();
    }

    // Stops ongoing compaction of a given type.
    future<> stop_compaction(sstring type, replica::table* table = nullptr);

    // Stops ongoing compaction of a given table and/or compaction_type.
    future<> stop_ongoing_compactions(sstring reason, replica::table* t = nullptr, std::optional<sstables::compaction_type> type_opt = {});

    double backlog() {
        return _backlog_manager.backlog();
    }

    void register_backlog_tracker(compaction_backlog_tracker& backlog_tracker) {
        _backlog_manager.register_backlog_tracker(backlog_tracker);
    }

    // Propagate replacement of sstables to all ongoing compaction of a given table
    void propagate_replacement(replica::table* t, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added);

    static sstables::compaction_data create_compaction_data();

    compaction::strategy_control& get_strategy_control() const noexcept;

    friend class compacting_sstable_registration;
    friend class compaction_weight_registration;
    friend class compaction_manager_test;
};

bool needs_cleanup(const sstables::shared_sstable& sst, const dht::token_range_vector& owned_ranges, schema_ptr s);

std::ostream& operator<<(std::ostream& os, const compaction_manager::task& task);
