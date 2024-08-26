/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/icl/interval_map.hpp>

#include <seastar/core/semaphore.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/condition-variable.hh>
#include "sstables/shared_sstable.hh"
#include "utils/exponential_backoff_retry.hh"
#include "utils/updateable_value.hh"
#include "utils/serialized_action.hh"
#include <vector>
#include <list>
#include <functional>
#include "compaction.hh"
#include "compaction_backlog_manager.hh"
#include "compaction/compaction_descriptor.hh"
#include "compaction/task_manager_module.hh"
#include "compaction_state.hh"
#include "strategy_control.hh"
#include "backlog_controller.hh"
#include "seastarx.hh"
#include "sstables/exceptions.hh"
#include "tombstone_gc.hh"

namespace db {
class system_keyspace;
class compaction_history_entry;
}

namespace sstables { class test_env_compaction_manager; }

class repair_history_map {
public:
    boost::icl::interval_map<dht::token, gc_clock::time_point, boost::icl::partial_absorber, std::less, boost::icl::inplace_max> map;
};

namespace compaction {
using throw_if_stopping = bool_class<struct throw_if_stopping_tag>;

class compaction_task_executor;
class sstables_task_executor;
class major_compaction_task_executor;
class custom_compaction_task_executor;
class regular_compaction_task_executor;
class offstrategy_compaction_task_executor;
class rewrite_sstables_compaction_task_executor;
class split_compaction_task_executor;
class cleanup_sstables_compaction_task_executor;
class validate_sstables_compaction_task_executor;

inline owned_ranges_ptr make_owned_ranges_ptr(dht::token_range_vector&& ranges) {
    return make_lw_shared<const dht::token_range_vector>(std::move(ranges));
}

}
// Compaction manager provides facilities to submit and track compaction jobs on
// behalf of existing tables.
class compaction_manager {
public:
    using compaction_stats_opt = std::optional<sstables::compaction_stats>;
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
        uint64_t active_tasks = 0; // Number of compaction going on.
        int64_t errors = 0;
    };
    using scheduling_group = backlog_controller::scheduling_group;
    struct config {
        scheduling_group compaction_sched_group;
        scheduling_group maintenance_sched_group;
        size_t available_memory = 0;
        utils::updateable_value<float> static_shares = utils::updateable_value<float>(0);
        utils::updateable_value<uint32_t> throughput_mb_per_sec = utils::updateable_value<uint32_t>(0);
        std::chrono::seconds flush_all_tables_before_major = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::days(1));
    };

public:
    class can_purge_tombstones_tag;
    using can_purge_tombstones = bool_class<can_purge_tombstones_tag>;

private:
    shared_ptr<compaction::task_manager_module> _task_manager_module;
    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::list<shared_ptr<compaction::compaction_task_executor>> _tasks;

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
    std::unordered_set<compaction::table_state*> _postponed;
    // tracks taken weights of ongoing compactions, only one compaction per weight is allowed.
    // weight is value assigned to a compaction job that is log base N of total size of all input sstables.
    std::unordered_set<int> _weight_tracker;

    std::unordered_map<compaction::table_state*, compaction_state> _compaction_state;

    // Purpose is to serialize all maintenance (non regular) compaction activity to reduce aggressiveness and space requirement.
    // If the operation must be serialized with regular, then the per-table write lock must be taken.
    seastar::named_semaphore _maintenance_ops_sem = {1, named_semaphore_exception_factory{"maintenance operation"}};

    // This semaphore ensures that off-strategy compaction will be serialized for
    // all tables, to limit space requirement and protect against candidates
    // being picked more than once.
    seastar::named_semaphore _off_strategy_sem = {1, named_semaphore_exception_factory{"off-strategy compaction"}};

    seastar::shared_ptr<db::system_keyspace> _sys_ks;

    std::function<void()> compaction_submission_callback();
    // all registered tables are reevaluated at a constant interval.
    // Submission is a NO-OP when there's nothing to do, so it's fine to call it regularly.
    static constexpr std::chrono::seconds periodic_compaction_submission_interval() { return std::chrono::seconds(3600); }

    config _cfg;
    timer<lowres_clock> _compaction_submission_timer;
    compaction_controller _compaction_controller;
    compaction_backlog_manager _backlog_manager;
    optimized_optional<abort_source::subscription> _early_abort_subscription;
    serialized_action _throughput_updater;
    std::optional<utils::observer<uint32_t>> _throughput_option_observer;
    serialized_action _update_compaction_static_shares_action;
    utils::observer<float> _compaction_static_shares_observer;
    uint64_t _validation_errors = 0;

    class strategy_control;
    std::unique_ptr<strategy_control> _strategy_control;

    per_table_history_maps _repair_history_maps;
    tombstone_gc_state _tombstone_gc_state;
private:
    // Requires task->_compaction_state.gate to be held and task to be registered in _tasks.
    future<compaction_stats_opt> perform_task(shared_ptr<compaction::compaction_task_executor> task, throw_if_stopping do_throw_if_stopping);

    // Return nullopt if compaction cannot be started
    std::optional<gate::holder> start_compaction(table_state& t);

    template<typename TaskExecutor, typename... Args>
    requires std::is_base_of_v<compaction_task_executor, TaskExecutor> &&
            std::is_base_of_v<compaction_task_impl, TaskExecutor> &&
    requires (compaction_manager& cm, throw_if_stopping do_throw_if_stopping, Args&&... args) {
        {TaskExecutor(cm, do_throw_if_stopping, std::forward<Args>(args)...)} -> std::same_as<TaskExecutor>;
    }
    future<compaction_manager::compaction_stats_opt> perform_compaction(throw_if_stopping do_throw_if_stopping, tasks::task_info parent_info, Args&&... args);

    future<> stop_tasks(std::vector<shared_ptr<compaction::compaction_task_executor>> tasks, sstring reason);
    future<> update_throughput(uint32_t value_mbs);

    // Return the largest fan-in of currently running compactions
    unsigned current_compaction_fan_in_threshold() const;

    // Return true if compaction can be initiated
    bool can_register_compaction(compaction::table_state& t, int weight, unsigned fan_in) const;
    // Register weight for a table. Do that only if can_register_weight()
    // returned true.
    void register_weight(int weight);
    // Deregister weight for a table.
    void deregister_weight(int weight);

    // Get candidates for compaction strategy, which are all sstables but the ones being compacted.
    std::vector<sstables::shared_sstable> get_candidates(compaction::table_state& t) const;

    bool eligible_for_compaction(const sstables::shared_sstable& sstable) const;
    bool eligible_for_compaction(const sstables::frozen_sstable_run& sstable_run) const;

    template <std::ranges::range Range>
    requires std::convertible_to<std::ranges::range_value_t<Range>, sstables::shared_sstable> || std::convertible_to<std::ranges::range_value_t<Range>, sstables::frozen_sstable_run>
    std::vector<std::ranges::range_value_t<Range>> get_candidates(table_state& t, const Range& sstables) const;

    template <std::ranges::range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, sstables::shared_sstable>
    void register_compacting_sstables(const Range& range);

    template <std::ranges::range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, sstables::shared_sstable>
    void deregister_compacting_sstables(const Range& range);

    // gets the table's compaction state
    // throws std::out_of_range exception if not found.
    compaction_state& get_compaction_state(compaction::table_state* t);
    const compaction_state& get_compaction_state(compaction::table_state* t) const {
        return const_cast<compaction_manager*>(this)->get_compaction_state(t);
    }

    // Return true if compaction manager is enabled and
    // table still exists and compaction is not disabled for the table.
    inline bool can_proceed(compaction::table_state* t) const;

    future<> postponed_compactions_reevaluation();
    void reevaluate_postponed_compactions() noexcept;
    // Postpone compaction for a table that couldn't be executed due to ongoing
    // similar-sized compaction.
    void postpone_compaction_for_table(compaction::table_state* t);

    future<compaction_stats_opt> perform_sstable_scrub_validate_mode(compaction::table_state& t, tasks::task_info info);
    future<> update_static_shares(float shares);

    using get_candidates_func = std::function<future<std::vector<sstables::shared_sstable>>()>;

    // Guarantees that a maintenance task, e.g. cleanup, will be performed on all files available at the time
    // by retrieving set of candidates only after all compactions for table T were stopped, if any.
    template<typename TaskType, typename... Args>
    requires std::derived_from<TaskType, compaction_task_executor> &&
            std::derived_from<TaskType, compaction_task_impl>
    future<compaction_manager::compaction_stats_opt> perform_task_on_all_files(tasks::task_info info, table_state& t, sstables::compaction_type_options options, owned_ranges_ptr owned_ranges_ptr, get_candidates_func get_func, Args... args);

    future<compaction_stats_opt> rewrite_sstables(compaction::table_state& t, sstables::compaction_type_options options, owned_ranges_ptr, get_candidates_func, tasks::task_info info,
                                                  can_purge_tombstones can_purge = can_purge_tombstones::yes, sstring options_desc = "");

    // Stop all fibers, without waiting. Safe to be called multiple times.
    void do_stop() noexcept;
    future<> really_do_stop();

    // Propagate replacement of sstables to all ongoing compaction of a given table
    void propagate_replacement(compaction::table_state& t, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added);

    // This constructor is supposed to only be used for testing so lets be more explicit
    // about invoking it. Ref #10146
    compaction_manager(tasks::task_manager& tm);
public:
    compaction_manager(config cfg, abort_source& as, tasks::task_manager& tm);
    ~compaction_manager();
    class for_testing_tag{};
    // An inline constructor for testing
    compaction_manager(tasks::task_manager& tm, for_testing_tag) : compaction_manager(tm) {}

    compaction::task_manager_module& get_task_manager_module() noexcept {
        return *_task_manager_module;
    }

    const scheduling_group& compaction_sg() const noexcept {
        return _cfg.compaction_sched_group;
    }

    const scheduling_group& maintenance_sg() const noexcept {
        return _cfg.maintenance_sched_group;
    }

    size_t available_memory() const noexcept {
        return _cfg.available_memory;
    }

    float static_shares() const noexcept {
        return _cfg.static_shares.get();
    }

    uint32_t throughput_mbs() const noexcept {
        return _cfg.throughput_mb_per_sec.get();
    }

    std::chrono::seconds flush_all_tables_before_major() const noexcept {
        return _cfg.flush_all_tables_before_major;
    }

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

    using compaction_history_consumer = noncopyable_function<future<>(const db::compaction_history_entry&)>;
    future<> get_compaction_history(compaction_history_consumer&& f);

    // Submit a table to be compacted.
    void submit(compaction::table_state& t);

    // Can regular compaction be performed in the given table
    bool can_perform_regular_compaction(compaction::table_state& t);

    // Maybe wait before adding more sstables
    // if there are too many sstables.
    future<> maybe_wait_for_sstable_count_reduction(compaction::table_state& t);

    // Submit a table to be off-strategy compacted.
    // Returns true iff off-strategy compaction was required and performed.
    future<bool> perform_offstrategy(compaction::table_state& t, tasks::task_info info);

    // Submit a table to be cleaned up and wait for its termination.
    //
    // Performs a cleanup on each sstable of the table, excluding
    // those ones that are irrelevant to this node or being compacted.
    // Cleanup is about discarding keys that are no longer relevant for a
    // given sstable, e.g. after node loses part of its token range because
    // of a newly added node.
    future<> perform_cleanup(owned_ranges_ptr sorted_owned_ranges, compaction::table_state& t, tasks::task_info info);
private:
    future<> try_perform_cleanup(owned_ranges_ptr sorted_owned_ranges, compaction::table_state& t, tasks::task_info info);

    // Add sst to or remove it from the respective compaction_state.sstables_requiring_cleanup set.
    bool update_sstable_cleanup_state(table_state& t, const sstables::shared_sstable& sst, const dht::token_range_vector& sorted_owned_ranges);

    future<> on_compaction_completion(table_state& t, sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy);
public:
    // Submit a table to be upgraded and wait for its termination.
    future<> perform_sstable_upgrade(owned_ranges_ptr sorted_owned_ranges, compaction::table_state& t, bool exclude_current_version, tasks::task_info info);

    // Submit a table to be scrubbed and wait for its termination.
    future<compaction_stats_opt> perform_sstable_scrub(compaction::table_state& t, sstables::compaction_type_options::scrub opts, tasks::task_info info);

    // Submit a table for major compaction.
    future<> perform_major_compaction(compaction::table_state& t, tasks::task_info info);

    // Splits a compaction group by segregating all its sstable according to the classifier[1].
    // [1]: See sstables::compaction_type_options::splitting::classifier.
    // Returns when all sstables in the main sstable set are split. The only exception is shutdown
    // or user aborted splitting using stop API.
    future<compaction_stats_opt> perform_split_compaction(compaction::table_state& t, sstables::compaction_type_options::split opt, tasks::task_info info);

    // Splits a single SSTable by segregating all its data according to the classifier.
    // If SSTable doesn't need split, the same input SSTable is returned as output.
    // If SSTable needs split, then output SSTables are returned and the input SSTable is deleted.
    future<std::vector<sstables::shared_sstable>> maybe_split_sstable(sstables::shared_sstable sst, table_state& t, sstables::compaction_type_options::split opt);

    // Run a custom job for a given table, defined by a function
    // it completes when future returned by job is ready or returns immediately
    // if manager was asked to stop.
    //
    // parameter type is the compaction type the operation can most closely be
    //      associated with, use compaction_type::Compaction, if none apply.
    // parameter job is a function that will carry the operation
    future<> run_custom_job(compaction::table_state& s, sstables::compaction_type type, const char *desc, noncopyable_function<future<>(sstables::compaction_data&, sstables::compaction_progress_monitor&)> job, tasks::task_info info, throw_if_stopping do_throw_if_stopping);

    class compaction_reenabler {
        compaction_manager& _cm;
        compaction::table_state* _table;
        compaction::compaction_state& _compaction_state;
        gate::holder _holder;

    public:
        compaction_reenabler(compaction_manager&, compaction::table_state&);
        compaction_reenabler(compaction_reenabler&&) noexcept;

        ~compaction_reenabler();

        compaction::table_state* compacting_table() const noexcept {
            return _table;
        }

        const compaction::compaction_state& compaction_state() const noexcept {
            return _compaction_state;
        }
    };

    // Disable compaction temporarily for a table t.
    // Caller should call the compaction_reenabler::reenable
    future<compaction_reenabler> stop_and_disable_compaction(compaction::table_state& t);

    // Run a function with compaction temporarily disabled for a table T.
    future<> run_with_compaction_disabled(compaction::table_state& t, std::function<future<> ()> func);

    void plug_system_keyspace(db::system_keyspace& sys_ks) noexcept;
    void unplug_system_keyspace() noexcept;

    // Adds a table to the compaction manager.
    // Creates a compaction_state structure that can be used for submitting
    // compaction jobs of all types.
    void add(compaction::table_state& t);

    // Remove a table from the compaction manager.
    // Cancel requests on table and wait for possible ongoing compactions.
    future<> remove(compaction::table_state& t, sstring reason = "table removal") noexcept;

    const stats& get_stats() const {
        return _stats;
    }

    const std::vector<sstables::compaction_info> get_compactions(compaction::table_state* t = nullptr) const;

    // Returns true if table has an ongoing compaction, running on its behalf
    bool has_table_ongoing_compaction(const compaction::table_state& t) const;

    bool compaction_disabled(compaction::table_state& t) const;

    // Stops ongoing compaction of a given type.
    future<> stop_compaction(sstring type, compaction::table_state* table = nullptr);

    // Stops ongoing compaction of a given table and/or compaction_type.
    future<> stop_ongoing_compactions(sstring reason, compaction::table_state* t = nullptr, std::optional<sstables::compaction_type> type_opt = {}) noexcept;

    double backlog() {
        return _backlog_manager.backlog();
    }

    void register_backlog_tracker(compaction_backlog_tracker& backlog_tracker) {
        _backlog_manager.register_backlog_tracker(backlog_tracker);
    }
    void register_backlog_tracker(compaction::table_state& t, compaction_backlog_tracker new_backlog_tracker);

    compaction_backlog_tracker& get_backlog_tracker(compaction::table_state& t);

    static sstables::compaction_data create_compaction_data();

    compaction::strategy_control& get_strategy_control() const noexcept;

    tombstone_gc_state& get_tombstone_gc_state() noexcept {
        return _tombstone_gc_state;
    };

    const tombstone_gc_state& get_tombstone_gc_state() const noexcept {
        return _tombstone_gc_state;
    };

    // Uncoditionally erase sst from `sstables_requiring_cleanup`
    // Returns true iff sst was found and erased.
    bool erase_sstable_cleanup_state(table_state& t, const sstables::shared_sstable& sst);

    // checks if the sstable is in the respective compaction_state.sstables_requiring_cleanup set.
    bool requires_cleanup(table_state& t, const sstables::shared_sstable& sst) const;
    const std::unordered_set<sstables::shared_sstable>& sstables_requiring_cleanup(table_state& t) const;

    friend class compacting_sstable_registration;
    friend class compaction_weight_registration;
    friend class sstables::test_env_compaction_manager;

    friend class compaction::compaction_task_executor;
    friend class compaction::sstables_task_executor;
    friend class compaction::major_compaction_task_executor;
    friend class compaction::split_compaction_task_executor;
    friend class compaction::custom_compaction_task_executor;
    friend class compaction::regular_compaction_task_executor;
    friend class compaction::offstrategy_compaction_task_executor;
    friend class compaction::rewrite_sstables_compaction_task_executor;
    friend class compaction::cleanup_sstables_compaction_task_executor;
    friend class compaction::validate_sstables_compaction_task_executor;
};

namespace compaction {

class compaction_task_executor : public enable_shared_from_this<compaction_task_executor> {
public:
    enum class state {
        none,       // initial and final state
        pending,    // task is blocked on a lock, may alternate with active
                    // counted in compaction_manager::stats::pending_tasks
        active,     // task initiated active compaction, may alternate with pending
                    // counted in compaction_manager::stats::active_tasks
        done,       // task completed successfully (may transition only to state::none, or
                    // state::pending for regular compaction)
                    // counted in compaction_manager::stats::completed_tasks
        postponed,  // task was postponed (may transition only to state::none)
                    // represented by the postponed_compactions metric
        failed,     // task failed (may transition only to state::none)
                    // counted in compaction_manager::stats::errors
    };
protected:
    compaction_manager& _cm;
    ::compaction::table_state* _compacting_table = nullptr;
    compaction::compaction_state& _compaction_state;
    sstables::compaction_data _compaction_data;
    state _state = state::none;
    throw_if_stopping _do_throw_if_stopping;
    sstables::compaction_progress_monitor _progress_monitor;

private:
    shared_future<compaction_manager::compaction_stats_opt> _compaction_done = make_ready_future<compaction_manager::compaction_stats_opt>();
    exponential_backoff_retry _compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
    sstables::compaction_type _type;
    sstables::run_id _output_run_identifier;
    sstring _description;
    compaction_manager::compaction_stats_opt _stats = std::nullopt;

public:
    explicit compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, ::compaction::table_state* t, sstables::compaction_type type, sstring desc);

    compaction_task_executor(compaction_task_executor&&) = delete;
    compaction_task_executor(const compaction_task_executor&) = delete;

    virtual ~compaction_task_executor() = default;

    // called when a compaction replaces the exhausted sstables with the new set
    struct on_replacement {
        virtual ~on_replacement() {}
        // called after the replacement completes
        // @param sstables the old sstable which are replaced in this replacement
        virtual void on_removal(const std::vector<sstables::shared_sstable>& sstables) = 0;
        // called before the replacement happens
        // @param sstables the new sstables to be added to the table's sstable set
        virtual void on_addition(const std::vector<sstables::shared_sstable>& sstables) = 0;
    };

protected:
    future<> perform();

    virtual future<compaction_manager::compaction_stats_opt> do_run() = 0;

    state switch_state(state new_state);

    future<semaphore_units<named_semaphore_exception_factory>> acquire_semaphore(named_semaphore& sem, size_t units = 1);

    // Return true if the task isn't stopped
    // and the compaction manager allows proceeding.
    inline bool can_proceed(throw_if_stopping do_throw_if_stopping = throw_if_stopping::no) const;
    void setup_new_compaction(sstables::run_id output_run_id = sstables::run_id::create_null_id());
    void finish_compaction(state finish_state = state::done) noexcept;

    // Compaction manager stop itself if it finds an storage I/O error which results in
    // stop of transportation services. It cannot make progress anyway.
    // Returns exception if error is judged fatal, and compaction task must be stopped,
    // otherwise, returns stop_iteration::no after sleep for exponential retry.
    future<stop_iteration> maybe_retry(std::exception_ptr err, bool throw_on_abort = false);

    future<sstables::compaction_result> compact_sstables_and_update_history(sstables::compaction_descriptor descriptor, sstables::compaction_data& cdata, on_replacement&,
                                compaction_manager::can_purge_tombstones can_purge = compaction_manager::can_purge_tombstones::yes);
    future<sstables::compaction_result> compact_sstables(sstables::compaction_descriptor descriptor, sstables::compaction_data& cdata, on_replacement&,
                                compaction_manager::can_purge_tombstones can_purge = compaction_manager::can_purge_tombstones::yes,
                                sstables::offstrategy offstrategy = sstables::offstrategy::no);
    future<> update_history(::compaction::table_state& t, const sstables::compaction_result& res, const sstables::compaction_data& cdata);
    bool should_update_history(sstables::compaction_type ct) {
        return ct == sstables::compaction_type::Compaction;
    }
public:
    compaction_manager::compaction_stats_opt get_stats() const noexcept {
        return _stats;
    }

    future<compaction_manager::compaction_stats_opt> run_compaction() noexcept;

    const ::compaction::table_state* compacting_table() const noexcept {
        return _compacting_table;
    }

    sstables::compaction_type compaction_type() const noexcept {
        return _type;
    }

    bool compaction_running() const noexcept {
        return _state == state::active;
    }

    const sstables::compaction_data& compaction_data() const noexcept {
        return _compaction_data;
    }

    sstables::compaction_data& compaction_data() noexcept {
        return _compaction_data;
    }

    bool generating_output_run() const noexcept {
        return compaction_running() && _output_run_identifier;
    }
    const sstables::run_id& output_run_id() const noexcept {
        return _output_run_identifier;
    }

    const sstring& description() const noexcept {
        return _description;
    }
private:
    // Before _compaction_done is set in compaction_task_executor::run_compaction(), compaction_done() returns ready future.
    future<compaction_manager::compaction_stats_opt> compaction_done() noexcept {
        return _compaction_done.get_future();
    }
public:
    bool stopping() const noexcept {
        return _compaction_data.abort.abort_requested();
    }

    void abort(abort_source& as) noexcept;

    void stop_compaction(sstring reason) noexcept;

    sstables::compaction_stopped_exception make_compaction_stopped_exception() const;

    template<typename TaskExecutor, typename... Args>
    requires std::is_base_of_v<compaction_task_executor, TaskExecutor> &&
            std::is_base_of_v<compaction_task_impl, TaskExecutor> &&
    requires (compaction_manager& cm, throw_if_stopping do_throw_if_stopping, Args&&... args) {
        {TaskExecutor(cm, do_throw_if_stopping, std::forward<Args>(args)...)} -> std::same_as<TaskExecutor>;
    }
    friend future<compaction_manager::compaction_stats_opt> compaction_manager::perform_compaction(throw_if_stopping do_throw_if_stopping, tasks::task_info parent_info, Args&&... args);
    friend future<compaction_manager::compaction_stats_opt> compaction_manager::perform_task(shared_ptr<compaction_task_executor> task, throw_if_stopping do_throw_if_stopping);
    friend fmt::formatter<compaction_task_executor>;
    friend future<> compaction_manager::stop_tasks(std::vector<shared_ptr<compaction_task_executor>> tasks, sstring reason);
    friend sstables::test_env_compaction_manager;
};

}

template <>
struct fmt::formatter<compaction::compaction_task_executor::state> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(compaction::compaction_task_executor::state c, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<compaction::compaction_task_executor> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const compaction::compaction_task_executor& ex, fmt::format_context& ctx) const  -> decltype(ctx.out());
};

bool needs_cleanup(const sstables::shared_sstable& sst, const dht::token_range_vector& owned_ranges);

// Return all sstables but those that are off-strategy like the ones in maintenance set and staging dir.
std::vector<sstables::shared_sstable> in_strategy_sstables(compaction::table_state& table_s);
