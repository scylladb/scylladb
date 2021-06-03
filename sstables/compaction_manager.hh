/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
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
#include "sstables/compaction.hh"
#include "compaction_weight_registration.hh"
#include "compaction_backlog_manager.hh"
#include "backlog_controller.hh"
#include "seastarx.hh"

class table;
using column_family = table;
class compacting_sstable_registration;

// Compaction manager is a feature used to manage compaction jobs from multiple
// column families pertaining to the same database.
class compaction_manager {
public:
    struct stats {
        int64_t pending_tasks = 0;
        int64_t completed_tasks = 0;
        uint64_t active_tasks = 0; // Number of compaction going on.
        int64_t errors = 0;
    };
private:
    struct task {
        column_family* compacting_cf = nullptr;
        shared_future<> compaction_done = make_ready_future<>();
        exponential_backoff_retry compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
        bool stopping = false;
        sstables::compaction_type type = sstables::compaction_type::Compaction;
        bool compaction_running = false;
    };

    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::list<lw_shared_ptr<task>> _tasks;

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

    std::list<lw_shared_ptr<sstables::compaction_info>> _compactions;

    // Store sstables that are being compacted at the moment. That's needed to prevent
    // a sstable from being compacted twice.
    std::unordered_set<sstables::shared_sstable> _compacting_sstables;

    future<> _waiting_reevalution = make_ready_future<>();
    condition_variable _postponed_reevaluation;
    // column families that wait for compaction but had its submission postponed due to ongoing compaction.
    std::vector<column_family*> _postponed;
    // tracks taken weights of ongoing compactions, only one compaction per weight is allowed.
    // weight is value assigned to a compaction job that is log base N of total size of all input sstables.
    std::unordered_set<int> _weight_tracker;

    // Purpose is to serialize major compaction across all column families, so as to
    // reduce disk space requirement.
    semaphore _major_compaction_sem{1};
    // Prevents column family from running major and minor compaction at same time.
    std::unordered_map<column_family*, rwlock> _compaction_locks;

    semaphore _custom_job_sem{1};
    seastar::named_semaphore _rewrite_sstables_sem = {1, named_semaphore_exception_factory{"rewrite sstables"}};

    std::function<void()> compaction_submission_callback();
    // all registered column families are submitted for compaction at a constant interval.
    // Submission is a NO-OP when there's nothing to do, so it's fine to call it regularly.
    timer<lowres_clock> _compaction_submission_timer = timer<lowres_clock>(compaction_submission_callback());
    static constexpr std::chrono::seconds periodic_compaction_submission_interval() { return std::chrono::seconds(3600); }
private:
    future<> task_stop(lw_shared_ptr<task> task);

    // Return true if weight is not registered.
    bool can_register_weight(column_family* cf, int weight) const;
    // Register weight for a column family. Do that only if can_register_weight()
    // returned true.
    void register_weight(int weight);
    // Deregister weight for a column family.
    void deregister_weight(int weight);

    // Get candidates for compaction strategy, which are all sstables but the ones being compacted.
    std::vector<sstables::shared_sstable> get_candidates(const column_family& cf);

    void register_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables);
    void deregister_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables);

    // Return true if compaction manager and task weren't asked to stop.
    inline bool can_proceed(const lw_shared_ptr<task>& task);

    // Check if column family is being cleaned up.
    inline bool check_for_cleanup(column_family *cf);

    inline future<> put_task_to_sleep(lw_shared_ptr<task>& task);

    // Compaction manager stop itself if it finds an storage I/O error which results in
    // stop of transportation services. It cannot make progress anyway.
    // Returns true if error is judged not fatal, and compaction can be retried.
    inline bool maybe_stop_on_error(future<> f, stop_iteration will_stop = stop_iteration::no);

    void postponed_compactions_reevaluation();
    void reevaluate_postponed_compactions();
    // Postpone compaction for a column family that couldn't be executed due to ongoing
    // similar-sized compaction.
    void postpone_compaction_for_column_family(column_family* cf);

    compaction_controller _compaction_controller;
    compaction_backlog_manager _backlog_manager;
    seastar::scheduling_group _scheduling_group;
    size_t _available_memory;

    using get_candidates_func = std::function<std::vector<sstables::shared_sstable>(const column_family&)>;

    future<> rewrite_sstables(column_family* cf, sstables::compaction_options options, get_candidates_func);

    future<> stop_ongoing_compactions(sstring reason);
    optimized_optional<abort_source::subscription> _early_abort_subscription;
public:
    compaction_manager(seastar::scheduling_group sg, const ::io_priority_class& iop, size_t available_memory, abort_source& as);
    compaction_manager(seastar::scheduling_group sg, const ::io_priority_class& iop, size_t available_memory, uint64_t shares, abort_source& as);
    compaction_manager();
    ~compaction_manager();

    void register_metrics();

    // enable/disable compaction manager.
    void enable();
    void disable();

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

    // Submit a column family to be compacted.
    void submit(column_family* cf);

    // Submit a column family to be off-strategy compacted.
    void submit_offstrategy(column_family* cf);

    // Submit a column family to be cleaned up and wait for its termination.
    //
    // Performs a cleanup on each sstable of the column family, excluding
    // those ones that are irrelevant to this node or being compacted.
    // Cleanup is about discarding keys that are no longer relevant for a
    // given sstable, e.g. after node loses part of its token range because
    // of a newly added node.
    future<> perform_cleanup(database& db, column_family* cf);

    // Submit a column family to be upgraded and wait for its termination.
    future<> perform_sstable_upgrade(database& db, column_family* cf, bool exclude_current_version);

    // Submit a column family to be scrubbed and wait for its termination.
    future<> perform_sstable_scrub(column_family* cf, sstables::compaction_options::scrub::mode scrub_mode);

    // Submit a column family for major compaction.
    future<> submit_major_compaction(column_family* cf);


    // Run a custom job for a given column family, defined by a function
    // it completes when future returned by job is ready or returns immediately
    // if manager was asked to stop.
    //
    // parameter job is a function that will carry the operation
    future<> run_custom_job(column_family* cf, sstring name, noncopyable_function<future<>()> job);

    // Remove a column family from the compaction manager.
    // Cancel requests on cf and wait for a possible ongoing compaction on cf.
    future<> remove(column_family* cf);

    // No longer interested in tracking backlog for compactions in this column
    // family. For instance, we could be ALTERing TABLE to a different strategy.
    void stop_tracking_ongoing_compactions(column_family* cf);

    const stats& get_stats() const {
        return _stats;
    }

    void register_compaction(lw_shared_ptr<sstables::compaction_info> c) {
        _compactions.push_back(c);
    }

    void deregister_compaction(lw_shared_ptr<sstables::compaction_info> c) {
        _compactions.remove(c);
    }

    const std::list<lw_shared_ptr<sstables::compaction_info>>& get_compactions() const {
        return _compactions;
    }

    // Returns true if table has an ongoing compaction, running on its behalf
    bool has_table_ongoing_compaction(column_family* cf) const {
        return std::any_of(_tasks.begin(), _tasks.end(), [cf] (const lw_shared_ptr<task>& task) {
            return task->compacting_cf == cf && task->compaction_running;
        });
    };

    // Stops ongoing compaction of a given type.
    void stop_compaction(sstring type);

    double backlog() {
        return _backlog_manager.backlog();
    }

    void register_backlog_tracker(compaction_backlog_tracker& backlog_tracker) {
        _backlog_manager.register_backlog_tracker(backlog_tracker);
    }

    // Propagate replacement of sstables to all ongoing compaction of a given column family
    void propagate_replacement(column_family*cf, const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added);

    friend class compacting_sstable_registration;
    friend class compaction_weight_registration;
};

bool needs_cleanup(const sstables::shared_sstable& sst, const dht::token_range_vector& owned_ranges, schema_ptr s);

