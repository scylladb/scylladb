/*
 * Copyright (C) 2015 ScyllaDB
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

#include "core/semaphore.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/gate.hh"
#include "core/shared_future.hh"
#include "core/rwlock.hh"
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/scheduling.hh>
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <vector>
#include <list>
#include <functional>
#include "sstables/compaction.hh"
#include "compaction_weight_registration.hh"
#include "compaction_backlog_manager.hh"
#include "backlog_controller.hh"

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
        bool cleanup = false;
        bool compaction_running = false;
    };

    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::list<lw_shared_ptr<task>> _tasks;

    // Used to assert that compaction_manager was explicitly stopped, if started.
    bool _stopped = true;

    stats _stats;
    seastar::metrics::metric_groups _metrics;

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

    semaphore _resharding_sem{1};

    std::function<void()> compaction_submission_callback();
    // all registered column families are submitted for compaction at a constant interval.
    // Submission is a NO-OP when there's nothing to do, so it's fine to call it regularly.
    timer<lowres_clock> _compaction_submission_timer = timer<lowres_clock>(compaction_submission_callback());
    static constexpr std::chrono::seconds periodic_compaction_submission_interval() { return std::chrono::seconds(3600); }
private:
    future<> task_stop(lw_shared_ptr<task> task);

    // Return true if weight is not registered.
    bool can_register_weight(column_family* cf, int weight);
    // Register weight for a column family. Do that only if can_register_weight()
    // returned true.
    void register_weight(int weight);
    // Deregister weight for a column family.
    void deregister_weight(int weight);

    // If weight of compaction job is taken, it will be trimmed until its new
    // weight is not taken or its size is equal to minimum threshold.
    // Return weight of compaction job.
    int trim_to_compact(column_family* cf, sstables::compaction_descriptor& descriptor);

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
    inline bool maybe_stop_on_error(future<> f);

    void postponed_compactions_reevaluation();
    void reevaluate_postponed_compactions();
    // Postpone compaction for a column family that couldn't be executed due to ongoing
    // similar-sized compaction.
    void postpone_compaction_for_column_family(column_family* cf);

    compaction_controller _compaction_controller;
    compaction_backlog_manager _backlog_manager;
    seastar::scheduling_group _scheduling_group;
    size_t _available_memory;
public:
    compaction_manager(seastar::scheduling_group sg, const ::io_priority_class& iop, size_t available_memory);
    compaction_manager(seastar::scheduling_group sg, const ::io_priority_class& iop, size_t available_memory, uint64_t shares);
    compaction_manager();
    ~compaction_manager();

    void register_metrics();

    // Start compaction manager.
    void start();

    // Stop all fibers. Ongoing compactions will be waited.
    future<> stop();

    bool stopped() const { return _stopped; }

    // Submit a column family to be compacted.
    void submit(column_family* cf);

    // Submit a column family to be cleaned up and wait for its termination.
    future<> perform_cleanup(column_family* cf);

    // Submit a column family for major compaction.
    future<> submit_major_compaction(column_family* cf);

    // Run a resharding job for a given column family.
    // it completes when future returned by job is ready or returns immediately
    // if manager was asked to stop.
    //
    // parameter job is a function that will carry the reshard operation on a set
    // of sstables that belong to different shards for this column family using
    // sstables::reshard_sstables(), and in the end, it will forward unshared
    // sstables created by the process to their owner shards.
    future<> run_resharding_job(column_family* cf, std::function<future<>()> job);

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

    // Stops ongoing compaction of a given type.
    void stop_compaction(sstring type);

    // Called by compaction procedure to release the weight lock assigned to it, such that
    // another compaction waiting on same weight can start as soon as possible. That's usually
    // called before compaction seals sstable and such and after all compaction work is done.
    void on_compaction_complete(compaction_weight_registration& weight_registration);

    float backlog() {
        return _backlog_manager.backlog();
    }

    void register_backlog_tracker(compaction_backlog_tracker& backlog_tracker) {
        _backlog_manager.register_backlog_tracker(backlog_tracker);
    }

    friend class compacting_sstable_registration;
    friend class compaction_weight_registration;
};

