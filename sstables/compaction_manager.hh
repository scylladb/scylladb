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
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <vector>
#include <list>
#include <functional>
#include "sstables/compaction.hh"

class column_family;

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
        future<> compaction_done = make_ready_future<>();
        seastar::gate compaction_gate;
        exponential_backoff_retry compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
        bool stopping = false;
        bool cleanup = false;
    };

    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::list<lw_shared_ptr<task>> _tasks;

    // Used to wake up a caller of perform_cleanup() which is waiting for termination
    // of cleanup operation.
    std::unordered_map<column_family*, promise<>> _cleanup_waiters;

    // Used to assert that compaction_manager was explicitly stopped, if started.
    bool _stopped = true;

    stats _stats;
    std::vector<scollectd::registration> _registrations;

    std::list<lw_shared_ptr<sstables::compaction_info>> _compactions;

    // Store sstables that are being compacted at the moment. That's needed to prevent
    // a sstable from being compacted twice.
    std::unordered_set<sstables::shared_sstable> _compacting_sstables;

    // Keep track of weight of ongoing compaction for each column family.
    // That's used to allow parallel compaction on the same column family.
    std::unordered_map<column_family*, std::unordered_set<int>> _weight_tracker;
private:
    void task_start(column_family* cf, bool cleanup);
    future<> task_stop(lw_shared_ptr<task> task);

    // Returns if this compaction manager is accepting new requests.
    // It will not accept new requests in case the manager was stopped.
    bool can_submit();

    // If weight is not taken for the column family, weight is registered and
    // true is returned. Return false otherwise.
    bool try_to_register_weight(column_family* cf, int weight);
    // Deregister weight for a column family.
    void deregister_weight(column_family* cf, int weight);

    // If weight of compaction job is taken, it will be trimmed until its new
    // weight is not taken or its size is equal to minimum threshold.
    // Return weight of compaction job.
    int trim_to_compact(column_family* cf, sstables::compaction_descriptor& descriptor);
public:
    compaction_manager();
    ~compaction_manager();

    void register_collectd_metrics();

    // Start compaction manager.
    void start();

    // Stop all fibers. Ongoing compactions will be waited.
    future<> stop();

    // Submit a column family to be compacted.
    void submit(column_family* cf);

    // Submit a column family to be cleaned up and wait for its termination.
    future<> perform_cleanup(column_family* cf);

    // Remove a column family from the compaction manager.
    // Cancel requests on cf and wait for a possible ongoing compaction on cf.
    future<> remove(column_family* cf);

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
};

