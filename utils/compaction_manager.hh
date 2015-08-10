/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include "core/semaphore.hh"
#include "core/sstring.hh"
#include "core/shared_ptr.hh"
#include "core/gate.hh"
#include "log.hh"
#include "utils/exponential_backoff_retry.hh"
#include <queue>
#include <vector>
#include <functional>

// Compaction manager is a feature used to manage compaction jobs from multiple
// column families pertaining to the same database.
// For each compaction job handler, there will be one fiber that will check for
// jobs, and if any, run it. FIFO ordering is implemented here.
class compaction_manager {
    struct task {
        future<> compaction_done = make_ready_future<>();
        semaphore compaction_sem = semaphore(0);
        seastar::gate compaction_gate;
        exponential_backoff_retry compaction_retry = exponential_backoff_retry(std::chrono::seconds(5), std::chrono::seconds(300));
        // Compaction job being currently executed.
        std::function<future<> ()> current_compaction_job;
    };
    // compaction manager may have N fibers to allow parallel compaction per shard.
    std::vector<lw_shared_ptr<task>> _tasks;

    // Job queue shared among all tasks.
    std::queue<std::function<future<> ()>> _compaction_jobs;

    // Used to assert that compaction_manager was explicitly stopped, if started.
    bool _stopped = true;
private:
    void task_start(lw_shared_ptr<task>& task);

    future<> task_stop(lw_shared_ptr<task>& task);
public:
    compaction_manager();
    ~compaction_manager();

    // Creates N fibers that will allow N compaction jobs to run in parallel.
    // Defaults to only one fiber.
    void start(int task_nr = 1);

    // Stop all fibers. Ongoing compactions will be waited.
    future<> stop();

    // Submit compaction job to a fiber with the lowest amount of pending jobs.
    void submit(std::function<future<> ()> compaction_job);
};

