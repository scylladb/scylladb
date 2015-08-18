/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "compaction_manager.hh"
#include "database.hh"

static logging::logger cmlog("compaction_manager");

void compaction_manager::task_start(lw_shared_ptr<compaction_manager::task>& task) {
    // NOTE: Compaction code runs in parallel to the rest of the system.
    // When it's time to shutdown, we need to prevent any new compaction
    // from starting and wait for a possible ongoing compaction.
    // That's possible by closing gate, busting semaphore and waiting for
    // the future compaction_done to resolve.

    task->compaction_done = keep_doing([this, task] {
        return task->compaction_sem.wait().then([this, task] {
            return seastar::with_gate(task->compaction_gate, [this, task] {
                if (_cfs_to_compact.empty() && !task->compacting_cf) {
                    return make_ready_future<>();
                }

                // Get a column family from the shared queue if and only
                // if, the previous compaction job succeeded.
                if (!task->compacting_cf) {
                    task->compacting_cf = _cfs_to_compact.front();
                    _cfs_to_compact.pop_front();
                    _stats.pending_tasks--;
                }

                return task->compacting_cf->run_compaction().then([this, task] {
                    // If compaction completed successfully, let's reset
                    // sleep time of compaction_retry.
                    task->compaction_retry.reset();

                    // Re-schedule compaction for compacting_cf, if needed.
                    if (task->compacting_cf->pending_compactions()) {
                        // If there are pending compactions for compacting cf,
                        // push it into the back of the queue.
                        add_column_family(task->compacting_cf);
                        task->compaction_sem.signal();
                    } else {
                        // If so, cf is no longer queued by compaction manager.
                        task->compacting_cf->set_compaction_manager_queued(false);
                    }
                    task->compacting_cf = nullptr;

                    _stats.completed_tasks++;
                });
            });
        }).then_wrapped([this, task] (future<> f) {
            bool retry = false;

            // seastar::gate_closed_exception is used for regular termination
            // of the fiber.
            try {
                f.get();
            } catch (seastar::gate_closed_exception& e) {
                cmlog.info("compaction task handler stopped due to shutdown");
                throw;
            } catch (std::exception& e) {
                cmlog.error("compaction failed: {}", e.what());
                retry = true;
            } catch (...) {
                cmlog.error("compaction failed: unknown error");
                retry = true;
            }

            if (retry) {
                cmlog.info("compaction task handler sleeping for {} seconds",
                    std::chrono::duration_cast<std::chrono::seconds>(task->compaction_retry.sleep_time()).count());
                return task->compaction_retry.retry().then([this, task] {
                    // pushing cf to the back, so if the error is persistent,
                    // at least the others get a chance.
                    add_column_family(task->compacting_cf);
                    task->compacting_cf = nullptr;

                    // after sleeping, signal semaphore for the next compaction attempt.
                    task->compaction_sem.signal();
                });
            }
            return make_ready_future<>();
        });
    }).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (seastar::gate_closed_exception& e) {
            // exception logged in keep_doing.
        } catch (...) {
            // this shouldn't happen, let's log it anyway.
            cmlog.error("compaction task: unexpected error");
        }
    });
}

future<> compaction_manager::task_stop(lw_shared_ptr<compaction_manager::task>& task) {
    return task->compaction_gate.close().then([task] {
        // NOTE: Signalling semaphore because we want task to finish with the
        // gate_closed_exception exception.
        task->compaction_sem.signal();
        return task->compaction_done.then([] {
            return make_ready_future<>();
        });
    });
}

void compaction_manager::add_column_family(column_family* cf) {
    _cfs_to_compact.push_back(cf);
    _stats.pending_tasks++;
}

compaction_manager::compaction_manager() = default;

compaction_manager::~compaction_manager() {
    // Assert that compaction manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is destroyed.
    assert(_stopped == true);
}

void compaction_manager::start(int task_nr) {
    _stopped = false;
    _tasks.reserve(task_nr);
    for (int i = 0; i < task_nr; i++) {
        auto task = make_lw_shared<compaction_manager::task>();
        task_start(task);
        _tasks.push_back(task);
    }
}

future<> compaction_manager::stop() {
    return do_for_each(_tasks, [this] (auto& task) {
        return this->task_stop(task);
    }).then([this] {
        _stopped = true;
        for (auto& cf : _cfs_to_compact) {
            cf->set_compaction_manager_queued(false);
        }
        _cfs_to_compact.clear();
        return make_ready_future<>();
    });
}

void compaction_manager::submit(column_family* cf) {
    if (_tasks.empty()) {
        return;
    }
    // To avoid having two or more entries of the same cf stored in the queue.
    if (cf->compaction_manager_queued()) {
        return;
    }
    // Signal the compaction task with the lowest amount of pending jobs.
    auto result = std::min_element(std::begin(_tasks), std::end(_tasks), [] (auto& i, auto& j) {
        return i->compaction_sem.current() < j->compaction_sem.current();
    });
    cf->set_compaction_manager_queued(true);
    add_column_family(cf);
    (*result)->compaction_sem.signal();
}

future<> compaction_manager::remove(column_family* cf) {
    // Remove every reference to cf from _cfs_to_compact.
    _cfs_to_compact.erase(
        std::remove_if(_cfs_to_compact.begin(), _cfs_to_compact.end(), [cf] (column_family* entry) {
            return cf == entry;
        }),
        _cfs_to_compact.end());
    cf->set_compaction_manager_queued(false);

    // Wait for the termination of an ongoing compaction on cf, if any.
    return do_for_each(_tasks, [this, cf] (auto& task) {
        if (task->compacting_cf == cf) {
            return this->task_stop(task).then([this, &task] {
                // assert that task finished successfully.
                assert(task->compacting_cf == nullptr);
                this->task_start(task);
                return make_ready_future<>();
            });
        }
        return make_ready_future<>();
    });
}
