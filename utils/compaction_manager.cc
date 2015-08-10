/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "compaction_manager.hh"

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
                if (_compaction_jobs.empty() && !task->current_compaction_job) {
                    return make_ready_future<>();
                }

                // Get a compaction job from the shared queue if and only
                // if, the previous compaction job succeeded.
                if (!task->current_compaction_job) {
                    task->current_compaction_job = _compaction_jobs.front();
                    _compaction_jobs.pop();
                }

                return task->current_compaction_job().then([task] {
                    // If compaction completed successfully, let's reset
                    // sleep time of compaction_retry.
                    task->compaction_retry.reset();

                    // current_compaction_job is made empty if compaction
                    // succeeded, meaning no retry is needed.
                    task->current_compaction_job = nullptr;
                });
            });
        }).then_wrapped([task] (future<> f) {
            bool retry = false;

            // Certain exceptions are used for regular termination of the fiber,
            // such as broken_semaphore and seastar::gate_closed_exception.
            try {
                f.get();
            } catch (broken_semaphore& e) {
                cmlog.info("compaction task handler stopped due to shutdown");
                throw;
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
                return task->compaction_retry.retry().then([task] {
                    // after sleeping, signal semaphore for the next compaction attempt.
                    task->compaction_sem.signal();
                });
            }
            return make_ready_future<>();
        });
    }).then_wrapped([] (future<> f) {
        try {
            f.get();
        } catch (broken_semaphore& e) {
            // exception logged in keep_doing.
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
        task->compaction_sem.broken();
        return task->compaction_done.then([] {
            return make_ready_future<>();
        });
    });
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
        return make_ready_future<>();
    });
}

void compaction_manager::submit(std::function<future<> ()> compaction_job) {
    if (_tasks.empty()) {
        return;
    }
    // Signal the compaction task with the lowest amount of pending jobs.
    auto result = std::min_element(std::begin(_tasks), std::end(_tasks), [] (auto& i, auto& j) {
        return i->compaction_sem.current() < j->compaction_sem.current();
    });
    _compaction_jobs.push(compaction_job);
    (*result)->compaction_sem.signal();
}
