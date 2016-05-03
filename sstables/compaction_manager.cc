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

#include "compaction_manager.hh"
#include "database.hh"
#include "core/scollectd.hh"
#include "exceptions.hh"
#include <cmath>

static logging::logger cmlog("compaction_manager");

static inline uint64_t get_total_size(const std::vector<sstables::shared_sstable>& sstables) {
    uint64_t total_size = 0;
    for (auto& sst : sstables) {
        total_size += sst->data_size();
    }
    return total_size;
}

// Calculate weight of compaction job.
static inline int calculate_weight(uint64_t total_size) {
    // At the moment, '4' is being used as log base for determining the weight
    // of a compaction job. With base of 4, what happens is that when you have
    // a 40-second compaction in progress, and a tiny 10-second compaction
    // comes along, you do them in parallel.
    // TODO: Find a possibly better log base through experimentation.
    static constexpr int WEIGHT_LOG_BASE = 4;

    // computes the logarithm (base WEIGHT_LOG_BASE) of total_size.
    return int(std::log(total_size) / std::log(WEIGHT_LOG_BASE));
}

static inline int calculate_weight(const std::vector<sstables::shared_sstable>& sstables) {
    if (sstables.empty()) {
        return 0;
    }
    return calculate_weight(get_total_size(sstables));
}

int compaction_manager::trim_to_compact(column_family* cf, sstables::compaction_descriptor& descriptor) {
    int weight = calculate_weight(descriptor.sstables);
    // NOTE: a compaction job with level > 0 cannot be trimmed because leveled
    // compaction relies on higher levels having no overlapping sstables.
    if (descriptor.level != 0 || descriptor.sstables.empty()) {
        return weight;
    }
    auto it = _weight_tracker.find(cf);
    if (it == _weight_tracker.end()) {
        return weight;
    }

    std::unordered_set<int>& s = it->second;
    uint64_t total_size = get_total_size(descriptor.sstables);
    int min_threshold = cf->schema()->min_compaction_threshold();

    while (descriptor.sstables.size() > size_t(min_threshold)) {
        if (s.count(weight)) {
            total_size -= descriptor.sstables.back()->data_size();
            descriptor.sstables.pop_back();
            weight = calculate_weight(total_size);
        } else {
            break;
        }
    }
    return weight;
}

bool compaction_manager::try_to_register_weight(column_family* cf, int weight) {
    auto it = _weight_tracker.find(cf);
    if (it == _weight_tracker.end()) {
        _weight_tracker.insert({cf, {weight}});
        return true;
    }
    std::unordered_set<int>& s = it->second;
    // TODO: Maybe allow only *smaller* compactions to start? That can be done
    // by returning true only if weight is not in the set and is lower than any
    // entry in the set.
    if (s.count(weight)) {
        // If reached this point, it means that there is an ongoing compaction
        // with the weight of the compaction job.
        return false;
    }
    s.insert(weight);
    return true;
}

void compaction_manager::deregister_weight(column_family* cf, int weight) {
    auto it = _weight_tracker.find(cf);
    assert(it != _weight_tracker.end());
    it->second.erase(weight);
}

void compaction_manager::task_start(column_family* cf, bool cleanup) {
    // NOTE: Compaction code runs in parallel to the rest of the system.
    // When it's time to shutdown, we need to prevent any new compaction
    // from starting and wait for a possible ongoing compaction.

    auto task = make_lw_shared<compaction_manager::task>();
    task->compacting_cf = cf;
    task->cleanup = cleanup;
    _tasks.push_back(task);
    _stats.pending_tasks++;

    task->compaction_done = repeat([this, task] {
            return seastar::with_gate(task->compaction_gate, [this, task] {
                if (_stopped || task->stopping) {
                    _stats.pending_tasks--;
                    return make_ready_future<>();
                }
                if (!task->cleanup && task->compacting_cf->pending_compactions() == 0) {
                    task->stopping = true;
                    _stats.pending_tasks--;
                    return make_ready_future<>();
                }

                column_family& cf = *task->compacting_cf;
                std::vector<sstables::shared_sstable> candidates; // candidates for compaction

                candidates.reserve(cf.sstables_count());
                // Filter out sstables that are being compacted.
                for (auto& entry : *cf.get_sstables()) {
                    auto& sst = entry.second;
                    if (!_compacting_sstables.count(sst)) {
                        candidates.push_back(sst);
                    }
                }

                sstables::compaction_descriptor descriptor;
                // Created to erase sstables from _compacting_sstables after compaction finishes.
                std::vector<sstables::shared_sstable> sstables_to_compact;
                int weight = -1;
                auto keep_track_of_compacting_sstables = [this, &sstables_to_compact, &descriptor] {
                    sstables_to_compact.reserve(descriptor.sstables.size());
                    for (auto& sst : descriptor.sstables) {
                        sstables_to_compact.push_back(sst);
                        _compacting_sstables.insert(sst);
                    }
                };

                future<> operation = make_ready_future<>();
                if (task->cleanup) {
                    descriptor = sstables::compaction_descriptor(std::move(candidates));
                    keep_track_of_compacting_sstables();
                    operation = cf.cleanup_sstables(std::move(descriptor));
                } else {
                    sstables::compaction_strategy cs = cf.get_compaction_strategy();
                    descriptor = cs.get_sstables_for_compaction(cf, std::move(candidates));
                    weight = trim_to_compact(&cf, descriptor);
                    if (!try_to_register_weight(&cf, weight)) {
                        // Refusing compaction job because of an ongoing compaction with same weight.
                        task->stopping = true;
                        _stats.pending_tasks--;
                        cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}.{}",
                            descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());
                        return make_ready_future<>();
                    }
                    keep_track_of_compacting_sstables();
                    cmlog.debug("Accepted compaction job ({} sstable(s)) of weight {} for {}.{}",
                        descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());
                    operation = cf.run_compaction(std::move(descriptor));
                }

                _stats.pending_tasks--;
                _stats.active_tasks++;
                return operation.then([this, task] {
                    _stats.completed_tasks++;
                    task->compaction_retry.reset();

                    if (task->cleanup) {
                        auto it = _cleanup_waiters.find(task->compacting_cf);
                        if (it != _cleanup_waiters.end()) {
                            // Notificate waiter of cleanup termination.
                            it->second.set_value();
                            _cleanup_waiters.erase(it);
                        }
                    }
                    return make_ready_future<>();
                }).finally([this, task, weight, sstables_to_compact = std::move(sstables_to_compact)] {
                    // Remove compacted sstables from the set of compacting sstables.
                    for (auto& sst : sstables_to_compact) {
                        _compacting_sstables.erase(sst);
                    }
                    if (weight != -1) {
                        deregister_weight(task->compacting_cf, weight);
                    }
                    _stats.active_tasks--;
                });
        }).then_wrapped([this, task] (future<> f) {
            bool retry = false;

            // seastar::gate_closed_exception is used for regular termination
            // of the fiber.
            try {
                f.get();
            } catch (seastar::gate_closed_exception& e) {
                cmlog.debug("compaction task handler stopped due to shutdown");
                throw;
            } catch (sstables::compaction_stop_exception& e) {
                cmlog.info("compaction info: {}", e.what());
                retry = true;
            } catch (std::exception& e) {
                cmlog.error("compaction failed: {}", e.what());
                retry = true;
            } catch (...) {
                cmlog.error("compaction failed: unknown error");
                retry = true;
            }

            // We shouldn't retry compaction if task was asked to stop or
            // compaction manager was stopped.
            if (!_stopped && !task->stopping) {
                if (retry) {
                    cmlog.info("compaction task handler sleeping for {} seconds",
                        std::chrono::duration_cast<std::chrono::seconds>(task->compaction_retry.sleep_time()).count());
                    _stats.errors++;
                    _stats.pending_tasks++;
                    return task->compaction_retry.retry().then([this, task] {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                } else if (!task->cleanup && task->compacting_cf->pending_compactions()) {
                    _stats.pending_tasks++;
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                }
            }
            return make_ready_future<stop_iteration>(stop_iteration::yes);
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
    }).finally([this, task] {
        _tasks.remove(task);
    });
}

future<> compaction_manager::task_stop(lw_shared_ptr<compaction_manager::task> task) {
    task->stopping = true;
    return task->compaction_gate.close().then([task] {
        return task->compaction_done.then([task] {
            task->compaction_done = make_ready_future<>();
            task->compaction_gate = seastar::gate();
            task->stopping = false;
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

void compaction_manager::register_collectd_metrics() {
    auto add = [this] (auto type_name, auto name, auto data_type, auto func) {
        _registrations.push_back(
            scollectd::add_polled_metric(scollectd::type_instance_id("compaction_manager",
                scollectd::per_cpu_plugin_instance,
                type_name, name),
                scollectd::make_typed(data_type, func)));
    };

    add("objects", "compactions", scollectd::data_type::GAUGE, [&] { return _stats.active_tasks; });
}

void compaction_manager::start() {
    _stopped = false;
    register_collectd_metrics();
}

future<> compaction_manager::stop() {
    cmlog.info("Asked to stop");
    if (_stopped) {
        return make_ready_future<>();
    }
    _stopped = true;
    _registrations.clear();
    // Stop all ongoing compaction.
    for (auto& info : _compactions) {
        info->stop("shutdown");
    }
    // Wait for each task handler to stop. Copy list because task remove itself
    // from the list when done.
    auto tasks = _tasks;
    return do_with(std::move(tasks), [this] (std::list<lw_shared_ptr<task>>& tasks) {
        return do_for_each(tasks, [this] (auto& task) {
            return this->task_stop(task);
        });
    }).then([this] {
        for (auto& it : _cleanup_waiters) {
            it.second.set_exception(std::runtime_error("cleanup interrupted due to shutdown"));
        }
        _cleanup_waiters.clear();
        _weight_tracker.clear();
        cmlog.info("Stopped");
        return make_ready_future<>();
    });
}

bool compaction_manager::can_submit() {
    return !_stopped;
}

void compaction_manager::submit(column_family* cf) {
    if (!can_submit()) {
        return;
    }
    task_start(cf, false);
}

future<> compaction_manager::perform_cleanup(column_family* cf) {
    if (!can_submit()) {
        throw std::runtime_error("cleanup request failed: compaction manager is either stopped or wasn't properly initialized");
    }
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf && task->cleanup) {
            throw std::runtime_error(sprint("cleanup request failed: there is an ongoing cleanup on %s.%s", cf->schema()->ks_name(), cf->schema()->cf_name()));
        }
    }
    task_start(cf, true);

    promise<> p;
    future<> f = p.get_future();
    _cleanup_waiters.insert(std::make_pair(cf, std::move(p)));
    // Wait for termination of cleanup operation.
    return std::move(f);
}

future<> compaction_manager::remove(column_family* cf) {
    // We need to guarantee that a task being stopped will not retry to compact
    // a column family being removed.
    auto tasks_to_stop = make_lw_shared<std::vector<lw_shared_ptr<task>>>();
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf) {
            tasks_to_stop->push_back(task);
            task->stopping = true;
        }
    }
    // Wait for the termination of an ongoing compaction on cf, if any.
    return do_for_each(*tasks_to_stop, [this, cf] (auto& task) {
        return this->task_stop(task);
    }).then([this, cf, tasks_to_stop] {
        _weight_tracker.erase(cf);
    });
}

void compaction_manager::stop_compaction(sstring type) {
    // TODO: this method only works for compaction of type compaction and cleanup.
    // Other types are: validation, scrub, index_build.
    sstables::compaction_type target_type;
    if (type == "COMPACTION") {
        target_type = sstables::compaction_type::Compaction;
    } else if (type == "CLEANUP") {
        target_type = sstables::compaction_type::Cleanup;
    } else {
        throw std::runtime_error(sprint("Compaction of type %s cannot be stopped by compaction manager", type.c_str()));
    }
    for (auto& info : _compactions) {
        if (target_type == info->type) {
            info->stop("user request");
        }
    }
}
