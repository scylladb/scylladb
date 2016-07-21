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

class compacting_sstable_registration {
    compaction_manager* _cm;
    std::vector<sstables::shared_sstable> _compacting;
public:
    compacting_sstable_registration(compaction_manager* cm, std::vector<sstables::shared_sstable> compacting)
        : _cm(cm)
        , _compacting(std::move(compacting))
    {
        _cm->register_compacting_sstables(_compacting);
    }

    compacting_sstable_registration& operator=(const compacting_sstable_registration&) = delete;
    compacting_sstable_registration(const compacting_sstable_registration&) = delete;

    compacting_sstable_registration& operator=(compacting_sstable_registration&& other) noexcept {
        if (this != &other) {
            this->~compacting_sstable_registration();
            new (this) compacting_sstable_registration(std::move(other));
        }
        return *this;
    }

    compacting_sstable_registration(compacting_sstable_registration&& other) noexcept
        : _cm(other._cm)
        , _compacting(std::move(other._compacting))
    {
        other._cm = nullptr;
    }

    ~compacting_sstable_registration() {
        if (_cm) {
            _cm->deregister_compacting_sstables(_compacting);
        }
    }
};

class compaction_weight_registration {
    compaction_manager* _cm;
    column_family* _cf;
    int _weight;
public:
    compaction_weight_registration(compaction_manager* cm, column_family* cf, int weight)
        : _cm(cm)
        , _cf(cf)
        , _weight(weight)
    {
        _cm->register_weight(_cf, _weight);
    }

    compaction_weight_registration& operator=(const compaction_weight_registration&) = delete;
    compaction_weight_registration(const compaction_weight_registration&) = delete;

    compaction_weight_registration& operator=(compaction_weight_registration&& other) noexcept {
        if (this != &other) {
            this->~compaction_weight_registration();
            new (this) compaction_weight_registration(std::move(other));
        }
        return *this;
    }

    compaction_weight_registration(compaction_weight_registration&& other) noexcept
        : _cm(other._cm)
        , _cf(other._cf)
        , _weight(other._weight)
    {
        other._cm = nullptr;
        other._cf = nullptr;
        other._weight = 0;
    }

    ~compaction_weight_registration() {
        if (_cm) {
            _cm->deregister_weight(_cf, _weight);
        }
    }
};

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

bool compaction_manager::can_register_weight(column_family* cf, int weight, bool parallel_compaction) {
    auto it = _weight_tracker.find(cf);
    if (it == _weight_tracker.end()) {
        return true;
    }
    std::unordered_set<int>& s = it->second;
    // Only one weight is allowed if parallel compaction is disabled.
    if (!parallel_compaction && !s.empty()) {
        return false;
    }
    // TODO: Maybe allow only *smaller* compactions to start? That can be done
    // by returning true only if weight is not in the set and is lower than any
    // entry in the set.
    if (s.count(weight)) {
        // If reached this point, it means that there is an ongoing compaction
        // with the weight of the compaction job.
        return false;
    }
    return true;
}

void compaction_manager::register_weight(column_family* cf, int weight) {
    auto it = _weight_tracker.find(cf);
    if (it == _weight_tracker.end()) {
        _weight_tracker.insert({cf, {weight}});
    } else {
        it->second.insert(weight);
    }
}

void compaction_manager::deregister_weight(column_family* cf, int weight) {
    auto it = _weight_tracker.find(cf);
    assert(it != _weight_tracker.end());
    it->second.erase(weight);
}

std::vector<sstables::shared_sstable> compaction_manager::get_candidates(const column_family& cf) {
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(cf.sstables_count());
    // Filter out sstables that are being compacted.
    for (auto& sst : *cf.get_sstables()) {
        if (!_compacting_sstables.count(sst)) {
            candidates.push_back(sst);
        }
    }
    return candidates;
}

void compaction_manager::register_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables) {
    for (auto& sst : sstables) {
        _compacting_sstables.insert(sst);
    }
}

void compaction_manager::deregister_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables) {
    // Remove compacted sstables from the set of compacting sstables.
    for (auto& sst : sstables) {
        _compacting_sstables.erase(sst);
    }
}

// submit_sstable_rewrite() starts a compaction task, much like submit(),
// But rather than asking a compaction policy what to compact, this function
// compacts just a single sstable, and writes one new sstable. This operation
// is useful to split an sstable containing data belonging to multiple shards
// into a separate sstable on each shard.
void compaction_manager::submit_sstable_rewrite(column_family* cf, sstables::shared_sstable sst) {
    // The semaphore ensures that the sstable rewrite operations submitted by
    // submit_sstable_rewrite are run in sequence, and not all of them in
    // parallel. Note that unlike general compaction which currently allows
    // different cfs to compact in parallel, here we don't have a semaphore
    // per cf, so we only get one rewrite at a time on each shard.
    static thread_local semaphore sem(1);
    // We cannot, and don't need to, compact an sstable which is already
    // being compacted anyway.
    if (_stopped || _compacting_sstables.count(sst)) {
        return;
    }
    // Conversely, we don't want another compaction job to compact the
    // sstable we are planning to work on:
    _compacting_sstables.insert(sst);
    auto task = make_lw_shared<compaction_manager::task>();
    _tasks.push_back(task);
    _stats.active_tasks++;
    task->compaction_done = with_semaphore(sem, 1, [cf, sst] {
        return cf->compact_sstables(sstables::compaction_descriptor(
                std::vector<sstables::shared_sstable>{sst},
                sst->get_sstable_level(),
                std::numeric_limits<uint64_t>::max()), false);
    }).then_wrapped([this, sst, task] (future<> f) {
        _compacting_sstables.erase(sst);
        _stats.active_tasks--;
        _tasks.remove(task);
        try {
            f.get();
            _stats.completed_tasks++;
        } catch (sstables::compaction_stop_exception& e) {
            cmlog.info("compaction info: {}", e.what());
            _stats.errors++;
        } catch (...) {
            cmlog.error("compaction failed: {}", std::current_exception());
            _stats.errors++;
        }
    });
}

future<> compaction_manager::task_stop(lw_shared_ptr<compaction_manager::task> task) {
    task->stopping = true;
    auto f = task->compaction_done.get_future();
    return f.then([task] {
        task->stopping = false;
        return make_ready_future<>();
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
        return parallel_for_each(tasks, [this] (auto& task) {
            return this->task_stop(task);
        });
    }).then([this] {
        _weight_tracker.clear();
        cmlog.info("Stopped");
        return make_ready_future<>();
    });
}

inline bool compaction_manager::can_proceed(const lw_shared_ptr<task>& task) {
    return !_stopped && !task->stopping;
}

inline future<> compaction_manager::put_task_to_sleep(lw_shared_ptr<task>& task) {
    cmlog.info("compaction task handler sleeping for {} seconds",
        std::chrono::duration_cast<std::chrono::seconds>(task->compaction_retry.sleep_time()).count());
    return task->compaction_retry.retry();
}

static inline bool check_for_error(future<> f) {
    bool error = false;
    try {
        f.get();
    } catch (sstables::compaction_stop_exception& e) {
        cmlog.info("compaction info: {}", e.what());
        error = true;
    } catch (...) {
        cmlog.error("compaction failed: {}", std::current_exception());
        error = true;
    }
    return error;
}

void compaction_manager::submit(column_family* cf) {
    auto task = make_lw_shared<compaction_manager::task>();
    task->compacting_cf = cf;
    _tasks.push_back(task);
    _stats.pending_tasks++;

    task->compaction_done = repeat([this, task] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        column_family& cf = *task->compacting_cf;
        sstables::compaction_strategy cs = cf.get_compaction_strategy();
        sstables::compaction_descriptor descriptor = cs.get_sstables_for_compaction(cf, get_candidates(cf));
        int weight = trim_to_compact(&cf, descriptor);

        // Stop compaction task immediately if strategy is satisfied or job cannot run in parallel.
        if (descriptor.sstables.empty() || !can_register_weight(&cf, weight, cs.parallel_compaction())) {
            _stats.pending_tasks--;
            cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}.{}",
                descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        auto compacting = compacting_sstable_registration(this, descriptor.sstables);
        auto c_weight = compaction_weight_registration(this, &cf, weight);
        cmlog.debug("Accepted compaction job ({} sstable(s)) of weight {} for {}.{}",
            descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());

        _stats.pending_tasks--;
        _stats.active_tasks++;
        return cf.run_compaction(std::move(descriptor))
                .then_wrapped([this, task, compacting = std::move(compacting), c_weight = std::move(c_weight)] (future<> f) mutable {
            _stats.active_tasks--;

            if (!can_proceed(task)) {
                f.ignore_ready_future();
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            if (check_for_error(std::move(f))) {
                _stats.errors++;
                _stats.pending_tasks++;
                return put_task_to_sleep(task).then([] {
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            }
            _stats.pending_tasks++;
            _stats.completed_tasks++;
            task->compaction_retry.reset();
            return make_ready_future<stop_iteration>(stop_iteration::no);
        });
    }).finally([this, task] {
        _tasks.remove(task);
    });
}

inline bool compaction_manager::check_for_cleanup(column_family* cf) {
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf && task->cleanup) {
            return true;
        }
    }
    return false;
}

future<> compaction_manager::perform_cleanup(column_family* cf) {
    if (check_for_cleanup(cf)) {
        throw std::runtime_error(sprint("cleanup request failed: there is an ongoing cleanup on %s.%s",
            cf->schema()->ks_name(), cf->schema()->cf_name()));
    }
    auto task = make_lw_shared<compaction_manager::task>();
    task->compacting_cf = cf;
    task->cleanup = true;
    _tasks.push_back(task);
    _stats.pending_tasks++;

    task->compaction_done = repeat([this, task] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        column_family& cf = *task->compacting_cf;
        sstables::compaction_descriptor descriptor = sstables::compaction_descriptor(get_candidates(cf));
        auto compacting = compacting_sstable_registration(this, descriptor.sstables);

        _stats.pending_tasks--;
        _stats.active_tasks++;
        return cf.cleanup_sstables(std::move(descriptor))
                .then_wrapped([this, task, compacting = std::move(compacting)] (future<> f) mutable {
            _stats.active_tasks--;
            if (!can_proceed(task)) {
                f.ignore_ready_future();
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            if (check_for_error(std::move(f))) {
                _stats.errors++;
                _stats.pending_tasks++;
                return put_task_to_sleep(task).then([] {
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            }
            _stats.completed_tasks++;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        });
    }).finally([this, task] {
        _tasks.remove(task);
    });

    return task->compaction_done.get_future().then([task] {});
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
