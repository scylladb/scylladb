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
#include "compaction_strategy.hh"
#include "compaction_backlog_manager.hh"
#include "sstables/sstables.hh"
#include "database.hh"
#include <seastar/core/metrics.hh>
#include "exceptions.hh"
#include <cmath>
#include <boost/range/algorithm/count_if.hpp>

static logging::logger cmlog("compaction_manager");
using namespace std::chrono_literals;

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

compaction_weight_registration::compaction_weight_registration(compaction_manager* cm, int weight)
    : _cm(cm)
    , _weight(weight)
{
    _cm->register_weight(_weight);
}

compaction_weight_registration& compaction_weight_registration::operator=(compaction_weight_registration&& other) noexcept {
    if (this != &other) {
        this->~compaction_weight_registration();
        new (this) compaction_weight_registration(std::move(other));
    }
    return *this;
}

compaction_weight_registration::compaction_weight_registration(compaction_weight_registration&& other) noexcept
    : _cm(other._cm)
    , _weight(other._weight)
{
    other._cm = nullptr;
    other._weight = 0;
}

compaction_weight_registration::~compaction_weight_registration() {
    if (_cm) {
        _cm->deregister_weight(_weight);
    }
}

void compaction_weight_registration::deregister() {
    _cm->deregister_weight(_weight);
    _cm = nullptr;
}

int compaction_weight_registration::weight() const {
    return _weight;
}

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

    uint64_t total_size = get_total_size(descriptor.sstables);
    int min_threshold = cf->schema()->min_compaction_threshold();

    while (descriptor.sstables.size() > size_t(min_threshold)) {
        if (_weight_tracker.count(weight)) {
            total_size -= descriptor.sstables.back()->data_size();
            descriptor.sstables.pop_back();
            weight = calculate_weight(total_size);
        } else {
            break;
        }
    }
    return weight;
}

bool compaction_manager::can_register_weight(column_family* cf, int weight) {
    auto has_cf_ongoing_compaction = [&] () -> bool {
        return boost::range::count_if(_tasks, [&] (const lw_shared_ptr<task>& task) {
            return task->compacting_cf == cf && task->compaction_running;
        });
    };

    // Only one weight is allowed if parallel compaction is disabled.
    if (!cf->get_compaction_strategy().parallel_compaction() && has_cf_ongoing_compaction()) {
        return false;
    }
    // TODO: Maybe allow only *smaller* compactions to start? That can be done
    // by returning true only if weight is not in the set and is lower than any
    // entry in the set.
    if (_weight_tracker.count(weight)) {
        // If reached this point, it means that there is an ongoing compaction
        // with the weight of the compaction job.
        return false;
    }
    return true;
}

void compaction_manager::register_weight(int weight) {
    _weight_tracker.insert(weight);
}

void compaction_manager::deregister_weight(int weight) {
    _weight_tracker.erase(weight);
    reevaluate_postponed_compactions();
}

std::vector<sstables::shared_sstable> compaction_manager::get_candidates(const column_family& cf) {
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(cf.sstables_count());
    // Filter out sstables that are being compacted.
    for (auto& sst : cf.candidates_for_compaction()) {
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

class user_initiated_backlog_tracker final : public compaction_backlog_tracker::impl {
public:
    explicit user_initiated_backlog_tracker(float added_backlog, size_t available_memory) : _added_backlog(added_backlog), _available_memory(available_memory) {}
private:
    float _added_backlog;
    size_t _available_memory;
    virtual double backlog(const compaction_backlog_tracker::ongoing_writes& ow, const compaction_backlog_tracker::ongoing_compactions& oc) const override {
        return _added_backlog * _available_memory;
    }
    virtual void add_sstable(sstables::shared_sstable sst)  override { }
    virtual void remove_sstable(sstables::shared_sstable sst)  override { }
};

future<> compaction_manager::submit_major_compaction(column_family* cf) {
    if (_stopped) {
        return make_ready_future<>();
    }
    auto task = make_lw_shared<compaction_manager::task>();
    task->compacting_cf = cf;
    _tasks.push_back(task);

    // first take major compaction semaphore, then exclusely take compaction lock for column family.
    // it cannot be the other way around, or minor compaction for this column family would be
    // prevented while an ongoing major compaction doesn't release the semaphore.
    task->compaction_done = with_semaphore(_major_compaction_sem, 1, [this, task, cf] {
        return with_lock(_compaction_locks[cf].for_write(), [this, task, cf] {
            _stats.active_tasks++;
            if (!can_proceed(task)) {
                return make_ready_future<>();
            }

            // candidates are sstables that aren't being operated on by other compaction types.
            // those are eligible for major compaction.
            // FIXME: we need to make major compaction compaction strategy aware. For example,
            // leveled strategy may want to promote the merged sstables of a level N.
            auto sstables = get_candidates(*cf);
            auto compacting = compacting_sstable_registration(this, sstables);

            cmlog.info0("User initiated compaction started on behalf of {}.{}", cf->schema()->ks_name(), cf->schema()->cf_name());
            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_compaction_controller.backlog_of_shares(200), _available_memory));
            return do_with(std::move(user_initiated), [this, cf, sstables = std::move(sstables)] (compaction_backlog_tracker& bt) mutable {
                register_backlog_tracker(bt);
                return with_scheduling_group(_scheduling_group, [this, cf, sstables = std::move(sstables)] () mutable {
                    return cf->compact_sstables(sstables::compaction_descriptor(std::move(sstables)));
                });
            }).then([compacting = std::move(compacting)] {});
        });
    }).then_wrapped([this, task] (future<> f) {
        _stats.active_tasks--;
        _tasks.remove(task);
        try {
            f.get();
            _stats.completed_tasks++;
        } catch (sstables::compaction_stop_exception& e) {
            cmlog.info("major compaction stopped, reason: {}", e.what());
            _stats.errors++;
        } catch (...) {
            cmlog.error("major compaction failed, reason: {}", std::current_exception());
            _stats.errors++;
        }
    });
    return task->compaction_done.get_future().then([task] {});
}

future<> compaction_manager::run_resharding_job(column_family* cf, std::function<future<>()> job) {
    if (_stopped) {
        return make_ready_future<>();
    }

    auto task = make_lw_shared<compaction_manager::task>();
    task->compacting_cf = cf;
    _tasks.push_back(task);

    task->compaction_done = with_semaphore(_resharding_sem, 1, [this, task, cf, job = std::move(job)] {
        // take read lock for cf, so major compaction and resharding can't proceed in parallel.
        return with_lock(_compaction_locks[cf].for_read(), [this, task, cf, job = std::move(job)] {
            _stats.active_tasks++;
            if (!can_proceed(task)) {
                return make_ready_future<>();
            }

            // NOTE:
            // no need to register shared sstables because they're excluded from non-resharding
            // compaction and some of them may not even belong to current shard.

            return with_scheduling_group(_scheduling_group, [job = std::move(job)] {
                return job();
            });
        });
    }).then_wrapped([this, task] (future<> f) {
        _stats.active_tasks--;
        _tasks.remove(task);
        try {
            f.get();
        } catch (sstables::compaction_stop_exception& e) {
            cmlog.info("resharding was abruptly stopped, reason: {}", e.what());
        } catch (...) {
            cmlog.error("resharding failed: {}", std::current_exception());
        }
    });
    return task->compaction_done.get_future().then([task] {});
}

future<> compaction_manager::task_stop(lw_shared_ptr<compaction_manager::task> task) {
    task->stopping = true;
    auto f = task->compaction_done.get_future();
    return f.then([task] {
        task->stopping = false;
        return make_ready_future<>();
    });
}

compaction_manager::compaction_manager(seastar::scheduling_group sg, const ::io_priority_class& iop, size_t available_memory)
    : _compaction_controller(sg, iop, 250ms, [this, available_memory] () -> float {
        auto b = backlog() / available_memory;
        // This means we are using an unimplemented strategy
        if (compaction_controller::backlog_disabled(b)) {
            // returning the normalization factor means that we'll return the maximum
            // output in the _control_points. We can get rid of this when we implement
            // all strategies.
            return compaction_controller::normalization_factor;
        }
        return b;
    })
    , _backlog_manager(_compaction_controller)
    , _scheduling_group(_compaction_controller.sg())
    , _available_memory(available_memory)
{}

compaction_manager::compaction_manager(seastar::scheduling_group sg, const ::io_priority_class& iop, size_t available_memory, uint64_t shares)
    : _compaction_controller(sg, iop, shares)
    , _backlog_manager(_compaction_controller)
    , _scheduling_group(_compaction_controller.sg())
, _available_memory(available_memory)
{}

compaction_manager::compaction_manager()
    : compaction_manager(seastar::default_scheduling_group(), default_priority_class(), 1)
{}

compaction_manager::~compaction_manager() {
    // Assert that compaction manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is destroyed.
    assert(_stopped == true);
}

void compaction_manager::register_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("compaction_manager", {
        sm::make_gauge("compactions", [this] { return _stats.active_tasks; },
                       sm::description("Holds the number of currently active compactions.")),
    });
}

void compaction_manager::start() {
    _stopped = false;
    register_metrics();
    _compaction_submission_timer.arm(periodic_compaction_submission_interval());
    postponed_compactions_reevaluation();
}

std::function<void()> compaction_manager::compaction_submission_callback() {
    return [this] () mutable {
        for (auto& e: _compaction_locks) {
            submit(e.first);
        }
    };
}

void compaction_manager::postponed_compactions_reevaluation() {
    _waiting_reevalution = repeat([this] {
        return _postponed_reevaluation.wait().then([this] {
            if (_stopped) {
                _postponed.clear();
                return stop_iteration::yes;
            }
            auto postponed = std::move(_postponed);
            try {
                for (auto& cf : postponed) {
                    submit(cf);
                }
            } catch (...) {
                _postponed = std::move(postponed);
            }
            return stop_iteration::no;
        });
    });
}

void compaction_manager::reevaluate_postponed_compactions() {
    _postponed_reevaluation.signal();
}

void compaction_manager::postpone_compaction_for_column_family(column_family* cf) {
    _postponed.push_back(cf);
}

future<> compaction_manager::stop() {
    cmlog.info("Asked to stop");
    if (_stopped) {
        return make_ready_future<>();
    }
    _stopped = true;
    // Reset the metrics registry
    _metrics.clear();
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
    }).then([this] () mutable {
        reevaluate_postponed_compactions();
        return std::move(_waiting_reevalution);
    }).then([this] {
        _weight_tracker.clear();
        _compaction_submission_timer.cancel();
        cmlog.info("Stopped");
        return _compaction_controller.shutdown();
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

inline bool compaction_manager::maybe_stop_on_error(future<> f) {
    bool retry = false;
    try {
        f.get();
    } catch (sstables::compaction_stop_exception& e) {
        // We want compaction stopped here to be retried because this may have
        // happened at user request (using nodetool stop), and to mimic C*
        // behavior, compaction is retried later on.
        cmlog.info("compaction info: {}", e.what());
        retry = true;
    } catch (storage_io_error& e) {
        cmlog.error("compaction failed due to storage io error: {}", e.what());
        retry = false;
        stop();
    } catch (...) {
        cmlog.error("compaction failed: {}", std::current_exception());
        retry = true;
    }
    return retry;
}

void compaction_manager::submit(column_family* cf) {
    auto task = make_lw_shared<compaction_manager::task>();
    task->compacting_cf = cf;
    _tasks.push_back(task);
    _stats.pending_tasks++;

    task->compaction_done = repeat([this, task, cf] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return with_lock(_compaction_locks[cf].for_read(), [this, task] () mutable {
          return with_scheduling_group(_scheduling_group, [this, task = std::move(task)] () mutable {
            column_family& cf = *task->compacting_cf;
            sstables::compaction_strategy cs = cf.get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_sstables_for_compaction(cf, get_candidates(cf));
            int weight = trim_to_compact(&cf, descriptor);

            if (descriptor.sstables.empty() || !can_proceed(task)) {
                _stats.pending_tasks--;
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            if (!can_register_weight(&cf, weight)) {
                _stats.pending_tasks--;
                cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}.{}, postponing it...",
                    descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());
                postpone_compaction_for_column_family(&cf);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            auto compacting = compacting_sstable_registration(this, descriptor.sstables);
            descriptor.weight_registration = compaction_weight_registration(this, weight);
            cmlog.debug("Accepted compaction job ({} sstable(s)) of weight {} for {}.{}",
                descriptor.sstables.size(), weight, cf.schema()->ks_name(), cf.schema()->cf_name());

            _stats.pending_tasks--;
            _stats.active_tasks++;
            task->compaction_running = true;
            return cf.run_compaction(std::move(descriptor)).then_wrapped([this, task, compacting = std::move(compacting)] (future<> f) mutable {
                _stats.active_tasks--;
                task->compaction_running = false;

                if (!can_proceed(task)) {
                    maybe_stop_on_error(std::move(f));
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                if (maybe_stop_on_error(std::move(f))) {
                    _stats.errors++;
                    _stats.pending_tasks++;
                    return put_task_to_sleep(task).then([] {
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    });
                }
                _stats.pending_tasks++;
                _stats.completed_tasks++;
                task->compaction_retry.reset();
                reevaluate_postponed_compactions();
                return make_ready_future<stop_iteration>(stop_iteration::no);
            });
          });
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
        // FIXME: lock cf here
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        column_family& cf = *task->compacting_cf;
        sstables::compaction_descriptor descriptor = sstables::compaction_descriptor(get_candidates(cf));
        auto compacting = compacting_sstable_registration(this, descriptor.sstables);

        _stats.pending_tasks--;
        _stats.active_tasks++;
        task->compaction_running = true;
        compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_compaction_controller.backlog_of_shares(200), _available_memory));
        return do_with(std::move(user_initiated), [this, &cf, descriptor = std::move(descriptor)] (compaction_backlog_tracker& bt) mutable {
            return with_scheduling_group(_scheduling_group, [this, &cf, descriptor = std::move(descriptor)] () mutable {
                return cf.cleanup_sstables(std::move(descriptor));
            });
        }).then_wrapped([this, task, compacting = std::move(compacting)] (future<> f) mutable {
            task->compaction_running = false;
            _stats.active_tasks--;
            if (!can_proceed(task)) {
                maybe_stop_on_error(std::move(f));
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            if (maybe_stop_on_error(std::move(f))) {
                _stats.errors++;
                _stats.pending_tasks++;
                return put_task_to_sleep(task).then([] {
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            }
            _stats.completed_tasks++;
            reevaluate_postponed_compactions();
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        });
    }).finally([this, task] {
        _tasks.remove(task);
    });

    return task->compaction_done.get_future().then([task] {});
}

future<> compaction_manager::remove(column_family* cf) {
    // FIXME: better way to iterate through compaction info for a given column family,
    // although this path isn't performance sensitive.
    for (auto& info : _compactions) {
        if (cf->schema()->ks_name() == info->ks && cf->schema()->cf_name() == info->cf) {
            info->stop("column family removal");
        }
    }

    // We need to guarantee that a task being stopped will not retry to compact
    // a column family being removed.
    auto tasks_to_stop = make_lw_shared<std::vector<lw_shared_ptr<task>>>();
    for (auto& task : _tasks) {
        if (task->compacting_cf == cf) {
            tasks_to_stop->push_back(task);
            task->stopping = true;
        }
    }
    _postponed.erase(boost::remove(_postponed, cf), _postponed.end());

    // Wait for the termination of an ongoing compaction on cf, if any.
    return do_for_each(*tasks_to_stop, [this, cf] (auto& task) {
        return this->task_stop(task);
    }).then([this, cf, tasks_to_stop] {
        _compaction_locks.erase(cf);
    });
}

void compaction_manager::stop_tracking_ongoing_compactions(column_family* cf) {
    for (auto& info : _compactions) {
        if (cf->schema()->ks_name() == info->ks && cf->schema()->cf_name() == info->cf) {
            info->stop_tracking();
        }
    }
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

void compaction_manager::on_compaction_complete(compaction_weight_registration& weight_registration) {
    weight_registration.deregister();
    reevaluate_postponed_compactions();
}

double compaction_backlog_tracker::backlog() const {
    return _disabled ? compaction_controller::disable_backlog : _impl->backlog(_ongoing_writes, _ongoing_compactions);
}

void compaction_backlog_tracker::add_sstable(sstables::shared_sstable sst) {
    if (_disabled) {
        return;
    }
    _ongoing_writes.erase(sst);
    try {
        _impl->add_sstable(std::move(sst));
    } catch (...) {
        cmlog.warn("Disabling backlog tracker due to exception {}", std::current_exception());
        disable();
    }
}

void compaction_backlog_tracker::remove_sstable(sstables::shared_sstable sst) {
    if (_disabled) {
        return;
    }

    _ongoing_compactions.erase(sst);
    try {
        _impl->remove_sstable(std::move(sst));
    } catch (...) {
        cmlog.warn("Disabling backlog tracker due to exception {}", std::current_exception());
        disable();
    }
}

void compaction_backlog_tracker::register_partially_written_sstable(sstables::shared_sstable sst, backlog_write_progress_manager& wp) {
    if (_disabled) {
        return;
    }
    try {
        _ongoing_writes.emplace(sst, &wp);
    } catch (...) {
        // We can potentially recover from adding ongoing compactions or writes when the process
        // ends. The backlog will just be temporarily wrong. If we are are suffering from something
        // more serious like memory exhaustion we will soon fail again in either add / remove and
        // then we'll disable the tracker. For now, try our best.
        cmlog.warn("backlog tracker couldn't register partially written SSTable to exception {}", std::current_exception());
    }
}

void compaction_backlog_tracker::register_compacting_sstable(sstables::shared_sstable sst, backlog_read_progress_manager& rp) {
    if (_disabled) {
        return;
    }

    try {
        _ongoing_compactions.emplace(sst, &rp);
    } catch (...) {
        cmlog.warn("backlog tracker couldn't register partially compacting SSTable to exception {}", std::current_exception());
    }
}

void compaction_backlog_tracker::transfer_ongoing_charges(compaction_backlog_tracker& new_bt, bool move_read_charges) {
    for (auto&& w : _ongoing_writes) {
        new_bt.register_partially_written_sstable(w.first, *w.second);
    }

    if (move_read_charges) {
        for (auto&& w : _ongoing_compactions) {
            new_bt.register_compacting_sstable(w.first, *w.second);
        }
    }
    _ongoing_writes = {};
    _ongoing_compactions = {};
}

void compaction_backlog_tracker::revert_charges(sstables::shared_sstable sst) {
    _ongoing_writes.erase(sst);
    _ongoing_compactions.erase(sst);
}

compaction_backlog_tracker::~compaction_backlog_tracker() {
    if (_manager) {
        _manager->remove_backlog_tracker(this);
    }
}

void compaction_backlog_manager::remove_backlog_tracker(compaction_backlog_tracker* tracker) {
    _backlog_trackers.erase(tracker);
}

double compaction_backlog_manager::backlog() const {
    try {
        double backlog = 0;

        for (auto& tracker: _backlog_trackers) {
            backlog += tracker->backlog();
        }
        if (compaction_controller::backlog_disabled(backlog)) {
            return compaction_controller::disable_backlog;
        } else {
            return backlog;
        }
    } catch (...) {
        return _compaction_controller->backlog_of_shares(1000);
    }
}

void compaction_backlog_manager::register_backlog_tracker(compaction_backlog_tracker& tracker) {
    tracker._manager = this;
    _backlog_trackers.insert(&tracker);
}

compaction_backlog_manager::~compaction_backlog_manager() {
    for (auto* tracker : _backlog_trackers) {
        tracker->_manager = nullptr;
    }
}
