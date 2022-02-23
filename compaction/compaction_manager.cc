/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "compaction_manager.hh"
#include "compaction_strategy.hh"
#include "compaction_backlog_manager.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include "replica/database.hh"
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include "sstables/exceptions.hh"
#include "locator/abstract_replication_strategy.hh"
#include "utils/fb_utilities.hh"
#include "utils/UUID_gen.hh"
#include <cmath>
#include <boost/algorithm/cxx11/any_of.hpp>

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

    // Explicitly release compacting sstables
    void release_compacting(const std::vector<sstables::shared_sstable>& sstables) {
        _cm->deregister_compacting_sstables(sstables);
        for (auto& sst : sstables) {
            _compacting.erase(boost::remove(_compacting, sst), _compacting.end());
        }
    }
};

sstables::compaction_data compaction_manager::create_compaction_data() {
    sstables::compaction_data cdata = {};
    cdata.compaction_uuid = utils::UUID_gen::get_time_UUID();
    return cdata;
}

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

// Calculate weight of compaction job.
static inline int calculate_weight(uint64_t total_size) {
    // At the moment, '4' is being used as log base for determining the weight
    // of a compaction job. With base of 4, what happens is that when you have
    // a 40-second compaction in progress, and a tiny 10-second compaction
    // comes along, you do them in parallel.
    // TODO: Find a possibly better log base through experimentation.
    static constexpr int WEIGHT_LOG_BASE = 4;
    // Fixed tax is added to size before taking the log, to make sure all jobs
    // smaller than the tax (i.e. 1MB) will be serialized.
    static constexpr int fixed_size_tax = 1024*1024;

    // computes the logarithm (base WEIGHT_LOG_BASE) of total_size.
    return int(std::log(total_size + fixed_size_tax) / std::log(WEIGHT_LOG_BASE));
}

static inline int calculate_weight(const sstables::compaction_descriptor& descriptor) {
    // Use weight 0 for compactions that are comprised solely of completely expired sstables.
    // We want these compactions to be in a separate weight class because they are very lightweight, fast and efficient.
    if (descriptor.sstables.empty() || descriptor.has_only_fully_expired) {
        return 0;
    }
    return calculate_weight(boost::accumulate(descriptor.sstables | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::data_size)), uint64_t(0)));
}

unsigned compaction_manager::current_compaction_fan_in_threshold() const {
    if (_tasks.empty()) {
        return 0;
    }
    auto largest_fan_in = std::ranges::max(_tasks | boost::adaptors::transformed([] (auto& task) {
        return task->compaction_running ? task->compaction_data.compaction_fan_in : 0;
    }));
    // conservatively limit fan-in threshold to 32, such that tons of small sstables won't accumulate if
    // running major on a leveled table, which can even have more than one thousand files.
    return std::min(unsigned(32), largest_fan_in);
}

bool compaction_manager::can_register_compaction(replica::table* t, int weight, unsigned fan_in) const {
    // Only one weight is allowed if parallel compaction is disabled.
    if (!t->get_compaction_strategy().parallel_compaction() && has_table_ongoing_compaction(t)) {
        return false;
    }
    // TODO: Maybe allow only *smaller* compactions to start? That can be done
    // by returning true only if weight is not in the set and is lower than any
    // entry in the set.
    if (_weight_tracker.contains(weight)) {
        // If reached this point, it means that there is an ongoing compaction
        // with the weight of the compaction job.
        return false;
    }
    // A compaction cannot proceed until its fan-in is greater than or equal to the current largest fan-in.
    // That's done to prevent a less efficient compaction from "diluting" a more efficient one.
    // Compactions with the same efficiency can run in parallel as long as they aren't similar sized,
    // i.e. an efficient small-sized job can proceed in parallel to an efficient big-sized one.
    if (fan_in < current_compaction_fan_in_threshold()) {
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

std::vector<sstables::shared_sstable> compaction_manager::get_candidates(const replica::table& t) {
    std::vector<sstables::shared_sstable> candidates;
    candidates.reserve(t.sstables_count());
    // prevents sstables that belongs to a partial run being generated by ongoing compaction from being
    // selected for compaction, which could potentially result in wrong behavior.
    auto partial_run_identifiers = boost::copy_range<std::unordered_set<utils::UUID>>(_tasks
            | boost::adaptors::filtered(std::mem_fn(&task::generating_output_run))
            | boost::adaptors::transformed(std::mem_fn(&task::output_run_id)));
    auto& cs = t.get_compaction_strategy();

    // Filter out sstables that are being compacted.
    for (auto& sst : t.in_strategy_sstables()) {
        if (_compacting_sstables.contains(sst)) {
            continue;
        }
        if (partial_run_identifiers.contains(sst->run_identifier())) {
            continue;
        }
        candidates.push_back(sst);
    }
    return candidates;
}

void compaction_manager::register_compacting_sstables(const std::vector<sstables::shared_sstable>& sstables) {
    std::unordered_set<sstables::shared_sstable> sstables_to_merge;
    sstables_to_merge.reserve(sstables.size());
    for (auto& sst : sstables) {
        sstables_to_merge.insert(sst);
    }

    // make all required allocations in advance to merge
    // so it should not throw
    _compacting_sstables.reserve(_compacting_sstables.size() + sstables.size());
    try {
        _compacting_sstables.merge(sstables_to_merge);
    } catch (...) {
        cmlog.error("Unexpected error when registering compacting SSTables: {}. Ignored...", std::current_exception());
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

compaction_manager::compaction_state& compaction_manager::get_compaction_state(replica::table* t) {
    try {
        return _compaction_state.at(t);
    } catch (std::out_of_range&) {
        // Note: don't dereference t as it might not exist
        throw std::out_of_range(format("Compaction state for table [{}] not found", fmt::ptr(t)));
    }
}

future<> compaction_manager::perform_major_compaction(replica::table* t) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }

    auto task = make_lw_shared<compaction_manager::task>(t, sstables::compaction_type::Compaction, get_compaction_state(t));
    _tasks.push_back(task);
    cmlog.debug("Major compaction task {} table={}: started", fmt::ptr(task.get()), fmt::ptr(t));

    // first take major compaction semaphore, then exclusely take compaction lock for table.
    // it cannot be the other way around, or minor compaction for this table would be
    // prevented while an ongoing major compaction doesn't release the semaphore.
    task->compaction_done = with_semaphore(_maintenance_ops_sem, 1, [this, task, t] {
        return with_lock(task->compaction_state.lock.for_write(), [this, task, t] {
            _stats.active_tasks++;
            if (!can_proceed(task)) {
                return make_ready_future<>();
            }

            // candidates are sstables that aren't being operated on by other compaction types.
            // those are eligible for major compaction.
            sstables::compaction_strategy cs = t->get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_major_compaction_job(t->as_table_state(), get_candidates(*t));
            auto compacting = make_lw_shared<compacting_sstable_registration>(this, descriptor.sstables);
            descriptor.release_exhausted = [compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting->release_compacting(exhausted_sstables);
            };
            task->setup_new_compaction();
            task->output_run_identifier = descriptor.run_identifier;

            cmlog.info0("User initiated compaction started on behalf of {}.{}", t->schema()->ks_name(), t->schema()->cf_name());
            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_compaction_controller.backlog_of_shares(200), _available_memory));
            return do_with(std::move(user_initiated), [this, t, descriptor = std::move(descriptor), task] (compaction_backlog_tracker& bt) mutable {
                register_backlog_tracker(bt);
                return with_scheduling_group(_compaction_controller.sg(), [this, t, descriptor = std::move(descriptor), task] () mutable {
                    return t->compact_sstables(std::move(descriptor), task->compaction_data);
                });
            }).then([compacting = std::move(compacting)] {});
        });
    }).then_wrapped([this, task] (future<> f) {
        _stats.active_tasks--;
        _tasks.remove(task);
        cmlog.debug("Major compaction task {} table={}: done", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
        try {
            f.get();
            _stats.completed_tasks++;
        } catch (sstables::compaction_stopped_exception& e) {
            cmlog.info("major compaction stopped, reason: {}", e.what());
        } catch (...) {
            cmlog.error("major compaction failed, reason: {}", std::current_exception());
            _stats.errors++;
        }
    });
    return task->compaction_done.get_future().then([task] {});
}

future<> compaction_manager::run_custom_job(replica::table* t, sstables::compaction_type type, noncopyable_function<future<>(sstables::compaction_data&)> job) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }

    auto task = make_lw_shared<compaction_manager::task>(t, type, get_compaction_state(t));
    _tasks.push_back(task);
    cmlog.debug("{} task {} table={}: started", type, fmt::ptr(task.get()), fmt::ptr(task->compacting_table));

    auto job_ptr = std::make_unique<noncopyable_function<future<>(sstables::compaction_data&)>>(std::move(job));

    task->compaction_done = with_semaphore(_maintenance_ops_sem, 1, [this, task, &job = *job_ptr] () mutable {
        // take read lock for table, so major compaction and resharding can't proceed in parallel.
        return with_lock(task->compaction_state.lock.for_read(), [this, task, &job] () mutable {
            // Allow caller to know that task (e.g. reshape) was asked to stop while waiting for a chance to run.
            if (task->stopping) {
                return make_exception_future<>(task->make_compaction_stopped_exception());
            }
            _stats.active_tasks++;
            if (!can_proceed(task)) {
                return make_ready_future<>();
            }
            task->setup_new_compaction();

            // NOTE:
            // no need to register shared sstables because they're excluded from non-resharding
            // compaction and some of them may not even belong to current shard.
            return job(task->compaction_data);
        });
    }).then_wrapped([this, task, job_ptr = std::move(job_ptr), type] (future<> f) {
        _stats.active_tasks--;
        _tasks.remove(task);
        cmlog.debug("{} task {} table={}: done", type, fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
        try {
            f.get();
        } catch (sstables::compaction_stopped_exception& e) {
            cmlog.info("{} was abruptly stopped, reason: {}", task->type, e.what());
            throw;
        } catch (...) {
            cmlog.error("{} failed: {}", task->type, std::current_exception());
            throw;
        }
    });
    return task->compaction_done.get_future().then([task] {});
}

future<>
compaction_manager::run_with_compaction_disabled(replica::table* t, std::function<future<> ()> func) {
    auto& c_state = _compaction_state[t];
    auto holder = c_state.gate.hold();

    c_state.compaction_disabled_counter++;

    std::exception_ptr err;
    try {
        co_await stop_ongoing_compactions("user-triggered operation", t);
        co_await func();
    } catch (...) {
        err = std::current_exception();
    }

#ifdef DEBUG
    assert(_compaction_state.contains(t));
#endif
    // submit compaction request if we're the last holder of the gate which is still opened.
    if (--c_state.compaction_disabled_counter == 0 && !c_state.gate.is_closed()) {
        submit(t);
    }
    if (err) {
        std::rethrow_exception(err);
    }
    co_return;
}

void compaction_manager::task::setup_new_compaction() {
    compaction_data = create_compaction_data();
    compaction_running = true;
}

void compaction_manager::task::finish_compaction() noexcept {
    compaction_running = false;
    output_run_identifier = utils::null_uuid();
}

void compaction_manager::task::stop(sstring reason) noexcept {
    stopping = true;
    compaction_data.stop(std::move(reason));
}

sstables::compaction_stopped_exception compaction_manager::task::make_compaction_stopped_exception() const {
    auto s = compacting_table->schema();
    return sstables::compaction_stopped_exception(s->ks_name(), s->cf_name(), compaction_data.stop_requested);
}

future<> compaction_manager::task_stop(lw_shared_ptr<compaction_manager::task> task, sstring reason) {
    cmlog.debug("Stopping task {} table={}", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
    task->stop(reason);
    auto f = task->compaction_done.get_future();
    return f.then_wrapped([task] (future<> f) {
        task->stopping = false;
        if (f.failed()) {
            auto ex = f.get_exception();
            cmlog.debug("Stopping task {} table={}: task returned error: {}", fmt::ptr(task.get()), fmt::ptr(task->compacting_table), ex);
            return make_exception_future<>(std::move(ex));
        }
        cmlog.debug("Stopping task {} table={}: done", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
        return make_ready_future<>();
    });
}

compaction_manager::compaction_manager(compaction_scheduling_group csg, maintenance_scheduling_group msg, size_t available_memory, abort_source& as)
    : _compaction_controller(csg.cpu, csg.io, 250ms, [this, available_memory] () -> float {
        _last_backlog = backlog();
        auto b = _last_backlog / available_memory;
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
    , _maintenance_sg(msg)
    , _available_memory(available_memory)
    , _early_abort_subscription(as.subscribe([this] () noexcept {
        do_stop();
    }))
    , _strategy_control(std::make_unique<strategy_control>(*this))
{
    register_metrics();
}

compaction_manager::compaction_manager(compaction_scheduling_group csg, maintenance_scheduling_group msg, size_t available_memory, uint64_t shares, abort_source& as)
    : _compaction_controller(csg.cpu, csg.io, shares)
    , _backlog_manager(_compaction_controller)
    , _maintenance_sg(msg)
    , _available_memory(available_memory)
    , _early_abort_subscription(as.subscribe([this] () noexcept {
        do_stop();
    }))
    , _strategy_control(std::make_unique<strategy_control>(*this))
{
    register_metrics();
}

compaction_manager::compaction_manager()
    : _compaction_controller(seastar::default_scheduling_group(), default_priority_class(), 1)
    , _backlog_manager(_compaction_controller)
    , _maintenance_sg(maintenance_scheduling_group{default_scheduling_group(), default_priority_class()})
    , _available_memory(1)
    , _strategy_control(std::make_unique<strategy_control>(*this))
{
    // No metric registration because this constructor is supposed to be used only by the testing
    // infrastructure.
}

compaction_manager::~compaction_manager() {
    // Assert that compaction manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is stopped.
    assert(_state == state::none || _state == state::stopped);
}

void compaction_manager::register_metrics() {
    namespace sm = seastar::metrics;

    _metrics.add_group("compaction_manager", {
        sm::make_gauge("compactions", [this] { return _stats.active_tasks; },
                       sm::description("Holds the number of currently active compactions.")),
        sm::make_gauge("pending_compactions", [this] { return _stats.pending_tasks; },
                       sm::description("Holds the number of compaction tasks waiting for an opportunity to run.")),
        sm::make_gauge("backlog", [this] { return _last_backlog; },
                       sm::description("Holds the sum of compaction backlog for all tables in the system.")),
    });
}

void compaction_manager::enable() {
    assert(_state == state::none || _state == state::disabled);
    _state = state::enabled;
    _compaction_submission_timer.arm(periodic_compaction_submission_interval());
    postponed_compactions_reevaluation();
}

void compaction_manager::disable() {
    assert(_state == state::none || _state == state::enabled);
    _state = state::disabled;
    _compaction_submission_timer.cancel();
}

std::function<void()> compaction_manager::compaction_submission_callback() {
    return [this] () mutable {
        for (auto& e: _compaction_state) {
            submit(e.first);
        }
    };
}

void compaction_manager::postponed_compactions_reevaluation() {
    _waiting_reevalution = repeat([this] {
        return _postponed_reevaluation.wait().then([this] {
            if (_state != state::enabled) {
                _postponed.clear();
                return stop_iteration::yes;
            }
            auto postponed = std::move(_postponed);
            try {
                for (auto& t : postponed) {
                    submit(t);
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

void compaction_manager::postpone_compaction_for_table(replica::table* t) {
    _postponed.insert(t);
}

future<> compaction_manager::stop_tasks(std::vector<lw_shared_ptr<task>> tasks, sstring reason) {
    // To prevent compaction from being postponed while tasks are being stopped, let's set all
    // tasks as stopping before the deferring point below.
    for (auto& t : tasks) {
        t->stopping = true;
    }
    return do_with(std::move(tasks), [this, reason] (std::vector<lw_shared_ptr<task>>& tasks) {
        return parallel_for_each(tasks, [this, reason] (auto& task) {
            return this->task_stop(task, reason).then_wrapped([](future <> f) {
                try {
                    f.get();
                } catch (sstables::compaction_stopped_exception& e) {
                    // swallow stop exception if a given procedure decides to propagate it to the caller,
                    // as it happens with reshard and reshape.
                } catch (...) {
                    throw;
                }
            });
        });
    });
}

future<> compaction_manager::stop_ongoing_compactions(sstring reason, replica::table* t, std::optional<sstables::compaction_type> type_opt) {
    auto ongoing_compactions = get_compactions(t).size();
    auto tasks = boost::copy_range<std::vector<lw_shared_ptr<task>>>(_tasks | boost::adaptors::filtered([t, type_opt] (auto& task) {
        return (!t || task->compacting_table == t) && (!type_opt || task->type == *type_opt);
    }));
    logging::log_level level = tasks.empty() ? log_level::debug : log_level::info;
    if (cmlog.is_enabled(level)) {
        std::string scope = "";
        if (t) {
            scope = fmt::format(" for table {}.{}", t->schema()->ks_name(), t->schema()->cf_name());
        }
        if (type_opt) {
            scope += fmt::format(" {} type={}", scope.size() ? "and" : "for", *type_opt);
        }
        cmlog.log(level, "Stopping {} tasks for {} ongoing compactions{} due to {}", tasks.size(), ongoing_compactions, scope, reason);
    }
    return stop_tasks(std::move(tasks), std::move(reason));
}

future<> compaction_manager::drain() {
    if (!*_early_abort_subscription) {
        return make_ready_future<>();
    }

    _state = state::disabled;
    return stop_ongoing_compactions("drain");
}

future<> compaction_manager::stop() {
    // never started
    if (_state == state::none) {
        return make_ready_future<>();
    } else {
        do_stop();
        return std::move(*_stop_future);
    }
}

void compaction_manager::really_do_stop() {
    if (_state == state::none || _state == state::stopped) {
        return;
    }

    _state = state::stopped;
    cmlog.info("Asked to stop");
    // Reset the metrics registry
    _metrics.clear();
    _stop_future.emplace(stop_ongoing_compactions("shutdown").then([this] () mutable {
        reevaluate_postponed_compactions();
        return std::move(_waiting_reevalution);
    }).then([this] {
        _weight_tracker.clear();
        _compaction_submission_timer.cancel();
        cmlog.info("Stopped");
        return _compaction_controller.shutdown();
    }));
}

void compaction_manager::do_stop() noexcept {
    try {
        really_do_stop();
    } catch (...) {
        try {
            cmlog.error("Failed to stop the manager: {}", std::current_exception());
        } catch (...) {
            // Nothing else we can do.
        }
    }
}

inline bool compaction_manager::can_proceed(const lw_shared_ptr<task>& task) {
    return (_state == state::enabled) && !task->stopping && _compaction_state.contains(task->compacting_table) &&
        !_compaction_state[task->compacting_table].compaction_disabled();
}

inline future<> compaction_manager::put_task_to_sleep(lw_shared_ptr<task>& task) {
    cmlog.info("compaction task handler sleeping for {} seconds",
        std::chrono::duration_cast<std::chrono::seconds>(task->compaction_retry.sleep_time()).count());
    return task->compaction_retry.retry(task->compaction_data.abort).handle_exception_type([task] (sleep_aborted&) {
        return make_exception_future<>(task->make_compaction_stopped_exception());
    });
}

inline bool compaction_manager::maybe_stop_on_error(std::exception_ptr err, bool can_retry) {
    bool requires_stop = true;

    try {
        std::rethrow_exception(err);
    } catch (sstables::compaction_stopped_exception& e) {
        cmlog.info("compaction info: {}: stopping", e.what());
    } catch (sstables::compaction_aborted_exception& e) {
        cmlog.error("compaction info: {}: stopping", e.what());
        _stats.errors++;
    } catch (storage_io_error& e) {
        _stats.errors++;
        cmlog.error("compaction failed due to storage io error: {}: stopping", e.what());
        do_stop();
    } catch (...) {
        _stats.errors++;
        requires_stop = !can_retry;
        cmlog.error("compaction failed: {}: {}", std::current_exception(), requires_stop ? "stopping" : "retrying");
    }
    return requires_stop;
}

void compaction_manager::submit(replica::table* t) {
    if (t->is_auto_compaction_disabled_by_user()) {
        return;
    }

    auto task = make_lw_shared<compaction_manager::task>(t, sstables::compaction_type::Compaction, get_compaction_state(t));
    _tasks.push_back(task);
    _stats.pending_tasks++;
    cmlog.debug("Compaction task {} table={}: started", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));

    task->compaction_done = repeat([this, task, t] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return with_lock(task->compaction_state.lock.for_read(), [this, task] () mutable {
          return with_scheduling_group(_compaction_controller.sg(), [this, task = std::move(task)] () mutable {
            replica::table& t = *task->compacting_table;
            sstables::compaction_strategy cs = t.get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_sstables_for_compaction(t.as_table_state(), get_strategy_control(), get_candidates(t));
            int weight = calculate_weight(descriptor);

            if (descriptor.sstables.empty() || !can_proceed(task) || t.is_auto_compaction_disabled_by_user()) {
                _stats.pending_tasks--;
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            if (!can_register_compaction(&t, weight, descriptor.fan_in())) {
                _stats.pending_tasks--;
                cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}.{}, postponing it...",
                    descriptor.sstables.size(), weight, t.schema()->ks_name(), t.schema()->cf_name());
                postpone_compaction_for_table(&t);
                return make_ready_future<stop_iteration>(stop_iteration::yes);
            }
            auto compacting = make_lw_shared<compacting_sstable_registration>(this, descriptor.sstables);
            auto weight_r = compaction_weight_registration(this, weight);
            descriptor.release_exhausted = [compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting->release_compacting(exhausted_sstables);
            };
            cmlog.debug("Accepted compaction job ({} sstable(s)) of weight {} for {}.{}",
                descriptor.sstables.size(), weight, t.schema()->ks_name(), t.schema()->cf_name());

            _stats.pending_tasks--;
            _stats.active_tasks++;
            task->setup_new_compaction();
            task->output_run_identifier = descriptor.run_identifier;
            return t.compact_sstables(std::move(descriptor), task->compaction_data).then_wrapped([this, task, compacting = std::move(compacting), weight_r = std::move(weight_r)] (future<> f) mutable {
                _stats.active_tasks--;
                task->finish_compaction();

                if (f.failed()) {
                    auto ex = f.get_exception();
                    if (!maybe_stop_on_error(std::move(ex), can_proceed(task))) {
                        _stats.pending_tasks++;
                        return put_task_to_sleep(task).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    }
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
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
        cmlog.debug("Compaction task {} table={}: done", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
    });
}

future<> compaction_manager::perform_offstrategy(replica::table* t) {
    auto task = make_lw_shared<compaction_manager::task>(t, sstables::compaction_type::Reshape, get_compaction_state(t));
    _tasks.push_back(task);
    _stats.pending_tasks++;
    cmlog.debug("Offstrategy compaction task {} table={}: started", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));

    task->compaction_done = repeat([this, task, t] () mutable {
        if (!can_proceed(task)) {
            _stats.pending_tasks--;
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        return with_semaphore(_maintenance_ops_sem, 1, [this, task, t] () mutable {
            return with_lock(task->compaction_state.lock.for_read(), [this, task, t] () mutable {
                _stats.pending_tasks--;
                if (!can_proceed(task)) {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
                _stats.active_tasks++;
                task->setup_new_compaction();

                return t->run_offstrategy_compaction(task->compaction_data).then_wrapped([this, task, schema = t->schema()] (future<> f) mutable {
                    _stats.active_tasks--;
                    task->finish_compaction();
                    try {
                        f.get();
                        _stats.completed_tasks++;
                    } catch (sstables::compaction_stopped_exception& e) {
                        cmlog.info("off-strategy compaction of {}.{} was stopped: {}", schema->ks_name(), schema->cf_name(), e.what());
                    } catch (sstables::compaction_aborted_exception& e) {
                        _stats.errors++;
                        cmlog.error("off-strategy compaction of {}.{} was aborted: {}", schema->ks_name(), schema->cf_name(), e.what());
                    } catch (...) {
                        _stats.errors++;
                        _stats.pending_tasks++;
                        cmlog.error("off-strategy compaction of {}.{} failed due to {}, retrying...", schema->ks_name(), schema->cf_name(), std::current_exception());
                        return put_task_to_sleep(task).then([] {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    }
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                });
            });
        });
    }).finally([this, task] {
        _tasks.remove(task);
        cmlog.debug("Offstrategy compaction task {} table={}: done", fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
    });
    return task->compaction_done.get_future().finally([task] {});
}

future<> compaction_manager::rewrite_sstables(replica::table* t, sstables::compaction_type_options options, get_candidates_func get_func, can_purge_tombstones can_purge) {
    auto task = make_lw_shared<compaction_manager::task>(t, options.type(), get_compaction_state(t));
    _tasks.push_back(task);
    cmlog.debug("{} task {} table={}: started", options.type(), fmt::ptr(task.get()), fmt::ptr(task->compacting_table));

    std::vector<sstables::shared_sstable> sstables;
    std::optional<compacting_sstable_registration> compacting;

    auto task_completion = defer([this, &task, &sstables, &options] {
        _stats.pending_tasks -= sstables.size();
        _tasks.remove(task);
        cmlog.debug("{} task {} table={}: done", options.type(), fmt::ptr(task.get()), fmt::ptr(task->compacting_table));
    });

    // since we might potentially have ongoing compactions, and we
    // must ensure that all sstables created before we run are included
    // in the re-write, we need to barrier out any previously running
    // compaction.
    co_await run_with_compaction_disabled(t, [&] () mutable -> future<> {
        sstables = co_await get_func();
        compacting = compacting_sstable_registration(this, sstables);
    });
    // sort sstables by size in descending order, such that the smallest files will be rewritten first
    // (as sstable to be rewritten is popped off from the back of container), so rewrite will have higher
    // chance to succeed when the biggest files are reached.
    std::sort(sstables.begin(), sstables.end(), [](sstables::shared_sstable& a, sstables::shared_sstable& b) {
        return a->data_size() > b->data_size();
    });

    _stats.pending_tasks += sstables.size();

    auto rewrite_sstable = [this, &task, &options, &compacting, can_purge] (const sstables::shared_sstable& sst) mutable -> future<>  {
        stop_iteration completed = stop_iteration::no;
        do {
            replica::table& t = *task->compacting_table;
            auto sstable_level = sst->get_sstable_level();
            auto run_identifier = sst->run_identifier();
            auto sstable_set_snapshot = can_purge ? std::make_optional(t.get_sstable_set()) : std::nullopt;
            // FIXME: this compaction should run with maintenance priority.
            auto descriptor = sstables::compaction_descriptor({ sst }, std::move(sstable_set_snapshot), service::get_local_compaction_priority(),
                sstable_level, sstables::compaction_descriptor::default_max_sstable_bytes, run_identifier, options);

            // Releases reference to cleaned sstable such that respective used disk space can be freed.
            descriptor.release_exhausted = [&compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting->release_compacting(exhausted_sstables);
            };

            auto maintenance_permit = co_await seastar::get_units(_maintenance_ops_sem, 1);
            // Take write lock for table to serialize cleanup/upgrade sstables/scrub with major compaction/reshape/reshard.
            auto write_lock_holder = co_await _compaction_state[&t].lock.hold_write_lock();

            _stats.pending_tasks--;
            _stats.active_tasks++;
            task->setup_new_compaction();
            task->output_run_identifier = descriptor.run_identifier;

            auto perform_rewrite = [this, &t, &descriptor, &task] () mutable -> future<stop_iteration> {
                std::exception_ptr ex;
                try {
                    auto compaction_completion = defer([&task, this] {
                        task->finish_compaction();
                        _stats.active_tasks--;
                    });
                    co_await t.compact_sstables(std::move(descriptor), task->compaction_data);
                } catch (...) {
                    ex = std::current_exception();
                }
                if (ex) {
                    if (!maybe_stop_on_error(std::move(ex), can_proceed(task))) {
                        _stats.pending_tasks++;
                        co_await put_task_to_sleep(task);
                        co_return stop_iteration::no;
                    }
                    co_return stop_iteration::yes;
                }
                _stats.completed_tasks++;
                reevaluate_postponed_compactions();
                co_return stop_iteration::yes;
            };

            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_compaction_controller.backlog_of_shares(200), _available_memory));
            completed = co_await with_scheduling_group(_compaction_controller.sg(), std::ref(perform_rewrite));
        } while (!completed);
    };

    shared_promise<> p;
    task->compaction_done = p.get_shared_future();
    try {
        while (!sstables.empty() && can_proceed(task)) {
            auto sst = sstables.back();
            sstables.pop_back();
            co_await rewrite_sstable(sst);
        }
        p.set_value();
    } catch (...) {
        p.set_exception(std::current_exception());
        throw;
    }
}

future<> compaction_manager::perform_sstable_scrub_validate_mode(replica::table* t) {
    // All sstables must be included, even the ones being compacted, such that everything in table is validated.
    auto all_sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*t->get_sstables());
    return run_custom_job(t, sstables::compaction_type::Scrub, [this, &t = *t, sstables = std::move(all_sstables)] (sstables::compaction_data& info) mutable -> future<> {
        class pending_tasks {
            compaction_manager::stats& _stats;
            size_t _n;
        public:
            pending_tasks(compaction_manager::stats& stats, size_t n) : _stats(stats), _n(n) { _stats.pending_tasks += _n; }
            ~pending_tasks() { _stats.pending_tasks -= _n; }
            void operator--(int) {
                --_stats.pending_tasks;
                --_n;
            }
        };
        pending_tasks pending(_stats, sstables.size());

        while (!sstables.empty()) {
            auto sst = sstables.back();
            sstables.pop_back();

            try {
                co_await with_scheduling_group(_maintenance_sg.cpu, [&] () {
                    auto desc = sstables::compaction_descriptor(
                            { sst },
                            {},
                            _maintenance_sg.io,
                            sst->get_sstable_level(),
                            sstables::compaction_descriptor::default_max_sstable_bytes,
                            sst->run_identifier(),
                            sstables::compaction_type_options::make_scrub(sstables::compaction_type_options::scrub::mode::validate));
                    return compact_sstables(std::move(desc), info, t.as_table_state());
                });
            } catch (sstables::compaction_stopped_exception&) {
                throw; // let run_custom_job() handle this
            } catch (storage_io_error&) {
                throw; // let run_custom_job() handle this
            } catch (...) {
                // We are validating potentially corrupt sstables, errors are
                // expected, just continue with the other sstables when seeing
                // one.
                _stats.errors++;
                cmlog.error("Scrubbing in validate mode {} failed due to {}, continuing.", sst->get_filename(), std::current_exception());
            }

            pending--;
        }
    });
}

bool needs_cleanup(const sstables::shared_sstable& sst,
                   const dht::token_range_vector& sorted_owned_ranges,
                   schema_ptr s) {
    auto first = sst->get_first_partition_key();
    auto last = sst->get_last_partition_key();
    auto first_token = dht::get_token(*s, first);
    auto last_token = dht::get_token(*s, last);
    dht::token_range sst_token_range = dht::token_range::make(first_token, last_token);

    auto r = std::lower_bound(sorted_owned_ranges.begin(), sorted_owned_ranges.end(), first_token,
            [] (const range<dht::token>& a, const dht::token& b) {
        // check that range a is before token b.
        return a.after(b, dht::token_comparator());
    });

    // return true iff sst partition range isn't fully contained in any of the owned ranges.
    if (r != sorted_owned_ranges.end()) {
        if (r->contains(sst_token_range, dht::token_comparator())) {
            return false;
        }
    }
    return true;
}

future<> compaction_manager::perform_cleanup(replica::database& db, replica::table* t) {
    auto check_for_cleanup = [this, t] {
        return boost::algorithm::any_of(_tasks, [t] (auto& task) {
            return task->compacting_table == t && task->type == sstables::compaction_type::Cleanup;
        });
    };
    if (check_for_cleanup()) {
        return make_exception_future<>(std::runtime_error(format("cleanup request failed: there is an ongoing cleanup on {}.{}",
            t->schema()->ks_name(), t->schema()->cf_name())));
    }

    auto sorted_owned_ranges = db.get_keyspace_local_ranges(t->schema()->ks_name());
    auto get_sstables = [this, &db, t, sorted_owned_ranges] () -> future<std::vector<sstables::shared_sstable>> {
        return seastar::async([this, &db, t, sorted_owned_ranges = std::move(sorted_owned_ranges)] {
            auto schema = t->schema();
            auto sstables = std::vector<sstables::shared_sstable>{};
            const auto candidates = get_candidates(*t);
            std::copy_if(candidates.begin(), candidates.end(), std::back_inserter(sstables), [&sorted_owned_ranges, schema] (const sstables::shared_sstable& sst) {
                seastar::thread::maybe_yield();
                return sorted_owned_ranges.empty() || needs_cleanup(sst, sorted_owned_ranges, schema);
            });
            return sstables;
        });
    };

    return rewrite_sstables(t, sstables::compaction_type_options::make_cleanup(std::move(sorted_owned_ranges)), std::move(get_sstables));
}

// Submit a table to be upgraded and wait for its termination.
future<> compaction_manager::perform_sstable_upgrade(replica::database& db, replica::table* t, bool exclude_current_version) {
    auto get_sstables = [this, &db, t, exclude_current_version] {
        std::vector<sstables::shared_sstable> tables;

        auto last_version = t->get_sstables_manager().get_highest_supported_format();

        for (auto& sst : get_candidates(*t)) {
            // if we are a "normal" upgrade, we only care about
            // tables with older versions, but potentially
            // we are to actually rewrite everything. (-a)
            if (!exclude_current_version || sst->get_version() < last_version) {
                tables.emplace_back(sst);
            }
        }

        return make_ready_future<std::vector<sstables::shared_sstable>>(tables);
    };

    // doing a "cleanup" is about as compacting as we need
    // to be, provided we get to decide the tables to process,
    // and ignoring any existing operations.
    // Note that we potentially could be doing multiple
    // upgrades here in parallel, but that is really the users
    // problem.
    return rewrite_sstables(t, sstables::compaction_type_options::make_upgrade(db.get_keyspace_local_ranges(t->schema()->ks_name())), std::move(get_sstables));
}

// Submit a table to be scrubbed and wait for its termination.
future<> compaction_manager::perform_sstable_scrub(replica::table* t, sstables::compaction_type_options::scrub opts) {
    auto scrub_mode = opts.operation_mode;
    if (scrub_mode == sstables::compaction_type_options::scrub::mode::validate) {
        return perform_sstable_scrub_validate_mode(t);
    }
    return rewrite_sstables(t, sstables::compaction_type_options::make_scrub(scrub_mode), [this, t, opts] {
        auto all_sstables = t->get_sstable_set().all();
        std::vector<sstables::shared_sstable> sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*all_sstables
                | boost::adaptors::filtered([&opts] (const sstables::shared_sstable& sst) {
            if (sst->requires_view_building()) {
                return false;
            }
            switch (opts.quarantine_operation_mode) {
            case sstables::compaction_type_options::scrub::quarantine_mode::include:
                return true;
            case sstables::compaction_type_options::scrub::quarantine_mode::exclude:
                return !sst->is_quarantined();
            case sstables::compaction_type_options::scrub::quarantine_mode::only:
                return sst->is_quarantined();
            }
        }));
        return make_ready_future<std::vector<sstables::shared_sstable>>(std::move(sstables));
    }, can_purge_tombstones::no);
}

void compaction_manager::add(replica::table* t) {
    auto [_, inserted] = _compaction_state.insert({t, compaction_state{}});
    if (!inserted) {
        auto s = t->schema();
        on_internal_error(cmlog, format("compaction_state for table {}.{} [{}] already exists", s->ks_name(), s->cf_name(), fmt::ptr(t)));
    }
}

future<> compaction_manager::remove(replica::table* t) {
    auto handle = _compaction_state.extract(t);

    if (!handle.empty()) {
        auto& c_state = handle.mapped();

        // We need to guarantee that a task being stopped will not retry to compact
        // a table being removed.
        // The requirement above is provided by stop_ongoing_compactions().
        _postponed.erase(t);

        // Wait for the termination of an ongoing compaction on table T, if any.
        co_await stop_ongoing_compactions("table removal", t);

        // Wait for all functions running under gate to terminate.
        co_await c_state.gate.close();
    }
#ifdef DEBUG
    auto found = false;
    sstring msg;
    for (auto& task : _tasks) {
        if (task->compacting_table == t) {
            if (!msg.empty()) {
                msg += "\n";
            }
            msg += format("Found compaction task {} table={} after remove", fmt::ptr(task.get()), fmt::ptr(t));
            found = true;
        }
    }
    if (found) {
        on_internal_error_noexcept(cmlog, msg);
    }
#endif
}

const std::vector<sstables::compaction_info> compaction_manager::get_compactions(replica::table* t) const {
    auto to_info = [] (const lw_shared_ptr<task>& task) {
        sstables::compaction_info ret;
        ret.compaction_uuid = task->compaction_data.compaction_uuid;
        ret.type = task->type;
        ret.ks_name = task->compacting_table->schema()->ks_name();
        ret.cf_name = task->compacting_table->schema()->cf_name();
        ret.total_partitions = task->compaction_data.total_partitions;
        ret.total_keys_written = task->compaction_data.total_keys_written;
        return ret;
    };
    using ret = std::vector<sstables::compaction_info>;
    return boost::copy_range<ret>(_tasks | boost::adaptors::filtered([t] (const lw_shared_ptr<task>& task) {
                return (!t || task->compacting_table == t) && task->compaction_running;
            }) | boost::adaptors::transformed(to_info));
}

future<> compaction_manager::stop_compaction(sstring type, replica::table* table) {
    sstables::compaction_type target_type;
    try {
        target_type = sstables::to_compaction_type(type);
    } catch (...) {
        throw std::runtime_error(format("Compaction of type {} cannot be stopped by compaction manager: {}", type.c_str(), std::current_exception()));
    }
    switch (target_type) {
    case sstables::compaction_type::Validation:
    case sstables::compaction_type::Index_build:
        throw std::runtime_error(format("Compaction type {} is unsupported", type.c_str()));
    case sstables::compaction_type::Reshard:
        throw std::runtime_error(format("Stopping compaction of type {} is disallowed", type.c_str()));
    default:
        break;
    }
    return stop_ongoing_compactions("user request", table, target_type);
}

void compaction_manager::propagate_replacement(replica::table* t,
        const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added) {
    for (auto& task : _tasks) {
        if (task->compacting_table == t && task->compaction_running) {
            task->compaction_data.pending_replacements.push_back({ removed, added });
        }
    }
}

class compaction_manager::strategy_control : public compaction::strategy_control {
    compaction_manager& _cm;
public:
    explicit strategy_control(compaction_manager& cm) noexcept : _cm(cm) {}

    bool has_ongoing_compaction(table_state& table_s) const noexcept override {
        return std::any_of(_cm._tasks.begin(), _cm._tasks.end(), [&s = table_s.schema()] (const lw_shared_ptr<task>& task) {
            return task->compaction_running
                && task->compacting_table->schema()->ks_name() == s->ks_name()
                && task->compacting_table->schema()->cf_name() == s->cf_name();
        });
    }
};

compaction::strategy_control& compaction_manager::get_strategy_control() const noexcept {
    return *_strategy_control;
}

double compaction_backlog_tracker::backlog() const {
    return _disabled ? compaction_controller::disable_backlog : _impl->backlog(_ongoing_writes, _ongoing_compactions);
}

void compaction_backlog_tracker::add_sstable(sstables::shared_sstable sst) {
    if (_disabled || !sstable_belongs_to_tracker(sst)) {
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
    if (_disabled || !sstable_belongs_to_tracker(sst)) {
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

bool compaction_backlog_tracker::sstable_belongs_to_tracker(const sstables::shared_sstable& sst) {
    return sstables::is_eligible_for_compaction(sst);
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
