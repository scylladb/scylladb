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
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "sstables/exceptions.hh"
#include "locator/abstract_replication_strategy.hh"
#include "utils/fb_utilities.hh"
#include "utils/UUID_gen.hh"
#include <cmath>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/remove_if.hpp>

static logging::logger cmlog("compaction_manager");
using namespace std::chrono_literals;

class compacting_sstable_registration {
    compaction_manager& _cm;
    std::unordered_set<sstables::shared_sstable> _compacting;
public:
    explicit compacting_sstable_registration(compaction_manager& cm) noexcept
        : _cm(cm)
    { }

    compacting_sstable_registration(compaction_manager& cm, std::vector<sstables::shared_sstable> compacting)
        : compacting_sstable_registration(cm)
    {
        register_compacting(compacting);
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
    { }

    ~compacting_sstable_registration() {
        // _compacting might be empty, but this should be just fine
        // for deregister_compacting_sstables.
        _cm.deregister_compacting_sstables(_compacting.begin(), _compacting.end());
    }

    void register_compacting(const std::vector<sstables::shared_sstable>& sstables) {
        _compacting.reserve(_compacting.size() + sstables.size());
        _compacting.insert(sstables.begin(), sstables.end());
        _cm.register_compacting_sstables(sstables.begin(), sstables.end());
    }

    // Explicitly release compacting sstables
    void release_compacting(const std::vector<sstables::shared_sstable>& sstables) {
        _cm.deregister_compacting_sstables(sstables.begin(), sstables.end());
        for (const auto& sst : sstables) {
            _compacting.erase(sst);
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
    return calculate_weight(descriptor.sstables_size());
}

unsigned compaction_manager::current_compaction_fan_in_threshold() const {
    if (_tasks.empty()) {
        return 0;
    }
    auto largest_fan_in = std::ranges::max(_tasks | boost::adaptors::transformed([] (auto& task) {
        return task->compaction_running() ? task->compaction_data().compaction_fan_in : 0;
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

template <typename Iterator, typename Sentinel>
requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
void compaction_manager::register_compacting_sstables(Iterator first, Sentinel last) {
    // make all required allocations in advance to merge
    // so it should not throw
    _compacting_sstables.reserve(_compacting_sstables.size() + std::distance(first, last));
    try {
        _compacting_sstables.insert(first, last);
    } catch (...) {
        cmlog.error("Unexpected error when registering compacting SSTables: {}. Ignored...", std::current_exception());
    }
}

template <typename Iterator, typename Sentinel>
requires std::same_as<Sentinel, Iterator> || std::sentinel_for<Sentinel, Iterator>
void compaction_manager::deregister_compacting_sstables(Iterator first, Sentinel last) {
    // Remove compacted sstables from the set of compacting sstables.
    for (; first != last; ++first) {
        _compacting_sstables.erase(*first);
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
    virtual void replace_sstables(std::vector<sstables::shared_sstable> old_ssts, std::vector<sstables::shared_sstable> new_ssts) override {}
};

compaction_manager::compaction_state& compaction_manager::get_compaction_state(replica::table* t) {
    try {
        return _compaction_state.at(t);
    } catch (std::out_of_range&) {
        // Note: don't dereference t as it might not exist
        throw std::out_of_range(format("Compaction state for table [{}] not found", fmt::ptr(t)));
    }
}

future<> compaction_manager::perform_task(shared_ptr<compaction_manager::task> task) {
    _tasks.push_back(task);
    auto unregister_task = defer([this, task] {
        _tasks.remove(task);
    });
    cmlog.debug("{}: started", *task);

    try {
        co_await task->run();
        cmlog.debug("{}: done", *task);
    } catch (sstables::compaction_stopped_exception& e) {
        cmlog.info("{}: stopped, reason: {}", *task, e.what());
    } catch (sstables::compaction_aborted_exception& e) {
        cmlog.error("{}: aborted, reason: {}", *task, e.what());
        _stats.errors++;
        throw;
    } catch (storage_io_error& e) {
        _stats.errors++;
        cmlog.error("{}: failed due to storage io error: {}: stopping", *task, e.what());
        do_stop();
        throw;
    } catch (...) {
        cmlog.error("{}: failed, reason {}: stopping", *task, std::current_exception());
        _stats.errors++;
        throw;
    }
}

future<> compaction_manager::task::compact_sstables_and_update_history(sstables::compaction_descriptor descriptor, sstables::compaction_data& cdata, release_exhausted_func_t release_exhausted, can_purge_tombstones can_purge) {
    if (!descriptor.sstables.size()) {
        // if there is nothing to compact, just return.
        co_return;
    }

    bool should_update_history = this->should_update_history(descriptor.options.type());
    sstables::compaction_result res = co_await compact_sstables(std::move(descriptor), cdata, std::move(release_exhausted), std::move(can_purge));

    if (should_update_history) {
        co_await update_history(*_compacting_table, res, cdata);
    }
}
future<sstables::compaction_result> compaction_manager::task::compact_sstables(sstables::compaction_descriptor descriptor, sstables::compaction_data& cdata, release_exhausted_func_t release_exhausted, can_purge_tombstones can_purge) {
    replica::table& t = *_compacting_table;
    if (can_purge) {
        descriptor.enable_garbage_collection(t.get_sstable_set());
    }
    descriptor.creator = [&t] (shard_id dummy) {
        auto sst = t.make_sstable();
        return sst;
    };
    descriptor.replacer = [this, &t, release_exhausted] (sstables::compaction_completion_desc desc) {
        t.get_compaction_strategy().notify_completion(desc.old_sstables, desc.new_sstables);
        _cm.propagate_replacement(&t, desc.old_sstables, desc.new_sstables);
        t.on_compaction_completion(desc);
        // Calls compaction manager's task for this compaction to release reference to exhausted SSTables.
        if (release_exhausted) {
            release_exhausted(desc.old_sstables);
        }
    };

    co_return co_await sstables::compact_sstables(std::move(descriptor), cdata, t.as_table_state());
}
future<> compaction_manager::task::update_history(replica::table& t, const sstables::compaction_result& res, const sstables::compaction_data& cdata) {
    auto ended_at = std::chrono::duration_cast<std::chrono::milliseconds>(res.ended_at.time_since_epoch());

    co_return co_await t.as_table_state().update_compaction_history(cdata.compaction_uuid, t.schema()->ks_name(), t.schema()->cf_name(), ended_at,
                                                                    res.start_size, res.end_size);
}

class compaction_manager::major_compaction_task : public compaction_manager::task {
public:
    major_compaction_task(compaction_manager& mgr, replica::table* t)
        : task(mgr, t, sstables::compaction_type::Compaction, "Major compaction")
    {}

protected:
    // first take major compaction semaphore, then exclusely take compaction lock for table.
    // it cannot be the other way around, or minor compaction for this table would be
    // prevented while an ongoing major compaction doesn't release the semaphore.
    virtual future<> do_run() override {
        co_await coroutine::switch_to(_cm._compaction_sg.cpu);

        switch_state(state::pending);
        auto units = co_await acquire_semaphore(_cm._maintenance_ops_sem);
        auto lock_holder = co_await _compaction_state.lock.hold_write_lock();
        if (!can_proceed()) {
            co_return;
        }

        // candidates are sstables that aren't being operated on by other compaction types.
        // those are eligible for major compaction.
        auto* t = _compacting_table;
        sstables::compaction_strategy cs = t->get_compaction_strategy();
        sstables::compaction_descriptor descriptor = cs.get_major_compaction_job(t->as_table_state(), _cm.get_candidates(*t));
        auto compacting = compacting_sstable_registration(_cm, descriptor.sstables);
        auto release_exhausted = [&compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
            compacting.release_compacting(exhausted_sstables);
        };
        setup_new_compaction(descriptor.run_identifier);

        cmlog.info0("User initiated compaction started on behalf of {}.{}", t->schema()->ks_name(), t->schema()->cf_name());
        compaction_backlog_tracker bt(std::make_unique<user_initiated_backlog_tracker>(_cm._compaction_controller.backlog_of_shares(200), _cm._available_memory));
        _cm.register_backlog_tracker(bt);

        co_await compact_sstables_and_update_history(std::move(descriptor), _compaction_data, std::move(release_exhausted));

        finish_compaction();
    }
};

future<> compaction_manager::perform_major_compaction(replica::table* t) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }
    return perform_task(make_shared<major_compaction_task>(*this, t));
}

class compaction_manager::custom_compaction_task : public compaction_manager::task {
    noncopyable_function<future<>(sstables::compaction_data&)> _job;

public:
    custom_compaction_task(compaction_manager& mgr, replica::table* t, sstables::compaction_type type, sstring desc, noncopyable_function<future<>(sstables::compaction_data&)> job)
        : task(mgr, t, type, std::move(desc))
        , _job(std::move(job))
    {}

protected:
    virtual future<> do_run() override {
        if (!can_proceed(throw_if_stopping::yes)) {
            co_return;
        }
        switch_state(state::pending);
        auto units = co_await acquire_semaphore(_cm._maintenance_ops_sem);

        if (!can_proceed(throw_if_stopping::yes)) {
            co_return;
        }
        setup_new_compaction();

        // NOTE:
        // no need to register shared sstables because they're excluded from non-resharding
        // compaction and some of them may not even belong to current shard.
        co_await _job(compaction_data());
        finish_compaction();
    }
};

future<> compaction_manager::run_custom_job(replica::table* t, sstables::compaction_type type, const char* desc, noncopyable_function<future<>(sstables::compaction_data&)> job) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }

    return perform_task(make_shared<custom_compaction_task>(*this, t, type, desc, std::move(job)));
}

compaction_manager::compaction_reenabler::compaction_reenabler(compaction_manager& cm, replica::table* t)
    : _cm(cm)
    , _table(t)
    , _compaction_state(cm.get_compaction_state(_table))
    , _holder(_compaction_state.gate.hold())
{
    _compaction_state.compaction_disabled_counter++;
    cmlog.debug("Temporarily disabled compaction for {}.{}. compaction_disabled_counter={}",
            _table->schema()->ks_name(), _table->schema()->cf_name(), _compaction_state.compaction_disabled_counter);
}

compaction_manager::compaction_reenabler::compaction_reenabler(compaction_reenabler&& o) noexcept
    : _cm(o._cm)
    , _table(std::exchange(o._table, nullptr))
    , _compaction_state(o._compaction_state)
    , _holder(std::move(o._holder))
{}

compaction_manager::compaction_reenabler::~compaction_reenabler() {
    // submit compaction request if we're the last holder of the gate which is still opened.
    if (_table && --_compaction_state.compaction_disabled_counter == 0 && !_compaction_state.gate.is_closed()) {
        cmlog.debug("Reenabling compaction for {}.{}",
                _table->schema()->ks_name(), _table->schema()->cf_name());
        try {
            _cm.submit(_table);
        } catch (...) {
            cmlog.warn("compaction_reenabler could not reenable compaction for {}.{}: {}",
                    _table->schema()->ks_name(), _table->schema()->cf_name(), std::current_exception());
        }
    }
}

future<compaction_manager::compaction_reenabler>
compaction_manager::stop_and_disable_compaction(replica::table* t) {
    compaction_reenabler cre(*this, t);
    co_await stop_ongoing_compactions("user-triggered operation", t);
    co_return cre;
}

future<>
compaction_manager::run_with_compaction_disabled(replica::table* t, std::function<future<> ()> func) {
    compaction_reenabler cre = co_await stop_and_disable_compaction(t);

    co_await func();
}

std::string_view compaction_manager::task::to_string(state s) {
    switch (s) {
    case state::none: return "none";
    case state::pending: return "pending";
    case state::active: return "active";
    case state::done: return "done";
    case state::postponed: return "postponed";
    case state::failed: return "failed";
    }
    __builtin_unreachable();
}

std::ostream& operator<<(std::ostream& os, compaction_manager::task::state s) {
    return os << compaction_manager::task::to_string(s);
}

std::ostream& operator<<(std::ostream& os, const compaction_manager::task& task) {
    return os << task.describe();
}

inline compaction_controller make_compaction_controller(compaction_manager::scheduling_group& csg, uint64_t static_shares, std::function<double()> fn) {
    if (static_shares > 0) {
        return compaction_controller(csg, static_shares);
    }

    return compaction_controller(csg, 250ms, std::move(fn));
}

std::string compaction_manager::task::describe() const {
    auto* t = _compacting_table;
    auto s = t->schema();
    return fmt::format("{} task {} for table {}.{} [{}]", _description, fmt::ptr(this), s->ks_name(), s->cf_name(), fmt::ptr(t));
}

compaction_manager::task::~task() {
    switch_state(state::none);
}

compaction_manager::sstables_task::~sstables_task() {
    _cm._stats.pending_tasks -= _sstables.size() - (_state == state::pending);
}

future<> compaction_manager::task::run() noexcept {
    try {
        _compaction_done = do_run();
        return compaction_done();
    } catch (...) {
        return current_exception_as_future();
    }
}

compaction_manager::task::state compaction_manager::task::switch_state(state new_state) {
    auto old_state = std::exchange(_state, new_state);
    switch (old_state) {
    case state::none:
    case state::done:
    case state::postponed:
    case state::failed:
        break;
    case state::pending:
        --_cm._stats.pending_tasks;
        break;
    case state::active:
        --_cm._stats.active_tasks;
        break;
    }
    switch (new_state) {
    case state::none:
    case state::postponed:
    case state::failed:
        break;
    case state::pending:
        ++_cm._stats.pending_tasks;
        break;
    case state::active:
        ++_cm._stats.active_tasks;
        break;
    case state::done:
        ++_cm._stats.completed_tasks;
        break;
    }
    cmlog.debug("{}: switch_state: {} -> {}: pending={} active={} done={} errors={}", *this, old_state, new_state,
            _cm._stats.pending_tasks, _cm._stats.active_tasks, _cm._stats.completed_tasks, _cm._stats.errors);
    return old_state;
}

void compaction_manager::sstables_task::set_sstables(std::vector<sstables::shared_sstable> new_sstables) {
    if (!_sstables.empty()) {
        on_internal_error(cmlog, format("sstables were already set"));
    }
    _sstables = std::move(new_sstables);
    cmlog.debug("{}: set_sstables: {} sstable{}", *this, _sstables.size(), _sstables.size() > 1 ? "s" : "");
    _cm._stats.pending_tasks += _sstables.size() - (_state == state::pending);
}

sstables::shared_sstable compaction_manager::sstables_task::consume_sstable() {
    if (_sstables.empty()) {
        on_internal_error(cmlog, format("no more sstables"));
    }
    auto sst = _sstables.back();
    _sstables.pop_back();
    --_cm._stats.pending_tasks; // from this point on, switch_state(pending|active) works the same way as any other task
    cmlog.debug("{}", format("consumed {}", sst->get_filename()));
    return sst;
}

future<semaphore_units<named_semaphore_exception_factory>> compaction_manager::task::acquire_semaphore(named_semaphore& sem, size_t units) {
    return seastar::get_units(sem, units, _compaction_data.abort).handle_exception_type([this] (const abort_requested_exception& e) {
        auto s = _compacting_table->schema();
        return make_exception_future<semaphore_units<named_semaphore_exception_factory>>(
                sstables::compaction_stopped_exception(s->ks_name(), s->cf_name(), e.what()));
    });
}

void compaction_manager::task::setup_new_compaction(utils::UUID output_run_id) {
    _compaction_data = create_compaction_data();
    _output_run_identifier = output_run_id;
    switch_state(state::active);
}

void compaction_manager::task::finish_compaction(state finish_state) noexcept {
    switch_state(finish_state);
    _output_run_identifier = utils::null_uuid();
    if (finish_state != state::failed) {
        _compaction_retry.reset();
    }
}

void compaction_manager::task::stop(sstring reason) noexcept {
    _compaction_data.stop(std::move(reason));
}

sstables::compaction_stopped_exception compaction_manager::task::make_compaction_stopped_exception() const {
    auto s = _compacting_table->schema();
    return sstables::compaction_stopped_exception(s->ks_name(), s->cf_name(), _compaction_data.stop_requested);
}

compaction_manager::compaction_manager(config cfg, abort_source& as)
    : _dmm(cfg.dmm)
    , _compaction_sg(cfg.compaction_sched_group)
    , _maintenance_sg(cfg.maintenance_sched_group)
    , _compaction_controller(make_compaction_controller(_compaction_sg, cfg.static_shares, [this, available_memory = cfg.available_memory] () -> float {
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
    }))
    , _backlog_manager(_compaction_controller)
    , _available_memory(cfg.available_memory)
    , _early_abort_subscription(as.subscribe([this] () noexcept {
        do_stop();
    }))
    , _strategy_control(std::make_unique<strategy_control>(*this))
{
    register_metrics();
}

compaction_manager::compaction_manager(dirty_memory_manager& dmm)
    : _dmm(dmm)
    , _compaction_sg(scheduling_group{default_scheduling_group(), default_priority_class()})
    , _maintenance_sg(scheduling_group{default_scheduling_group(), default_priority_class()})
    , _compaction_controller(_compaction_sg, 1)
    , _backlog_manager(_compaction_controller)
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
        sm::make_counter("completed_compactions", [this] { return _stats.completed_tasks; },
                       sm::description("Holds the number of completed compaction tasks.")),
        sm::make_counter("failed_compactions", [this] { return _stats.errors; },
                       sm::description("Holds the number of failed compaction tasks.")),
        sm::make_gauge("postponed_compactions", [this] { return _postponed.size(); },
                       sm::description("Holds the number of tables with postponed compaction.")),
        sm::make_gauge("backlog", [this] { return _last_backlog; },
                       sm::description("Holds the sum of compaction backlog for all tables in the system.")),
        sm::make_gauge("normalized_backlog", [this] { return _last_backlog / _available_memory; },
                       sm::description("Holds the sum of normalized compaction backlog for all tables in the system. Backlog is normalized by dividing backlog by shard's available memory.")),
    });
}

void compaction_manager::enable() {
    assert(_state == state::none || _state == state::disabled);
    _state = state::enabled;
    _compaction_submission_timer.arm(periodic_compaction_submission_interval());
    postponed_compactions_reevaluation();
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
                    auto s = t->schema();
                    cmlog.debug("resubmitting postponed compaction for table {}.{} [{}]", s->ks_name(), s->cf_name(), fmt::ptr(t));
                    submit(t);
                }
            } catch (...) {
                _postponed = std::move(postponed);
            }
            return stop_iteration::no;
        });
    });
}

void compaction_manager::reevaluate_postponed_compactions() noexcept {
    _postponed_reevaluation.signal();
}

void compaction_manager::postpone_compaction_for_table(replica::table* t) {
    _postponed.insert(t);
}

future<> compaction_manager::stop_tasks(std::vector<shared_ptr<task>> tasks, sstring reason) {
    // To prevent compaction from being postponed while tasks are being stopped,
    // let's stop all tasks before the deferring point below.
    for (auto& t : tasks) {
        cmlog.debug("Stopping {}", *t);
        t->stop(reason);
    }
    co_await coroutine::parallel_for_each(tasks, [this] (auto& task) -> future<> {
        try {
            co_await task->compaction_done();
        } catch (sstables::compaction_stopped_exception&) {
            // swallow stop exception if a given procedure decides to propagate it to the caller,
            // as it happens with reshard and reshape.
        } catch (...) {
            cmlog.debug("Stopping {}: task returned error: {}", *task, std::current_exception());
            throw;
        }
        cmlog.debug("Stopping {}: done", *task);
    });
}

future<> compaction_manager::stop_ongoing_compactions(sstring reason, replica::table* t, std::optional<sstables::compaction_type> type_opt) {
    auto ongoing_compactions = get_compactions(t).size();
    auto tasks = boost::copy_range<std::vector<shared_ptr<task>>>(_tasks | boost::adaptors::filtered([t, type_opt] (auto& task) {
        return (!t || task->compacting_table() == t) && (!type_opt || task->type() == *type_opt);
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

future<> compaction_manager::really_do_stop() {
    cmlog.info("Asked to stop");
    // Reset the metrics registry
    _metrics.clear();
    co_await stop_ongoing_compactions("shutdown");
    reevaluate_postponed_compactions();
    co_await std::move(_waiting_reevalution);
    _weight_tracker.clear();
    _compaction_submission_timer.cancel();
    co_await _compaction_controller.shutdown();
    cmlog.info("Stopped");
}

void compaction_manager::do_stop() noexcept {
    if (_state == state::none || _state == state::stopped) {
        return;
    }

    try {
        _state = state::stopped;
        _stop_future = really_do_stop();
    } catch (...) {
        cmlog.error("Failed to stop the manager: {}", std::current_exception());
    }
}

inline bool compaction_manager::can_proceed(replica::table* t) const {
    return (_state == state::enabled) && _compaction_state.contains(t) && !_compaction_state.at(t).compaction_disabled();
}

inline bool compaction_manager::task::can_proceed(throw_if_stopping do_throw_if_stopping) const {
    if (stopping()) {
        // Allow caller to know that task (e.g. reshape) was asked to stop while waiting for a chance to run.
        if (do_throw_if_stopping) {
            throw make_compaction_stopped_exception();
        }
        return false;
    }
    return _cm.can_proceed(_compacting_table);
}

future<stop_iteration> compaction_manager::task::maybe_retry(std::exception_ptr err) {
    try {
        std::rethrow_exception(err);
    } catch (sstables::compaction_stopped_exception& e) {
        cmlog.info("{}: {}: stopping", *this, e.what());
    } catch (sstables::compaction_aborted_exception& e) {
        cmlog.error("{}: {}: stopping", *this, e.what());
        _cm._stats.errors++;
    } catch (storage_io_error& e) {
        cmlog.error("{}: failed due to storage io error: {}: stopping", *this, e.what());
        _cm._stats.errors++;
        _cm.do_stop();
        throw;
    } catch (...) {
        if (can_proceed()) {
            _cm._stats.errors++;
            cmlog.error("{}: failed: {}. Will retry in {} seconds", *this, std::current_exception(),
                    std::chrono::duration_cast<std::chrono::seconds>(_compaction_retry.sleep_time()).count());
            switch_state(state::pending);
            return _compaction_retry.retry(_compaction_data.abort).handle_exception_type([this] (sleep_aborted&) {
                return make_exception_future<>(make_compaction_stopped_exception());
            }).then([] {
                return make_ready_future<stop_iteration>(false);
            });
        }
        throw;
    }
    return make_ready_future<stop_iteration>(true);
}

class compaction_manager::regular_compaction_task : public compaction_manager::task {
public:
    regular_compaction_task(compaction_manager& mgr, replica::table* t)
        : task(mgr, t, sstables::compaction_type::Compaction, "Compaction")
    {}
protected:
    virtual future<> do_run() override {
        co_await coroutine::switch_to(_cm._compaction_sg.cpu);

        for (;;) {
            if (!can_proceed()) {
                co_return;
            }
            switch_state(state::pending);
            // take read lock for table, so major and regular compaction can't proceed in parallel.
            auto lock_holder = co_await _compaction_state.lock.hold_read_lock();
            if (!can_proceed()) {
                co_return;
            }

            replica::table& t = *_compacting_table;
            sstables::compaction_strategy cs = t.get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_sstables_for_compaction(t.as_table_state(), _cm.get_strategy_control(), _cm.get_candidates(t));
            int weight = calculate_weight(descriptor);

            if (descriptor.sstables.empty() || !can_proceed() || t.is_auto_compaction_disabled_by_user()) {
                cmlog.debug("{}: sstables={} can_proceed={} auto_compaction={}", *this, descriptor.sstables.size(), can_proceed(), t.is_auto_compaction_disabled_by_user());
                co_return;
            }
            if (!_cm.can_register_compaction(&t, weight, descriptor.fan_in())) {
                cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}.{}, postponing it...",
                    descriptor.sstables.size(), weight, t.schema()->ks_name(), t.schema()->cf_name());
                switch_state(state::postponed);
                _cm.postpone_compaction_for_table(&t);
                co_return;
            }
            auto compacting = compacting_sstable_registration(_cm, descriptor.sstables);
            auto weight_r = compaction_weight_registration(&_cm, weight);
            auto release_exhausted = [&compacting] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                compacting.release_compacting(exhausted_sstables);
            };
            cmlog.debug("Accepted compaction job: task={} ({} sstable(s)) of weight {} for {}.{}",
                fmt::ptr(this), descriptor.sstables.size(), weight, t.schema()->ks_name(), t.schema()->cf_name());

            setup_new_compaction(descriptor.run_identifier);
            std::exception_ptr ex;

            try {
                bool should_update_history = this->should_update_history(descriptor.options.type());
                sstables::compaction_result res = co_await compact_sstables(std::move(descriptor), _compaction_data, std::move(release_exhausted));
                finish_compaction();
                if (should_update_history) {
                    // update_history can take a long time compared to
                    // compaction, as a call issued on shard S1 can be
                    // handled on shard S2. If the other shard is under
                    // heavy load, we may unnecessarily block kicking off a
                    // new compaction. Normally it isn't a problem, but there were
                    // edge cases where the described behaviour caused
                    // compaction to fail to keep up with excessive
                    // flushing, leading to too many sstables on disk and
                    // OOM during a read.  There is no need to wait with
                    // next compaction until history is updated, so release
                    // the weight earlier to remove unnecessary
                    // serialization.
                    weight_r.deregister();
                    co_await update_history(*_compacting_table, res, _compaction_data);
                }
                _cm.reevaluate_postponed_compactions();
                continue;
            } catch (...) {
                ex = std::current_exception();
            }

            finish_compaction(state::failed);
            if ((co_await maybe_retry(std::move(ex))) == stop_iteration::yes) {
                co_return;
            }
        }
    }
};

void compaction_manager::submit(replica::table* t) {
    if (_state != state::enabled || t->is_auto_compaction_disabled_by_user()) {
        return;
    }

    // OK to drop future.
    // waited via task->stop()
    (void)perform_task(make_shared<regular_compaction_task>(*this, t));
}

class compaction_manager::offstrategy_compaction_task : public compaction_manager::task {
    bool _performed = false;
public:
    offstrategy_compaction_task(compaction_manager& mgr, replica::table* t)
        : task(mgr, t, sstables::compaction_type::Reshape, "Offstrategy compaction")
    {}

    bool performed() const noexcept {
        return _performed;
    }

    future<> run_offstrategy_compaction(sstables::compaction_data& cdata) {
        // This procedure will reshape sstables in maintenance set until it's ready for
        // integration into main set.
        // It may require N reshape rounds before the set satisfies the strategy invariant.
        // This procedure also only updates maintenance set at the end, on success.
        // Otherwise, some overlapping could be introduced in the set after each reshape
        // round, progressively degrading read amplification until integration happens.
        // The drawback of this approach is the 2x space requirement as the old sstables
        // will only be deleted at the end. The impact of this space requirement is reduced
        // by the fact that off-strategy is serialized across all tables, meaning that the
        // actual requirement is the size of the largest table's maintenance set.

        replica::table& t = *_compacting_table;
        const auto& maintenance_sstables = t.maintenance_sstable_set();

        const auto old_sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*maintenance_sstables.all());
        std::vector<sstables::shared_sstable> reshape_candidates = old_sstables;
        std::vector<sstables::shared_sstable> sstables_to_remove;
        std::unordered_set<sstables::shared_sstable> new_unused_sstables;

        auto cleanup_new_unused_sstables_on_failure = defer([&new_unused_sstables] {
            for (auto& sst : new_unused_sstables) {
                sst->mark_for_deletion();
            }
        });

        for (;;) {
            auto& iop = service::get_local_streaming_priority(); // run reshape in maintenance mode
            auto desc = t.get_compaction_strategy().get_reshaping_job(reshape_candidates, t.schema(), iop, sstables::reshape_mode::strict);
            if (desc.sstables.empty()) {
                // at this moment reshape_candidates contains a set of sstables ready for integration into main set
                co_await t.update_sstable_lists_on_off_strategy_completion(old_sstables, reshape_candidates);
                break;
            }

            desc.creator = [this, &new_unused_sstables, &t] (shard_id dummy) {
                auto sst = t.make_sstable();
                new_unused_sstables.insert(sst);
                return sst;
            };
            auto input = boost::copy_range<std::unordered_set<sstables::shared_sstable>>(desc.sstables);

            auto ret = co_await sstables::compact_sstables(std::move(desc), cdata, t.as_table_state());
            _performed = true;

            // update list of reshape candidates without input but with output added to it
            auto it = boost::remove_if(reshape_candidates, [&] (auto& s) { return input.contains(s); });
            reshape_candidates.erase(it, reshape_candidates.end());
            std::move(ret.new_sstables.begin(), ret.new_sstables.end(), std::back_inserter(reshape_candidates));

            // If compaction strategy is unable to reshape input data in a single round, it may happen that a SSTable A
            // created in round 1 will be compacted in a next round producing SSTable B. As SSTable A is no longer needed,
            // it can be removed immediately. Let's remove all such SSTables immediately to reduce off-strategy space requirement.
            // Input SSTables from maintenance set can only be removed later, as SSTable sets are only updated on completion.
            auto can_remove_now = [&] (const sstables::shared_sstable& s) { return new_unused_sstables.contains(s); };
            for (auto&& sst : input) {
                if (can_remove_now(sst)) {
                    co_await sst->unlink();
                    new_unused_sstables.erase(std::move(sst));
                } else {
                    sstables_to_remove.push_back(std::move(sst));
                }
            }
        }

        cleanup_new_unused_sstables_on_failure.cancel();
        // By marking input sstables for deletion instead, the ones which require view building will stay in the staging
        // directory until they're moved to the main dir when the time comes. Also, that allows view building to resume
        // on restart if there's a crash midway.
        for (auto& sst : sstables_to_remove) {
            sst->mark_for_deletion();
        }
    }
protected:
    virtual future<> do_run() override {
        co_await coroutine::switch_to(_cm._maintenance_sg.cpu);

        for (;;) {
            if (!can_proceed()) {
                co_return;
            }
            switch_state(state::pending);
            auto units = co_await acquire_semaphore(_cm._off_strategy_sem);
            if (!can_proceed()) {
                co_return;
            }
            setup_new_compaction();

            std::exception_ptr ex;
            try {
                replica::table& t = *_compacting_table;
                auto maintenance_sstables = t.maintenance_sstable_set().all();
                cmlog.info("Starting off-strategy compaction for {}.{}, {} candidates were found",
                        t.schema()->ks_name(), t.schema()->cf_name(), maintenance_sstables->size());
                co_await run_offstrategy_compaction(_compaction_data);
                finish_compaction();
                cmlog.info("Done with off-strategy compaction for {}.{}", t.schema()->ks_name(), t.schema()->cf_name());
                co_return;
            } catch (...) {
                ex = std::current_exception();
            }

            finish_compaction(state::failed);
            if ((co_await maybe_retry(std::move(ex))) == stop_iteration::yes) {
                co_return;
            }
        }
    }
};

future<bool> compaction_manager::perform_offstrategy(replica::table* t) {
    if (_state != state::enabled) {
        co_return false;
    }
    auto task = make_shared<offstrategy_compaction_task>(*this, t);
    co_await perform_task(task);
    co_return task->performed();
}

class compaction_manager::rewrite_sstables_compaction_task : public compaction_manager::sstables_task {
    sstables::compaction_type_options _options;
    compacting_sstable_registration _compacting;
    can_purge_tombstones _can_purge;

public:
    rewrite_sstables_compaction_task(compaction_manager& mgr, replica::table* t, sstables::compaction_type_options options,
                                     std::vector<sstables::shared_sstable> sstables, compacting_sstable_registration compacting,
                                     can_purge_tombstones can_purge)
        : sstables_task(mgr, t, options.type(), sstring(sstables::to_string(options.type())), std::move(sstables))
        , _options(std::move(options))
        , _compacting(std::move(compacting))
        , _can_purge(can_purge)
    {}

protected:
    virtual future<> do_run() override {
        switch_state(state::pending);
        auto maintenance_permit = co_await acquire_semaphore(_cm._maintenance_ops_sem);

        while (!_sstables.empty() && can_proceed()) {
            auto sst = consume_sstable();
            co_await rewrite_sstable(std::move(sst));
        }
    }

private:
    future<> rewrite_sstable(const sstables::shared_sstable sst) {
        co_await coroutine::switch_to(_cm._compaction_sg.cpu);

        for (;;) {
            switch_state(state::active);
            replica::table& t = *_compacting_table;
            auto sstable_level = sst->get_sstable_level();
            auto run_identifier = sst->run_identifier();
            // FIXME: this compaction should run with maintenance priority.
            auto descriptor = sstables::compaction_descriptor({ sst }, service::get_local_compaction_priority(),
                sstable_level, sstables::compaction_descriptor::default_max_sstable_bytes, run_identifier, _options);

            // Releases reference to cleaned sstable such that respective used disk space can be freed.
            auto release_exhausted = [this] (const std::vector<sstables::shared_sstable>& exhausted_sstables) {
                _compacting.release_compacting(exhausted_sstables);
            };

            setup_new_compaction(descriptor.run_identifier);

            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_cm._compaction_controller.backlog_of_shares(200), _cm._available_memory));
            _cm.register_backlog_tracker(user_initiated);

            std::exception_ptr ex;
            try {
                co_await compact_sstables_and_update_history(std::move(descriptor), _compaction_data, std::move(release_exhausted), _can_purge);
                finish_compaction();
                _cm.reevaluate_postponed_compactions();
                co_return;  // done with current sstable
            } catch (...) {
                ex = std::current_exception();
            }

            finish_compaction(state::failed);
            // retry current sstable or rethrows exception
            if ((co_await maybe_retry(std::move(ex))) == stop_iteration::yes) {
                co_return;
            }
        }
    }
};

template<typename TaskType, typename... Args>
requires std::derived_from<TaskType, compaction_manager::task>
future<> compaction_manager::perform_task_on_all_files(replica::table* t, sstables::compaction_type_options options, get_candidates_func get_func, Args... args) {
    if (_state != state::enabled) {
        co_return;
    }

    // since we might potentially have ongoing compactions, and we
    // must ensure that all sstables created before we run are included
    // in the re-write, we need to barrier out any previously running
    // compaction.
    std::vector<sstables::shared_sstable> sstables;
    compacting_sstable_registration compacting(*this);
    co_await run_with_compaction_disabled(t, [this, &sstables, &compacting, get_func = std::move(get_func)] () -> future<> {
        // Getting sstables and registering them as compacting must be atomic, to avoid a race condition where
        // regular compaction runs in between and picks the same files.
        sstables = co_await get_func();
        compacting.register_compacting(sstables);

        // sort sstables by size in descending order, such that the smallest files will be rewritten first
        // (as sstable to be rewritten is popped off from the back of container), so rewrite will have higher
        // chance to succeed when the biggest files are reached.
        std::sort(sstables.begin(), sstables.end(), [](sstables::shared_sstable& a, sstables::shared_sstable& b) {
            return a->data_size() > b->data_size();
        });
    });
    co_await perform_task(seastar::make_shared<TaskType>(*this, t, std::move(options), std::move(sstables), std::move(compacting), std::forward<Args>(args)...));
}

future<> compaction_manager::rewrite_sstables(replica::table* t, sstables::compaction_type_options options, get_candidates_func get_func, can_purge_tombstones can_purge) {
    return perform_task_on_all_files<rewrite_sstables_compaction_task>(t, std::move(options), std::move(get_func), can_purge);
}

class compaction_manager::validate_sstables_compaction_task : public compaction_manager::sstables_task {
public:
    validate_sstables_compaction_task(compaction_manager& mgr, replica::table* t, std::vector<sstables::shared_sstable> sstables)
        : sstables_task(mgr, t, sstables::compaction_type::Scrub, "Scrub compaction in validate mode", std::move(sstables))
    {}

protected:
    virtual future<> do_run() override {
        while (!_sstables.empty() && can_proceed()) {
            auto sst = consume_sstable();
            co_await validate_sstable(std::move(sst));
        }
    }

private:
    future<> validate_sstable(const sstables::shared_sstable& sst) {
        co_await coroutine::switch_to(_cm._maintenance_sg.cpu);

        switch_state(state::active);
        std::exception_ptr ex;
        try {
            auto desc = sstables::compaction_descriptor(
                    { sst },
                    _cm._maintenance_sg.io,
                    sst->get_sstable_level(),
                    sstables::compaction_descriptor::default_max_sstable_bytes,
                    sst->run_identifier(),
                    sstables::compaction_type_options::make_scrub(sstables::compaction_type_options::scrub::mode::validate, {}));
            co_await sstables::compact_sstables(std::move(desc), _compaction_data, _compacting_table->as_table_state());
        } catch (sstables::compaction_stopped_exception&) {
            // ignore, will be handled by can_proceed()
        } catch (storage_io_error& e) {
            cmlog.error("{}: failed due to storage io error: {}: stopping", *this, e.what());
            _cm._stats.errors++;
            _cm.do_stop();
            throw;
        } catch (...) {
            // We are validating potentially corrupt sstables, errors are
            // expected, just continue with the other sstables when seeing
            // one.
            _cm._stats.errors++;
            cmlog.error("Scrubbing in validate mode {} failed due to {}, continuing.", sst->get_filename(), std::current_exception());
        }
    }
};

future<> compaction_manager::perform_sstable_scrub_validate_mode(replica::table* t) {
    if (_state != state::enabled) {
        return make_ready_future<>();
    }
    // All sstables must be included, even the ones being compacted, such that everything in table is validated.
    auto all_sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*t->get_sstables());
    return perform_task(seastar::make_shared<validate_sstables_compaction_task>(*this, t, std::move(all_sstables)));
}

class compaction_manager::cleanup_sstables_compaction_task : public compaction_manager::task {
    const sstables::compaction_type_options _cleanup_options;
    compacting_sstable_registration _compacting;
    std::vector<sstables::compaction_descriptor> _pending_cleanup_jobs;
public:
    cleanup_sstables_compaction_task(compaction_manager& mgr, replica::table* t, sstables::compaction_type_options options,
                                     std::vector<sstables::shared_sstable> candidates, compacting_sstable_registration compacting)
            : task(mgr, t, options.type(), sstring(sstables::to_string(options.type())))
            , _cleanup_options(std::move(options))
            , _compacting(std::move(compacting))
            , _pending_cleanup_jobs(t->get_compaction_strategy().get_cleanup_compaction_jobs(t->as_table_state(), std::move(candidates)))
    {
        // Cleanup is made more resilient under disk space pressure, by cleaning up smaller jobs first, so larger jobs
        // will have more space available released by previous jobs.
        std::ranges::sort(_pending_cleanup_jobs, std::ranges::greater(), std::mem_fn(&sstables::compaction_descriptor::sstables_size));
        _cm._stats.pending_tasks += _pending_cleanup_jobs.size();
    }

    virtual ~cleanup_sstables_compaction_task() {
        _cm._stats.pending_tasks -= _pending_cleanup_jobs.size();
    }
protected:
    virtual future<> do_run() override {
        switch_state(state::pending);
        auto maintenance_permit = co_await acquire_semaphore(_cm._maintenance_ops_sem);

        while (!_pending_cleanup_jobs.empty() && can_proceed()) {
            auto active_job = std::move(_pending_cleanup_jobs.back());
            active_job.options = _cleanup_options;
            co_await run_cleanup_job(std::move(active_job));
            _pending_cleanup_jobs.pop_back();
            _cm._stats.pending_tasks--;
        }
    }
private:
    // Releases reference to cleaned files such that respective used disk space can be freed.
    void release_exhausted(std::vector<sstables::shared_sstable> exhausted_sstables) {
        _compacting.release_compacting(exhausted_sstables);
    }

    future<> run_cleanup_job(sstables::compaction_descriptor descriptor) {
        co_await coroutine::switch_to(_cm._compaction_sg.cpu);

        for (;;) {
            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_cm._compaction_controller.backlog_of_shares(200), _cm._available_memory));
            _cm.register_backlog_tracker(user_initiated);

            std::exception_ptr ex;
            try {
                setup_new_compaction(descriptor.run_identifier);
                co_await compact_sstables_and_update_history(descriptor, _compaction_data,
                                          std::bind(&cleanup_sstables_compaction_task::release_exhausted, this, std::placeholders::_1));
                finish_compaction();
                _cm.reevaluate_postponed_compactions();
                co_return;  // done with current job
            } catch (...) {
                ex = std::current_exception();
            }

            finish_compaction(state::failed);
            // retry current job or rethrows exception
            if ((co_await maybe_retry(std::move(ex))) == stop_iteration::yes) {
                co_return;
            }
        }
    }
};

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
            return task->compacting_table() == t && task->type() == sstables::compaction_type::Cleanup;
        });
    };
    if (check_for_cleanup()) {
        throw std::runtime_error(format("cleanup request failed: there is an ongoing cleanup on {}.{}",
            t->schema()->ks_name(), t->schema()->cf_name()));
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

    co_await perform_task_on_all_files<cleanup_sstables_compaction_task>(t, sstables::compaction_type_options::make_cleanup(std::move(sorted_owned_ranges)),
                                                                         std::move(get_sstables));
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
future<> compaction_manager::perform_sstable_scrub(replica::table* t, sstables::compaction_type_options::scrub::mode scrub_mode,
        sstables::compaction_type_options::scrub::quarantine_mode quarantine_mode) {
    if (scrub_mode == sstables::compaction_type_options::scrub::mode::validate) {
        return perform_sstable_scrub_validate_mode(t);
    }
    auto mt_factory = [dmm = &_dmm] (schema_ptr s) {
        return make_lw_shared<replica::memtable>(std::move(s), *dmm);
    };
    return rewrite_sstables(t, sstables::compaction_type_options::make_scrub(scrub_mode, mt_factory), [this, t, quarantine_mode] {
        auto all_sstables = t->get_sstable_set().all();
        std::vector<sstables::shared_sstable> sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(*all_sstables
                | boost::adaptors::filtered([&quarantine_mode] (const sstables::shared_sstable& sst) {
            if (sst->requires_view_building()) {
                return false;
            }
            switch (quarantine_mode) {
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
        if (task->compacting_table() == t) {
            if (!msg.empty()) {
                msg += "\n";
            }
            msg += format("Found {} after remove", *task.get());
            found = true;
        }
    }
    if (found) {
        on_internal_error_noexcept(cmlog, msg);
    }
#endif
}

const std::vector<sstables::compaction_info> compaction_manager::get_compactions(replica::table* t) const {
    auto to_info = [] (const shared_ptr<task>& task) {
        sstables::compaction_info ret;
        ret.compaction_uuid = task->compaction_data().compaction_uuid;
        ret.type = task->type();
        ret.ks_name = task->compacting_table()->schema()->ks_name();
        ret.cf_name = task->compacting_table()->schema()->cf_name();
        ret.total_partitions = task->compaction_data().total_partitions;
        ret.total_keys_written = task->compaction_data().total_keys_written;
        return ret;
    };
    using ret = std::vector<sstables::compaction_info>;
    return boost::copy_range<ret>(_tasks | boost::adaptors::filtered([t] (const shared_ptr<task>& task) {
                return (!t || task->compacting_table() == t) && task->compaction_running();
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
        if (task->compacting_table() == t && task->compaction_running()) {
            task->compaction_data().pending_replacements.push_back({ removed, added });
        }
    }
}

class compaction_manager::strategy_control : public compaction::strategy_control {
    compaction_manager& _cm;
public:
    explicit strategy_control(compaction_manager& cm) noexcept : _cm(cm) {}

    bool has_ongoing_compaction(table_state& table_s) const noexcept override {
        return std::any_of(_cm._tasks.begin(), _cm._tasks.end(), [&s = table_s.schema()] (const shared_ptr<task>& task) {
            return task->compaction_running()
                && task->compacting_table()->schema()->ks_name() == s->ks_name()
                && task->compacting_table()->schema()->cf_name() == s->cf_name();
        });
    }
};

compaction::strategy_control& compaction_manager::get_strategy_control() const noexcept {
    return *_strategy_control;
}

double compaction_backlog_tracker::backlog() const {
    return disabled() ? compaction_controller::disable_backlog : _impl->backlog(_ongoing_writes, _ongoing_compactions);
}

void compaction_backlog_tracker::replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) {
    if (disabled()) {
        return;
    }
    auto filter_and_revert_charges = [this] (const std::vector<sstables::shared_sstable>& ssts) {
        std::vector<sstables::shared_sstable> ret;
        for (auto& sst : ssts) {
            if (sstable_belongs_to_tracker(sst)) {
                revert_charges(sst);
                ret.push_back(sst);
            }
        }
        return ret;
    };

    try {
        _impl->replace_sstables(filter_and_revert_charges(old_ssts), filter_and_revert_charges(new_ssts));
    } catch (...) {
        cmlog.error("Disabling backlog tracker due to exception {}", std::current_exception());
        // FIXME: tracker should be able to recover from a failure, e.g. OOM, by having its state reset. More details on https://github.com/scylladb/scylla/issues/10297.
        disable();
    }
}

bool compaction_backlog_tracker::sstable_belongs_to_tracker(const sstables::shared_sstable& sst) {
    return sstables::is_eligible_for_compaction(sst);
}

void compaction_backlog_tracker::register_partially_written_sstable(sstables::shared_sstable sst, backlog_write_progress_manager& wp) {
    if (disabled()) {
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
    if (disabled()) {
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
