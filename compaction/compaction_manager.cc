/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "compaction_manager.hh"
#include "compaction_descriptor.hh"
#include "compaction_strategy.hh"
#include "compaction_backlog_manager.hh"
#include "compaction_weight_registration.hh"
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include <memory>
#include <fmt/ranges.h>
#include <seastar/core/metrics.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "sstables/exceptions.hh"
#include "sstables/sstable_directory.hh"
#include "utils/assert.hh"
#include "utils/error_injection.hh"
#include "utils/UUID_gen.hh"
#include "db/system_keyspace.hh"
#include <cmath>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/remove_if.hpp>

static logging::logger cmlog("compaction_manager");
using namespace std::chrono_literals;
using namespace compaction;

class compacting_sstable_registration {
    compaction_manager& _cm;
    compaction::compaction_state& _cs;
    std::unordered_set<sstables::shared_sstable> _compacting;
public:
    explicit compacting_sstable_registration(compaction_manager& cm, compaction::compaction_state& cs) noexcept
        : _cm(cm)
        , _cs(cs)
    { }

    compacting_sstable_registration(compaction_manager& cm, compaction::compaction_state& cs, const std::vector<sstables::shared_sstable>& compacting)
        : compacting_sstable_registration(cm, cs)
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
        , _cs(other._cs)
        , _compacting(std::move(other._compacting))
    { }

    ~compacting_sstable_registration() {
        // _compacting might be empty, but this should be just fine
        // for deregister_compacting_sstables.
        _cm.deregister_compacting_sstables(_compacting);
    }

    void register_compacting(const std::vector<sstables::shared_sstable>& sstables) {
        _compacting.reserve(_compacting.size() + sstables.size());
        _compacting.insert(sstables.begin(), sstables.end());
        _cm.register_compacting_sstables(sstables);
    }

    // Explicitly release compacting sstables
    void release_compacting(const std::vector<sstables::shared_sstable>& sstables) {
        _cm.deregister_compacting_sstables(sstables);
        for (const auto& sst : sstables) {
            _compacting.erase(sst);
            _cs.sstables_requiring_cleanup.erase(sst);
        }
        if (_cs.sstables_requiring_cleanup.empty()) {
            _cs.owned_ranges_ptr = nullptr;
        }
    }

    void release_all() noexcept {
        _cm.deregister_compacting_sstables(_compacting);
        _compacting = {};
    }

    class update_me : public compaction_task_executor::on_replacement {
        compacting_sstable_registration& _registration;
        public:
            update_me(compacting_sstable_registration& registration)
                : _registration{registration} {}
            void on_removal(const std::vector<sstables::shared_sstable>& sstables) override {
                _registration.release_compacting(sstables);
            }
            void on_addition(const std::vector<sstables::shared_sstable>& sstables) override {
                _registration.register_compacting(sstables);
            }
    };

    auto update_on_sstable_replacement() {
        return update_me(*this);
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

bool compaction_manager::can_register_compaction(table_state& t, int weight, unsigned fan_in) const {
    // Only one weight is allowed if parallel compaction is disabled.
    if (!t.get_compaction_strategy().parallel_compaction() && has_table_ongoing_compaction(t)) {
        return false;
    }
    // Weightless compaction doesn't have to be serialized, and won't dillute overall efficiency.
    if (!weight) {
        return true;
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

std::vector<sstables::shared_sstable> in_strategy_sstables(table_state& table_s) {
    auto sstables = table_s.main_sstable_set().all();
    return boost::copy_range<std::vector<sstables::shared_sstable>>(*sstables | boost::adaptors::filtered([] (const sstables::shared_sstable& sst) {
        return sstables::is_eligible_for_compaction(sst);
    }));
}

std::vector<sstables::shared_sstable> compaction_manager::get_candidates(table_state& t) const {
    return get_candidates(t, *t.main_sstable_set().all());
}

bool compaction_manager::eligible_for_compaction(const sstables::shared_sstable& sstable) const {
    return sstables::is_eligible_for_compaction(sstable) && !_compacting_sstables.contains(sstable);
}

bool compaction_manager::eligible_for_compaction(const sstables::frozen_sstable_run& sstable_run) const {
    return std::ranges::all_of(sstable_run->all(), [this] (const sstables::shared_sstable& sstable) {
        return eligible_for_compaction(sstable);
    });
}

template <std::ranges::range Range>
requires std::convertible_to<std::ranges::range_value_t<Range>, sstables::shared_sstable> || std::convertible_to<std::ranges::range_value_t<Range>, sstables::frozen_sstable_run>
std::vector<std::ranges::range_value_t<Range>> compaction_manager::get_candidates(table_state& t, const Range& sstables) const {
    using range_candidates_t = std::ranges::range_value_t<Range>;
    std::vector<range_candidates_t> candidates;
    candidates.reserve(sstables.size());
    // prevents sstables that belongs to a partial run being generated by ongoing compaction from being
    // selected for compaction, which could potentially result in wrong behavior.
    auto partial_run_identifiers = boost::copy_range<std::unordered_set<sstables::run_id>>(_tasks
            | boost::adaptors::filtered(std::mem_fn(&compaction_task_executor::generating_output_run))
            | boost::adaptors::transformed(std::mem_fn(&compaction_task_executor::output_run_id)));

    // Filter out sstables that are being compacted.
    for (const auto& sst : sstables) {
        if (!eligible_for_compaction(sst)) {
            continue;
        }
        if (partial_run_identifiers.contains(sst->run_identifier())) {
            continue;
        }
        candidates.push_back(sst);
    }
    return candidates;
}

template <std::ranges::range Range>
requires std::same_as<std::ranges::range_value_t<Range>, sstables::shared_sstable>
void compaction_manager::register_compacting_sstables(const Range& sstables) {
    // make all required allocations in advance to merge
    // so it should not throw
    _compacting_sstables.reserve(_compacting_sstables.size() + std::ranges::size(sstables));
    try {
        _compacting_sstables.insert(std::ranges::begin(sstables), std::ranges::end(sstables));
    } catch (...) {
        cmlog.error("Unexpected error when registering compacting SSTables: {}. Ignored...", std::current_exception());
    }
}

template <std::ranges::range Range>
requires std::same_as<std::ranges::range_value_t<Range>, sstables::shared_sstable>
void compaction_manager::deregister_compacting_sstables(const Range& sstables) {
    // Remove compacted sstables from the set of compacting sstables.
    for (auto& sstable : sstables) {
        _compacting_sstables.erase(sstable);
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
    virtual void replace_sstables(const std::vector<sstables::shared_sstable>& old_ssts, const std::vector<sstables::shared_sstable>& new_ssts) override {}
};

compaction::compaction_state& compaction_manager::get_compaction_state(table_state* t) {
    try {
        return _compaction_state.at(t);
    } catch (std::out_of_range&) {
        // Note: don't dereference t as it might not exist
        throw std::out_of_range(format("Compaction state for table [{}] not found", fmt::ptr(t)));
    }
}

compaction_task_executor::compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, sstables::compaction_type type, sstring desc)
    : _cm(mgr)
    , _compacting_table(t)
    , _compaction_state(_cm.get_compaction_state(t))
    , _do_throw_if_stopping(do_throw_if_stopping)
    , _type(type)
    , _description(std::move(desc))
{}

future<compaction_manager::compaction_stats_opt> compaction_manager::perform_task(shared_ptr<compaction_task_executor> task, throw_if_stopping do_throw_if_stopping) {
    cmlog.debug("{}: started", *task);

    try {
        auto&& res = co_await task->run_compaction();
        cmlog.debug("{}: done", *task);
        co_return res;
    } catch (sstables::compaction_stopped_exception& e) {
        cmlog.info("{}: stopped, reason: {}", *task, e.what());
        if (do_throw_if_stopping) {
            throw;
        }
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

    co_return std::nullopt;
}

future<> compaction_manager::on_compaction_completion(table_state& t, sstables::compaction_completion_desc desc, sstables::offstrategy offstrategy) {
    auto& cs = get_compaction_state(&t);
    auto new_sstables = boost::copy_range<std::unordered_set<sstables::shared_sstable>>(desc.new_sstables);
    for (const auto& sst : desc.old_sstables) {
        if (!new_sstables.contains(sst)) {
            cs.sstables_requiring_cleanup.erase(sst);
        }
    }
    if (cs.sstables_requiring_cleanup.empty()) {
        cs.owned_ranges_ptr = nullptr;
    }
    return t.on_compaction_completion(std::move(desc), offstrategy);
}

future<sstables::compaction_result> compaction_task_executor::compact_sstables_and_update_history(sstables::compaction_descriptor descriptor, sstables::compaction_data& cdata, on_replacement& on_replace, compaction_manager::can_purge_tombstones can_purge) {
    if (!descriptor.sstables.size()) {
        // if there is nothing to compact, just return.
        co_return sstables::compaction_result{};
    }

    bool should_update_history = this->should_update_history(descriptor.options.type());
    sstables::compaction_result res = co_await compact_sstables(std::move(descriptor), cdata, on_replace, std::move(can_purge));

    if (should_update_history) {
        co_await update_history(*_compacting_table, res, cdata);
    }

    co_return res;
}
future<sstables::compaction_result> compaction_task_executor::compact_sstables(sstables::compaction_descriptor descriptor, sstables::compaction_data& cdata, on_replacement& on_replace, compaction_manager::can_purge_tombstones can_purge,
                                                                               sstables::offstrategy offstrategy) {
    table_state& t = *_compacting_table;
    if (can_purge) {
        descriptor.enable_garbage_collection(t.main_sstable_set());
    }
    descriptor.creator = [&t] (shard_id dummy) {
        auto sst = t.make_sstable();
        return sst;
    };
    descriptor.replacer = [this, &t, &on_replace, offstrategy] (sstables::compaction_completion_desc desc) {
        t.get_compaction_strategy().notify_completion(t, desc.old_sstables, desc.new_sstables);
        _cm.propagate_replacement(t, desc.old_sstables, desc.new_sstables);
        // on_replace updates the compacting registration with the old and new
        // sstables. while on_compaction_completion() removes the old sstables
        // from the table's sstable set, and adds the new ones to the sstable
        // set.
        // since the regular compactions exclude the sstables in the sstable
        // set which are currently being compacted, if we want to ensure the
        // exclusive access of compactions to an sstable we should guard it
        // with the registration when adding/removing it to/from the sstable
        // set. otherwise, the regular compaction would pick it up in the time
        // window, where the sstables:
        // - are still in the main set
        // - are not being compacted.
        on_replace.on_addition(desc.new_sstables);
        auto old_sstables = desc.old_sstables;
        _cm.on_compaction_completion(t, std::move(desc), offstrategy).get();
        on_replace.on_removal(old_sstables);
    };

    // retrieve owned_ranges if_required
    if (!descriptor.owned_ranges) {
        std::vector<sstables::shared_sstable> sstables_requiring_cleanup;
        const auto& cs = _cm.get_compaction_state(_compacting_table);
        for (const auto& sst : descriptor.sstables) {
            if (cs.sstables_requiring_cleanup.contains(sst)) {
                sstables_requiring_cleanup.emplace_back(sst);
            }
        }
        if (!sstables_requiring_cleanup.empty()) {
            cmlog.info("The following SSTables require cleanup in this compaction: {}", sstables_requiring_cleanup);
            if (!cs.owned_ranges_ptr) {
                on_internal_error_noexcept(cmlog, "SSTables require cleanup but compaction state has null owned ranges");
            }
            descriptor.owned_ranges = cs.owned_ranges_ptr;
        }
    }

    co_return co_await sstables::compact_sstables(std::move(descriptor), cdata, t, _progress_monitor);
}
future<> compaction_task_executor::update_history(table_state& t, const sstables::compaction_result& res, const sstables::compaction_data& cdata) {
    auto ended_at = std::chrono::duration_cast<std::chrono::milliseconds>(res.stats.ended_at.time_since_epoch());

    if (_cm._sys_ks) {
        // FIXME: add support to merged_rows. merged_rows is a histogram that
        // shows how many sstables each row is merged from. This information
        // cannot be accessed until we make combined_reader more generic,
        // for example, by adding a reducer method.
        auto sys_ks = _cm._sys_ks; // hold pointer on sys_ks
        co_await sys_ks->update_compaction_history(cdata.compaction_uuid, t.schema()->ks_name(), t.schema()->cf_name(),
                ended_at.count(), res.stats.start_size, res.stats.end_size, std::unordered_map<int32_t, int64_t>{});
    }
}

future<> compaction_manager::get_compaction_history(compaction_history_consumer&& f) {
    if (!_sys_ks) {
        return make_ready_future<>();
    }

    return _sys_ks->get_compaction_history(std::move(f)).finally([s = _sys_ks] {});
}

template<std::derived_from<compaction::compaction_task_executor> Executor>
struct fmt::formatter<Executor> : fmt::formatter<compaction::compaction_task_executor> {};

namespace compaction {

class sstables_task_executor : public compaction_task_executor, public sstables_compaction_task_impl {
protected:
    std::vector<sstables::shared_sstable> _sstables;

    void set_sstables(std::vector<sstables::shared_sstable> new_sstables);
    sstables::shared_sstable consume_sstable();

public:
    explicit sstables_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, sstables::compaction_type compaction_type, sstring desc, std::vector<sstables::shared_sstable> sstables, tasks::task_id parent_id, sstring entity = "")
        : compaction_task_executor(mgr, do_throw_if_stopping, t, compaction_type, std::move(desc))
        , sstables_compaction_task_impl(mgr._task_manager_module, tasks::task_id::create_random_id(), 0, "compaction group", t->schema()->ks_name(), t->schema()->cf_name(), std::move(entity), parent_id)
    {
        _status.progress_units = "bytes";
        set_sstables(std::move(sstables));
    }

    virtual ~sstables_task_executor() = default;

    virtual void release_resources() noexcept override;

    virtual future<tasks::task_manager::task::progress> get_progress() const override {
        return compaction_task_impl::get_progress(_compaction_data, _progress_monitor);
    }

    virtual void abort() noexcept override {
        return compaction_task_executor::abort(_as);
    }
protected:
    virtual future<> run() override {
        return perform();
    }
};

class major_compaction_task_executor : public compaction_task_executor, public major_compaction_task_impl {
public:
    major_compaction_task_executor(compaction_manager& mgr,
            throw_if_stopping do_throw_if_stopping,
            table_state* t,
            tasks::task_id parent_id)
        : compaction_task_executor(mgr, do_throw_if_stopping, t, sstables::compaction_type::Compaction, "Major compaction")
        , major_compaction_task_impl(mgr._task_manager_module, tasks::task_id::create_random_id(), 0, "compaction group", t->schema()->ks_name(), t->schema()->cf_name(), "", parent_id)
    {
        _status.progress_units = "bytes";
    }

    virtual future<tasks::task_manager::task::progress> get_progress() const override {
        return compaction_task_impl::get_progress(_compaction_data, _progress_monitor);
    }

    virtual void abort() noexcept override {
        return compaction_task_executor::abort(_as);
    }
protected:
    virtual future<> run() override {
        return perform();
    }

    // first take major compaction semaphore, then exclusely take compaction lock for table.
    // it cannot be the other way around, or minor compaction for this table would be
    // prevented while an ongoing major compaction doesn't release the semaphore.
    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        co_await coroutine::switch_to(_cm.maintenance_sg());

        switch_state(state::pending);
        auto units = co_await acquire_semaphore(_cm._maintenance_ops_sem);
        auto lock_holder = co_await _compaction_state.lock.hold_write_lock();
        if (!can_proceed()) {
            co_return std::nullopt;
        }

        // candidates are sstables that aren't being operated on by other compaction types.
        // those are eligible for major compaction.
        table_state* t = _compacting_table;
        sstables::compaction_strategy cs = t->get_compaction_strategy();
        sstables::compaction_descriptor descriptor = cs.get_major_compaction_job(*t, _cm.get_candidates(*t));
        auto compacting = compacting_sstable_registration(_cm, _cm.get_compaction_state(t), descriptor.sstables);
        auto on_replace = compacting.update_on_sstable_replacement();
        setup_new_compaction(descriptor.run_identifier);

        cmlog.info0("User initiated compaction started on behalf of {}", *t);

        // Now that the sstables for major compaction are registered
        // and the user_initiated_backlog_tracker is set up
        // the exclusive lock can be freed to let regular compaction run in parallel to major
        lock_holder.return_all();

        co_await utils::get_local_injector().inject("major_compaction_wait", [this] (auto& handler) -> future<> {
            cmlog.info("major_compaction_wait: waiting");
            while (!handler.poll_for_message() && !_compaction_data.is_stop_requested()) {
                co_await sleep(std::chrono::milliseconds(5));
            }
            cmlog.info("major_compaction_wait: released");
        });

        co_await compact_sstables_and_update_history(std::move(descriptor), _compaction_data, on_replace);

        finish_compaction();

        co_return std::nullopt;
    }
};

}

template<typename TaskExecutor, typename... Args>
requires std::is_base_of_v<compaction_task_executor, TaskExecutor> &&
        std::is_base_of_v<compaction_task_impl, TaskExecutor> &&
requires (compaction_manager& cm, throw_if_stopping do_throw_if_stopping, Args&&... args) {
    {TaskExecutor(cm, do_throw_if_stopping, std::forward<Args>(args)...)} -> std::same_as<TaskExecutor>;
}
future<compaction_manager::compaction_stats_opt> compaction_manager::perform_compaction(throw_if_stopping do_throw_if_stopping, tasks::task_info parent_info, Args&&... args) {
    auto task_executor = seastar::make_shared<TaskExecutor>(*this, do_throw_if_stopping, std::forward<Args>(args)...);
    _tasks.push_back(task_executor);
    auto unregister_task = defer([this, task_executor] {
        _tasks.remove(task_executor);
        task_executor->switch_state(compaction_task_executor::state::none);
    });

    auto task = co_await get_task_manager_module().make_task(task_executor, parent_info);
    task->start();
    co_await task->done();
    co_return task_executor->get_stats();
}

std::optional<gate::holder> compaction_manager::start_compaction(table_state& t) {
    if (_state != state::enabled) {
        return std::nullopt;
    }

    auto it = _compaction_state.find(&t);
    if (it == _compaction_state.end() || it->second.gate.is_closed()) {
        return std::nullopt;
    }

    return it->second.gate.hold();
}

future<> compaction_manager::perform_major_compaction(table_state& t, tasks::task_info info) {
    auto gh = start_compaction(t);
    if (!gh) {
        co_return;
    }

    co_await perform_compaction<major_compaction_task_executor>(throw_if_stopping::no, info, &t, info.id).discard_result();
}

namespace compaction {

class custom_compaction_task_executor : public compaction_task_executor, public compaction_task_impl {
    noncopyable_function<future<>(sstables::compaction_data&, sstables::compaction_progress_monitor&)> _job;

public:
    custom_compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, tasks::task_id parent_id, sstables::compaction_type type, sstring desc, noncopyable_function<future<>(sstables::compaction_data&, sstables::compaction_progress_monitor&)> job)
        : compaction_task_executor(mgr, do_throw_if_stopping, t, type, std::move(desc))
        , compaction_task_impl(mgr._task_manager_module, tasks::task_id::create_random_id(), 0, "compaction group", t->schema()->ks_name(), t->schema()->cf_name(), "", parent_id)
        , _job(std::move(job))
    {
        _status.progress_units = "bytes";
    }

    virtual std::string type() const override {
        return fmt::format("{} compaction", compaction_type());
    }

    virtual future<tasks::task_manager::task::progress> get_progress() const override {
        return compaction_task_impl::get_progress(_compaction_data, _progress_monitor);
    }

    virtual void abort() noexcept override {
        return compaction_task_executor::abort(_as);
    }
protected:
    virtual future<> run() override {
        return perform();
    }

    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        if (!can_proceed(throw_if_stopping::yes)) {
            co_return std::nullopt;
        }
        switch_state(state::pending);
        auto units = co_await acquire_semaphore(_cm._maintenance_ops_sem);

        if (!can_proceed(throw_if_stopping::yes)) {
            co_return std::nullopt;
        }
        setup_new_compaction();

        // NOTE:
        // no need to register shared sstables because they're excluded from non-resharding
        // compaction and some of them may not even belong to current shard.
        co_await _job(compaction_data(), _progress_monitor);
        finish_compaction();

        co_return std::nullopt;
    }
};

}

future<> compaction_manager::run_custom_job(table_state& t, sstables::compaction_type type, const char* desc, noncopyable_function<future<>(sstables::compaction_data&, sstables::compaction_progress_monitor&)> job, tasks::task_info info, throw_if_stopping do_throw_if_stopping) {
    auto gh = start_compaction(t);
    if (!gh) {
        co_return;
    }

    co_return co_await perform_compaction<custom_compaction_task_executor>(do_throw_if_stopping, info, &t, info.id, type, desc, std::move(job)).discard_result();
}

future<> compaction_manager::update_static_shares(float static_shares) {
    cmlog.info("Updating static shares to {}", static_shares);
    return _compaction_controller.update_static_shares(static_shares);
}

compaction_manager::compaction_reenabler::compaction_reenabler(compaction_manager& cm, table_state& t)
    : _cm(cm)
    , _table(&t)
    , _compaction_state(cm.get_compaction_state(_table))
    , _holder(_compaction_state.gate.hold())
{
    _compaction_state.compaction_disabled_counter++;
    cmlog.debug("Temporarily disabled compaction for {}. compaction_disabled_counter={}",
            t, _compaction_state.compaction_disabled_counter);
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
        cmlog.debug("Reenabling compaction for {}", *_table);
        try {
            _cm.submit(*_table);
        } catch (...) {
            cmlog.warn("compaction_reenabler could not reenable compaction for {}: {}",
                    *_table, std::current_exception());
        }
    }
}

future<compaction_manager::compaction_reenabler>
compaction_manager::stop_and_disable_compaction(table_state& t) {
    compaction_reenabler cre(*this, t);
    co_await stop_ongoing_compactions("user-triggered operation", &t);
    co_return cre;
}

future<>
compaction_manager::run_with_compaction_disabled(table_state& t, std::function<future<> ()> func) {
    compaction_reenabler cre = co_await stop_and_disable_compaction(t);

    co_await func();
}

auto fmt::formatter<compaction::compaction_task_executor::state>::format(compaction::compaction_task_executor::state s,
                                                                         fmt::format_context& ctx) const -> decltype(ctx.out()) {
    std::string_view name;
    switch (s) {
    using enum compaction::compaction_task_executor::state;
    case none:
        name = "none";
        break;
    case pending:
        name = "pending";
        break;
    case active:
        name = "active";
        break;
    case done:
        name = "done";
        break;
    case postponed:
        name = "postponed";
        break;
    case failed:
        name = "failed";
        break;
    }
    return fmt::format_to(ctx.out(), "{}", name);
}

auto fmt::formatter<compaction::compaction_task_executor>::format(const compaction::compaction_task_executor& ex,
                                                                  fmt::format_context& ctx) const -> decltype(ctx.out()) {
    auto* t = ex._compacting_table;
    return fmt::format_to(ctx.out(), "{} task {} for table {} [{}]",
                          ex._description, fmt::ptr(&ex), *t, fmt::ptr(t));
}

inline compaction_controller make_compaction_controller(const compaction_manager::scheduling_group& csg, uint64_t static_shares, std::function<double()> fn) {
    return compaction_controller(csg, static_shares, 250ms, std::move(fn));
}

compaction::compaction_state::~compaction_state() {
    compaction_done.broken();
}

void sstables_task_executor::release_resources() noexcept {
    _cm._stats.pending_tasks -= _sstables.size() - (_state == state::pending);
    _sstables = {};
}

future<compaction_manager::compaction_stats_opt> compaction_task_executor::run_compaction() noexcept {
    try {
        _compaction_done = stopping() ? make_exception_future<compaction_manager::compaction_stats_opt>(make_compaction_stopped_exception())
            : do_run();
        return compaction_done();
    } catch (...) {
        return current_exception_as_future<compaction_manager::compaction_stats_opt>();
    }
}

compaction_task_executor::state compaction_task_executor::switch_state(state new_state) {
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

void sstables_task_executor::set_sstables(std::vector<sstables::shared_sstable> new_sstables) {
    if (!_sstables.empty()) {
        on_internal_error(cmlog, format("sstables were already set"));
    }
    _sstables = std::move(new_sstables);
    cmlog.debug("{}: set_sstables: {} sstable{}", *this, _sstables.size(), _sstables.size() > 1 ? "s" : "");
    _cm._stats.pending_tasks += _sstables.size() - (_state == state::pending);
}

sstables::shared_sstable sstables_task_executor::consume_sstable() {
    if (_sstables.empty()) {
        on_internal_error(cmlog, format("no more sstables"));
    }
    auto sst = _sstables.back();
    _sstables.pop_back();
    --_cm._stats.pending_tasks; // from this point on, switch_state(pending|active) works the same way as any other task
    cmlog.debug("{}", format("consumed {}", sst->get_filename()));
    return sst;
}

future<semaphore_units<named_semaphore_exception_factory>> compaction_task_executor::acquire_semaphore(named_semaphore& sem, size_t units) {
    return seastar::get_units(sem, units, _compaction_data.abort).handle_exception_type([this] (const abort_requested_exception& e) {
        auto s = _compacting_table->schema();
        return make_exception_future<semaphore_units<named_semaphore_exception_factory>>(
                sstables::compaction_stopped_exception(s->ks_name(), s->cf_name(), e.what()));
    });
}

void compaction_task_executor::setup_new_compaction(sstables::run_id output_run_id) {
    _compaction_data = _cm.create_compaction_data();
    _output_run_identifier = output_run_id;
    switch_state(state::active);
}

void compaction_task_executor::finish_compaction(state finish_state) noexcept {
    switch_state(finish_state);
    _output_run_identifier = sstables::run_id::create_null_id();
    if (finish_state != state::failed) {
        _compaction_retry.reset();
    }
    _compaction_state.compaction_done.signal();
}

void compaction_task_executor::abort(abort_source& as) noexcept {
    if (!as.abort_requested()) {
        as.request_abort();
        stop_compaction("user requested abort");
    }
}

void compaction_task_executor::stop_compaction(sstring reason) noexcept {
    _compaction_data.stop(std::move(reason));
}

sstables::compaction_stopped_exception compaction_task_executor::make_compaction_stopped_exception() const {
    auto s = _compacting_table->schema();
    return sstables::compaction_stopped_exception(s->ks_name(), s->cf_name(), _compaction_data.stop_requested);
}

class compaction_manager::strategy_control : public compaction::strategy_control {
    compaction_manager& _cm;
public:
    explicit strategy_control(compaction_manager& cm) noexcept : _cm(cm) {}

    bool has_ongoing_compaction(table_state& table_s) const noexcept override {
        return std::any_of(_cm._tasks.begin(), _cm._tasks.end(), [&s = table_s.schema()] (const shared_ptr<compaction_task_executor>& task) {
            return task->compaction_running()
                && task->compacting_table()->schema()->ks_name() == s->ks_name()
                && task->compacting_table()->schema()->cf_name() == s->cf_name();
        });
    }

    std::vector<sstables::shared_sstable> candidates(table_state& t) const override {
        return _cm.get_candidates(t, *t.main_sstable_set().all());
    }

    std::vector<sstables::frozen_sstable_run> candidates_as_runs(table_state& t) const override {
        return _cm.get_candidates(t, t.main_sstable_set().all_sstable_runs());
    }
};

compaction_manager::compaction_manager(config cfg, abort_source& as, tasks::task_manager& tm)
    : _task_manager_module(make_shared<task_manager_module>(tm))
    , _cfg(std::move(cfg))
    , _compaction_submission_timer(compaction_sg(), compaction_submission_callback())
    , _compaction_controller(make_compaction_controller(compaction_sg(), static_shares(), [this] () -> float {
        _last_backlog = backlog();
        auto b = _last_backlog / available_memory();
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
    , _early_abort_subscription(as.subscribe([this] () noexcept {
        do_stop();
    }))
    , _throughput_updater(serialized_action([this] { return update_throughput(throughput_mbs()); }))
    , _update_compaction_static_shares_action([this] { return update_static_shares(static_shares()); })
    , _compaction_static_shares_observer(_cfg.static_shares.observe(_update_compaction_static_shares_action.make_observer()))
    , _strategy_control(std::make_unique<strategy_control>(*this))
    , _tombstone_gc_state(&_repair_history_maps)
{
    tm.register_module(_task_manager_module->get_name(), _task_manager_module);
    register_metrics();
    // Bandwidth throttling is node-wide, updater is needed on single shard
    if (this_shard_id() == 0) {
        _throughput_option_observer.emplace(_cfg.throughput_mb_per_sec.observe(_throughput_updater.make_observer()));
        // Start throttling (if configured) right at once. Any boot-time compaction
        // jobs (reshape/reshard) run in unlimited streaming group
        (void)_throughput_updater.trigger_later();
    }
}

compaction_manager::compaction_manager(tasks::task_manager& tm)
    : _task_manager_module(make_shared<task_manager_module>(tm))
    , _cfg(config{ .available_memory = 1 })
    , _compaction_submission_timer(compaction_sg(), compaction_submission_callback())
    , _compaction_controller(make_compaction_controller(compaction_sg(), 1, [] () -> float { return 1.0; }))
    , _backlog_manager(_compaction_controller)
    , _throughput_updater(serialized_action([this] { return update_throughput(throughput_mbs()); }))
    , _update_compaction_static_shares_action([] { return make_ready_future<>(); })
    , _compaction_static_shares_observer(_cfg.static_shares.observe(_update_compaction_static_shares_action.make_observer()))
    , _strategy_control(std::make_unique<strategy_control>(*this))
    , _tombstone_gc_state(&_repair_history_maps)
{
    tm.register_module(_task_manager_module->get_name(), _task_manager_module);
    // No metric registration because this constructor is supposed to be used only by the testing
    // infrastructure.
}

compaction_manager::~compaction_manager() {
    // Assert that compaction manager was explicitly stopped, if started.
    // Otherwise, fiber(s) will be alive after the object is stopped.
    SCYLLA_ASSERT(_state == state::none || _state == state::stopped);
}

future<> compaction_manager::update_throughput(uint32_t value_mbs) {
    uint64_t bps = ((uint64_t)(value_mbs != 0 ? value_mbs : std::numeric_limits<uint32_t>::max())) << 20;
    return compaction_sg().update_io_bandwidth(bps).then_wrapped([value_mbs] (auto f) {
        if (f.failed()) {
            cmlog.warn("Couldn't update compaction bandwidth: {}", f.get_exception());
        } else if (value_mbs != 0) {
            cmlog.info("Set compaction bandwidth to {}MB/s", value_mbs);
        } else {
            cmlog.info("Set unlimited compaction bandwidth");
        }
    });
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
        sm::make_gauge("normalized_backlog", [this] { return _last_backlog / available_memory(); },
                       sm::description("Holds the sum of normalized compaction backlog for all tables in the system. Backlog is normalized by dividing backlog by shard's available memory.")),
        sm::make_counter("validation_errors", [this] { return _validation_errors; },
                       sm::description("Holds the number of encountered validation errors.")),
    });
}

void compaction_manager::enable() {
    SCYLLA_ASSERT(_state == state::none || _state == state::disabled);
    _state = state::enabled;
    _compaction_submission_timer.arm_periodic(periodic_compaction_submission_interval());
    _waiting_reevalution = postponed_compactions_reevaluation();
}

std::function<void()> compaction_manager::compaction_submission_callback() {
    return [this] () mutable {
        auto now = gc_clock::now();
        for (auto& [table, state] : _compaction_state) {
            if (now - state.last_regular_compaction > periodic_compaction_submission_interval()) {
                postpone_compaction_for_table(table);
            }
        }
        reevaluate_postponed_compactions();
    };
}

future<> compaction_manager::postponed_compactions_reevaluation() {
     while (true) {
        co_await _postponed_reevaluation.when();
        if (_state != state::enabled) {
            _postponed.clear();
            co_return;
        }
        // A task_state being reevaluated can re-insert itself into postponed list, which is the reason
        // for moving the list to be processed into a local.
        auto postponed = std::exchange(_postponed, {});
        try {
            for (auto it = postponed.begin(); it != postponed.end();) {
                table_state* t = *it;
                it = postponed.erase(it);
                // skip reevaluation of a table_state that became invalid post its removal
                if (!_compaction_state.contains(t)) {
                    continue;
                }
                cmlog.debug("resubmitting postponed compaction for table {} [{}]", *t, fmt::ptr(t));
                submit(*t);
                co_await coroutine::maybe_yield();
            }
        } catch (...) {
            _postponed.insert(postponed.begin(), postponed.end());
        }
    }
}

void compaction_manager::reevaluate_postponed_compactions() noexcept {
    _postponed_reevaluation.signal();
}

void compaction_manager::postpone_compaction_for_table(table_state* t) {
    _postponed.insert(t);
}

future<> compaction_manager::stop_tasks(std::vector<shared_ptr<compaction_task_executor>> tasks, sstring reason) {
    // To prevent compaction from being postponed while tasks are being stopped,
    // let's stop all tasks before the deferring point below.
    for (auto& t : tasks) {
        cmlog.debug("Stopping {}", *t);
        t->stop_compaction(reason);
    }
    co_await coroutine::parallel_for_each(tasks, [] (auto& task) -> future<> {
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

future<> compaction_manager::stop_ongoing_compactions(sstring reason, table_state* t, std::optional<sstables::compaction_type> type_opt) noexcept {
    try {
        auto ongoing_compactions = get_compactions(t).size();
        auto tasks = boost::copy_range<std::vector<shared_ptr<compaction_task_executor>>>(_tasks | boost::adaptors::filtered([t, type_opt] (auto& task) {
            return (!t || task->compacting_table() == t) && (!type_opt || task->compaction_type() == *type_opt);
        }));
        logging::log_level level = tasks.empty() ? log_level::debug : log_level::info;
        if (cmlog.is_enabled(level)) {
            std::string scope = "";
            if (t) {
                scope = fmt::format(" for table {}", *t);
            }
            if (type_opt) {
                scope += fmt::format(" {} type={}", scope.size() ? "and" : "for", *type_opt);
            }
            cmlog.log(level, "Stopping {} tasks for {} ongoing compactions{} due to {}", tasks.size(), ongoing_compactions, scope, reason);
        }
        return stop_tasks(std::move(tasks), std::move(reason));
    } catch (...) {
        return current_exception_as_future<>();
    }
}

future<> compaction_manager::drain() {
    cmlog.info("Asked to drain");
    if (*_early_abort_subscription) {
        _state = state::disabled;
        co_await stop_ongoing_compactions("drain");
    }
    cmlog.info("Drained");
}

future<> compaction_manager::stop() {
    do_stop();
    if (auto cm = std::exchange(_task_manager_module, nullptr)) {
        co_await cm->stop();
    }
    if (_state != state::none) {
        co_return co_await std::move(*_stop_future);
    }
}

future<> compaction_manager::really_do_stop() {
    cmlog.info("Asked to stop");
    // Reset the metrics registry
    _metrics.clear();
    co_await stop_ongoing_compactions("shutdown");
    co_await coroutine::parallel_for_each(_compaction_state | boost::adaptors::map_values, [] (compaction_state& cs) -> future<> {
        if (!cs.gate.is_closed()) {
            co_await cs.gate.close();
        }
    });
    reevaluate_postponed_compactions();
    co_await std::move(_waiting_reevalution);
    _weight_tracker.clear();
    _compaction_submission_timer.cancel();
    co_await _compaction_controller.shutdown();
    co_await _throughput_updater.join();
    co_await _update_compaction_static_shares_action.join();
    cmlog.info("Stopped");
}

// Should return immediately when _state == state::none.
void compaction_manager::do_stop() noexcept {
    if (_state == state::none || _stop_future) {
        return;
    }

    try {
        _state = state::stopped;
        _stop_future = really_do_stop();
    } catch (...) {
        cmlog.error("Failed to stop the manager: {}", std::current_exception());
    }
}

inline bool compaction_manager::can_proceed(table_state* t) const {
    return (_state == state::enabled) && _compaction_state.contains(t) && !_compaction_state.at(t).compaction_disabled();
}

future<> compaction_task_executor::perform() {
    _stats = co_await _cm.perform_task(shared_from_this(), _do_throw_if_stopping);
}

inline bool compaction_task_executor::can_proceed(throw_if_stopping do_throw_if_stopping) const {
    if (stopping()) {
        // Allow caller to know that task (e.g. reshape) was asked to stop while waiting for a chance to run.
        if (do_throw_if_stopping) {
            throw make_compaction_stopped_exception();
        }
        return false;
    }
    return _cm.can_proceed(_compacting_table);
}

future<stop_iteration> compaction_task_executor::maybe_retry(std::exception_ptr err, bool throw_on_abort) {
    try {
        std::rethrow_exception(err);
    } catch (sstables::compaction_stopped_exception& e) {
        cmlog.info("{}: {}: stopping", *this, e.what());
    } catch (sstables::compaction_aborted_exception& e) {
        cmlog.error("{}: {}: stopping", *this, e.what());
        _cm._stats.errors++;
        if (throw_on_abort) {
            throw;
        }
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

namespace compaction {

class regular_compaction_task_executor : public compaction_task_executor, public regular_compaction_task_impl {
public:
    regular_compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state& t)
        : compaction_task_executor(mgr, do_throw_if_stopping, &t, sstables::compaction_type::Compaction, "Compaction")
        , regular_compaction_task_impl(mgr._task_manager_module, tasks::task_id::create_random_id(), mgr._task_manager_module->new_sequence_number(), t.schema()->ks_name(), t.schema()->cf_name(), "", tasks::task_id::create_null_id())
    {}

    virtual void abort() noexcept override {
        return compaction_task_executor::abort(_as);
    }
protected:
    virtual future<> run() override {
        return perform();
    }

    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        if (!is_system_keyspace(_status.keyspace)) {
            co_await utils::get_local_injector().inject("compaction_regular_compaction_task_executor_do_run",
                [] (auto& handler) { return handler.wait_for_message(db::timeout_clock::now() + 10s); });
        }

        co_await coroutine::switch_to(_cm.compaction_sg());

        for (;;) {
            if (!can_proceed()) {
                co_return std::nullopt;
            }
            switch_state(state::pending);
            // take read lock for table, so major and regular compaction can't proceed in parallel.
            auto lock_holder = co_await _compaction_state.lock.hold_read_lock();
            if (!can_proceed()) {
                co_return std::nullopt;
            }

            table_state& t = *_compacting_table;
            sstables::compaction_strategy cs = t.get_compaction_strategy();
            sstables::compaction_descriptor descriptor = cs.get_sstables_for_compaction(t, _cm.get_strategy_control());
            int weight = calculate_weight(descriptor);

            if (descriptor.sstables.empty() || !can_proceed() || t.is_auto_compaction_disabled_by_user()) {
                cmlog.debug("{}: sstables={} can_proceed={} auto_compaction={}", *this, descriptor.sstables.size(), can_proceed(), t.is_auto_compaction_disabled_by_user());
                co_return std::nullopt;
            }
            if (!_cm.can_register_compaction(t, weight, descriptor.fan_in())) {
                cmlog.debug("Refused compaction job ({} sstable(s)) of weight {} for {}, postponing it...",
                    descriptor.sstables.size(), weight, t);
                switch_state(state::postponed);
                _cm.postpone_compaction_for_table(&t);
                co_return std::nullopt;
            }
            auto compacting = compacting_sstable_registration(_cm, _cm.get_compaction_state(&t), descriptor.sstables);
            auto weight_r = compaction_weight_registration(&_cm, weight);
            auto on_replace = compacting.update_on_sstable_replacement();
            cmlog.debug("Accepted compaction job: task={} ({} sstable(s)) of weight {} for {}",
                fmt::ptr(this), descriptor.sstables.size(), weight, t);

            setup_new_compaction(descriptor.run_identifier);
            _compaction_state.last_regular_compaction = gc_clock::now();
            std::exception_ptr ex;

            try {
                bool should_update_history = this->should_update_history(descriptor.options.type());
                sstables::compaction_result res = co_await compact_sstables(std::move(descriptor), _compaction_data, on_replace);
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
                co_return std::nullopt;
            }
        }

        co_return std::nullopt;
    }
};

}

void compaction_manager::submit(table_state& t) {
    if (t.is_auto_compaction_disabled_by_user()) {
        return;
    }

    auto gh = start_compaction(t);
    if (!gh) {
        return;
    }

    // OK to drop future.
    // waited via compaction_task_executor::compaction_done()
    (void)perform_compaction<regular_compaction_task_executor>(throw_if_stopping::no, tasks::task_info{}, t).then_wrapped([gh = std::move(gh)] (auto f) { f.ignore_ready_future(); });
}

bool compaction_manager::can_perform_regular_compaction(table_state& t) {
    return can_proceed(&t) && !t.is_auto_compaction_disabled_by_user();
}

future<> compaction_manager::maybe_wait_for_sstable_count_reduction(table_state& t) {
    auto schema = t.schema();
    if (!can_perform_regular_compaction(t)) {
        cmlog.trace("maybe_wait_for_sstable_count_reduction in {}: cannot perform regular compaction", t);
        co_return;
    }
    auto num_runs_for_compaction = [&, this] {
        auto& cs = t.get_compaction_strategy();
        auto desc = cs.get_sstables_for_compaction(t, get_strategy_control());
        return boost::copy_range<std::unordered_set<sstables::run_id>>(
            desc.sstables
            | boost::adaptors::transformed(std::mem_fn(&sstables::sstable::run_identifier))).size();
    };
    const auto threshold = size_t(std::max(schema->max_compaction_threshold(), 32));
    auto count = num_runs_for_compaction();
    if (count <= threshold) {
        cmlog.trace("No need to wait for sstable count reduction in {}: {} <= {}",
                t, count, threshold);
        co_return;
    }
    // Reduce the chances of falling into an endless wait, if compaction
    // wasn't scheduled for the table due to a problem.
    submit(t);
    using namespace std::chrono_literals;
    auto start = db_clock::now();
    auto& cstate = get_compaction_state(&t);
    try {
        co_await cstate.compaction_done.wait([this, &num_runs_for_compaction, threshold, &t] {
            return num_runs_for_compaction() <= threshold || !can_perform_regular_compaction(t);
        });
    } catch (const broken_condition_variable&) {
        co_return;
    }
    auto end = db_clock::now();
    auto elapsed_ms = (end - start) / 1ms;
    cmlog.warn("Waited {}ms for compaction of {} to catch up on {} sstable runs",
            elapsed_ms, t, count);
}

namespace compaction {

class offstrategy_compaction_task_executor : public compaction_task_executor, public offstrategy_compaction_task_impl {
    bool& _performed;
public:
    offstrategy_compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, tasks::task_id parent_id, bool& performed)
        : compaction_task_executor(mgr, do_throw_if_stopping, t, sstables::compaction_type::Reshape, "Offstrategy compaction")
        , offstrategy_compaction_task_impl(mgr._task_manager_module, tasks::task_id::create_random_id(), parent_id ? 0 : mgr._task_manager_module->new_sequence_number(), "compaction group", t->schema()->ks_name(), t->schema()->cf_name(), "", parent_id)
        , _performed(performed)
    {
        _status.progress_units = "bytes";
        _performed = false;
    }

    bool performed() const noexcept {
        return _performed;
    }

    virtual future<tasks::task_manager::task::progress> get_progress() const override {
        return compaction_task_impl::get_progress(_compaction_data, _progress_monitor);
    }

    virtual void abort() noexcept override {
        return compaction_task_executor::abort(_as);
    }
protected:
    virtual future<> run() override {
        return perform();
    }
private:
    future<> run_offstrategy_compaction(sstables::compaction_data& cdata) {
        // Incrementally reshape the SSTables in maintenance set. The output of each reshape
        // round is merged into the main set. The common case is that off-strategy input
        // is mostly disjoint, e.g. repair-based node ops, then all the input will be
        // reshaped in a single round. The incremental approach allows us to be space
        // efficient (avoiding a 100% overhead) as we will incrementally replace input
        // SSTables from maintenance set by output ones into main set.

        table_state& t = *_compacting_table;

        // Filter out sstables that require view building, to avoid a race between off-strategy
        // and view building. Refs: #11882
        auto get_reshape_candidates = [&t] () {
            return boost::copy_range<std::vector<sstables::shared_sstable>>(*t.maintenance_sstable_set().all()
                | boost::adaptors::filtered([](const sstables::shared_sstable &sst) {
                        return !sst->requires_view_building();
                }));
        };

        auto get_next_job = [&] () -> future<std::optional<sstables::compaction_descriptor>> {
            auto candidates = get_reshape_candidates();
            if (candidates.empty()) {
                co_return std::nullopt;
            }
            // all sstables added to maintenance set share the same underlying storage.
            auto& storage = candidates.front()->get_storage();
            sstables::reshape_config cfg = co_await sstables::make_reshape_config(storage, sstables::reshape_mode::strict);
            auto desc = t.get_compaction_strategy().get_reshaping_job(get_reshape_candidates(), t.schema(), cfg);
            co_return desc.sstables.size() ? std::make_optional(std::move(desc)) : std::nullopt;
        };

        std::exception_ptr err;
        while (auto desc = co_await get_next_job()) {
            auto compacting = compacting_sstable_registration(_cm, _cm.get_compaction_state(&t), desc->sstables);
            auto on_replace = compacting.update_on_sstable_replacement();

            try {
                sstables::compaction_result _ = co_await compact_sstables(std::move(*desc), _compaction_data, on_replace,
                                                                          compaction_manager::can_purge_tombstones::no,
                                                                          sstables::offstrategy::yes);
            } catch (sstables::compaction_stopped_exception&) {
                // If off-strategy compaction stopped on user request, let's not discard the partial work.
                // Therefore, both un-reshaped and reshaped data will be integrated into main set, allowing
                // regular compaction to continue from where off-strategy left off.
                err = std::current_exception();
                break;
            }
            _performed = true;
        }

        // There might be some remaining sstables in maintenance set that didn't require reshape, or the
        // user has aborted off-strategy. So we can only integrate them into the main set, such that
        // they become candidates for regular compaction. We cannot hold them forever in maintenance set,
        // as that causes read and space amplification issues.
        if (auto sstables = get_reshape_candidates(); sstables.size()) {
            auto completion_desc = sstables::compaction_completion_desc{
                .old_sstables = sstables, // removes from maintenance set.
                .new_sstables = sstables, // adds into main set.
            };
            co_await _cm.on_compaction_completion(t, std::move(completion_desc), sstables::offstrategy::yes);
        }

        if (err) {
            co_await coroutine::return_exception_ptr(std::move(err));
        }
    }
protected:
    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        co_await coroutine::switch_to(_cm.maintenance_sg());

        for (;;) {
            if (!can_proceed()) {
                co_return std::nullopt;
            }
            switch_state(state::pending);
            auto units = co_await acquire_semaphore(_cm._off_strategy_sem);
            if (!can_proceed()) {
                co_return std::nullopt;
            }
            setup_new_compaction();

            std::exception_ptr ex;
            try {
                table_state& t = *_compacting_table;
                auto size = t.maintenance_sstable_set().size();
                if (!size) {
                    cmlog.debug("Skipping off-strategy compaction for {}, No candidates were found", t);
                    finish_compaction();
                    co_return std::nullopt;
                }
                cmlog.info("Starting off-strategy compaction for {}, {} candidates were found", t, size);
                co_await run_offstrategy_compaction(_compaction_data);
                finish_compaction();
                cmlog.info("Done with off-strategy compaction for {}", t);
                co_return std::nullopt;
            } catch (...) {
                ex = std::current_exception();
            }

            finish_compaction(state::failed);
            if ((co_await maybe_retry(std::move(ex))) == stop_iteration::yes) {
                co_return std::nullopt;
            }
        }

        co_return std::nullopt;
    }
};

}

future<bool> compaction_manager::perform_offstrategy(table_state& t, tasks::task_info info) {
    auto gh = start_compaction(t);
    if (!gh) {
        co_return false;
    }

    bool performed;
    co_await perform_compaction<offstrategy_compaction_task_executor>(throw_if_stopping::no, info, &t, info.id, performed);
    co_return performed;
}

namespace compaction {

class rewrite_sstables_compaction_task_executor : public sstables_task_executor {
    sstables::compaction_type_options _options;
    owned_ranges_ptr _owned_ranges_ptr;
    compacting_sstable_registration _compacting;
    compaction_manager::can_purge_tombstones _can_purge;

public:
    rewrite_sstables_compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, tasks::task_id parent_id, sstables::compaction_type_options options, owned_ranges_ptr owned_ranges_ptr,
                                     std::vector<sstables::shared_sstable> sstables, compacting_sstable_registration compacting,
                                     compaction_manager::can_purge_tombstones can_purge, sstring type_options_desc = "")
        : sstables_task_executor(mgr, do_throw_if_stopping, t, options.type(), sstring(sstables::to_string(options.type())), std::move(sstables), parent_id, std::move(type_options_desc))
        , _options(std::move(options))
        , _owned_ranges_ptr(std::move(owned_ranges_ptr))
        , _compacting(std::move(compacting))
        , _can_purge(can_purge)
    {}

    virtual void release_resources() noexcept override {
        _compacting.release_all();
        _owned_ranges_ptr = nullptr;
        sstables_task_executor::release_resources();
    }

protected:
    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        sstables::compaction_stats stats{};

        switch_state(state::pending);
        auto maintenance_permit = co_await acquire_semaphore(_cm._maintenance_ops_sem);

        while (!_sstables.empty() && can_proceed()) {
            auto sst = consume_sstable();
            auto res = co_await rewrite_sstable(std::move(sst));
            _cm._validation_errors += res.stats.validation_errors;
            stats += res.stats;
        }

        co_return stats;
    }

    static sstables::compaction_descriptor
    make_descriptor(const sstables::shared_sstable& sst, const sstables::compaction_type_options& opt, owned_ranges_ptr owned_ranges = {}) {
        auto sstable_level = sst->get_sstable_level();
        auto run_identifier = sst->run_identifier();
        return sstables::compaction_descriptor({ sst },
            sstable_level, sstables::compaction_descriptor::default_max_sstable_bytes, run_identifier, opt, owned_ranges);
    }

    virtual sstables::compaction_descriptor make_descriptor(const sstables::shared_sstable& sst) const {
        return make_descriptor(sst, _options, _owned_ranges_ptr);
    }

    virtual future<sstables::compaction_result> rewrite_sstable(const sstables::shared_sstable sst) {
        co_await coroutine::switch_to(_cm.maintenance_sg());

        for (;;) {
            switch_state(state::active);

            auto descriptor = make_descriptor(sst);

            // Releases reference to cleaned sstable such that respective used disk space can be freed.
            auto on_replace = _compacting.update_on_sstable_replacement();

            setup_new_compaction(descriptor.run_identifier);

            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_cm._compaction_controller.backlog_of_shares(200), _cm.available_memory()));
            _cm.register_backlog_tracker(user_initiated);

            std::exception_ptr ex;
            try {
                sstables::compaction_result res = co_await compact_sstables_and_update_history(std::move(descriptor), _compaction_data, on_replace, _can_purge);
                finish_compaction();
                _cm.reevaluate_postponed_compactions();
                co_return res;  // done with current sstable
            } catch (...) {
                ex = std::current_exception();
            }

            finish_compaction(state::failed);
            // retry current sstable or rethrows exception
            if ((co_await maybe_retry(std::move(ex), true)) == stop_iteration::yes) {
                co_return sstables::compaction_result{};
            }
        }
    }
};

class split_compaction_task_executor final : public rewrite_sstables_compaction_task_executor {
    sstables::compaction_type_options::split _opt;
public:
    split_compaction_task_executor(compaction_manager& mgr,
                                       throw_if_stopping do_throw_if_stopping,
                                       table_state* t,
                                       tasks::task_id parent_id,
                                       sstables::compaction_type_options options,
                                       owned_ranges_ptr owned_ranges,
                                       std::vector<sstables::shared_sstable> sstables,
                                       compacting_sstable_registration compacting)
            : rewrite_sstables_compaction_task_executor(mgr, do_throw_if_stopping, t, parent_id, options, std::move(owned_ranges),
                std::move(sstables), std::move(compacting), compaction_manager::can_purge_tombstones::yes)
            , _opt(options.as<sstables::compaction_type_options::split>())
    {
    }

    static bool sstable_needs_split(const sstables::shared_sstable& sst, const sstables::compaction_type_options::split& opt) {
        return opt.classifier(sst->get_first_decorated_key().token()) != opt.classifier(sst->get_last_decorated_key().token());
    }

    static sstables::compaction_descriptor
    make_descriptor(const sstables::shared_sstable& sst, const sstables::compaction_type_options::split& split_opt) {
        auto opt = sstables::compaction_type_options::make_split(split_opt.classifier);
        return rewrite_sstables_compaction_task_executor::make_descriptor(sst, std::move(opt));
    }
private:
    bool sstable_needs_split(const sstables::shared_sstable& sst) const {
        return sstable_needs_split(sst, _opt);
    }

protected:
    sstables::compaction_descriptor make_descriptor(const sstables::shared_sstable& sst) const override {
        return make_descriptor(sst, _opt);
    }

    future<sstables::compaction_result> rewrite_sstable(const sstables::shared_sstable sst) override {
        if (sstable_needs_split(sst)) {
            return rewrite_sstables_compaction_task_executor::rewrite_sstable(std::move(sst));
        }
        // SSTable that doesn't require split can bypass compaction and the table will be able to place
        // it into the correct compaction group. Similar approach is done in off-strategy compaction for
        // sstables that don't require reshape and are ready to be moved across sets.
        sstables::compaction_completion_desc desc { .old_sstables = {sst}, .new_sstables = {sst} };
        return _compacting_table->on_compaction_completion(std::move(desc), sstables::offstrategy::no).then([] {
            // It's fine to return empty results (zeroed stats) as compaction was bypassed.
            return sstables::compaction_result{};
        });
    }
};

}

template<typename TaskType, typename... Args>
requires std::derived_from<TaskType, compaction_task_executor> &&
         std::derived_from<TaskType, compaction_task_impl>
future<compaction_manager::compaction_stats_opt> compaction_manager::perform_task_on_all_files(tasks::task_info info, table_state& t, sstables::compaction_type_options options, owned_ranges_ptr owned_ranges_ptr, get_candidates_func get_func, Args... args) {
    auto gh = start_compaction(t);
    if (!gh) {
        co_return std::nullopt;
    }

    // since we might potentially have ongoing compactions, and we
    // must ensure that all sstables created before we run are included
    // in the re-write, we need to barrier out any previously running
    // compaction.
    std::vector<sstables::shared_sstable> sstables;
    compacting_sstable_registration compacting(*this, get_compaction_state(&t));
    co_await run_with_compaction_disabled(t, [&sstables, &compacting, get_func = std::move(get_func)] () -> future<> {
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
    if (sstables.empty()) {
        co_return std::nullopt;
    }
    co_return co_await perform_compaction<TaskType>(throw_if_stopping::no, info, &t, info.id, std::move(options), std::move(owned_ranges_ptr), std::move(sstables), std::move(compacting), std::forward<Args>(args)...);
}

future<compaction_manager::compaction_stats_opt>
compaction_manager::rewrite_sstables(table_state& t, sstables::compaction_type_options options, owned_ranges_ptr owned_ranges_ptr,
                                     get_candidates_func get_func, tasks::task_info info, can_purge_tombstones can_purge,
                                     sstring options_desc) {
    return perform_task_on_all_files<rewrite_sstables_compaction_task_executor>(info, t, std::move(options), std::move(owned_ranges_ptr), std::move(get_func), can_purge, std::move(options_desc));
}

namespace compaction {

class validate_sstables_compaction_task_executor : public sstables_task_executor {
public:
    validate_sstables_compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, tasks::task_id parent_id, std::vector<sstables::shared_sstable> sstables)
        : sstables_task_executor(mgr, do_throw_if_stopping, t, sstables::compaction_type::Scrub, "Scrub compaction in validate mode", std::move(sstables), parent_id)
    {}

protected:
    virtual future<compaction_manager::compaction_stats_opt> do_run() override {
        sstables::compaction_stats stats{};

        while (!_sstables.empty() && can_proceed()) {
            auto sst = consume_sstable();
            auto res = co_await validate_sstable(std::move(sst));
            _cm._validation_errors += res.stats.validation_errors;
            stats += res.stats;
        }

        co_return stats;
    }

private:
    future<sstables::compaction_result> validate_sstable(const sstables::shared_sstable& sst) {
        co_await coroutine::switch_to(_cm.maintenance_sg());

        switch_state(state::active);
        std::exception_ptr ex;
        try {
            auto desc = sstables::compaction_descriptor(
                    { sst },
                    sst->get_sstable_level(),
                    sstables::compaction_descriptor::default_max_sstable_bytes,
                    sst->run_identifier(),
                    sstables::compaction_type_options::make_scrub(sstables::compaction_type_options::scrub::mode::validate));
            co_return co_await sstables::compact_sstables(std::move(desc), _compaction_data, *_compacting_table, _progress_monitor);
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

        co_return sstables::compaction_result{};
    }
};

}

static std::vector<sstables::shared_sstable> get_all_sstables(table_state& t) {
    auto s = boost::copy_range<std::vector<sstables::shared_sstable>>(*t.main_sstable_set().all());
    auto maintenance_set = t.maintenance_sstable_set().all();
    s.insert(s.end(), maintenance_set->begin(), maintenance_set->end());
    return s;
}

future<compaction_manager::compaction_stats_opt> compaction_manager::perform_sstable_scrub_validate_mode(table_state& t, tasks::task_info info) {
    auto gh = start_compaction(t);
    if (!gh) {
        co_return compaction_stats_opt{};
    }
    // All sstables must be included, even the ones being compacted, such that everything in table is validated.
    auto all_sstables = get_all_sstables(t);
    co_return co_await perform_compaction<validate_sstables_compaction_task_executor>(throw_if_stopping::no, info, &t, info.id, std::move(all_sstables));
}

namespace compaction {

class cleanup_sstables_compaction_task_executor : public compaction_task_executor, public cleanup_compaction_task_impl {
    const sstables::compaction_type_options _cleanup_options;
    owned_ranges_ptr _owned_ranges_ptr;
    compacting_sstable_registration _compacting;
    std::vector<sstables::compaction_descriptor> _pending_cleanup_jobs;
public:
    cleanup_sstables_compaction_task_executor(compaction_manager& mgr, throw_if_stopping do_throw_if_stopping, table_state* t, tasks::task_id parent_id, sstables::compaction_type_options options, owned_ranges_ptr owned_ranges_ptr,
                                     std::vector<sstables::shared_sstable> candidates, compacting_sstable_registration compacting)
            : compaction_task_executor(mgr, do_throw_if_stopping, t, options.type(), sstring(sstables::to_string(options.type())))
            , cleanup_compaction_task_impl(mgr._task_manager_module, tasks::task_id::create_random_id(), 0, "compaction group", t->schema()->ks_name(), t->schema()->cf_name(), "", parent_id)
            , _cleanup_options(std::move(options))
            , _owned_ranges_ptr(std::move(owned_ranges_ptr))
            , _compacting(std::move(compacting))
            , _pending_cleanup_jobs(t->get_compaction_strategy().get_cleanup_compaction_jobs(*t, std::move(candidates)))
    {
        // Cleanup is made more resilient under disk space pressure, by cleaning up smaller jobs first, so larger jobs
        // will have more space available released by previous jobs.
        std::ranges::sort(_pending_cleanup_jobs, std::ranges::greater(), std::mem_fn(&sstables::compaction_descriptor::sstables_size));
        _cm._stats.pending_tasks += _pending_cleanup_jobs.size();
        _status.progress_units = "bytes";
    }

    virtual ~cleanup_sstables_compaction_task_executor() = default;

    virtual void release_resources() noexcept override {
        _cm._stats.pending_tasks -= _pending_cleanup_jobs.size();
        _pending_cleanup_jobs = {};
        _compacting.release_all();
        _owned_ranges_ptr = nullptr;
    }

    virtual future<tasks::task_manager::task::progress> get_progress() const override {
        return compaction_task_impl::get_progress(_compaction_data, _progress_monitor);
    }

    virtual void abort() noexcept override {
        return compaction_task_executor::abort(_as);
    }
protected:
    virtual future<> run() override {
        return perform();
    }

    virtual future<compaction_manager::compaction_stats_opt>  do_run() override {
        switch_state(state::pending);
        auto maintenance_permit = co_await acquire_semaphore(_cm._maintenance_ops_sem);

        while (!_pending_cleanup_jobs.empty() && can_proceed()) {
            auto active_job = std::move(_pending_cleanup_jobs.back());
            active_job.options = _cleanup_options;
            active_job.owned_ranges = _owned_ranges_ptr;
            co_await run_cleanup_job(std::move(active_job));
            _pending_cleanup_jobs.pop_back();
            _cm._stats.pending_tasks--;
        }

        co_return std::nullopt;
    }
private:
    future<> run_cleanup_job(sstables::compaction_descriptor descriptor) {
        co_await coroutine::switch_to(_cm.compaction_sg());

        // Releases reference to cleaned files such that respective used disk space can be freed.
        using update_registration = compacting_sstable_registration::update_me;
        class release_exhausted : public update_registration {
            sstables::compaction_descriptor& _desc;
        public:
            release_exhausted(compacting_sstable_registration& registration, sstables::compaction_descriptor& desc)
                : update_registration{registration}
                , _desc{desc} {}
            void on_removal(const std::vector<sstables::shared_sstable>& sstables) override {
                auto exhausted = boost::copy_range<std::unordered_set<sstables::shared_sstable>>(sstables);
                std::erase_if(_desc.sstables, [&] (const sstables::shared_sstable& sst) {
                    return exhausted.contains(sst);
                });
                update_registration::on_removal(sstables);
            }
        };
        release_exhausted on_replace{_compacting, descriptor};
        for (;;) {
            compaction_backlog_tracker user_initiated(std::make_unique<user_initiated_backlog_tracker>(_cm._compaction_controller.backlog_of_shares(200), _cm.available_memory()));
            _cm.register_backlog_tracker(user_initiated);

            std::exception_ptr ex;
            try {
                setup_new_compaction(descriptor.run_identifier);
                co_await compact_sstables_and_update_history(descriptor, _compaction_data, on_replace);
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

}

bool needs_cleanup(const sstables::shared_sstable& sst,
                   const dht::token_range_vector& sorted_owned_ranges) {
    // Finish early if the keyspace has no owned token ranges (in this data center)
    if (sorted_owned_ranges.empty()) {
        return true;
    }

    auto first_token = sst->get_first_decorated_key().token();
    auto last_token = sst->get_last_decorated_key().token();
    dht::token_range sst_token_range = dht::token_range::make(first_token, last_token);

    auto r = std::lower_bound(sorted_owned_ranges.begin(), sorted_owned_ranges.end(), first_token,
            [] (const wrapping_interval<dht::token>& a, const dht::token& b) {
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

bool compaction_manager::update_sstable_cleanup_state(table_state& t, const sstables::shared_sstable& sst, const dht::token_range_vector& sorted_owned_ranges) {
    auto& cs = get_compaction_state(&t);
    if (sst->is_shared()) {
        throw std::runtime_error(format("Shared SSTable {} cannot be marked as requiring cleanup, as it can only be processed by resharding",
                                        sst->get_filename()));
    }
    if (needs_cleanup(sst, sorted_owned_ranges)) {
        cs.sstables_requiring_cleanup.insert(sst);
        return true;
    } else {
        cs.sstables_requiring_cleanup.erase(sst);
        return false;
    }
}

bool compaction_manager::erase_sstable_cleanup_state(table_state& t, const sstables::shared_sstable& sst) {
    auto& cs = get_compaction_state(&t);
    return cs.sstables_requiring_cleanup.erase(sst);
}

bool compaction_manager::requires_cleanup(table_state& t, const sstables::shared_sstable& sst) const {
    const auto& cs = get_compaction_state(&t);
    return cs.sstables_requiring_cleanup.contains(sst);
}

const std::unordered_set<sstables::shared_sstable>& compaction_manager::sstables_requiring_cleanup(table_state& t) const {
    const auto& cs = get_compaction_state(&t);
    return cs.sstables_requiring_cleanup;
}

future<> compaction_manager::perform_cleanup(owned_ranges_ptr sorted_owned_ranges, table_state& t, tasks::task_info info) {
    auto gh = start_compaction(t);
    if (!gh) {
        co_return;
    }

    constexpr auto sleep_duration = std::chrono::seconds(10);
    constexpr auto max_idle_duration = std::chrono::seconds(300);
    auto& cs = get_compaction_state(&t);

    co_await try_perform_cleanup(sorted_owned_ranges, t, info);
    auto last_idle = seastar::lowres_clock::now();

    while (!cs.sstables_requiring_cleanup.empty()) {
        auto idle = seastar::lowres_clock::now() - last_idle;
        if (idle >= max_idle_duration) {
            auto msg = ::format("Cleanup timed out after {} seconds of no progress", std::chrono::duration_cast<std::chrono::seconds>(idle).count());
            cmlog.warn("{}", msg);
            co_await coroutine::return_exception(std::runtime_error(msg));
        }

        auto has_sstables_eligible_for_compaction = [&] {
            for (auto& sst : cs.sstables_requiring_cleanup) {
                if (eligible_for_compaction(sst)) {
                    return true;
                }
            }
            return false;
        };

        cmlog.debug("perform_cleanup: waiting for sstables to become eligible for cleanup");
        try {
            co_await t.get_staging_done_condition().when(sleep_duration, [&] { return has_sstables_eligible_for_compaction(); });
        } catch (const seastar::condition_variable_timed_out&) {
            // Ignored.  Keep retrying for max_idle_duration
        }

        if (!has_sstables_eligible_for_compaction()) {
            continue;
        }
        co_await try_perform_cleanup(sorted_owned_ranges, t, info);
        last_idle = seastar::lowres_clock::now();
    }
}

future<> compaction_manager::try_perform_cleanup(owned_ranges_ptr sorted_owned_ranges, table_state& t, tasks::task_info info) {
    auto check_for_cleanup = [this, &t] {
        return boost::algorithm::any_of(_tasks, [&t] (auto& task) {
            return task->compacting_table() == &t && task->compaction_type() == sstables::compaction_type::Cleanup;
        });
    };
    if (check_for_cleanup()) {
        throw std::runtime_error(format("cleanup request failed: there is an ongoing cleanup on {}", t));
    }

    auto& cs = get_compaction_state(&t);
    co_await run_with_compaction_disabled(t, [&] () -> future<> {
        auto update_sstables_cleanup_state = [&] (const sstables::sstable_set& set) -> future<> {
            // Hold on to the sstable set since it may be overwritten
            // while we yield in this loop.
            auto set_holder = set.shared_from_this();
            co_await set.for_each_sstable_gently([&] (const sstables::shared_sstable& sst) {
                update_sstable_cleanup_state(t, sst, *sorted_owned_ranges);
            });
        };
        co_await update_sstables_cleanup_state(t.main_sstable_set());
        co_await update_sstables_cleanup_state(t.maintenance_sstable_set());

        // Some sstables may remain in sstables_requiring_cleanup
        // for later processing if they can't be cleaned up right now.
        // They are erased from sstables_requiring_cleanup by compacting.release_compacting
        if (!cs.sstables_requiring_cleanup.empty()) {
            cs.owned_ranges_ptr = std::move(sorted_owned_ranges);
        }
    });

    if (cs.sstables_requiring_cleanup.empty()) {
        cmlog.debug("perform_cleanup for {} found no sstables requiring cleanup", t);
        co_return;
    }

    auto found_maintenance_sstables = bool(t.maintenance_sstable_set().for_each_sstable_until([this, &t] (const sstables::shared_sstable& sst) {
        return stop_iteration(requires_cleanup(t, sst));
    }));
    if (found_maintenance_sstables) {
        co_await perform_offstrategy(t, info);
    }
    if (utils::get_local_injector().enter("major_compaction_before_cleanup")) {
        co_await perform_major_compaction(t, info);
    }

    // Called with compaction_disabled
    auto get_sstables = [this, &t] () -> future<std::vector<sstables::shared_sstable>> {
        auto& cs = get_compaction_state(&t);
        co_return get_candidates(t, cs.sstables_requiring_cleanup);
    };

    co_await perform_task_on_all_files<cleanup_sstables_compaction_task_executor>(info, t, sstables::compaction_type_options::make_cleanup(), std::move(sorted_owned_ranges),
                                                                         std::move(get_sstables));
}

// Submit a table to be upgraded and wait for its termination.
future<> compaction_manager::perform_sstable_upgrade(owned_ranges_ptr sorted_owned_ranges, table_state& t, bool exclude_current_version, tasks::task_info info) {
    auto get_sstables = [this, &t, exclude_current_version] {
        std::vector<sstables::shared_sstable> tables;

        auto last_version = t.get_sstables_manager().get_highest_supported_format();

        for (auto& sst : get_candidates(t)) {
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
    return rewrite_sstables(t, sstables::compaction_type_options::make_upgrade(), std::move(sorted_owned_ranges), std::move(get_sstables), info).discard_result();
}

future<compaction_manager::compaction_stats_opt> compaction_manager::perform_split_compaction(table_state& t, sstables::compaction_type_options::split opt, tasks::task_info info) {
    auto get_sstables = [this, &t] {
        return make_ready_future<std::vector<sstables::shared_sstable>>(get_candidates(t));
    };
    owned_ranges_ptr owned_ranges_ptr = {};
    auto options = sstables::compaction_type_options::make_split(std::move(opt.classifier));

    return perform_task_on_all_files<split_compaction_task_executor>(info, t, std::move(options), std::move(owned_ranges_ptr), std::move(get_sstables));
}

future<std::vector<sstables::shared_sstable>>
compaction_manager::maybe_split_sstable(sstables::shared_sstable sst, table_state& t, sstables::compaction_type_options::split opt) {
    if (!split_compaction_task_executor::sstable_needs_split(sst, opt)) {
        co_return std::vector<sstables::shared_sstable>{sst};
    }
    std::vector<sstables::shared_sstable> ret;

    co_await run_custom_job(t, sstables::compaction_type::Split, "Split SSTable",
                            [&] (sstables::compaction_data& info, sstables::compaction_progress_monitor& monitor) -> future<> {
        sstables::compaction_descriptor desc = split_compaction_task_executor::make_descriptor(sst, opt);
        desc.creator = [&t] (shard_id _) {
            return t.make_sstable();
        };
        desc.replacer = [&] (sstables::compaction_completion_desc d) {
            std::move(d.new_sstables.begin(), d.new_sstables.end(), std::back_inserter(ret));
        };

        co_await sstables::compact_sstables(std::move(desc), info, t, monitor);
        co_await sst->unlink();
    }, tasks::task_info{}, throw_if_stopping::yes);

    co_return ret;
}

// Submit a table to be scrubbed and wait for its termination.
future<compaction_manager::compaction_stats_opt> compaction_manager::perform_sstable_scrub(table_state& t, sstables::compaction_type_options::scrub opts, tasks::task_info info) {
    auto scrub_mode = opts.operation_mode;
    if (scrub_mode == sstables::compaction_type_options::scrub::mode::validate) {
        return perform_sstable_scrub_validate_mode(t, info);
    }
    owned_ranges_ptr owned_ranges_ptr = {};
    sstring option_desc = fmt::format("mode: {};\nquarantine_mode: {}\n", opts.operation_mode, opts.quarantine_operation_mode);
    return rewrite_sstables(t, sstables::compaction_type_options::make_scrub(scrub_mode), std::move(owned_ranges_ptr), [&t, opts] {
        auto all_sstables = get_all_sstables(t);
        std::vector<sstables::shared_sstable> sstables = boost::copy_range<std::vector<sstables::shared_sstable>>(all_sstables
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
            on_internal_error(cmlog, "bad scrub quarantine mode");
        }));
        return make_ready_future<std::vector<sstables::shared_sstable>>(std::move(sstables));
    }, info, can_purge_tombstones::no, std::move(option_desc));
}

compaction::compaction_state::compaction_state(table_state& t)
    : backlog_tracker(t.get_compaction_strategy().make_backlog_tracker())
{
}

void compaction_manager::add(table_state& t) {
    auto [_, inserted] = _compaction_state.try_emplace(&t, t);
    if (!inserted) {
        on_internal_error(cmlog, format("compaction_state for table {} [{}] already exists", t, fmt::ptr(&t)));
    }
}

future<> compaction_manager::remove(table_state& t, sstring reason) noexcept {
    auto& c_state = get_compaction_state(&t);
    auto erase_state = defer([&t, &c_state, this] () noexcept {
       c_state.backlog_tracker->disable();
       _compaction_state.erase(&t);
    });

    // We need to guarantee that a task being stopped will not retry to compact
    // a table being removed.
    // The requirement above is provided by stop_ongoing_compactions().
    _postponed.erase(&t);

    // Wait for all compaction tasks running under gate to terminate
    // and prevent new tasks from entering the gate.
    if (!c_state.gate.is_closed()) {
        auto close_gate = c_state.gate.close();
        co_await stop_ongoing_compactions(reason, &t);
        co_await std::move(close_gate);
    }

#ifdef DEBUG
    auto found = false;
    sstring msg;
    for (auto& task : _tasks) {
        if (task->compacting_table() == &t) {
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

const std::vector<sstables::compaction_info> compaction_manager::get_compactions(table_state* t) const {
    auto to_info = [] (const shared_ptr<compaction_task_executor>& task) {
        sstables::compaction_info ret;
        ret.compaction_uuid = task->compaction_data().compaction_uuid;
        ret.type = task->compaction_type();
        ret.ks_name = task->compacting_table()->schema()->ks_name();
        ret.cf_name = task->compacting_table()->schema()->cf_name();
        ret.total_partitions = task->compaction_data().total_partitions;
        ret.total_keys_written = task->compaction_data().total_keys_written;
        return ret;
    };
    using ret = std::vector<sstables::compaction_info>;
    return boost::copy_range<ret>(_tasks | boost::adaptors::filtered([t] (const shared_ptr<compaction_task_executor>& task) {
                return (!t || task->compacting_table() == t) && task->compaction_running();
            }) | boost::adaptors::transformed(to_info));
}

bool compaction_manager::has_table_ongoing_compaction(const table_state& t) const {
    return std::any_of(_tasks.begin(), _tasks.end(), [&t] (const shared_ptr<compaction_task_executor>& task) {
        return task->compacting_table() == &t && task->compaction_running();
    });
};

bool compaction_manager::compaction_disabled(table_state& t) const {
    return _compaction_state.contains(&t) && _compaction_state.at(&t).compaction_disabled();
}

future<> compaction_manager::stop_compaction(sstring type, table_state* table) {
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

void compaction_manager::propagate_replacement(table_state& t,
        const std::vector<sstables::shared_sstable>& removed, const std::vector<sstables::shared_sstable>& added) {
    for (auto& task : _tasks) {
        if (task->compacting_table() == &t && task->compaction_running()) {
            task->compaction_data().pending_replacements.push_back({ removed, added });
        }
    }
}

strategy_control& compaction_manager::get_strategy_control() const noexcept {
    return *_strategy_control;
}

void compaction_manager::plug_system_keyspace(db::system_keyspace& sys_ks) noexcept {
    _sys_ks = sys_ks.shared_from_this();
}

void compaction_manager::unplug_system_keyspace() noexcept {
    _sys_ks = nullptr;
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

    // FIXME: propagate exception to caller once all replace_sstables implementations provide strong exception safety guarantees.
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

void compaction_backlog_tracker::copy_ongoing_charges(compaction_backlog_tracker& new_bt, bool move_read_charges) const {
    for (auto&& w : _ongoing_writes) {
        new_bt.register_partially_written_sstable(w.first, *w.second);
    }

    if (move_read_charges) {
        for (auto&& w : _ongoing_compactions) {
            new_bt.register_compacting_sstable(w.first, *w.second);
        }
    }
}

void compaction_backlog_tracker::revert_charges(sstables::shared_sstable sst) {
    _ongoing_writes.erase(sst);
    _ongoing_compactions.erase(sst);
}

compaction_backlog_tracker::compaction_backlog_tracker(compaction_backlog_tracker&& other)
        : _impl(std::move(other._impl))
        , _ongoing_writes(std::move(other._ongoing_writes))
        , _ongoing_compactions(std::move(other._ongoing_compactions))
{
    if (other._manager) {
        on_internal_error(cmlog, "compaction_backlog_tracker is moved while registered");
    }
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

void compaction_manager::register_backlog_tracker(table_state& t, compaction_backlog_tracker new_backlog_tracker) {
    auto& cs = get_compaction_state(&t);
    cs.backlog_tracker.emplace(std::move(new_backlog_tracker));
    register_backlog_tracker(*cs.backlog_tracker);
}

compaction_backlog_tracker& compaction_manager::get_backlog_tracker(table_state& t) {
    auto& cs = get_compaction_state(&t);
    return *cs.backlog_tracker;
}
