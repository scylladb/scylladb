/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <boost/range/adaptors.hpp>

#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/when_all.hh>
#include <seastar/rpc/rpc_types.hh>
#include <seastar/util/defer.hh>

#include <boost/range/adaptors.hpp>

#include "db/timeout_clock.hh"
#include "message/messaging_service.hh"
#include "utils/assert.hh"
#include "utils/overloaded_functor.hh"
#include "tasks/task_handler.hh"
#include "task_manager.hh"
#include "utils/error_injection.hh"

using namespace std::chrono_literals;

template <typename T>
std::vector<T> concat(std::vector<T> a, std::vector<T>&& b) {
    std::move(b.begin(), b.end(), std::back_inserter(a));
    return a;
}

namespace tasks {

logging::logger tmlogger("task_manager");

bool task_manager::task::children::all_finished() const noexcept {
    return _children.empty();
}

size_t task_manager::task::children::size() const noexcept {
    return _children.size() + _finished_children.size();
}

future<> task_manager::task::children::add_child(foreign_task_ptr task) {
    rwlock::holder exclusive_holder = co_await _lock.hold_write_lock();

    auto id = task->id();
    auto inserted = _children.emplace(id, std::move(task)).second;
    SCYLLA_ASSERT(inserted);
}

future<> task_manager::task::children::mark_as_finished(task_id id, task_essentials essentials) const {
    rwlock::holder exclusive_holder = co_await _lock.hold_write_lock();

    auto it = _children.find(id);
    _finished_children.push_back(essentials);
    [&] () noexcept {   // erase is expected to not throw
        _children.erase(it);
    }();
}

future<task_manager::task::progress> task_manager::task::children::get_progress(const std::string& progress_units) const {
    rwlock::holder shared_holder = co_await _lock.hold_read_lock();

    tasks::task_manager::task::progress progress{};
    co_await coroutine::parallel_for_each(_children, [&] (const auto& child_entry) -> future<> {
        const auto& child = child_entry.second;
        auto local_progress = co_await smp::submit_to(child.get_owner_shard(), [&child, &progress_units] {
            SCYLLA_ASSERT(child->get_status().progress_units == progress_units);
            return child->get_progress();
        });
        progress += local_progress;
    });
    for (const auto& child: _finished_children) {
        progress += child.task_progress;
    }
    co_return progress;
}

future<> task_manager::task::children::for_each_task(std::function<future<>(const foreign_task_ptr&)> f_children,
        std::function<future<>(const task_essentials&)> f_finished_children) const {
    rwlock::holder shared_holder = co_await _lock.hold_read_lock();

    for (const auto& [_, child]: _children) {
        co_await f_children(child);
    }
    for (const auto& child: _finished_children) {
        co_await f_finished_children(child);
    }
}

task_manager::task::impl::impl(module_ptr module, task_id id, uint64_t sequence_number, std::string scope, std::string keyspace, std::string table, std::string entity, task_id parent_id) noexcept
    : _status({
        .id = id,
        .state = task_state::created,
        .sequence_number = sequence_number,
        .shard = this_shard_id(),
        .scope = std::move(scope),
        .keyspace = std::move(keyspace),
        .table = std::move(table),
        .entity = std::move(entity)
        })
    , _parent_id(parent_id)
    , _module(module)
{
    // Child tasks of regular tasks do not need to subscribe to abort source because they will be aborted recursively by their parents.
    if (!parent_id || _parent_kind == task_kind::cluster) {
        _shutdown_subscription = module->abort_source().subscribe([this] () noexcept {
            abort();
        });
    }
}

future<std::optional<double>> task_manager::task::impl::expected_total_workload() const {
    return make_ready_future<std::optional<double>>(std::nullopt);
}

std::optional<double> task_manager::task::impl::expected_children_number() const {
    return std::nullopt;
}

task_manager::task::progress task_manager::task::impl::get_binary_progress() const {
    return tasks::task_manager::task::progress{
        .completed = is_complete(),
        .total = 1.0
    };
}

future<task_manager::task::progress> task_manager::task::impl::get_progress() const {
    auto children_num = _children.size();
    if (children_num == 0) {
        co_return get_binary_progress();
    }

    std::optional<double> expected_workload = std::nullopt;
    auto expected_children_num = expected_children_number();
    // When get_progress is called, the task can have some of its children unregistered yet.
    // Then if total workload is not known, progress obtained from children may be deceiving.
    // In such a situation it's safer to return binary progress value.
    if (expected_children_num.value_or(0) != children_num && !(expected_workload = co_await expected_total_workload())) {
        co_return get_binary_progress();
    }

    auto progress = co_await _children.get_progress(_status.progress_units);
    progress.total = expected_workload.value_or(progress.total);
    co_return progress;
}

is_abortable task_manager::task::impl::is_abortable() const noexcept {
    return is_abortable::no;
}

is_internal task_manager::task::impl::is_internal() const noexcept {
    return tasks::is_internal(bool(_parent_id));
}

static future<> abort_children(task_manager::module_ptr module, task_id parent_id) noexcept {
    co_await utils::get_local_injector().inject("tasks_abort_children",
            [] (auto& handler) { return handler.wait_for_message(db::timeout_clock::now() + 10s); });

    auto entered = module->async_gate().try_enter();
    if (!entered) {
        co_return;
    }
    auto leave_gate = defer([&module] () {
        module->async_gate().leave();
    });
    co_await module->get_task_manager().container().invoke_on_all([parent_id] (task_manager& tm) {
        for (auto& task : tm.get_local_tasks()) {
            if (task.second->get_parent_id() == parent_id) {
                task.second->abort();
            }
        }
    });
}

void task_manager::task::impl::abort() noexcept {
    if (!_as.abort_requested()) {
        _as.request_abort();

        (void)abort_children(_module, _status.id);
    }
}

bool task_manager::task::impl::is_complete() const noexcept {
    return _status.state == tasks::task_manager::task_state::done || _status.state == tasks::task_manager::task_state::failed;
}

bool task_manager::task::impl::is_done() const noexcept {
    return _status.state == tasks::task_manager::task_state::done;
}

future<std::vector<task_manager::task::task_essentials>> task_manager::task::impl::get_failed_children() const {
    return _children.map_each_task<task_essentials>([] (const foreign_task_ptr&) { return std::nullopt; },
        [] (const task_essentials& child) -> std::optional<task_essentials> {
            if (child.task_status.state == task_state::failed || !child.failed_children.empty()) {
                return child;
            }
            return std::nullopt;
        }
    );
}

void task_manager::task::impl::set_virtual_parent() noexcept {
    _parent_kind = task_kind::cluster;
}

void task_manager::task::impl::run_to_completion() {
    (void)run().then([this] {
        _as.check();
        return finish();
    }).handle_exception([this] (std::exception_ptr ex) {
        return finish_failed(std::move(ex));
    });
}

future<> task_manager::task::impl::maybe_fold_into_parent() const noexcept {
    try {
        if (is_internal() && _parent_id && _parent_kind == task_kind::node && _children.all_finished()) {
            auto parent = co_await _module->get_task_manager().lookup_task_on_all_shards(_module->get_task_manager().container(), _parent_id);
            task_essentials child{
                .task_status = _status,
                .task_progress = co_await get_progress(),
                .parent_id = _parent_id,
                .type = type(),
                .abortable = is_abortable(),
                .failed_children = co_await get_failed_children(),
            };
            co_await smp::submit_to(parent.get_owner_shard(), [&parent, id = _status.id, child = std::move(child)] () {
                return parent->get_children().mark_as_finished(id, std::move(child));
            });
        }
    } catch (...) {
        // If folding fails, leave the subtree unfolded.
        tasks::tmlogger.warn("Folding of task with id={} failed due to {}. Ignored", _status.id, std::current_exception());
    }
}

future<> task_manager::task::impl::finish() noexcept {
    if (!_done.available()) {
        _status.end_time = db_clock::now();
        _status.state = task_manager::task_state::done;
        co_await maybe_fold_into_parent();
        _done.set_value();
        release_resources();
    }
}

future<> task_manager::task::impl::finish_failed(std::exception_ptr ex, std::string error) noexcept {
    if (!_done.available()) {
        _status.end_time = db_clock::now();
        _status.state = task_manager::task_state::failed;
        _status.error = std::move(error);
        co_await maybe_fold_into_parent();
        _done.set_exception(ex);
        release_resources();
    }
}

future<> task_manager::task::impl::finish_failed(std::exception_ptr ex) noexcept {
    std::string error;
    try {
        error = fmt::format("{}", ex);
    } catch (...) {
        error = "Failed to get error message";
    }
    return finish_failed(ex, std::move(error));
}

task_manager::task::task(task_impl_ptr&& impl, gate::holder gh) noexcept : _impl(std::move(impl)), _gate_holder(std::move(gh)) {
    register_task();
}

task_id task_manager::task::id() {
    return _impl->_status.id;
}

std::string task_manager::task::type() const {
    return _impl->type();
}

task_manager::task::status& task_manager::task::get_status() noexcept {
    return _impl->_status;
}

uint64_t task_manager::task::get_sequence_number() const noexcept {
    return _impl->_status.sequence_number;
}

task_id task_manager::task::get_parent_id() const noexcept {
    return _impl->_parent_id;
}

void task_manager::task::change_state(task_state state) noexcept {
    _impl->_status.state = state;
}

future<> task_manager::task::add_child(foreign_task_ptr&& child) {
    return _impl->_children.add_child(std::move(child));
}

void task_manager::task::start() {
    if (_impl->_status.state != task_state::created) {
        on_fatal_internal_error(tmlogger, format("{} task with id = {} was started twice", _impl->_module->get_name(), id()));
    }
    _impl->_status.start_time = db_clock::now();

    try {
        // Background fiber does not capture task ptr, so the task can be unregistered and destroyed independently in the foreground.
        // After the ttl expires, the task id will be used to unregister the task if that didn't happen in any other way.
        auto module = _impl->_module;
        bool drop_after_complete = (get_parent_id() && _impl->_parent_kind == task_kind::node) || is_internal();
        (void)done().finally([module, drop_after_complete] {
            if (drop_after_complete) {
                return make_ready_future<>();
            }
            return sleep_abortable(module->get_task_manager().get_task_ttl(), module->abort_source());
        }).then_wrapped([module, id = id()] (auto f) {
            f.ignore_ready_future();
            module->unregister_task(id);
        });
        _impl->_as.check();
        _impl->_status.state = task_manager::task_state::running;
        _impl->run_to_completion();
    } catch (...) {
        (void)_impl->finish_failed(std::current_exception()).then([impl = _impl] {});
    }
}

std::string task_manager::task::get_module_name() const noexcept {
    return _impl->_module->get_name();
}

task_manager::module_ptr task_manager::task::get_module() const noexcept {
    return _impl->_module;
}

future<task_manager::task::progress> task_manager::task::get_progress() const {
    return _impl->get_progress();
}

is_abortable task_manager::task::is_abortable() const noexcept {
    return _impl->is_abortable();
};

is_internal task_manager::task::is_internal() const noexcept {
    return _impl->is_internal();
}

void task_manager::task::abort() noexcept {
    _impl->abort();
}

bool task_manager::task::abort_requested() const noexcept {
    return _impl->_as.abort_requested();
}

future<> task_manager::task::done() const noexcept {
    return _impl->_done.get_shared_future();
}

void task_manager::task::register_task() {
    _impl->_module->register_task(shared_from_this());
}

void task_manager::task::unregister_task() noexcept {
    _impl->_module->unregister_task(id());
}

const task_manager::task::children& task_manager::task::get_children() const noexcept {
    return _impl->_children;
}

bool task_manager::task::is_complete() const noexcept {
    return _impl->is_complete();
}

future<std::vector<task_manager::task::task_essentials>> task_manager::task::get_failed_children() const {
    return _impl->get_failed_children();
}

void task_manager::task::set_virtual_parent() noexcept {
    _impl->set_virtual_parent();
}

task_manager::virtual_task::impl::impl(module_ptr module) noexcept
    : _module(std::move(module))
{}

future<std::vector<task_identity>> task_manager::virtual_task::impl::get_children(module_ptr module, task_id parent_id) {
    auto ms = module->get_task_manager()._messaging;
    if (!ms) {
        auto ids = co_await module->get_task_manager().get_virtual_task_children(parent_id);
        co_return boost::copy_range<std::vector<task_identity>>(ids | boost::adaptors::transformed([&tm = module->get_task_manager()] (auto id) {
            return task_identity{
                .node = tm.get_broadcast_address(),
                .task_id = id
            };
        }));
    }

    auto nodes = module->get_nodes();
    co_return co_await map_reduce(nodes, [ms, parent_id] (auto addr) -> future<std::vector<task_identity>> {
        return ms->send_tasks_get_children(netw::msg_addr{addr}, parent_id).then([addr] (auto resp) {
            return boost::copy_range<std::vector<task_identity>>(resp | boost::adaptors::transformed([addr] (auto id) {
                return task_identity{
                    .node = addr,
                    .task_id = id
                };
            }));
        });
    }, std::vector<task_identity>{}, concat<task_identity>);
}

task_manager::module_ptr task_manager::virtual_task::impl::get_module() const noexcept {
    return _module;
}

task_manager& task_manager::virtual_task::impl::get_task_manager() const noexcept {
    return _module->get_task_manager();
}

future<tasks::is_abortable> task_manager::virtual_task::impl::is_abortable() const {
    return make_ready_future<tasks::is_abortable>(is_abortable::no);
}

task_manager::virtual_task::virtual_task(virtual_task_impl_ptr&& impl) noexcept
    : _impl(std::move(impl))
{
    SCYLLA_ASSERT(this_shard_id() == 0);
}

future<std::set<task_id>> task_manager::virtual_task::get_ids() const {
    return _impl->get_ids();
}

task_manager::module_ptr task_manager::virtual_task::get_module() const noexcept {
    return _impl->get_module();
}

task_manager::task_group task_manager::virtual_task::get_group() const noexcept {
    return _impl->get_group();
}

future<tasks::is_abortable> task_manager::virtual_task::is_abortable() const {
    return _impl->is_abortable();
}

future<std::optional<task_status>> task_manager::virtual_task::get_status(task_id id) {
    return _impl->get_status(id);
}

future<std::optional<task_status>> task_manager::virtual_task::wait(task_id id) {
    return _impl->wait(id);
}

future<> task_manager::virtual_task::abort(task_id id) noexcept {
    return _impl->abort(id);
}

future<std::vector<task_stats>> task_manager::virtual_task::get_stats() {
    return _impl->get_stats();
}

task_manager::module::module(task_manager& tm, std::string name) noexcept : _tm(tm), _name(std::move(name)) {
    _abort_subscription = _tm.abort_source().subscribe([this] () noexcept {
        abort_source().request_abort();
    });
}

uint64_t task_manager::module::new_sequence_number() noexcept {
    return ++_sequence_number;
}

task_manager& task_manager::module::get_task_manager() noexcept {
    return _tm;
}

abort_source& task_manager::module::abort_source() noexcept {
    return _as;
}

gate& task_manager::module::async_gate() noexcept {
    return _gate;
}

const std::string& task_manager::module::get_name() const noexcept {
    return _name;
}

task_manager::task_map& task_manager::module::get_local_tasks() noexcept {
    return _tasks._local_tasks;
}

const task_manager::task_map& task_manager::module::get_local_tasks() const noexcept {
    return _tasks._local_tasks;
}

task_manager::virtual_task_map& task_manager::module::get_virtual_tasks() noexcept {
    return _tasks._virtual_tasks;
}

const task_manager::virtual_task_map& task_manager::module::get_virtual_tasks() const noexcept {
    return _tasks._virtual_tasks;
}

task_manager::tasks_collection& task_manager::module::get_tasks_collection() noexcept {
    return _tasks;
}

const task_manager::tasks_collection& task_manager::module::get_tasks_collection() const noexcept {
    return _tasks;
}

std::set<gms::inet_address> task_manager::module::get_nodes() const noexcept {
    return {_tm.get_broadcast_address()};
}

future<utils::chunked_vector<task_stats>> task_manager::module::get_stats(is_internal internal, std::function<bool(std::string& keyspace, std::string& table)> filter) const {
    utils::chunked_vector<task_stats> stats;
    for (auto [_, task]: get_local_tasks()) {
        if ((internal || !task->is_internal()) && filter(task->get_status().keyspace, task->get_status().table)) {
            stats.push_back(task_stats{
                .task_id = task->id(),
                .type = task->type(),
                .kind = task_kind::node,
                .scope = task->get_status().scope,
                .state = task->get_status().state,
                .sequence_number = task->get_sequence_number(),
                .keyspace = task->get_status().keyspace,
                .table = task->get_status().table,
                .entity = task->get_status().entity
            });
        }
    }
    if (this_shard_id() == 0) {
        auto virtual_tasks = get_virtual_tasks(); // Copy to make sure iterators are valid.
        for (auto [_, vt]: virtual_tasks) {
            auto vstats = co_await vt->get_stats();
            for (auto&& s: vstats) {
                if (filter(s.keyspace, s.table)) {
                    stats.push_back(std::move(s));
                }
            }
        }
    }
    co_return stats;
}

void task_manager::module::register_task(task_ptr task) {
    get_local_tasks()[task->id()] = task;
    try {
        _tm.register_task(task);
    } catch (...) {
        get_local_tasks().erase(task->id());
        throw;
    }
}

void task_manager::module::register_virtual_task(virtual_task_ptr task) {
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto group = task->get_group();
    get_virtual_tasks()[group] = task;
    try {
        _tm.register_virtual_task(task);
    } catch (...) {
        get_virtual_tasks().erase(group);
        throw;
    }
}

void task_manager::module::unregister_task(task_id id) noexcept {
    get_local_tasks().erase(id);
    _tm.unregister_task(id);
}

future<> task_manager::module::stop() noexcept {
    tmlogger.info("Stopping module {}", _name);
    abort_source().request_abort();
    co_await _gate.close();
    if (this_shard_id() == 0) {
        for (auto& [group, _]: _tasks._virtual_tasks) {
            _tm.unregister_virtual_task(group);
        }
        _tasks._virtual_tasks = {};
    }
    _tm.unregister_module(_name);
}

future<task_manager::task_ptr> task_manager::module::make_task(task::task_impl_ptr task_impl_ptr, task_info parent_d) {
    auto task = make_lw_shared<task_manager::task>(std::move(task_impl_ptr), async_gate().hold());
    bool abort = false;
    if (parent_d) {
        // Regular task as a parent.
        auto sequence_number = co_await _tm.container().invoke_on(parent_d.shard, coroutine::lambda([id = parent_d.id, task = make_foreign(task), &abort] (task_manager& tm) mutable -> future<std::optional<uint64_t>> {
            const auto& all_tasks = tm.get_local_tasks();
            if (auto it = all_tasks.find(id); it != all_tasks.end()) {
                co_await it->second->add_child(std::move(task));
                abort = it->second->abort_requested();
                co_return it->second->get_sequence_number();
            }
            co_return std::nullopt;
        }));

        if (sequence_number) {
            task->get_status().sequence_number = sequence_number.value();
        } else { // Virtual task as a parent.
            sequence_number = new_sequence_number();
            task->set_virtual_parent();
        }
    }
    if (abort) {
        task->abort();
    }
    co_return task;
}

task_manager::task_manager(config cfg, class abort_source& as) noexcept
    : _cfg(std::move(cfg))
    , _update_task_ttl_action([this] { return update_task_ttl(); })
    , _task_ttl_observer(_cfg.task_ttl.observe(_update_task_ttl_action.make_observer()))
    , _task_ttl(_cfg.task_ttl.get())
{
    _abort_subscription = as.subscribe([this] () noexcept {
        _as.request_abort();
    });
    tmlogger.debug("Started task manager (TTL={})", get_task_ttl());
}

task_manager::task_manager() noexcept
    : _update_task_ttl_action([this] { return update_task_ttl(); })
    , _task_ttl_observer(_cfg.task_ttl.observe(_update_task_ttl_action.make_observer()))
    , _task_ttl(0)
{}

gms::inet_address task_manager::get_broadcast_address() const noexcept {
    return _cfg.broadcast_address;
}

task_manager::modules& task_manager::get_modules() noexcept {
    return _modules;
}

const task_manager::modules& task_manager::get_modules() const noexcept {
    return _modules;
}

task_manager::task_map& task_manager::get_local_tasks() noexcept {
    return _tasks._local_tasks;
}

const task_manager::task_map& task_manager::get_local_tasks() const noexcept {
    return _tasks._local_tasks;
}

task_manager::virtual_task_map& task_manager::get_virtual_tasks() noexcept {
    return _tasks._virtual_tasks;
}

const task_manager::virtual_task_map& task_manager::get_virtual_tasks() const noexcept {
    return _tasks._virtual_tasks;
}

task_manager::tasks_collection& task_manager::get_tasks_collection() noexcept {
    return _tasks;
}

const task_manager::tasks_collection& task_manager::get_tasks_collection() const noexcept {
    return _tasks;
}

future<std::vector<task_id>> task_manager::get_virtual_task_children(task_id parent_id) {
    return container().map_reduce0([parent_id] (task_manager& tm) {
        return boost::copy_range<std::vector<task_id>>(tm.get_local_tasks() |
            boost::adaptors::map_values |
            boost::adaptors::filtered([parent_id] (const auto& task) { return task->get_parent_id() == parent_id; }) |
            boost::adaptors::transformed([] (const auto& task) { return task->id(); }));
    }, std::vector<task_id>{}, concat<task_id>);
}

task_manager::module_ptr task_manager::make_module(std::string name) {
    auto m = seastar::make_shared<task_manager::module>(*this, name);
    register_module(std::move(name), m);
    return m;
}

task_manager::module_ptr task_manager::find_module(std::string module_name) {
    auto it = _modules.find(module_name);
    if (it == _modules.end()) {
        throw std::runtime_error(format("module {} not found", module_name));
    }
    return it->second;
}

future<> task_manager::stop() noexcept {
    if (!_modules.empty()) {
        on_internal_error(tmlogger, "Tried to stop task manager while some modules were not unregistered");
    }
    return make_ready_future<>();
}

future<task_manager::foreign_task_ptr> task_manager::lookup_task_on_all_shards(sharded<task_manager>& tm, task_id tid) {
    return task_manager::invoke_on_task(tm, tid, std::function([tid] (task_variant task_v) {
        return std::visit(overloaded_functor{
            [] (tasks::task_manager::task_ptr task) {
                return make_ready_future<task_manager::foreign_task_ptr>(make_foreign(task));
            },
            [tid] (tasks::task_manager::virtual_task_ptr task) -> future<tasks::task_manager::foreign_task_ptr> {
                throw tasks::task_manager::task_not_found(tid); // The method is designed for regular tasks.
            }
        }, task_v);
    }));
}

future<task_manager::virtual_task_ptr> task_manager::lookup_virtual_task(task_manager& tm, task_id id) {
    auto vts = tm.get_virtual_tasks();
    for (auto [_, vt]: tm.get_virtual_tasks()) {
        if ((co_await vt->get_ids()).contains(id)) {
            co_return vt;
        }
    }
    co_return nullptr;
}

future<> task_manager::invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<> (task_manager::task_variant)> func) {
    co_await task_manager::invoke_on_task(tm, id, std::function([func = std::move(func)] (task_manager::task_variant task_v) -> future<bool> {
        co_await func(task_v);
        co_return true;
    }));
}

abort_source& task_manager::abort_source() noexcept {
    return _as;
}

std::chrono::seconds task_manager::get_task_ttl() const noexcept {
    return std::chrono::seconds(_task_ttl);
}

void task_manager::register_module(std::string name, module_ptr module) {
    _modules[name] = module;
    tmlogger.info("Registered module {}", name);
}

void task_manager::unregister_module(std::string name) noexcept {
    _modules.erase(name);
    tmlogger.info("Unregistered module {}", name);
}

void task_manager::register_task(task_ptr task) {
    _tasks._local_tasks[task->id()] = task;
}

void task_manager::register_virtual_task(virtual_task_ptr task) {
    _tasks._virtual_tasks[task->get_group()] = task;
}

void task_manager::unregister_task(task_id id) noexcept {
    _tasks._local_tasks.erase(id);
}

void task_manager::unregister_virtual_task(task_group group) noexcept {
    _tasks._virtual_tasks.erase(group);
}

void task_manager::init_ms_handlers(netw::messaging_service& ms) {
    _messaging = &ms;

    ms.register_tasks_get_children([this] (const rpc::client_info& cinfo, tasks::get_children_request req) -> future<tasks::get_children_response> {
        return get_virtual_task_children(task_id{req.id});
    });
}

future<> task_manager::uninit_ms_handlers() {
    if (auto* ms = std::exchange(_messaging, nullptr)) {
        return ms->unregister_tasks_get_children().discard_result();
    }
    return make_ready_future();
}

}
