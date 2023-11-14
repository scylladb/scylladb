/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>

#include "task_manager.hh"
#include "test_module.hh"

namespace tasks {

logging::logger tmlogger("task_manager");

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
    // Child tasks do not need to subscribe to abort source because they will be aborted recursively by their parents.
    if (!parent_id) {
        _shutdown_subscription = module->abort_source().subscribe([this] () noexcept {
            (void)abort();
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

    tasks::task_manager::task::progress progress{};
    // _children vector may be modified in the meantime. Do not use foreach loop as the iterators may get invalidated.
    for (unsigned i = 0; i < _children.size(); ++i) {
        auto& child = _children[i];
        progress += co_await smp::submit_to(child.get_owner_shard(), [&child] {
            return child->get_progress();
        });
    }
    progress.total = expected_workload.value_or(progress.total);
    co_return progress;
}

is_abortable task_manager::task::impl::is_abortable() const noexcept {
    return is_abortable::no;
}

is_internal task_manager::task::impl::is_internal() const noexcept {
    return tasks::is_internal(bool(_parent_id));
}

future<> task_manager::task::impl::abort() noexcept {
    if (!_as.abort_requested()) {
        _as.request_abort();

        std::vector<task_info> children_info{_children.size()};
        boost::transform(_children, children_info.begin(), [] (const auto& child) {
            return task_info{child->id(), child.get_owner_shard()};
        });

        co_await coroutine::parallel_for_each(children_info, [this] (auto info) {
            return smp::submit_to(info.shard, [info, &tm = _module->get_task_manager().container()] {
                auto& tasks = tm.local().get_all_tasks();
                if (auto it = tasks.find(info.id); it != tasks.end()) {
                    return it->second->abort();
                }
                return make_ready_future<>();
            });
        });
    }
}

bool task_manager::task::impl::is_complete() const noexcept {
    return _status.state == tasks::task_manager::task_state::done || _status.state == tasks::task_manager::task_state::failed;
}

bool task_manager::task::impl::is_done() const noexcept {
    return _status.state == tasks::task_manager::task_state::done;
}

void task_manager::task::impl::run_to_completion() {
    (void)run().then_wrapped([this] (auto f) {
        if (f.failed()) {
            finish_failed(f.get_exception());
        } else {
            try {
                _as.check();
                finish();
            } catch (...) {
                finish_failed(std::current_exception());
            }
        }
    });
}

void task_manager::task::impl::finish() noexcept {
    if (!_done.available()) {
        _status.end_time = db_clock::now();
        _status.state = task_manager::task_state::done;
        _done.set_value();
    }
}

void task_manager::task::impl::finish_failed(std::exception_ptr ex, std::string error) noexcept {
    if (!_done.available()) {
        _status.end_time = db_clock::now();
        _status.state = task_manager::task_state::failed;
        _status.error = std::move(error);
        _done.set_exception(ex);
    }
}

void task_manager::task::impl::finish_failed(std::exception_ptr ex) {
    finish_failed(ex, fmt::format("{}", ex));
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

void task_manager::task::add_child(foreign_task_ptr&& child) {
    _impl->_children.push_back(std::move(child));
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
        (void)done().finally([module] {
            return sleep_abortable(module->get_task_manager().get_task_ttl(), module->abort_source());
        }).then_wrapped([module, id = id()] (auto f) {
            f.ignore_ready_future();
            module->unregister_task(id);
        });
        _impl->_as.check();
        _impl->_status.state = task_manager::task_state::running;
        _impl->run_to_completion();
    } catch (...) {
        _impl->finish_failed(std::current_exception());
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

future<> task_manager::task::abort() noexcept {
    return _impl->abort();
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

const task_manager::foreign_task_vector& task_manager::task::get_children() const noexcept {
    return _impl->_children;
}

bool task_manager::task::is_complete() const noexcept {
    return _impl->is_complete();
}

void task_manager::task::release_resources() noexcept {
    return _impl->release_resources();
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

task_manager::task_map& task_manager::module::get_tasks() noexcept {
    return _tasks;
}

const task_manager::task_map& task_manager::module::get_tasks() const noexcept {
    return _tasks;
}

void task_manager::module::register_task(task_ptr task) {
    _tasks[task->id()] = task;
    try {
        _tm.register_task(task);
    } catch (...) {
        _tasks.erase(task->id());
        throw;
    }
}

void task_manager::module::unregister_task(task_id id) noexcept {
    _tasks.erase(id);
    _tm.unregister_task(id);
}

future<> task_manager::module::stop() noexcept {
    tmlogger.info("Stopping module {}", _name);
    abort_source().request_abort();
    co_await _gate.close();
    _tm.unregister_module(_name);
}

future<task_manager::task_ptr> task_manager::module::make_task(task::task_impl_ptr task_impl_ptr, task_info parent_d) {
    auto task = make_lw_shared<task_manager::task>(std::move(task_impl_ptr), async_gate().hold());
    bool abort = false;
    if (parent_d) {
        task->get_status().sequence_number = co_await _tm.container().invoke_on(parent_d.shard, [id = parent_d.id, task = make_foreign(task), &abort] (task_manager& tm) mutable {
            const auto& all_tasks = tm.get_all_tasks();
            if (auto it = all_tasks.find(id); it != all_tasks.end()) {
                it->second->add_child(std::move(task));
                abort = it->second->abort_requested();
                return it->second->get_sequence_number();
            } else {
                throw task_manager::task_not_found(id);
            }
        });
    }
    if (abort) {
        co_await task->abort();
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
}

task_manager::task_manager() noexcept
    : _update_task_ttl_action([this] { return update_task_ttl(); })
    , _task_ttl_observer(_cfg.task_ttl.observe(_update_task_ttl_action.make_observer()))
    , _task_ttl(0)
{}

task_manager::modules& task_manager::get_modules() noexcept {
    return _modules;
}

const task_manager::modules& task_manager::get_modules() const noexcept {
    return _modules;
}

task_manager::task_map& task_manager::get_all_tasks() noexcept {
    return _all_tasks;
}

const task_manager::task_map& task_manager::get_all_tasks() const noexcept {
    return _all_tasks;
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
    return task_manager::invoke_on_task(tm, tid, std::function([] (task_ptr task) {
        return make_ready_future<task_manager::foreign_task_ptr>(make_foreign(task));
    }));
}

future<> task_manager::invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<> (task_manager::task_ptr)> func) {
    co_await task_manager::invoke_on_task(tm, id, std::function([func = std::move(func)] (task_manager::task_ptr task) -> future<bool> {
        co_await func(task);
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
    _all_tasks[task->id()] = task;
}

void task_manager::unregister_task(task_id id) noexcept {
    _all_tasks.erase(id);
}

}
