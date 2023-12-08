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

#include <boost/range/adaptors.hpp>

#include "message/messaging_service.hh"
#include "utils/overloaded_functor.hh"
#include "tasks/task_handler.hh"
#include "task_manager.hh"
#include "test_module.hh"

template <typename T>
std::vector<T> concat(std::vector<T> a, std::vector<T>&& b) {
    std::move(b.begin(), b.end(), std::back_inserter(a));
    return a;
}

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
    for (auto& child: _children) {
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
    return tasks::is_internal(bool(_parent_id));    // TODO: is direct child of virtual task internal?
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

const task_manager::foreign_task_list& task_manager::task::get_children() const noexcept {
    return _impl->_children;
}

bool task_manager::task::is_complete() const noexcept {
    return _impl->is_complete();
}

void task_manager::task::release_resources() noexcept {
    return _impl->release_resources();
}

task_manager::virtual_task::impl::impl(module_ptr module) noexcept
    : _module(std::move(module))
{
    _shutdown_subscription = _module->abort_source().subscribe([this] () noexcept {
        (void)abort_all_children();
    });
}

future<std::vector<task_identity>> task_manager::virtual_task::impl::get_children(task_id parent_id) {
    auto ms = get_module()->get_task_manager()._messaging;
    if (!ms) {
        auto ids = co_await get_module()->get_task_manager().get_virtual_task_children(parent_id);
        co_return boost::copy_range<std::vector<task_identity>>(ids | boost::adaptors::transformed([&tm = get_task_manager()] (auto id) {
            return task_identity{
                .node = tm.get_broadcast_address(),
                .task_id = id
            };
        }));
    }

    auto nodes = _module->get_nodes();
    co_return co_await map_reduce(nodes, [ms, parent_id] (auto addr) -> future<std::vector<task_identity>> {
        auto resp = co_await ms->send_tasks_get_children(netw::msg_addr{addr}, parent_id);
        co_return boost::copy_range<std::vector<task_identity>>(resp | boost::adaptors::transformed([addr] (auto id) {
            return task_identity{
                .node = addr,
                .task_id = id
            };
        }));
    }, std::vector<task_identity>{}, concat<task_identity>);
}

future<> task_manager::virtual_task::impl::abort_all_children() noexcept {
    const auto ids = co_await get_ids();
    co_await get_task_manager().container().invoke_on_all([&ids] (auto& tm) {
        auto tasks = tm.get_all_tasks() | boost::adaptors::map_values |
            boost::adaptors::filtered([&ids] (const auto& task) { return ids.contains(task->get_parent_id()); });
        return parallel_for_each(std::move(tasks), [] (auto task) {
            return task->abort();
        });
    });
}

uint64_t task_manager::virtual_task::impl::get_sequence_number(task_id id) const {
    if (auto it = _sequence_number.find(id); it != _sequence_number.end()) {
        return it->second;
    }

    auto sn = get_module()->new_sequence_number();
    _sequence_number[id] = sn;
    return sn;
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

tasks::is_internal task_manager::virtual_task::impl::is_internal() const noexcept {
    return is_internal::yes;
}

task_manager::virtual_task::virtual_task(virtual_task_impl_ptr&& impl, task_group group) noexcept
    : _impl(std::move(impl))
    , _group(group)
{
    assert(this_shard_id() == 0);
    register_task();
}

future<std::set<task_id>> task_manager::virtual_task::get_ids() const {
    return _impl->get_ids();
}

uint64_t task_manager::virtual_task::get_sequence_number(task_id id) const {
    return _impl->get_sequence_number(id);
}

task_manager::module_ptr task_manager::virtual_task::get_module() const noexcept {
    return _impl->get_module();
}

future<tasks::is_abortable> task_manager::virtual_task::is_abortable() const {
    return _impl->is_abortable();
}

tasks::is_internal task_manager::virtual_task::is_internal() const noexcept {
    return _impl->is_internal();
}

void task_manager::virtual_task::register_task() {
    get_module()->register_virtual_task(_group, shared_from_this());
}

void task_manager::virtual_task::unregister_task() noexcept {
    get_module()->unregister_virtual_task(_group);
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

task_manager::task_map& task_manager::module::get_all_tasks() noexcept {
    return _tasks._all_tasks;
}

const task_manager::task_map& task_manager::module::get_all_tasks() const noexcept {
    return _tasks._all_tasks;
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

std::vector<gms::inet_address> task_manager::module::get_nodes() const noexcept {
    return {_tm.get_broadcast_address()};
}

void task_manager::module::register_task(task_ptr task) {
    get_all_tasks()[task->id()] = task;
    try {
        _tm.register_task(task);
    } catch (...) {
        get_all_tasks().erase(task->id());
        throw;
    }
}

void task_manager::module::register_virtual_task(task_group group, virtual_task_ptr task) {
    get_virtual_tasks()[group] = task;
    try {
        _tm.register_virtual_task(group, task);
    } catch (...) {
        get_virtual_tasks().erase(group);
        throw;
    }
}

void task_manager::module::unregister_task(task_id id) noexcept {
    get_all_tasks().erase(id);
    _tm.unregister_task(id);
}

void task_manager::module::unregister_virtual_task(task_group group) noexcept { // TODO do we really need unregistering?
    get_virtual_tasks().erase(group);
    _tm.unregister_virtual_task(group);
}

future<> task_manager::module::stop() noexcept {
    tmlogger.info("Stopping module {}", _name);
    abort_source().request_abort();
    co_await _gate.close();
    for (auto [_, vt]: get_virtual_tasks()) {
        vt->unregister_task();
    }
    _tm.unregister_module(_name);
}

future<task_manager::task_ptr> task_manager::module::make_task(task::task_impl_ptr task_impl_ptr, task_info parent_d) {
    auto task = make_lw_shared<task_manager::task>(std::move(task_impl_ptr), async_gate().hold());
    bool abort = false;
    if (parent_d) {
        task->get_status().sequence_number = co_await _tm.container().invoke_on(parent_d.shard, [id = parent_d.id, task = make_foreign(task), &abort] (task_manager& tm) mutable -> future<uint64_t> {
            const auto& all_tasks = tm.get_all_tasks();
            if (auto it = all_tasks.find(id); it != all_tasks.end()) {  // regular task as a parent
                it->second->add_child(std::move(task));
                abort = it->second->abort_requested();
                co_return it->second->get_sequence_number();
            } else {    // virtual task as a parent
                for (auto [_, vt]: tm.get_virtual_tasks()) {
                    if ((co_await vt->get_ids()).contains(id)) {
                        co_return vt->get_sequence_number(id);
                    }
                }
            }
            throw task_manager::task_not_found(id);
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

gms::inet_address task_manager::get_broadcast_address() const noexcept {
    return _cfg.broadcast_address;
}

task_manager::modules& task_manager::get_modules() noexcept {
    return _modules;
}

const task_manager::modules& task_manager::get_modules() const noexcept {
    return _modules;
}

task_manager::task_map& task_manager::get_all_tasks() noexcept {
    return _tasks._all_tasks;
}

const task_manager::task_map& task_manager::get_all_tasks() const noexcept {
    return _tasks._all_tasks;
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
        return boost::copy_range<std::vector<task_id>>(tm.get_all_tasks() |
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
    _tasks._all_tasks[task->id()] = task;
}

void task_manager::register_virtual_task(task_group group, virtual_task_ptr task) {
    _tasks._virtual_tasks[group] = task;
}

void task_manager::unregister_task(task_id id) noexcept {
    _tasks._all_tasks.erase(id);
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
