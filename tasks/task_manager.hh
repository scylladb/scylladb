/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/range/algorithm/transform.hpp>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "db_clock.hh"
#include "log.hh"
#include "tasks/types.hh"
#include "utils/UUID.hh"
#include "utils/serialized_action.hh"
#include "utils/updateable_value.hh"

namespace tasks {

using is_abortable = bool_class <struct abortable_tag>;
using is_internal = bool_class<struct internal_tag>;

extern logging::logger tmlogger;

class task_manager : public peering_sharded_service<task_manager> {
public:
    class task;
    class module;
    struct config {
        utils::updateable_value<uint32_t> task_ttl;
    };
    using task_ptr = lw_shared_ptr<task_manager::task>;
    using task_map = std::unordered_map<task_id, task_ptr>;
    using foreign_task_ptr = foreign_ptr<task_ptr>;
    using foreign_task_vector = std::vector<foreign_task_ptr>;
    using module_ptr = shared_ptr<module>;
    using modules = std::unordered_map<std::string, module_ptr>;
private:
    task_map _all_tasks;
    modules _modules;
    config _cfg;
    abort_source& _as;
    serialized_action _update_task_ttl_action;
    utils::observer<uint32_t> _task_ttl_observer;
    uint32_t _task_ttl;
public:
    class task_not_found : public std::exception {
        sstring _cause;
    public:
        explicit task_not_found(task_id tid)
            : _cause(format("task with id {} not found", tid))
        { }

        virtual const char* what() const noexcept override { return _cause.c_str(); }
    };

    enum class task_state {
        created,
        running,
        done,
        failed
    };

    class task : public enable_lw_shared_from_this<task> {
    public:
        struct progress {
            double completed = 0.0;         // Number of units completed so far.
            double total = 0.0;             // Total number of units to complete the task.
        };

        struct status {
            task_id id;
            std::string type;
            task_state state = task_state::created;
            db_clock::time_point start_time;
            db_clock::time_point end_time;
            std::string error;
            uint64_t sequence_number = 0;   // A running sequence number of the task.
            unsigned shard = 0;
            std::string keyspace;
            std::string table;
            std::string entity;             // Additional entity specific for the given type of task.
            std::string progress_units;     // A description of the units progress.
        };

        class impl {
        protected:
            status _status;
            progress _progress;             // Reliable only for tasks with no descendants.
            task_id _parent_id;
            foreign_task_vector _children;
            shared_promise<> _done;
            module_ptr _module;
            abort_source _as;
        public:
            impl(module_ptr module, task_id id, uint64_t sequence_number, std::string keyspace, std::string table, std::string type, std::string entity, task_id parent_id)
                : _status({
                    .id = id,
                    .type = std::move(type),
                    .state = task_state::created,
                    .sequence_number = sequence_number,
                    .shard = this_shard_id(),
                    .keyspace = std::move(keyspace),
                    .table = std::move(table),
                    .entity = std::move(entity)
                    })
                , _parent_id(parent_id)
                , _module(module)
            {}
            virtual future<task_manager::task::progress> get_progress() const {
                if (!_children.empty()) {
                    co_return progress{};
                }
                co_return _progress;
            }

            virtual is_abortable is_abortable() const noexcept {
                return is_abortable::no;
            }

            virtual is_internal is_internal() const noexcept {
                return is_internal::no;
            }

            virtual future<> abort() noexcept {
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
        protected:
            virtual future<> run() = 0;

            void run_to_completion() {
                (void)run().then_wrapped([this] (auto f) {
                    if (f.failed()) {
                        finish_failed(f.get_exception());
                    } else {
                        finish();
                    }
                });
            }

            void finish() noexcept {
                if (!_done.available()) {
                    _status.end_time = db_clock::now();
                    _status.state = task_manager::task_state::done;
                    _done.set_value();
                }
            }

            void finish_failed(std::exception_ptr ex, std::string error) noexcept {
                if (!_done.available()) {
                    _status.end_time = db_clock::now();
                    _status.state = task_manager::task_state::failed;
                    _status.error = std::move(error);
                    _done.set_exception(ex);
                }
            }

            void finish_failed(std::exception_ptr ex) {
                finish_failed(ex, fmt::format("{}", ex));
            }

            friend task;
        };
        using task_impl_ptr = std::unique_ptr<impl>;
    protected:
        task_impl_ptr _impl;
    public:
        task(task_impl_ptr&& impl) noexcept : _impl(std::move(impl)) {
            register_task();
        }

        task_id id() {
            return _impl->_status.id;
        }

        status& get_status() noexcept {
            return _impl->_status;
        }

        uint64_t get_sequence_number() const noexcept {
            return _impl->_status.sequence_number;
        }

        task_id get_parent_id() const noexcept {
            return _impl->_parent_id;
        }

        void set_type(std::string type) noexcept {
            _impl->_status.type = std::move(type);
        }

        void change_state(task_state state) noexcept {
            _impl->_status.state = state;
        }

        void add_child(foreign_task_ptr&& child) {
            _impl->_children.push_back(std::move(child));
        }

        void start() {
            _impl->_status.start_time = db_clock::now();

            try {
                // Background fiber does not capture task ptr, so the task can be unregistered and destroyed independently in the foreground.
                // After the ttl expires, the task id will be used to unregister the task if that didn't happen in any other way.
                (void)with_gate(_impl->_module->async_gate(), [f = done(), module = _impl->_module, id = id()] () mutable {
                    return std::move(f).finally([module, id] {
                        return sleep_abortable(module->get_task_manager().get_task_ttl(), module->abort_source());
                    }).then_wrapped([module, id] (auto f) {
                        f.ignore_ready_future();
                        module->unregister_task(id);
                    });
                });
                _impl->_as.check();
                _impl->_status.state = task_manager::task_state::running;
                _impl->run_to_completion();
            } catch (...) {
                _impl->finish_failed(std::current_exception());
            }
        }

        std::string get_module_name() const noexcept {
            return _impl->_module->get_name();
        }

        module_ptr get_module() const noexcept {
            return _impl->_module;
        }

        future<progress> get_progress() const {
            return _impl->get_progress();
        }

        is_abortable is_abortable() const noexcept {
            return _impl->is_abortable();
        };

        is_internal is_internal() const noexcept {
            return _impl->is_internal();
        }

        future<> abort() noexcept {
            return _impl->abort();
        }

        bool abort_requested() const noexcept {
            return _impl->_as.abort_requested();
        }

        future<> done() const noexcept {
            return _impl->_done.get_shared_future();
        }

        void register_task() {
            _impl->_module->register_task(shared_from_this());
        }

        void unregister_task() noexcept {
            _impl->_module->unregister_task(id());
        }

        friend class test_task;
    };

    class module : public enable_shared_from_this<module> {
    protected:
        task_manager& _tm;
        std::string _name;
        task_map _tasks;
        gate _gate;
        uint64_t _sequence_number = 0;
    public:
        module(task_manager& tm, std::string name) noexcept : _tm(tm), _name(std::move(name)) {}

        uint64_t new_sequence_number() noexcept {
            return ++_sequence_number;
        }

        task_manager& get_task_manager() noexcept {
            return _tm;
        }

        virtual abort_source& abort_source() noexcept {
            return _tm.abort_source();
        }

        gate& async_gate() noexcept {
            return _gate;
        }

        const std::string& get_name() const noexcept {
            return _name;
        }

        task_manager::task_map& get_tasks() noexcept {
            return _tasks;
        }

        const task_manager::task_map& get_tasks() const noexcept {
            return _tasks;
        }

        void register_task(task_ptr task) {
            _tasks[task->id()] = task;
            try {
                _tm.register_task(task);
            } catch (...) {
                _tasks.erase(task->id());
                throw;
            }
        }

        void unregister_task(task_id id) noexcept {
            _tasks.erase(id);
            _tm.unregister_task(id);
        }

        virtual future<> stop() noexcept {
            tmlogger.info("Stoppping module {}", _name);
            co_await _gate.close();
            _tm.unregister_module(_name);
        }
    public:
        template<typename T>
        requires std::is_base_of_v<task_manager::task::impl, T>
        future<task_id> make_task(unsigned shard, task_id id = task_id::create_null_id(), std::string keyspace = "", std::string table = "", std::string type = "", std::string entity = "", task_info parent_d = task_info{}) {
            return _tm.container().invoke_on(shard, [id, module = _name, keyspace = std::move(keyspace), table = std::move(table), type = std::move(type), entity = std::move(entity), parent_d] (task_manager& tm) {
                auto module_ptr = tm.find_module(module);
                auto task_impl_ptr = std::make_unique<T>(module_ptr, id ? id : task_id::create_random_id(), parent_d ? 0 : module_ptr->new_sequence_number(), std::move(keyspace), std::move(table), std::move(type), std::move(entity), parent_d.id);
                return module_ptr->make_task(std::move(task_impl_ptr), parent_d).then([] (auto task) {
                    return task->id();
                });
            });
        }

        // Must be called on target shard.
        // If task has a parent, data concerning its children is updated and sequence number is inherited
        // from a parent and set. Otherwise, it must be set by caller.
        future<task_ptr> make_task(task::task_impl_ptr task_impl_ptr, task_info parent_d = task_info{}) {
            auto task = make_lw_shared<task_manager::task>(std::move(task_impl_ptr));
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
    };
public:
    task_manager(config cfg, abort_source& as) noexcept
        : _cfg(std::move(cfg))
        , _as(as)
        , _update_task_ttl_action([this] { return update_task_ttl(); })
        , _task_ttl_observer(_cfg.task_ttl.observe(_update_task_ttl_action.make_observer()))
        , _task_ttl(_cfg.task_ttl.get())
    {}

    modules& get_modules() noexcept {
        return _modules;
    }

    const modules& get_modules() const noexcept {
        return _modules;
    }

    task_map& get_all_tasks() noexcept {
        return _all_tasks;
    }

    const task_map& get_all_tasks() const noexcept {
        return _all_tasks;
    }

    module_ptr make_module(std::string name);
    void register_module(std::string name, module_ptr module);
    module_ptr find_module(std::string module_name);
    future<> stop() noexcept;

    static future<task_manager::foreign_task_ptr> lookup_task_on_all_shards(sharded<task_manager>& tm, task_id tid);
    static future<> invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<> (task_manager::task_ptr)> func);
    template<typename T>
    static future<T> invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<T> (task_manager::task_ptr)> func);
protected:
    abort_source& abort_source() noexcept {
        return _as;
    }

    std::chrono::seconds get_task_ttl() const noexcept {
        return std::chrono::seconds(_task_ttl);
    }
private:
    future<> update_task_ttl() noexcept {
        _task_ttl = _cfg.task_ttl.get();
        return make_ready_future<>();
    }
protected:
    void unregister_module(std::string name) noexcept;
    void register_task(task_ptr task);
    void unregister_task(task_id id) noexcept;
};

}
