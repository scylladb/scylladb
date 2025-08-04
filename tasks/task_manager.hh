/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <list>
#include <ranges>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/rwlock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "db_clock.hh"
#include "utils/log.hh"
#include "locator/host_id.hh"
#include "schema/schema_fwd.hh"
#include "tasks/types.hh"
#include "utils/chunked_vector.hh"
#include "utils/serialized_action.hh"
#include "utils/updateable_value.hh"

namespace repair {
class task_manager_module;
}

namespace service {
class storage_service;
}

namespace netw {
class messaging_service;
}

namespace tasks {

using is_abortable = bool_class <struct abortable_tag>;
using is_internal = bool_class<struct internal_tag>;
using is_user_task = bool_class<struct user_task_tag>;

extern logging::logger tmlogger;

enum class task_kind {
    cluster,
    node,
};

struct task_identity;
struct task_status;
struct task_stats;
struct virtual_task_hint;

class task_manager : public peering_sharded_service<task_manager> {
public:
    class task;
    class virtual_task;
    class module;
    enum class task_group;
    struct config {
        utils::updateable_value<uint32_t> task_ttl;
        utils::updateable_value<uint32_t> user_task_ttl;
    };
    using task_ptr = lw_shared_ptr<task_manager::task>;
    using virtual_task_ptr = lw_shared_ptr<task_manager::virtual_task>;
    using task_variant = std::variant<task_manager::task_ptr, task_manager::virtual_task_ptr>;
    using task_map = std::unordered_map<task_id, task_ptr>;
    using virtual_task_map = std::unordered_map<task_group, virtual_task_ptr>;
    using foreign_task_ptr = foreign_ptr<task_ptr>;
    using foreign_task_map = std::unordered_map<task_id, foreign_task_ptr>;
    using module_ptr = shared_ptr<module>;
    using modules = std::unordered_map<std::string, module_ptr>;

    struct tasks_collection {
        task_map _local_tasks;
        virtual_task_map _virtual_tasks;
    };
private:
    tasks_collection _tasks;
    modules _modules;
    config _cfg;
    locator::host_id _host_id = locator::host_id::create_null_id();
    seastar::abort_source _as;
    optimized_optional<seastar::abort_source::subscription> _abort_subscription;
    utils::updateable_value<uint32_t> _task_ttl;
    utils::updateable_value<uint32_t> _user_task_ttl;
    netw::messaging_service* _messaging = nullptr;
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
        failed,
        suspended
    };

    enum class task_group {
        // Each virtual task needs to have its group.
        topology_change_group,
        tablets_group,
    };

    class task : public enable_lw_shared_from_this<task> {
    public:
        struct progress {
            double completed = 0.0;         // Number of units completed so far.
            double total = 0.0;             // Total number of units to complete the task.

            progress& operator+=(const progress& rhs) {
                completed += rhs.completed;
                total += rhs.total;
                return *this;
            }

            friend progress operator+(progress lhs, const progress& rhs) {
                lhs += rhs;
                return lhs;
            }
        };

        struct status {
            task_id id;
            task_state state = task_state::created;
            db_clock::time_point start_time;
            db_clock::time_point end_time;
            std::string error;
            uint64_t sequence_number = 0;   // A running sequence number of the task.
            unsigned shard = 0;
            std::string scope;
            std::string keyspace;
            std::string table;
            std::string entity;             // Additional entity specific for the given type of task.
            std::string progress_units;     // A description of the units progress.
        };

        struct task_essentials {
            status task_status;
            progress task_progress;
            task_id parent_id;
            std::string type;
            is_abortable abortable;
            std::vector<task_essentials> failed_children;
        };

        class children {
            mutable foreign_task_map _children;
            mutable utils::chunked_vector<task_essentials> _finished_children;
            mutable rwlock _lock;
        public:
            bool all_finished() const noexcept;
            size_t size() const noexcept;
            future<> add_child(foreign_task_ptr task);
            future<> mark_as_finished(task_id id, task_essentials essentials) const;
            future<progress> get_progress(const std::string& progress_units) const;
            future<> for_each_task(std::function<future<>(const foreign_task_ptr&)> f_children,
                    std::function<future<>(const task_essentials&)> f_finished_children) const;

            // Make sure there is no race between map_children and the child's owner shard.
            template<typename Res>
            future<std::vector<Res>> map_each_task(std::function<std::optional<Res>(const foreign_task_ptr&)> map_children,
                    std::function<std::optional<Res>(const task_essentials&)> map_finished_children) const {
                auto shared_holder = co_await _lock.hold_read_lock();

                auto deopt = std::views::filter([] (const auto& task) { return bool(task); })
                        | std::views::transform([] (const auto& res) { return res.value(); });

                auto kids = _children | std::views::values | std::views::transform(map_children) | deopt;
                auto finished_kids = _finished_children | std::views::transform(map_finished_children) | deopt;
                std::vector<Res> result;
                // Want to use insert_range(), but libstd++ hasn't implemented it yet.
                result.insert(result.end(), kids.begin(), kids.end());
                result.insert(result.end(), finished_kids.begin(), finished_kids.end());
                co_return result;
            }
        };

        class impl {
        protected:
            status _status;
            task_id _parent_id;
            task_kind _parent_kind = task_kind::node;
            children _children;
            shared_promise<> _done;
            module_ptr _module;
            seastar::abort_source _as;
            optimized_optional<seastar::abort_source::subscription> _shutdown_subscription;
        public:
            impl(module_ptr module, task_id id, uint64_t sequence_number, std::string scope, std::string keyspace, std::string table, std::string entity, task_id parent_id) noexcept;
            // impl is always created as a smart pointer so it does not need to be moved or copied.
            impl(const impl&) = delete;
            impl(impl&&) = delete;
            virtual ~impl() = default;

            virtual std::string type() const = 0;
            virtual future<task_manager::task::progress> get_progress() const;
            virtual tasks::is_abortable is_abortable() const noexcept;
            virtual tasks::is_internal is_internal() const noexcept;
            virtual tasks::is_user_task is_user_task() const noexcept;
            virtual void abort() noexcept;
            bool is_complete() const noexcept;
            bool is_done() const noexcept;
            virtual future<> release_resources() noexcept {
                return make_ready_future();
            }
            future<std::vector<task_essentials>> get_failed_children() const;
            void set_virtual_parent() noexcept;
        protected:
            virtual future<> run() = 0;
            void run_to_completion();
            future<> maybe_fold_into_parent() const noexcept;
            future<> finish() noexcept;
            future<> finish_failed(std::exception_ptr ex, std::string error) noexcept;
            future<> finish_failed(std::exception_ptr ex) noexcept;
            virtual future<std::optional<double>> expected_total_workload() const;
            task_manager::task::progress get_binary_progress() const;

            friend task;
        };
        using task_impl_ptr = shared_ptr<impl>;
    protected:
        task_impl_ptr _impl;
    private:
        gate::holder _gate_holder;
    public:
        task(task_impl_ptr&& impl, gate::holder) noexcept;

        task_id id();
        std::string type() const;
        status& get_status() noexcept;
        uint64_t get_sequence_number() const noexcept;
        task_id get_parent_id() const noexcept;
        void change_state(task_state state) noexcept;
        future<> add_child(foreign_task_ptr&& child);
        void start();
        std::string get_module_name() const noexcept;
        module_ptr get_module() const noexcept;
        future<progress> get_progress() const;
        tasks::is_abortable is_abortable() const noexcept;
        tasks::is_internal is_internal() const noexcept;
        tasks::is_user_task is_user_task() const noexcept;
        void abort() noexcept;
        bool abort_requested() const noexcept;
        future<> done() const noexcept;
        void register_task();
        void unregister_task() noexcept;
        const children& get_children() const noexcept;
        bool is_complete() const noexcept;
        future<std::vector<task_essentials>> get_failed_children() const;
        void set_virtual_parent() noexcept;

        friend class test_task;
        friend class ::repair::task_manager_module;
    };

    class virtual_task : public enable_lw_shared_from_this<virtual_task> {
    public:
        class impl {
        protected:
            module_ptr _module;
        public:
            impl(module_ptr module) noexcept;
            impl(const impl&) = delete;
            impl& operator=(const impl&) = delete;
            impl(impl&&) = delete;
            impl& operator=(impl&&) = delete;
            virtual ~impl() = default;
        protected:
            static future<std::vector<task_identity>> get_children(module_ptr module, task_id parent_id, std::function<bool(locator::host_id)> is_host_alive);
        public:
            virtual task_group get_group() const noexcept = 0;
            // Returns std::nullopt if an operation with task_id isn't tracked by this virtual_task.
            // Returns empty virtual_task_hint if an operation with task_id is tracked by this virtual_task,
            // but no additional information about the task is passed.
            virtual future<std::optional<virtual_task_hint>> contains(tasks::task_id task_id) const = 0;
            module_ptr get_module() const noexcept;
            task_manager& get_task_manager() const noexcept;
            virtual future<tasks::is_abortable> is_abortable(virtual_task_hint hint) const;

            virtual future<std::optional<task_status>> get_status(task_id id, virtual_task_hint hint) = 0;
            virtual future<std::optional<task_status>> wait(task_id id, virtual_task_hint hint) = 0;
            virtual future<> abort(task_id id, virtual_task_hint hint) noexcept = 0;
            virtual future<std::vector<task_stats>> get_stats() = 0;
        };
        using virtual_task_impl_ptr = std::unique_ptr<impl>;
    private:
        virtual_task_impl_ptr _impl;
    public:
        virtual_task(virtual_task_impl_ptr&& impl) noexcept;

        future<std::optional<virtual_task_hint>> contains(tasks::task_id task_id) const;
        module_ptr get_module() const noexcept;
        task_group get_group() const noexcept;
        future<tasks::is_abortable> is_abortable(virtual_task_hint hint) const;

        future<std::optional<task_status>> get_status(task_id id, virtual_task_hint hint);
        future<std::optional<task_status>> wait(task_id id, virtual_task_hint hint);
        future<> abort(task_id id, virtual_task_hint hint) noexcept;
        future<std::vector<task_stats>> get_stats();
    };

    class module : public enable_shared_from_this<module> {
    protected:
        task_manager& _tm;
        std::string _name;
        tasks_collection _tasks;
        named_gate _gate;
        uint64_t _sequence_number = 0;
    private:
        abort_source _as;
        optimized_optional<abort_source::subscription> _abort_subscription;
    public:
        module(task_manager& tm, std::string name) noexcept;
        virtual ~module() = default;

        uint64_t new_sequence_number() noexcept;
        task_manager& get_task_manager() noexcept;
        const task_manager& get_task_manager() const noexcept;
        seastar::abort_source& abort_source() noexcept;
        named_gate& async_gate() noexcept;
        const std::string& get_name() const noexcept;
        task_manager::task_map& get_local_tasks() noexcept;
        const task_manager::task_map& get_local_tasks() const noexcept;
        task_manager::virtual_task_map& get_virtual_tasks() noexcept;
        const task_manager::virtual_task_map& get_virtual_tasks() const noexcept;
        tasks_collection& get_tasks_collection() noexcept;
        const tasks_collection& get_tasks_collection() const noexcept;
        // Returns a set of nodes on which some of virtual tasks on this module can have their children.
        virtual std::set<locator::host_id> get_nodes() const;
        future<utils::chunked_vector<task_stats>> get_stats(is_internal internal, std::function<bool(std::string&, std::string&)> filter) const;

        void register_task(task_ptr task);
        void register_virtual_task(virtual_task_ptr task);
        void unregister_task(task_id id) noexcept;
        virtual future<> stop() noexcept;
    public:
        // Must be called on target shard.
        // If task has a parent, data concerning its children is updated and sequence number is inherited
        // from a parent and set. Otherwise, it must be set by caller.
        future<task_ptr> make_task(task::task_impl_ptr task_impl_ptr, task_info parent_d = task_info{});

        // Must be called on target shard.
        template<typename TaskImpl, typename... Args>
        requires std::is_base_of_v<task::impl, TaskImpl> &&
        requires (module_ptr module, Args&&... args) {
            {TaskImpl(module, std::forward<Args>(args)...)} -> std::same_as<TaskImpl>;
        }
        future<task_ptr> make_and_start_task(tasks::task_info parent_info, Args&&... args) {
            auto task_impl_ptr = seastar::make_shared<TaskImpl>(shared_from_this(), std::forward<Args>(args)...);
            auto task = co_await make_task(std::move(task_impl_ptr), parent_info);
            task->start();
            co_return task;
        }

        // Must be called on target shard.
        template<typename VirtualTaskImpl, typename... Args>
        requires std::is_base_of_v<virtual_task::impl, VirtualTaskImpl> &&
        requires (module_ptr module, Args&&... args) {
            {VirtualTaskImpl(module, std::forward<Args>(args)...)} -> std::same_as<VirtualTaskImpl>;
        }
        void make_virtual_task(Args&&... args) {
            auto virtual_task_impl_ptr = std::make_unique<VirtualTaskImpl>(shared_from_this(), std::forward<Args>(args)...);
            auto vt = make_lw_shared<virtual_task>(std::move(virtual_task_impl_ptr));
            register_virtual_task(std::move(vt));
        }
    };
public:
    task_manager(config cfg, seastar::abort_source& as) noexcept;
    task_manager() noexcept;

    // Returns empty host_id if local info isn't resolved yet.
    locator::host_id get_host_id() const noexcept;
    void set_host_id(locator::host_id host_id) noexcept;
    modules& get_modules() noexcept;
    const modules& get_modules() const noexcept;
    task_map& get_local_tasks() noexcept;
    const task_map& get_local_tasks() const noexcept;
    virtual_task_map& get_virtual_tasks() noexcept;
    const virtual_task_map& get_virtual_tasks() const noexcept;
    tasks_collection& get_tasks_collection() noexcept;
    const tasks_collection& get_tasks_collection() const noexcept;
    future<std::vector<task_id>> get_virtual_task_children(task_id parent_id);

    std::set<locator::host_id> get_nodes(service::storage_service& ss) const;

    module_ptr make_module(std::string name);
    void register_module(std::string name, module_ptr module);
    module_ptr find_module(std::string module_name);
    future<> stop() noexcept;

    static future<task_manager::foreign_task_ptr> lookup_task_on_all_shards(sharded<task_manager>& tm, task_id tid);
    // Must be called from shard 0.
    static future<std::pair<task_manager::virtual_task_ptr, tasks::virtual_task_hint>> lookup_virtual_task(task_manager& tm, task_id id);
    static future<> invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<> (task_manager::task_variant, virtual_task_hint)> func);
    template<typename T>
    static future<T> invoke_on_task(sharded<task_manager>& tm, task_id id, std::function<future<T> (task_manager::task_variant, virtual_task_hint)> func) {
        std::optional<T> res;
        co_await coroutine::parallel_for_each(std::views::iota(0u, smp::count), [&tm, id, &res, &func] (unsigned shard) -> future<> {
            auto local_res = co_await tm.invoke_on(shard, [id, func] (const task_manager& local_tm) -> future<std::optional<T>> {
                const auto& all_tasks = local_tm.get_local_tasks();
                if (auto it = all_tasks.find(id); it != all_tasks.end()) {
                    co_return co_await func(it->second, {});
                }
                co_return std::nullopt;
            });
            if (!res) {
                res = std::move(local_res);
            } else if (local_res) {
                on_internal_error(tmlogger, format("task_id {} found on more than one shard", id));
            }
        });
        if (!res) {
            res = co_await tm.invoke_on(0, coroutine::lambda([id, &func] (auto& tm_local) -> future<std::optional<T>> {
                auto [task_ptr, hint] = co_await lookup_virtual_task(tm_local, id);
                if (task_ptr) {
                    co_return co_await func(task_ptr, std::move(hint));
                }
                co_return std::nullopt;
            }));
            if (!res) {
                co_await coroutine::return_exception(task_manager::task_not_found(id));
            }
        }
        co_return std::move(res.value());
    }

    seastar::abort_source& abort_source() noexcept;
public:
    std::chrono::seconds get_task_ttl() const noexcept;
    std::chrono::seconds get_user_task_ttl() const noexcept;
protected:
    void unregister_module(std::string name) noexcept;
    void register_task(task_ptr task);
    void register_virtual_task(virtual_task_ptr task);
    void unregister_task(task_id id) noexcept;
    void unregister_virtual_task(task_group group) noexcept;
public:
    void init_ms_handlers(netw::messaging_service& ms);
    future<> uninit_ms_handlers();
};

}
