/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "task_manager.hh"
#include "api/api-doc/task_manager.json.hh"
#include "db/system_keyspace.hh"
#include "column_family.hh"
#include "unimplemented.hh"
#include "storage_service.hh"

#include <utility>
#include <boost/range/adaptors.hpp>

namespace api {

namespace tm = httpd::task_manager_json;
using namespace json;
using namespace seastar::httpd;

inline bool filter_tasks(tasks::task_manager::task_ptr task, std::unordered_map<sstring, sstring>& query_params) {
    return (!query_params.contains("keyspace") || query_params["keyspace"] == task->get_status().keyspace) &&
        (!query_params.contains("table") || query_params["table"] == task->get_status().table);
}

struct full_task_status {
    tasks::task_manager::task::status task_status;
    std::string type;
    tasks::task_manager::task::progress progress;
    std::string module;
    tasks::task_id parent_id;
    tasks::is_abortable abortable;
    std::vector<std::string> children_ids;
};

struct task_stats {
    task_stats(tasks::task_manager::task_ptr task)
        : task_id(task->id().to_sstring())
        , state(task->get_status().state)
        , type(task->type())
        , scope(task->get_status().scope)
        , keyspace(task->get_status().keyspace)
        , table(task->get_status().table)
        , entity(task->get_status().entity)
        , sequence_number(task->get_status().sequence_number)
    { }

    sstring task_id;
    tasks::task_manager::task_state state;
    std::string type;
    std::string scope;
    std::string keyspace;
    std::string table;
    std::string entity;
    uint64_t sequence_number;
};

tm::task_status make_status(full_task_status status) {
    auto start_time = db_clock::to_time_t(status.task_status.start_time);
    auto end_time = db_clock::to_time_t(status.task_status.end_time);
    ::tm st, et;
    ::gmtime_r(&end_time, &et);
    ::gmtime_r(&start_time, &st);

    tm::task_status res{};
    res.id = status.task_status.id.to_sstring();
    res.type = status.type;
    res.scope = status.task_status.scope;
    res.state = status.task_status.state;
    res.is_abortable = bool(status.abortable);
    res.start_time = st;
    res.end_time = et;
    res.error = status.task_status.error;
    res.parent_id = status.parent_id.to_sstring();
    res.sequence_number = status.task_status.sequence_number;
    res.shard = status.task_status.shard;
    res.keyspace = status.task_status.keyspace;
    res.table = status.task_status.table;
    res.entity = status.task_status.entity;
    res.progress_units = status.task_status.progress_units;
    res.progress_total = status.progress.total;
    res.progress_completed = status.progress.completed;
    res.children_ids = std::move(status.children_ids);
    return res;
}

future<full_task_status> retrieve_status(const tasks::task_manager::foreign_task_ptr& task) {
    if (task.get() == nullptr) {
        co_return coroutine::return_exception(httpd::bad_param_exception("Task not found"));
    }
    auto progress = co_await task->get_progress();
    full_task_status s;
    s.task_status = task->get_status();
    s.type = task->type();
    s.parent_id = task->get_parent_id();
    s.abortable = task->is_abortable();
    s.module = task->get_module_name();
    s.progress.completed = progress.completed;
    s.progress.total = progress.total;
    std::vector<std::string> ct{task->get_children().size()};
    boost::transform(task->get_children(), ct.begin(), [] (const auto& child) {
        return child->id().to_sstring();
    });
    s.children_ids = std::move(ct);
    co_return s;
}

void set_task_manager(http_context& ctx, routes& r, db::config& cfg) {
    tm::get_modules.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        std::vector<std::string> v = boost::copy_range<std::vector<std::string>>(ctx.tm.local().get_modules() | boost::adaptors::map_keys);
        co_return v;
    });

    tm::get_tasks.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        using chunked_stats = utils::chunked_vector<task_stats>;
        auto internal = tasks::is_internal{req_param<bool>(*req, "internal", false)};
        std::vector<chunked_stats> res = co_await ctx.tm.map([&req, internal] (tasks::task_manager& tm) {
            chunked_stats local_res;
            tasks::task_manager::module_ptr module;
            try {
                module = tm.find_module(req->param["module"]);
            } catch (...) {
                throw bad_param_exception(fmt::format("{}", std::current_exception()));
            }
            const auto& filtered_tasks = module->get_tasks() | boost::adaptors::filtered([&params = req->query_parameters, internal] (const auto& task) {
                return (internal || !task.second->is_internal()) && filter_tasks(task.second, params);
            });
            for (auto& [task_id, task] : filtered_tasks) {
                local_res.push_back(task_stats{task});
            }
            return local_res;
        });

        std::function<future<>(output_stream<char>&&)> f = [r = std::move(res)] (output_stream<char>&& os) -> future<> {
            auto s = std::move(os);
            auto res = std::move(r);
            co_await s.write("[");
            std::string delim = "";
            for (auto& v: res) {
                for (auto& stats: v) {
                    co_await s.write(std::exchange(delim, ", "));
                    tm::task_stats ts;
                    ts = stats;
                    co_await formatter::write(s, ts);
                }
            }
            co_await s.write("]");
            co_await s.close();
        };
        co_return std::move(f);
    });

    tm::get_task_status.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->param["task_id"]}};
        tasks::task_manager::foreign_task_ptr task;
        try {
            task = co_await tasks::task_manager::invoke_on_task(ctx.tm, id, std::function([] (tasks::task_manager::task_ptr task) -> future<tasks::task_manager::foreign_task_ptr> {
                if (task->is_complete()) {
                    task->unregister_task();
                }
                co_return std::move(task);
            }));
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        auto s = co_await retrieve_status(task);
        co_return make_status(s);
    });

    tm::abort_task.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->param["task_id"]}};
        try {
            co_await tasks::task_manager::invoke_on_task(ctx.tm, id, [] (tasks::task_manager::task_ptr task) -> future<> {
                if (!task->is_abortable()) {
                    co_await coroutine::return_exception(std::runtime_error("Requested task cannot be aborted"));
                }
                co_await task->abort();
            });
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        co_return json_void();
    });

    tm::wait_task.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->param["task_id"]}};
        tasks::task_manager::foreign_task_ptr task;
        try {
            task = co_await tasks::task_manager::invoke_on_task(ctx.tm, id, std::function([] (tasks::task_manager::task_ptr task) {
                return task->done().then_wrapped([task] (auto f) {
                    task->unregister_task();
                    // done() is called only because we want the task to be complete before getting its status.
                    // The future should be ignored here as the result does not matter.
                    f.ignore_ready_future();
                    return make_foreign(task);
                });
            }));
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        auto s = co_await retrieve_status(task);
        co_return make_status(s);
    });

    tm::get_task_status_recursively.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& _ctx = ctx;
        auto id = tasks::task_id{utils::UUID{req->param["task_id"]}};
        std::queue<tasks::task_manager::foreign_task_ptr> q;
        utils::chunked_vector<full_task_status> res;

        tasks::task_manager::foreign_task_ptr task;
        try {
            // Get requested task.
            task = co_await tasks::task_manager::invoke_on_task(_ctx.tm, id, std::function([] (tasks::task_manager::task_ptr task) -> future<tasks::task_manager::foreign_task_ptr> {
                if (task->is_complete()) {
                    task->unregister_task();
                }
                co_return task;
            }));
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }

        // Push children's statuses in BFS order.
        q.push(co_await task.copy());   // Task cannot be moved since we need it to be alive during whole loop execution.
        while (!q.empty()) {
            auto& current = q.front();
            res.push_back(co_await retrieve_status(current));
            for (auto& child: current->get_children()) {
                q.push(co_await child.copy());
            }
            q.pop();
        }

        std::function<future<>(output_stream<char>&&)> f = [r = std::move(res)] (output_stream<char>&& os) -> future<> {
            auto s = std::move(os);
            auto res = std::move(r);
            co_await s.write("[");
            std::string delim = "";
            for (auto& status: res) {
                co_await s.write(std::exchange(delim, ", "));
                co_await formatter::write(s, make_status(status));
            }
            co_await s.write("]");
            co_await s.close();
        };
        co_return f;
    });

    tm::get_and_update_ttl.set(r, [&cfg] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        uint32_t ttl = cfg.task_ttl_seconds();
        try {
            co_await cfg.task_ttl_seconds.set_value_on_all_shards(req->query_parameters["ttl"], utils::config_file::config_source::API);
        } catch (...) {
            throw bad_param_exception(fmt::format("{}", std::current_exception()));
        }
        co_return json::json_return_type(ttl);
    });
}

}
