/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/exception.hh>

#include "task_manager.hh"
#include "api/api.hh"
#include "api/api-doc/task_manager.json.hh"
#include "db/system_keyspace.hh"
#include "gms/gossiper.hh"
#include "tasks/task_handler.hh"
#include "utils/overloaded_functor.hh"

#include <utility>

namespace api {

namespace tm = httpd::task_manager_json;
using namespace json;
using namespace seastar::httpd;

static ::tm get_time(db_clock::time_point tp) {
    auto time = db_clock::to_time_t(tp);
    ::tm t;
    ::gmtime_r(&time, &t);
    return t;
}

tm::task_status make_status(tasks::task_status status, sharded<gms::gossiper>& gossiper) {
    std::vector<tm::task_identity> tis{status.children.size()};
    std::ranges::transform(status.children, tis.begin(), [&gossiper] (const auto& child) {
        tm::task_identity ident;
        gms::inet_address addr{};
        if (gossiper.local_is_initialized()) {
            addr = gossiper.local().get_address_map().find(child.host_id).value_or(gms::inet_address{});
        }
        ident.task_id = child.task_id.to_sstring();
        ident.node = fmt::format("{}", addr);
        return ident;
    });

    tm::task_status res{};
    res.id = status.task_id.to_sstring();
    res.type = status.type;
    res.kind = status.kind;
    res.scope = status.scope;
    res.state = status.state;
    res.is_abortable = bool(status.is_abortable);
    res.start_time = get_time(status.start_time);
    res.end_time = get_time(status.end_time);
    res.error = status.error;
    res.parent_id = status.parent_id ? status.parent_id.to_sstring() : "none";
    res.sequence_number = status.sequence_number;
    res.shard = status.shard;
    res.keyspace = status.keyspace;
    res.table = status.table;
    res.entity = status.entity;
    res.progress_units = status.progress_units;
    res.progress_total = status.progress.total;
    res.progress_completed = status.progress.completed;
    res.children_ids = std::move(tis);
    return res;
}

tm::task_stats make_stats(tasks::task_stats stats) {
    tm::task_stats res{};
    res.task_id = stats.task_id.to_sstring();
    res.type = stats.type;
    res.kind = stats.kind;
    res.scope = stats.scope;
    res.state = stats.state;
    res.sequence_number = stats.sequence_number;
    res.keyspace = stats.keyspace;
    res.table = stats.table;
    res.entity = stats.entity;
    res.shard = stats.shard;
    res.start_time = get_time(stats.start_time);
    res.end_time = get_time(stats.end_time);;
    return res;
}

void set_task_manager(http_context& ctx, routes& r, sharded<tasks::task_manager>& tm, db::config& cfg, sharded<gms::gossiper>& gossiper) {
    tm::get_modules.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        std::vector<std::string> v = tm.local().get_modules() | std::views::keys | std::ranges::to<std::vector>();
        co_return v;
    });

    tm::get_tasks.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        using chunked_stats = utils::chunked_vector<tasks::task_stats>;
        auto internal = tasks::is_internal{req_param<bool>(*req, "internal", false)};
        std::vector<chunked_stats> res = co_await tm.map([&req, internal] (tasks::task_manager& tm) {
            tasks::task_manager::module_ptr module;
            std::optional<std::string> keyspace = std::nullopt;
            std::optional<std::string> table = std::nullopt;
            try {
                module = tm.find_module(req->get_path_param("module"));
            } catch (...) {
                throw bad_param_exception(fmt::format("{}", std::current_exception()));
            }

            if (auto it = req->query_parameters.find("keyspace"); it != req->query_parameters.end()) {
                keyspace = it->second;
            }
            if (auto it = req->query_parameters.find("table"); it != req->query_parameters.end()) {
                table = it->second;
            }

            return module->get_stats(internal, [keyspace = std::move(keyspace), table = std::move(table)] (std::string& ks, std::string& t) {
                return (!keyspace || keyspace == ks) && (!table || table == t);
            });
        });

        std::function<future<>(output_stream<char>&&)> f = [r = std::move(res)] (output_stream<char>&& os) -> future<> {
            auto s = std::move(os);
            std::exception_ptr ex;
            try {
                auto res = std::move(r);
                co_await s.write("[");
                std::string delim = "";
                for (auto& v: res) {
                    for (auto& stats: v) {
                        co_await s.write(std::exchange(delim, ", "));
                        tm::task_stats ts = make_stats(stats);
                        co_await formatter::write(s, ts);
                    }
                }
                co_await s.write("]");
                co_await s.flush();
            } catch (...) {
                ex = std::current_exception();
            }
            co_await s.close();
            if (ex) {
                co_await coroutine::return_exception_ptr(std::move(ex));
            }
        };
        co_return std::move(f);
    });

    tm::get_task_status.set(r, [&tm, &gossiper] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        tasks::task_status status;
        try {
            auto task = tasks::task_handler{tm.local(), id};
            status = co_await task.get_status();
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        co_return make_status(status, gossiper);
    });

    tm::abort_task.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        try {
            auto task = tasks::task_handler{tm.local(), id};
            co_await task.abort();
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        } catch (tasks::task_not_abortable& e) {
            throw httpd::base_exception{e.what(), http::reply::status_type::forbidden};
        }
        co_return json_void();
    });

    tm::wait_task.set(r, [&tm, &gossiper] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        tasks::task_status status;
        std::optional<std::chrono::seconds> timeout = std::nullopt;
        if (auto it = req->query_parameters.find("timeout"); it != req->query_parameters.end()) {
            timeout = std::chrono::seconds(boost::lexical_cast<uint32_t>(it->second));
        }
        try {
            auto task = tasks::task_handler{tm.local(), id};
            status = co_await task.wait_for_task(timeout);
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        } catch (timed_out_error& e) {
            throw httpd::base_exception{e.what(), http::reply::status_type::request_timeout};
        }
        co_return make_status(status, gossiper);
    });

    tm::get_task_status_recursively.set(r, [&_tm = tm, &gossiper] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& tm = _tm;
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        try {
            auto task = tasks::task_handler{tm.local(), id};
            auto res = co_await task.get_status_recursively(true);

            std::function<future<>(output_stream<char>&&)> f = [r = std::move(res), &gossiper] (output_stream<char>&& os) -> future<> {
                auto s = std::move(os);
                auto res = std::move(r);
                co_await s.write("[");
                std::string delim = "";
                for (auto& status: res) {
                    co_await s.write(std::exchange(delim, ", "));
                    co_await formatter::write(s, make_status(status, gossiper));
                }
                co_await s.write("]");
                co_await s.close();
            };
            co_return f;
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
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

    tm::get_ttl.set(r, [&cfg] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        uint32_t ttl = cfg.task_ttl_seconds();
        co_return json::json_return_type(ttl);
    });

    tm::get_and_update_user_ttl.set(r, [&cfg] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        uint32_t user_ttl = cfg.user_task_ttl_seconds();
        try {
            co_await cfg.user_task_ttl_seconds.set_value_on_all_shards(req->query_parameters["user_ttl"], utils::config_file::config_source::API);
        } catch (...) {
            throw bad_param_exception(fmt::format("{}", std::current_exception()));
        }
        co_return json::json_return_type(user_ttl);
    });

    tm::get_user_ttl.set(r, [&cfg] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        uint32_t user_ttl = cfg.user_task_ttl_seconds();
        co_return json::json_return_type(user_ttl);
    });

    tm::drain_tasks.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await tm.invoke_on_all([&req] (tasks::task_manager& tm) -> future<> {
            tasks::task_manager::module_ptr module;
            try {
                module = tm.find_module(req->get_path_param("module"));
            } catch (...) {
                throw bad_param_exception(fmt::format("{}", std::current_exception()));
            }

            const auto& local_tasks = module->get_local_tasks();
            std::vector<tasks::task_id> ids;
            ids.reserve(local_tasks.size());
            std::transform(begin(local_tasks), end(local_tasks), std::back_inserter(ids), [] (const auto& task) {
                return task.second->is_complete() ? task.first : tasks::task_id::create_null_id();
            });

            for (auto&& id : ids) {
                if (id) {
                    module->unregister_task(id);
                }
                co_await maybe_yield();
            }
        });
        co_return json_void();
    });
}

void unset_task_manager(http_context& ctx, routes& r) {
    tm::get_modules.unset(r);
    tm::get_tasks.unset(r);
    tm::get_task_status.unset(r);
    tm::abort_task.unset(r);
    tm::wait_task.unset(r);
    tm::get_task_status_recursively.unset(r);
    tm::get_and_update_ttl.unset(r);
    tm::get_ttl.unset(r);
    tm::drain_tasks.unset(r);
}

}
