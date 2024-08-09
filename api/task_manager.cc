/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/exception.hh>

#include "task_manager.hh"
#include "api/api.hh"
#include "api/api-doc/task_manager.json.hh"
#include "db/system_keyspace.hh"
#include "tasks/task_handler.hh"
#include "utils/overloaded_functor.hh"

#include <utility>
#include <boost/range/adaptors.hpp>

namespace api {

namespace tm = httpd::task_manager_json;
using namespace json;
using namespace seastar::httpd;

tm::task_status make_status(tasks::task_status status) {
    auto start_time = db_clock::to_time_t(status.start_time);
    auto end_time = db_clock::to_time_t(status.end_time);
    ::tm st, et;
    ::gmtime_r(&end_time, &et);
    ::gmtime_r(&start_time, &st);

    std::vector<tm::task_identity> tis{status.children.size()};
    boost::transform(status.children, tis.begin(), [] (const auto& child) {
        tm::task_identity ident;
        ident.task_id = child.task_id.to_sstring();
        ident.node = fmt::format("{}", child.node);
        return ident;
    });

    tm::task_status res{};
    res.id = status.task_id.to_sstring();
    res.type = status.type;
    res.kind = status.kind;
    res.scope = status.scope;
    res.state = status.state;
    res.is_abortable = bool(status.is_abortable);
    res.start_time = st;
    res.end_time = et;
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
    return res;
}

void set_task_manager(http_context& ctx, routes& r, sharded<tasks::task_manager>& tm, db::config& cfg) {
    tm::get_modules.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        std::vector<std::string> v = boost::copy_range<std::vector<std::string>>(tm.local().get_modules() | boost::adaptors::map_keys);
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

    tm::get_task_status.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        tasks::task_status status;
        try {
            auto task = tasks::task_handler{tm.local(), id};
            status = co_await task.get_status();
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        co_return make_status(status);
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

    tm::wait_task.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
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
        co_return make_status(status);
    });

    tm::get_task_status_recursively.set(r, [&_tm = tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& tm = _tm;
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        try {
            auto task = tasks::task_handler{tm.local(), id};
            auto res = co_await task.get_status_recursively(true);

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
}

}
