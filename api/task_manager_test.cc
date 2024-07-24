/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#ifndef SCYLLA_BUILD_MODE_RELEASE

#include <seastar/core/coroutine.hh>

#include "task_manager_test.hh"
#include "api/api-doc/task_manager_test.json.hh"
#include "tasks/test_module.hh"
#include "utils/overloaded_functor.hh"

namespace api {

namespace tmt = httpd::task_manager_test_json;
using namespace json;
using namespace seastar::httpd;

void set_task_manager_test(http_context& ctx, routes& r, sharded<tasks::task_manager>& tm) {
    tmt::register_test_module.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await tm.invoke_on_all([] (tasks::task_manager& tm) {
            auto m = make_shared<tasks::test_module>(tm);
            tm.register_module("test", m);
        });
        co_return json_void();
    });

    tmt::unregister_test_module.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await tm.invoke_on_all([] (tasks::task_manager& tm) -> future<> {
            auto module_name = "test";
            auto module = tm.find_module(module_name);
            co_await module->stop();
        });
        co_return json_void();
    });

    tmt::register_test_task.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        sharded<tasks::task_manager>& tms = tm;
        auto it = req->query_parameters.find("task_id");
        auto id = it != req->query_parameters.end() ? tasks::task_id{utils::UUID{it->second}} : tasks::task_id::create_null_id();
        it = req->query_parameters.find("shard");
        unsigned shard = it != req->query_parameters.end() ? boost::lexical_cast<unsigned>(it->second) : 0;
        it = req->query_parameters.find("keyspace");
        std::string keyspace = it != req->query_parameters.end() ? it->second : "";
        it = req->query_parameters.find("table");
        std::string table = it != req->query_parameters.end() ? it->second : "";
        it = req->query_parameters.find("entity");
        std::string entity = it != req->query_parameters.end() ? it->second : "";
        it = req->query_parameters.find("parent_id");
        tasks::task_info data;
        if (it != req->query_parameters.end()) {
            data.id = tasks::task_id{utils::UUID{it->second}};
            auto parent_ptr = co_await tasks::task_manager::lookup_task_on_all_shards(tm, data.id);
            data.shard = parent_ptr->get_status().shard;
        }

        auto module = tms.local().find_module("test");
        id = co_await module->make_task<tasks::test_task_impl>(shard, id, keyspace, table, entity, data);
        co_await tms.invoke_on(shard, [id] (tasks::task_manager& tm) {
            auto it = tm.get_local_tasks().find(id);
            if (it != tm.get_local_tasks().end()) {
                it->second->start();
            }
        });
        co_return id.to_sstring();
    });

    tmt::unregister_test_task.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->query_parameters["task_id"]}};
        try {
            co_await tasks::task_manager::invoke_on_task(tm, id, [] (tasks::task_manager::task_variant task_v) -> future<> {
                return std::visit(overloaded_functor{
                    [] (tasks::task_manager::task_ptr task) -> future<> {
                        tasks::test_task test_task{task};
                        co_await test_task.unregister_task();
                    },
                    [] (tasks::task_manager::virtual_task_ptr task) {
                        return make_ready_future();
                    }
                }, task_v);
            });
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        co_return json_void();
    });

    tmt::finish_test_task.set(r, [&tm] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto id = tasks::task_id{utils::UUID{req->get_path_param("task_id")}};
        auto it = req->query_parameters.find("error");
        bool fail = it != req->query_parameters.end();
        std::string error = fail ? it->second : "";

        try {
            co_await tasks::task_manager::invoke_on_task(tm, id, [fail, error = std::move(error)] (tasks::task_manager::task_variant task_v) -> future<> {
                return std::visit(overloaded_functor{
                    [fail, error = std::move(error)] (tasks::task_manager::task_ptr task) -> future<> {
                        tasks::test_task test_task{task};
                        if (fail) {
                            co_await test_task.finish_failed(std::make_exception_ptr(std::runtime_error(error)));
                        } else {
                            co_await test_task.finish();
                        }
                    },
                    [] (tasks::task_manager::virtual_task_ptr task) {
                        return make_ready_future();
                    }
                }, task_v);
            });
        } catch (tasks::task_manager::task_not_found& e) {
            throw bad_param_exception(e.what());
        }
        co_return json_void();
    });
}

void unset_task_manager_test(http_context& ctx, routes& r) {
    tmt::register_test_module.unset(r);
    tmt::unregister_test_module.unset(r);
    tmt::register_test_task.unset(r);
    tmt::unregister_test_task.unset(r);
    tmt::finish_test_task.unset(r);
}

}

#endif
