/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <fmt/ranges.h>

#include "api/api.hh"
#include "api/storage_service.hh"
#include "api/api-doc/tasks.json.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/task_manager_module.hh"
#include "service/storage_service.hh"
#include "tasks/task_manager.hh"

using namespace seastar::httpd;

extern logging::logger apilog;

namespace api {

namespace t = httpd::tasks_json;
using namespace json;

using ks_cf_func = std::function<future<json::json_return_type>(http_context&, std::unique_ptr<http::request>, sstring, std::vector<table_info>)>;

static auto wrap_ks_cf(http_context &ctx, ks_cf_func f) {
    return [&ctx, f = std::move(f)](std::unique_ptr<http::request> req) {
        auto keyspace = validate_keyspace(ctx, req);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        return f(ctx, std::move(req), std::move(keyspace), std::move(table_infos));
    };
}

void set_tasks_compaction_module(http_context& ctx, routes& r, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>& snap_ctl) {
    t::force_keyspace_compaction_async.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto params = req_params({
            std::pair("keyspace", mandatory::yes),
            std::pair("cf", mandatory::no),
            std::pair("flush_memtables", mandatory::no),
        });
        params.process(*req);
        auto keyspace = validate_keyspace(ctx, *params.get("keyspace"));
        auto table_infos = parse_table_infos(keyspace, ctx, params.get("cf").value_or(""));
        auto flush = params.get_as<bool>("flush_memtables").value_or(true);
        apilog.debug("force_keyspace_compaction_async: keyspace={} tables={}, flush={}", keyspace, table_infos, flush);

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        std::optional<flush_mode> fmopt;
        if (!flush) {
            fmopt = flush_mode::skip;
        }
        auto task = co_await compaction_module.make_and_start_task<major_keyspace_compaction_task_impl>({}, std::move(keyspace), tasks::task_id::create_null_id(), db, table_infos, fmopt);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    });

    t::force_keyspace_cleanup_async.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto keyspace = validate_keyspace(ctx, req);
        auto table_infos = parse_table_infos(keyspace, ctx, req->query_parameters, "cf");
        apilog.info("force_keyspace_cleanup_async: keyspace={} tables={}", keyspace, table_infos);
        if (!co_await ss.local().is_cleanup_allowed(keyspace)) {
            auto msg = "Can not perform cleanup operation when topology changes";
            apilog.warn("force_keyspace_cleanup_async: keyspace={} tables={}: {}", keyspace, table_infos, msg);
            co_await coroutine::return_exception(std::runtime_error(msg));
        }

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<cleanup_keyspace_compaction_task_impl>({}, std::move(keyspace), db, table_infos, flush_mode::all_tables);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    });

    t::perform_keyspace_offstrategy_compaction_async.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        apilog.info("perform_keyspace_offstrategy_compaction: keyspace={} tables={}", keyspace, table_infos);
        auto& compaction_module = ctx.db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<offstrategy_keyspace_compaction_task_impl>({}, std::move(keyspace), ctx.db, table_infos, nullptr);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    }));

    t::upgrade_sstables_async.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        auto& db = ctx.db;
        bool exclude_current_version = req_param<bool>(*req, "exclude_current_version", false);

        apilog.info("upgrade_sstables: keyspace={} tables={} exclude_current_version={}", keyspace, table_infos, exclude_current_version);

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<upgrade_sstables_compaction_task_impl>({}, std::move(keyspace), db, table_infos, exclude_current_version);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    }));

    t::scrub_async.set(r, [&ctx, &snap_ctl] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto info = co_await parse_scrub_options(ctx, snap_ctl, std::move(req));

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<scrub_sstables_compaction_task_impl>({}, std::move(info.keyspace), db, std::move(info.column_families), info.opts, nullptr);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    });
}

void unset_tasks_compaction_module(http_context& ctx, httpd::routes& r) {
    t::force_keyspace_compaction_async.unset(r);
    t::force_keyspace_cleanup_async.unset(r);
    t::perform_keyspace_offstrategy_compaction_async.unset(r);
    t::upgrade_sstables_async.unset(r);
    t::scrub_async.unset(r);
}

}
