/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>
#include <fmt/ranges.h>

#include "api/api.hh"
#include "api/storage_service.hh"
#include "api/api-doc/tasks.json.hh"
#include "api/api-doc/storage_service.json.hh"
#include "compaction/compaction_manager.hh"
#include "compaction/task_manager_module.hh"
#include "service/storage_service.hh"
#include "tasks/task_manager.hh"
#include "replica/database.hh"

using namespace seastar::httpd;

extern logging::logger apilog;

namespace api {

namespace t = httpd::tasks_json;
namespace ss = httpd::storage_service_json;
using namespace json;

using ks_cf_func = std::function<future<json::json_return_type>(http_context&, std::unique_ptr<http::request>, sstring, std::vector<table_info>)>;

static auto wrap_ks_cf(http_context &ctx, ks_cf_func f) {
    return [&ctx, f = std::move(f)](std::unique_ptr<http::request> req) {
        auto [keyspace, table_infos] = parse_table_infos(ctx, *req);
        return f(ctx, std::move(req), std::move(keyspace), std::move(table_infos));
    };
}

static future<shared_ptr<compaction::major_keyspace_compaction_task_impl>> force_keyspace_compaction(http_context& ctx, std::unique_ptr<http::request> req) {
    auto& db = ctx.db;
    auto [ keyspace, table_infos ] = parse_table_infos(ctx, *req, "cf");
    auto flush = validate_bool_x(req->get_query_param("flush_memtables"), true);
    auto consider_only_existing_data = validate_bool_x(req->get_query_param("consider_only_existing_data"), false);
    apilog.info("force_keyspace_compaction: keyspace={} tables={}, flush={} consider_only_existing_data={}", keyspace, table_infos, flush, consider_only_existing_data);

    auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
    std::optional<compaction::flush_mode> fmopt;
    if (!flush && !consider_only_existing_data) {
        fmopt = compaction::flush_mode::skip;
    }
    return compaction_module.make_and_start_task<compaction::major_keyspace_compaction_task_impl>({}, std::move(keyspace), tasks::task_id::create_null_id(), db, table_infos, fmopt, consider_only_existing_data);
}

static future<shared_ptr<compaction::upgrade_sstables_compaction_task_impl>> upgrade_sstables(http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) {
    auto& db = ctx.db;
    bool exclude_current_version = req_param<bool>(*req, "exclude_current_version", false);

    apilog.info("upgrade_sstables: keyspace={} tables={} exclude_current_version={}", keyspace, table_infos, exclude_current_version);

    auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
    return compaction_module.make_and_start_task<compaction::upgrade_sstables_compaction_task_impl>({}, std::move(keyspace), db, table_infos, exclude_current_version);
}

static future<shared_ptr<compaction::cleanup_keyspace_compaction_task_impl>> force_keyspace_cleanup(http_context& ctx, sharded<service::storage_service>& ss, std::unique_ptr<http::request> req) {
    auto& db = ctx.db;
    auto [keyspace, table_infos] = parse_table_infos(ctx, *req);
    const auto& rs = db.local().find_keyspace(keyspace).get_replication_strategy();
    if (rs.is_local() || !rs.is_vnode_based()) {
        auto reason = rs.is_local() ? "require" : "support";
        apilog.info("Keyspace {} does not {} cleanup", keyspace, reason);
        co_return nullptr;
    }
    apilog.info("force_keyspace_cleanup: keyspace={} tables={}", keyspace, table_infos);
    if (!co_await ss.local().is_vnodes_cleanup_allowed(keyspace)) {
        auto msg = "Can not perform cleanup operation when topology changes";
        apilog.warn("force_keyspace_cleanup: keyspace={} tables={}: {}", keyspace, table_infos, msg);
        co_await coroutine::return_exception(std::runtime_error(msg));
    }

    auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
    co_return co_await compaction_module.make_and_start_task<compaction::cleanup_keyspace_compaction_task_impl>(
        {}, std::move(keyspace), db, table_infos, compaction::flush_mode::all_tables, tasks::is_user_task::yes);
}

void set_tasks_compaction_module(http_context& ctx, routes& r, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>& snap_ctl) {
    t::force_keyspace_compaction_async.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto task = co_await force_keyspace_compaction(ctx, std::move(req));
        co_return json::json_return_type(task->get_status().id.to_sstring());
    });

    ss::force_keyspace_compaction.set(r, [&ctx](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto task = co_await force_keyspace_compaction(ctx, std::move(req));
        co_await task->done();
        co_return json_void();
    });

    t::force_keyspace_cleanup_async.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        tasks::task_id id = tasks::task_id::create_null_id();
        auto task = co_await force_keyspace_cleanup(ctx, ss, std::move(req));
        if (task) {
            id = task->get_status().id;
        }
        co_return json::json_return_type(id.to_sstring());
    });

    ss::force_keyspace_cleanup.set(r, [&ctx, &ss](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto task = co_await force_keyspace_cleanup(ctx, ss, std::move(req));
        if (task) {
            co_await task->done();
        }
        co_return json::json_return_type(0);
    });

    t::perform_keyspace_offstrategy_compaction_async.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        apilog.info("perform_keyspace_offstrategy_compaction: keyspace={} tables={}", keyspace, table_infos);
        auto& compaction_module = ctx.db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<compaction::offstrategy_keyspace_compaction_task_impl>({}, std::move(keyspace), ctx.db, table_infos, nullptr);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    }));

    ss::perform_keyspace_offstrategy_compaction.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        apilog.info("perform_keyspace_offstrategy_compaction: keyspace={} tables={}", keyspace, table_infos);
        bool res = false;
        auto& compaction_module = ctx.db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<compaction::offstrategy_keyspace_compaction_task_impl>({}, std::move(keyspace), ctx.db, table_infos, &res);
        co_await task->done();
        co_return json::json_return_type(res);
    }));

    t::upgrade_sstables_async.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        auto task = co_await upgrade_sstables(ctx, std::move(req), std::move(keyspace), std::move(table_infos));
        co_return json::json_return_type(task->get_status().id.to_sstring());
    }));

    ss::upgrade_sstables.set(r, wrap_ks_cf(ctx, [] (http_context& ctx, std::unique_ptr<http::request> req, sstring keyspace, std::vector<table_info> table_infos) -> future<json::json_return_type> {
        auto task = co_await upgrade_sstables(ctx, std::move(req), std::move(keyspace), std::move(table_infos));
        co_await task->done();
        co_return json::json_return_type(0);
    }));

    t::scrub_async.set(r, [&ctx, &snap_ctl] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto info = parse_scrub_options(ctx, std::move(req));

        if (!info.snapshot_tag.empty()) {
            db::snapshot_options opts = {.skip_flush = false};
            co_await snap_ctl.local().take_column_family_snapshot(info.keyspace, info.column_families, info.snapshot_tag, opts);
        }

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        auto task = co_await compaction_module.make_and_start_task<compaction::scrub_sstables_compaction_task_impl>({}, std::move(info.keyspace), db, std::move(info.column_families), info.opts, nullptr);

        co_return json::json_return_type(task->get_status().id.to_sstring());
    });

    ss::force_compaction.set(r, [&ctx] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto& db = ctx.db;
        auto flush = validate_bool_x(req->get_query_param("flush_memtables"), true);
        auto consider_only_existing_data = validate_bool_x(req->get_query_param("consider_only_existing_data"), false);
        apilog.info("force_compaction: flush={} consider_only_existing_data={}", flush, consider_only_existing_data);

        auto& compaction_module = db.local().get_compaction_manager().get_task_manager_module();
        std::optional<compaction::flush_mode> fmopt;
        if (!flush && !consider_only_existing_data) {
            fmopt = compaction::flush_mode::skip;
        }
        auto task = co_await compaction_module.make_and_start_task<compaction::global_major_compaction_task_impl>({}, db, fmopt, consider_only_existing_data);
        co_await task->done();
        co_return json_void();
    });
}

void unset_tasks_compaction_module(http_context& ctx, httpd::routes& r) {
    t::force_keyspace_compaction_async.unset(r);
    ss::force_keyspace_compaction.unset(r);
    t::force_keyspace_cleanup_async.unset(r);
    ss::force_keyspace_cleanup.unset(r);
    t::perform_keyspace_offstrategy_compaction_async.unset(r);
    ss::perform_keyspace_offstrategy_compaction.unset(r);
    t::upgrade_sstables_async.unset(r);
    ss::upgrade_sstables.unset(r);
    t::scrub_async.unset(r);
    ss::force_compaction.unset(r);
}

}
