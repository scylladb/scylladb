/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "commitlog.hh"
#include "db/commitlog/commitlog.hh"
#include "api/api-doc/commitlog.json.hh"
#include "api/api-doc/storage_service.json.hh"
#include "api/api_init.hh"
#include "replica/database.hh"
#include <vector>

namespace api {
using namespace seastar::httpd;
namespace ss = httpd::storage_service_json;

template<typename T>
static auto acquire_cl_metric(sharded<replica::database>& db, std::function<T (const db::commitlog*)> func) {
    typedef T ret_type;

    return db.map_reduce0([func = std::move(func)](replica::database& db) {
        if (db.commitlog() == nullptr) {
            return make_ready_future<ret_type>();
        }
        return make_ready_future<ret_type>(func(db.commitlog()));
    }, ret_type(), std::plus<ret_type>()).then([](ret_type res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

void set_commitlog(http_context& ctx, routes& r, sharded<replica::database>& db) {
    httpd::commitlog_json::get_active_segment_names.set(r,
            [&db](std::unique_ptr<request> req) {
        auto res = make_shared<std::vector<sstring>>();
        return db.map_reduce([res](std::vector<sstring> names) {
            res->insert(res->end(), names.begin(), names.end());
        }, [](replica::database& db) {
            if (db.commitlog() == nullptr) {
                return make_ready_future<std::vector<sstring>>(std::vector<sstring>());
            }
            return make_ready_future<std::vector<sstring>>(db.commitlog()->get_active_segment_names());
        }).then([res] {
            return make_ready_future<json::json_return_type>(*res.get());
        });
    });

    // We currently do not support archive segments
    httpd::commitlog_json::get_archiving_segment_names.set(r, [](const_req req) {
        std::vector<sstring> res;
        return res;
    });

    httpd::commitlog_json::get_completed_tasks.set(r, [&db](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(db, std::bind(&db::commitlog::get_completed_tasks, std::placeholders::_1));
    });

    httpd::commitlog_json::get_pending_tasks.set(r, [&db](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(db, std::bind(&db::commitlog::get_pending_tasks, std::placeholders::_1));
    });

    httpd::commitlog_json::get_total_commit_log_size.set(r, [&db](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(db, std::bind(&db::commitlog::get_total_size, std::placeholders::_1));
    });
    httpd::commitlog_json::get_max_disk_size.set(r, [&db](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(db, std::bind(&db::commitlog::disk_limit, std::placeholders::_1));
    });

    ss::get_commitlog.set(r, [&db](const_req req) {
        return db.local().commitlog()->active_config().commit_log_location;
    });

}

void unset_commitlog(http_context& ctx, routes& r) {
    httpd::commitlog_json::get_active_segment_names.unset(r);
    httpd::commitlog_json::get_archiving_segment_names.unset(r);
    httpd::commitlog_json::get_completed_tasks.unset(r);
    httpd::commitlog_json::get_pending_tasks.unset(r);
    httpd::commitlog_json::get_total_commit_log_size.unset(r);
    httpd::commitlog_json::get_max_disk_size.unset(r);
    ss::get_commitlog.unset(r);
}

}
