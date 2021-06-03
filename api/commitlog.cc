/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "commitlog.hh"
#include "db/commitlog/commitlog.hh"
#include "api/api-doc/commitlog.json.hh"
#include "database.hh"
#include <vector>

namespace api {

template<typename T>
static auto acquire_cl_metric(http_context& ctx, std::function<T (db::commitlog*)> func) {
    typedef T ret_type;

    return ctx.db.map_reduce0([func = std::move(func)](database& db) {
        if (db.commitlog() == nullptr) {
            return make_ready_future<ret_type>();
        }
        return make_ready_future<ret_type>(func(db.commitlog()));
    }, ret_type(), std::plus<ret_type>()).then([](ret_type res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

void set_commitlog(http_context& ctx, routes& r) {
    httpd::commitlog_json::get_active_segment_names.set(r,
            [&ctx](std::unique_ptr<request> req) {
        auto res = make_shared<std::vector<sstring>>();
        return ctx.db.map_reduce([res](std::vector<sstring> names) {
            res->insert(res->end(), names.begin(), names.end());
        }, [](database& db) {
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

    httpd::commitlog_json::get_completed_tasks.set(r, [&ctx](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(ctx, std::bind(&db::commitlog::get_completed_tasks, std::placeholders::_1));
    });

    httpd::commitlog_json::get_pending_tasks.set(r, [&ctx](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(ctx, std::bind(&db::commitlog::get_pending_tasks, std::placeholders::_1));
    });

    httpd::commitlog_json::get_total_commit_log_size.set(r, [&ctx](std::unique_ptr<request> req) {
        return acquire_cl_metric<uint64_t>(ctx, std::bind(&db::commitlog::get_total_size, std::placeholders::_1));
    });
}

}
