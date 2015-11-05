/*
 * Copyright 2015 Cloudius Systems
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

#include "compaction_manager.hh"
#include "api/api-doc/compaction_manager.json.hh"

namespace api {

using namespace scollectd;
namespace cm = httpd::compaction_manager_json;


static future<json::json_return_type> get_cm_stats(http_context& ctx,
        int64_t compaction_manager::stats::*f) {
    return ctx.db.map_reduce0([f](database& db) {
        return db.get_compaction_manager().get_stats().*f;
    }, int64_t(0), std::plus<int64_t>()).then([](const int64_t& res) {
        return make_ready_future<json::json_return_type>(res);
    });
}

void set_compaction_manager(http_context& ctx, routes& r) {
    cm::get_compactions.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        std::vector<cm::jsonmap> map;
        return make_ready_future<json::json_return_type>(map);
    });

    cm::get_compaction_summary.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    cm::force_user_defined_compaction.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>("");
    });

    cm::stop_compaction.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>("");
    });

    cm::get_pending_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cm_stats(ctx, &compaction_manager::stats::pending_tasks);
    });

    cm::get_completed_tasks.set(r, [&ctx] (std::unique_ptr<request> req) {
        return get_cm_stats(ctx, &compaction_manager::stats::completed_tasks);
    });

    cm::get_total_compactions_completed.set(r, [] (std::unique_ptr<request> req) {
        // FIXME
        // We are currently dont have an API for compaction
        // so returning a 0 as the number of total compaction is ok
        return make_ready_future<json::json_return_type>(0);
    });

    cm::get_bytes_compacted.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        return make_ready_future<json::json_return_type>(0);
    });

    cm::get_compaction_history.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        std::vector<cm::history> res;
        return make_ready_future<json::json_return_type>(res);
    });

}

}

