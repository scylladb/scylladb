/*
 * Copyright 2015 Cloudius Systems
 */

#include "commitlog.hh"
#include <db/commitlog/commitlog.hh>
#include "api/api-doc/commitlog.json.hh"
#include <vector>

namespace api {

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
}

}
