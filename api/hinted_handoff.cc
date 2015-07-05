/*
 * Copyright 2015 Cloudius Systems
 */

#include "hinted_handoff.hh"
#include "api/api-doc/hinted_handoff.json.hh"

namespace api {

using namespace scollectd;
namespace hh = httpd::hinted_handoff_json;

void set_hinted_handoff(http_context& ctx, routes& r) {
    hh::list_endpoints_pending_hints.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    hh::truncate_all_hints.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>("");
    });

    hh::schedule_hint_delivery.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>("");
    });

    hh::pause_hints_delivery.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        sstring pause = req->get_query_param("pause");
        return make_ready_future<json::json_return_type>("");
    });
}

}

