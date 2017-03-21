/*
 * Copyright (C) 2015 ScyllaDB
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

#include "hinted_handoff.hh"
#include "api/api-doc/hinted_handoff.json.hh"

namespace api {

using namespace json;
namespace hh = httpd::hinted_handoff_json;

void set_hinted_handoff(http_context& ctx, routes& r) {
    hh::list_endpoints_pending_hints.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    hh::truncate_all_hints.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::schedule_hint_delivery.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::pause_hints_delivery.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring pause = req->get_query_param("pause");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::get_create_hint_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(0);
    });

    hh::get_not_stored_hints_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(0);
    });
}

}

