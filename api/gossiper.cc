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

#include "gossiper.hh"
#include "api/api-doc/gossiper.json.hh"
#include <gms/gossiper.hh>

namespace api {
using namespace json;

void set_gossiper(http_context& ctx, routes& r) {
    httpd::gossiper_json::get_down_endpoint.set(r, [] (const_req req) {
        auto res = gms::get_local_gossiper().get_unreachable_members();
        return container_to_vec(res);
    });

    httpd::gossiper_json::get_live_endpoint.set(r, [] (const_req req) {
        auto res = gms::get_local_gossiper().get_live_members();
        return container_to_vec(res);
    });

    httpd::gossiper_json::get_endpoint_downtime.set(r, [] (const_req req) {
        gms::inet_address ep(req.param["addr"]);
        return gms::get_local_gossiper().get_endpoint_downtime(ep);
    });

    httpd::gossiper_json::get_current_generation_number.set(r, [] (std::unique_ptr<request> req) {
        gms::inet_address ep(req->param["addr"]);
        return gms::get_local_gossiper().get_current_generation_number(ep).then([] (int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    httpd::gossiper_json::get_current_heart_beat_version.set(r, [] (std::unique_ptr<request> req) {
        gms::inet_address ep(req->param["addr"]);
        return gms::get_local_gossiper().get_current_heart_beat_version(ep).then([] (int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    httpd::gossiper_json::assassinate_endpoint.set(r, [](std::unique_ptr<request> req) {
        if (req->get_query_param("unsafe") != "True") {
            return gms::get_local_gossiper().assassinate_endpoint(req->param["addr"]).then([] {
                return make_ready_future<json::json_return_type>(json_void());
            });
        }
        return gms::get_local_gossiper().unsafe_assassinate_endpoint(req->param["addr"]).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });
}

}
