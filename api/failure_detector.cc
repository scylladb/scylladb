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

#include "failure_detector.hh"
#include "api/api-doc/failure_detector.json.hh"
#include "gms/failure_detector.hh"
namespace api {

namespace fd = httpd::failure_detector_json;

void set_failure_detector(http_context& ctx, routes& r) {
    fd::get_all_endpoint_states.set(r, [](std::unique_ptr<request> req) {
        return gms::get_all_endpoint_states().then([](const sstring& str) {
            return make_ready_future<json::json_return_type>(str);
        });
    });

    fd::get_up_endpoint_count.set(r, [](std::unique_ptr<request> req) {
        return gms::get_up_endpoint_count().then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    fd::get_down_endpoint_count.set(r, [](std::unique_ptr<request> req) {
        return gms::get_down_endpoint_count().then([](int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    fd::get_phi_convict_threshold.set(r, [] (std::unique_ptr<request> req) {
        return gms::get_phi_convict_threshold().then([](double res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    fd::get_simple_states.set(r, [] (std::unique_ptr<request> req) {
        return gms::get_simple_states().then([](const std::map<sstring, sstring>& map) {
            return make_ready_future<json::json_return_type>(map_to_key_value<fd::mapper>(map));
        });
    });

    fd::set_phi_convict_threshold.set(r, [](std::unique_ptr<request> req) {
        double phi = atof(req->get_query_param("phi").c_str());
        return gms::set_phi_convict_threshold(phi).then([]() {
            return make_ready_future<json::json_return_type>("");
        });
    });

    fd::get_endpoint_state.set(r, [](std::unique_ptr<request> req) {
        return gms::get_endpoint_state(req->param["addr"]).then([](const sstring& state) {
            return make_ready_future<json::json_return_type>(state);
        });
    });
}

}

