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

#include "failure_detector.hh"
#include "api/api-doc/failure_detector.json.hh"
#include "gms/failure_detector.hh"
#include "gms/application_state.hh"
#include "gms/gossiper.hh"
namespace api {

namespace fd = httpd::failure_detector_json;

void set_failure_detector(http_context& ctx, routes& r) {
    fd::get_all_endpoint_states.set(r, [](std::unique_ptr<request> req) {
        std::vector<fd::endpoint_state> res;
        for (auto i : gms::get_local_gossiper().endpoint_state_map) {
            fd::endpoint_state val;
            val.addrs = boost::lexical_cast<std::string>(i.first);
            val.is_alive = i.second.is_alive();
            val.generation = i.second.get_heart_beat_state().get_generation();
            val.version = i.second.get_heart_beat_state().get_heart_beat_version();
            val.update_time = i.second.get_update_timestamp().time_since_epoch().count();
            for (auto a : i.second.get_application_state_map()) {
                fd::version_value version_val;
                // We return the enum index and not it's name to stay compatible to origin
                // method that the state index are static but the name can be changed.
                version_val.application_state = static_cast<std::underlying_type<gms::application_state>::type>(a.first);
                version_val.value = a.second.value;
                version_val.version = a.second.version;
                val.application_state.push(version_val);
            }
            res.push_back(val);
        }
        return make_ready_future<json::json_return_type>(res);
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

    fd::get_endpoint_phi_values.set(r, [](std::unique_ptr<request> req) {
        return gms::get_arrival_samples().then([](std::map<gms::inet_address, gms::arrival_window> map) {
            std::vector<fd::endpoint_phi_value> res;
            auto now = gms::arrival_window::clk::now();
            for (auto& p : map) {
                fd::endpoint_phi_value val;
                val.endpoint = p.first.to_sstring();
                val.phi = p.second.phi(now);
                res.emplace_back(std::move(val));
            }
            return make_ready_future<json::json_return_type>(res);
        });
    });
}

}

