/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "failure_detector.hh"
#include "api/api-doc/failure_detector.json.hh"
#include "gms/application_state.hh"
#include "gms/gossiper.hh"

namespace api {
using namespace seastar::httpd;

namespace fd = httpd::failure_detector_json;

void set_failure_detector(http_context& ctx, routes& r, gms::gossiper& g) {
    fd::get_all_endpoint_states.set(r, [&g](std::unique_ptr<request> req) {
        std::vector<fd::endpoint_state> res;
        for (auto i : g.get_endpoint_states()) {
            fd::endpoint_state val;
            val.addrs = fmt::to_string(i.first);
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

    fd::get_up_endpoint_count.set(r, [&g](std::unique_ptr<request> req) {
        int res = g.get_up_endpoint_count();
        return make_ready_future<json::json_return_type>(res);
    });

    fd::get_down_endpoint_count.set(r, [&g](std::unique_ptr<request> req) {
        int res = g.get_down_endpoint_count();
        return make_ready_future<json::json_return_type>(res);
    });

    fd::get_phi_convict_threshold.set(r, [] (std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(8);
    });

    fd::get_simple_states.set(r, [&g] (std::unique_ptr<request> req) {
        std::map<sstring, sstring> nodes_status;
        for (auto& entry : g.get_endpoint_states()) {
            nodes_status.emplace(entry.first.to_sstring(), entry.second.is_alive() ? "UP" : "DOWN");
        }
        return make_ready_future<json::json_return_type>(map_to_key_value<fd::mapper>(nodes_status));
    });

    fd::set_phi_convict_threshold.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        std::ignore = atof(req->get_query_param("phi").c_str());
        return make_ready_future<json::json_return_type>("");
    });

    fd::get_endpoint_state.set(r, [&g] (std::unique_ptr<request> req) {
        auto* state = g.get_endpoint_state_for_endpoint_ptr(gms::inet_address(req->param["addr"]));
        if (!state) {
            return make_ready_future<json::json_return_type>(format("unknown endpoint {}", req->param["addr"]));
        }
        std::stringstream ss;
        g.append_endpoint_state(ss, *state);
        return make_ready_future<json::json_return_type>(sstring(ss.str()));
    });

    fd::get_endpoint_phi_values.set(r, [](std::unique_ptr<request> req) {
        // We no longer have a phi failure detector,
        // just returning the empty value is good enough.
        std::vector<fd::endpoint_phi_value> res;
        return make_ready_future<json::json_return_type>(res);
    });
}

}

