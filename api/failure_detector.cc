/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "failure_detector.hh"
#include "api/api.hh"
#include "api/api-doc/failure_detector.json.hh"
#include "gms/application_state.hh"
#include "gms/gossiper.hh"

namespace api {
using namespace seastar::httpd;

namespace fd = httpd::failure_detector_json;

void set_failure_detector(http_context& ctx, routes& r, gms::gossiper& g) {
    fd::get_all_endpoint_states.set(r, [&g](std::unique_ptr<request> req) {
        return g.container().invoke_on(0, [] (gms::gossiper& g) {
            std::vector<fd::endpoint_state> res;
            res.reserve(g.num_endpoints());
            g.for_each_endpoint_state([&] (const gms::endpoint_state& eps) {
                fd::endpoint_state val;
                val.addrs = fmt::to_string(eps.get_ip());
                val.is_alive = g.is_alive(eps.get_host_id());
                val.generation = eps.get_heart_beat_state().get_generation().value();
                val.version = eps.get_heart_beat_state().get_heart_beat_version().value();
                val.update_time = eps.get_update_timestamp().time_since_epoch().count();
                for (const auto& [as_type, app_state] : eps.get_application_state_map()) {
                    fd::version_value version_val;
                    // We return the enum index and not it's name to stay compatible to origin
                    // method that the state index are static but the name can be changed.
                    version_val.application_state = static_cast<std::underlying_type<gms::application_state>::type>(as_type);
                    version_val.value = app_state.value();
                    version_val.version = app_state.version().value();
                    val.application_state.push(version_val);
                }
                res.emplace_back(std::move(val));
            });
            return make_ready_future<json::json_return_type>(res);
        });
    });

    fd::get_up_endpoint_count.set(r, [&g](std::unique_ptr<request> req) {
        return g.container().invoke_on(0, [] (gms::gossiper& g) {
            int res = g.get_up_endpoint_count();
            return make_ready_future<json::json_return_type>(res);
        });
    });

    fd::get_down_endpoint_count.set(r, [&g](std::unique_ptr<request> req) {
        return g.container().invoke_on(0, [] (gms::gossiper& g) {
            int res = g.get_down_endpoint_count();
            return make_ready_future<json::json_return_type>(res);
        });
    });

    fd::get_phi_convict_threshold.set(r, [] (std::unique_ptr<request> req) {
        return make_ready_future<json::json_return_type>(8);
    });

    fd::get_simple_states.set(r, [&g] (std::unique_ptr<request> req) {
        return g.container().invoke_on(0, [] (gms::gossiper& g) {
            std::vector<fd::mapper> nodes_status;
            nodes_status.reserve(g.num_endpoints());
            g.for_each_endpoint_state([&] (const gms::endpoint_state& es) {
                fd::mapper val;
                val.key = fmt::to_string(es.get_ip());
                val.value = g.is_alive(es.get_host_id()) ? "UP" : "DOWN";
                nodes_status.emplace_back(std::move(val));
            });
            return make_ready_future<json::json_return_type>(std::move(nodes_status));
        });
    });

    fd::set_phi_convict_threshold.set(r, [](std::unique_ptr<request> req) {
        // TBD
        unimplemented();
        std::ignore = atof(req->get_query_param("phi").c_str());
        return make_ready_future<json::json_return_type>("");
    });

    fd::get_endpoint_state.set(r, [&g] (std::unique_ptr<request> req) {
        return g.container().invoke_on(0, [req = std::move(req)] (gms::gossiper& g) {
            auto state = g.get_endpoint_state_ptr(g.get_host_id(gms::inet_address(req->get_path_param("addr"))));
            if (!state) {
                return make_ready_future<json::json_return_type>(format("unknown endpoint {}", req->get_path_param("addr")));
            }
            std::stringstream ss;
            g.append_endpoint_state(ss, *state);
            return make_ready_future<json::json_return_type>(sstring(ss.str()));
        });
    });

    fd::get_endpoint_phi_values.set(r, [](std::unique_ptr<request> req) {
        // We no longer have a phi failure detector,
        // just returning the empty value is good enough.
        std::vector<fd::endpoint_phi_value> res;
        return make_ready_future<json::json_return_type>(res);
    });
}

void unset_failure_detector(http_context& ctx, routes& r) {
    fd::get_all_endpoint_states.unset(r);
    fd::get_up_endpoint_count.unset(r);
    fd::get_down_endpoint_count.unset(r);
    fd::get_phi_convict_threshold.unset(r);
    fd::get_simple_states.unset(r);
    fd::set_phi_convict_threshold.unset(r);
    fd::get_endpoint_state.unset(r);
    fd::get_endpoint_phi_values.unset(r);
}

}

