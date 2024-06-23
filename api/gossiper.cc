/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "gossiper.hh"
#include "api/api-doc/gossiper.json.hh"
#include "gms/endpoint_state.hh"
#include "gms/gossiper.hh"
#include "api/api.hh"

namespace api {
using namespace seastar::httpd;
using namespace json;

void set_gossiper(http_context& ctx, routes& r, gms::gossiper& g) {
    httpd::gossiper_json::get_down_endpoint.set(r, [&g] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto res = co_await g.get_unreachable_members_synchronized();
        co_return json::json_return_type(container_to_vec(res));
    });


    httpd::gossiper_json::get_live_endpoint.set(r, [&g] (std::unique_ptr<request> req) {
        return g.get_live_members_synchronized().then([] (auto res) {
            return make_ready_future<json::json_return_type>(container_to_vec(res));
        });
    });

    httpd::gossiper_json::get_endpoint_downtime.set(r, [&g] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        gms::inet_address ep(req->get_path_param("addr"));
        // synchronize unreachable_members on all shards
        co_await g.get_unreachable_members_synchronized();
        co_return g.get_endpoint_downtime(ep);
    });

    httpd::gossiper_json::get_current_generation_number.set(r, [&g] (std::unique_ptr<http::request> req) {
        gms::inet_address ep(req->get_path_param("addr"));
        return g.get_current_generation_number(ep).then([] (gms::generation_type res) {
            return make_ready_future<json::json_return_type>(res.value());
        });
    });

    httpd::gossiper_json::get_current_heart_beat_version.set(r, [&g] (std::unique_ptr<http::request> req) {
        gms::inet_address ep(req->get_path_param("addr"));
        return g.get_current_heart_beat_version(ep).then([] (gms::version_type res) {
            return make_ready_future<json::json_return_type>(res.value());
        });
    });

    httpd::gossiper_json::assassinate_endpoint.set(r, [&g](std::unique_ptr<http::request> req) {
        if (req->get_query_param("unsafe") != "True") {
            return g.assassinate_endpoint(req->get_path_param("addr")).then([] {
                return make_ready_future<json::json_return_type>(json_void());
            });
        }
        return g.unsafe_assassinate_endpoint(req->get_path_param("addr")).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    httpd::gossiper_json::force_remove_endpoint.set(r, [&g](std::unique_ptr<http::request> req) {
        gms::inet_address ep(req->get_path_param("addr"));
        return g.force_remove_endpoint(ep, gms::null_permit_id).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });
}

void unset_gossiper(http_context& ctx, routes& r) {
    httpd::gossiper_json::get_down_endpoint.unset(r);
    httpd::gossiper_json::get_live_endpoint.unset(r);
    httpd::gossiper_json::get_endpoint_downtime.unset(r);
    httpd::gossiper_json::get_current_generation_number.unset(r);
    httpd::gossiper_json::get_current_heart_beat_version.unset(r);
    httpd::gossiper_json::assassinate_endpoint.unset(r);
    httpd::gossiper_json::force_remove_endpoint.unset(r);
}

}
