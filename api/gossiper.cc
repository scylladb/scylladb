/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "gossiper.hh"
#include "api/api-doc/gossiper.json.hh"
#include "gms/gossiper.hh"

namespace api {
using namespace json;

static unsigned parse_shard_number(const std::string& str) {
    try {
        long s = std::stol(str);
        if (s < 0 || s >= smp::count) {
            throw std::runtime_error{format(
                "Shard number {} outside valid shard range [0, {})", s, smp::count)};
        }
        return s;
    } catch (...) {
        throw std::runtime_error{format(
            "Invalid shard number \"{}\": {}", str, std::current_exception())};
    }
}

void set_gossiper(http_context& ctx, routes& r, gms::gossiper& g) {
    httpd::gossiper_json::get_down_endpoint.set(r, [&g] (std::unique_ptr<request> req) {
        std::string shard_str = req->get_query_param("shard");
        unsigned shard = shard_str.empty() ? 0 : parse_shard_number(shard_str);

        return g.container().invoke_on(shard, [] (gms::gossiper& g) {
            return g.get_unreachable_members();
        }).then([] (std::set<gms::inet_address> s) {
            return make_ready_future<json::json_return_type>(container_to_vec(s));
        });
    });

    httpd::gossiper_json::get_live_endpoint.set(r, [&g] (std::unique_ptr<request> req) {
        std::string shard_str = req->get_query_param("shard");
        unsigned shard = shard_str.empty() ? 0 : parse_shard_number(shard_str);

        return g.container().invoke_on(shard, [] (gms::gossiper& g) {
            return g.get_live_members();
        }).then([] (std::set<gms::inet_address> s) {
            return make_ready_future<json::json_return_type>(container_to_vec(s));
        });
    });

    httpd::gossiper_json::get_endpoint_downtime.set(r, [&g] (const_req req) {
        gms::inet_address ep(req.param["addr"]);
        return g.get_endpoint_downtime(ep);
    });

    httpd::gossiper_json::get_current_generation_number.set(r, [&g] (std::unique_ptr<request> req) {
        gms::inet_address ep(req->param["addr"]);
        return g.get_current_generation_number(ep).then([] (int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    httpd::gossiper_json::get_current_heart_beat_version.set(r, [&g] (std::unique_ptr<request> req) {
        gms::inet_address ep(req->param["addr"]);
        return g.get_current_heart_beat_version(ep).then([] (int res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    httpd::gossiper_json::assassinate_endpoint.set(r, [&g](std::unique_ptr<request> req) {
        if (req->get_query_param("unsafe") != "True") {
            return g.assassinate_endpoint(req->param["addr"]).then([] {
                return make_ready_future<json::json_return_type>(json_void());
            });
        }
        return g.unsafe_assassinate_endpoint(req->param["addr"]).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });

    httpd::gossiper_json::force_remove_endpoint.set(r, [&g](std::unique_ptr<request> req) {
        gms::inet_address ep(req->param["addr"]);
        return g.force_remove_endpoint(ep).then([] {
            return make_ready_future<json::json_return_type>(json_void());
        });
    });
}

}
