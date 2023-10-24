/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "service/storage_service.hh"

using namespace seastar::httpd;

namespace api {

namespace ss = httpd::storage_service_json;
using namespace json;

void set_token_metadata(http_context& ctx, routes& r, sharded<service::storage_service>& ss) {
    ss::local_hostid.set(r, [&ss](std::unique_ptr<http::request> req) {
        auto id = ss.local().get_token_metadata().get_my_id();
        return make_ready_future<json::json_return_type>(id.to_sstring());
    });

    ss::get_tokens.set(r, [&ss] (std::unique_ptr<http::request> req) {
        return make_ready_future<json::json_return_type>(stream_range_as_array(ss.local().get_token_metadata().sorted_tokens(), [](const dht::token& i) {
           return fmt::to_string(i);
        }));
    });

    ss::get_node_tokens.set(r, [&ss] (std::unique_ptr<http::request> req) {
        gms::inet_address addr(req->param["endpoint"]);
        return make_ready_future<json::json_return_type>(stream_range_as_array(ss.local().get_token_metadata().get_tokens(addr), [](const dht::token& i) {
           return fmt::to_string(i);
       }));
    });

    ss::get_leaving_nodes.set(r, [&ss](const_req req) {
        return container_to_vec(ss.local().get_token_metadata().get_leaving_endpoints());
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        std::unordered_set<sstring> addr;
        return container_to_vec(addr);
    });

    ss::get_joining_nodes.set(r, [&ss](const_req req) {
        auto points = ss.local().get_token_metadata().get_bootstrap_tokens();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(fmt::to_string(i.second));
        }
        return container_to_vec(addr);
    });

    ss::get_host_id_map.set(r, [&ss](const_req req) {
        std::vector<ss::mapper> res;
        return map_to_key_value(ss.local().get_token_metadata().get_endpoint_to_host_id_map_for_reading(), res);
    });
}

void unset_token_metadata(http_context& ctx, routes& r) {
    ss::local_hostid.unset(r);
    ss::get_tokens.unset(r);
    ss::get_node_tokens.unset(r);
    ss::get_leaving_nodes.unset(r);
    ss::get_moving_nodes.unset(r);
    ss::get_joining_nodes.unset(r);
    ss::get_host_id_map.unset(r);
}

}
