/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"
#include "locator/token_metadata.hh"
#include "utils/fb_utilities.hh"

using namespace seastar::httpd;

namespace api {

namespace ss = httpd::storage_service_json;
using namespace json;

void set_token_metadata(http_context& ctx, routes& r, sharded<locator::shared_token_metadata>& tm) {
    ss::local_hostid.set(r, [&tm](std::unique_ptr<http::request> req) {
        auto id = tm.local().get()->get_my_id();
        return make_ready_future<json::json_return_type>(id.to_sstring());
    });

    ss::get_tokens.set(r, [&tm] (std::unique_ptr<http::request> req) {
        return make_ready_future<json::json_return_type>(stream_range_as_array(tm.local().get()->sorted_tokens(), [](const dht::token& i) {
           return fmt::to_string(i);
        }));
    });

    ss::get_node_tokens.set(r, [&tm] (std::unique_ptr<http::request> req) {
        gms::inet_address addr(req->param["endpoint"]);
        return make_ready_future<json::json_return_type>(stream_range_as_array(tm.local().get()->get_tokens(addr), [](const dht::token& i) {
           return fmt::to_string(i);
       }));
    });

    ss::get_leaving_nodes.set(r, [&tm](const_req req) {
        return container_to_vec(tm.local().get()->get_leaving_endpoints());
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        std::unordered_set<sstring> addr;
        return container_to_vec(addr);
    });

    ss::get_joining_nodes.set(r, [&tm](const_req req) {
        auto points = tm.local().get()->get_bootstrap_tokens();
        std::unordered_set<sstring> addr;
        for (auto i: points) {
            addr.insert(fmt::to_string(i.second));
        }
        return container_to_vec(addr);
    });

    ss::get_host_id_map.set(r, [&tm](const_req req) {
        std::vector<ss::mapper> res;
        return map_to_key_value(tm.local().get()->get_endpoint_to_host_id_map_for_reading(), res);
    });

    static auto host_or_broadcast = [](const_req req) {
        auto host = req.get_query_param("host");
        return host.empty() ? gms::inet_address(utils::fb_utilities::get_broadcast_address()) : gms::inet_address(host);
    };

    httpd::endpoint_snitch_info_json::get_datacenter.set(r, [&tm](const_req req) {
        auto& topology = tm.local().get()->get_topology();
        auto ep = host_or_broadcast(req);
        if (!topology.has_endpoint(ep)) {
            // Cannot return error here, nodetool status can race, request
            // info about just-left node and not handle it nicely
            return locator::endpoint_dc_rack::default_location.dc;
        }
        return topology.get_datacenter(ep);
    });

    httpd::endpoint_snitch_info_json::get_rack.set(r, [&tm](const_req req) {
        auto& topology = tm.local().get()->get_topology();
        auto ep = host_or_broadcast(req);
        if (!topology.has_endpoint(ep)) {
            // Cannot return error here, nodetool status can race, request
            // info about just-left node and not handle it nicely
            return locator::endpoint_dc_rack::default_location.rack;
        }
        return topology.get_rack(ep);
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
    httpd::endpoint_snitch_info_json::get_datacenter.unset(r);
    httpd::endpoint_snitch_info_json::get_rack.unset(r);
}

}
