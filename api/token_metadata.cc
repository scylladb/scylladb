/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "api/api.hh"
#include "api/api-doc/storage_service.json.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"
#include "locator/token_metadata.hh"
#include "gms/gossiper.hh"

using namespace seastar::httpd;

namespace api {

namespace ss = httpd::storage_service_json;
using namespace json;

void set_token_metadata(http_context& ctx, routes& r, sharded<locator::shared_token_metadata>& tm, sharded<gms::gossiper>& g) {
    ss::local_hostid.set(r, [&tm](std::unique_ptr<http::request> req) {
        auto id = tm.local().get()->get_my_id();
        if (!bool(id)) {
            throw not_found_exception("local host ID is not yet set");
        }
        return make_ready_future<json::json_return_type>(id.to_sstring());
    });

    ss::get_tokens.set(r, [&tm] (std::unique_ptr<http::request> req) {
        return make_ready_future<json::json_return_type>(stream_range_as_array(tm.local().get()->sorted_tokens(), [](const dht::token& i) {
           return fmt::to_string(i);
        }));
    });

    ss::get_node_tokens.set(r, [&tm, &g] (std::unique_ptr<http::request> req) {
        gms::inet_address addr(req->get_path_param("endpoint"));
        auto& local_tm = *tm.local().get();
        std::optional<locator::host_id> host_id;
        try {
            host_id = g.local().get_host_id(addr);
        } catch (...) {}
        return make_ready_future<json::json_return_type>(stream_range_as_array(host_id ? local_tm.get_tokens(*host_id): std::vector<dht::token>{}, [](const dht::token& i) {
            return fmt::to_string(i);
        }));
    });

    ss::get_leaving_nodes.set(r, [&tm, &g](const_req req) {
        const auto& local_tm = *tm.local().get();
        const auto& leaving_host_ids = local_tm.get_leaving_endpoints();
        std::unordered_set<gms::inet_address> eps;
        eps.reserve(leaving_host_ids.size());
        for (const auto host_id: leaving_host_ids) {
            eps.insert(g.local().get_address_map().get(host_id));
        }
        return container_to_vec(eps);
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        std::unordered_set<sstring> addr;
        return container_to_vec(addr);
    });

    ss::get_joining_nodes.set(r, [&tm, &g](const_req req) {
        const auto& local_tm = *tm.local().get();
        const auto& points = local_tm.get_bootstrap_tokens();
        std::unordered_set<gms::inet_address> eps;
        eps.reserve(points.size());
        for (const auto& [token, host_id]: points) {
            eps.insert(g.local().get_address_map().get(host_id));
        }
        return container_to_vec(eps);
    });

    ss::get_host_id_map.set(r, [&tm, &g](const_req req) {
        std::vector<ss::mapper> res;
        auto map = tm.local().get()->get_host_ids() |
            std::views::transform([&g] (locator::host_id id) { return std::make_pair(g.local().get_address_map().get(id), id); }) |
            std::ranges::to<std::unordered_map>();
        return map_to_key_value(std::move(map), res);
    });

    static auto host_or_broadcast = [&tm](const_req req) {
        auto host = req.get_query_param("host");
        return host.empty() ? tm.local().get()->get_topology().my_address() : gms::inet_address(host);
    };

    httpd::endpoint_snitch_info_json::get_datacenter.set(r, [&tm, &g](const_req req) {
        auto& topology = tm.local().get()->get_topology();
        auto ep = host_or_broadcast(req);
        std::optional<locator::host_id> host_id;
        try {
            host_id = g.local().get_host_id(ep);
        } catch (...) {}
        if (!host_id || !topology.has_node(*host_id)) {
            // Cannot return error here, nodetool status can race, request
            // info about just-left node and not handle it nicely
            return locator::endpoint_dc_rack::default_location.dc;
        }
        return topology.get_datacenter(*host_id);
    });

    httpd::endpoint_snitch_info_json::get_rack.set(r, [&tm, &g](const_req req) {
        auto& topology = tm.local().get()->get_topology();
        auto ep = host_or_broadcast(req);
        std::optional<locator::host_id> host_id;
        try {
            host_id = g.local().get_host_id(ep);
        } catch (...) {}
        if (!host_id || !topology.has_node(*host_id)) {
            // Cannot return error here, nodetool status can race, request
            // info about just-left node and not handle it nicely
            return locator::endpoint_dc_rack::default_location.rack;
        }
        return topology.get_rack(*host_id);
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
