/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/coroutine/maybe_yield.hh>
#include "api/api.hh"
#include "api/storage_service.hh"
#include "api/api-doc/storage_service.json.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"
#include "locator/token_metadata.hh"
#include "locator/tablets.hh"
#include "replica/database.hh"
#include "utils/chunked_vector.hh"
#include "gms/gossiper.hh"

using namespace seastar::httpd;

namespace api {

namespace ss = httpd::storage_service_json;
using namespace json;

static json::json_return_type tokens_to_json(auto tokens) {
    return stream_range_as_array(std::move(tokens), [](const dht::token& i) {
        return fmt::to_string(i);
    });
}

static future<json::json_return_type> get_tokens_of(
        http_context& ctx, locator::token_metadata_ptr tmptr, std::optional<locator::host_id> host_id, std::unique_ptr<http::request> req) {
    const auto keyspace = req->get_query_param("keyspace");
    const auto table = req->get_query_param("cf");
    if (keyspace.empty() != table.empty()) {
        throw bad_param_exception("Either provide both keyspace and table (for tablet table) or neither (for vnodes)");
    }
    std::optional<table_id> tid;
    if (!keyspace.empty()) {
        tid = validate_table(ctx.db.local(), keyspace, table);
    }
    if (!host_id) {
        co_return tokens_to_json(std::vector<dht::token>{});
    }
    if (!tid || !ctx.db.local().find_column_family(*tid).uses_tablets()) {
        co_return tokens_to_json(tmptr->get_tokens(*host_id));
    }
    const auto& tmap = tmptr->tablets().get_tablet_map(*tid);
    // Any replica, not only primary: the node holds data for all of these ranges.
    utils::chunked_vector<dht::token> tokens;
    for (std::optional<locator::tablet_id> t = tmap.first_tablet(); t; t = tmap.next_tablet(*t)) {
        if (locator::contains(tmap.get_tablet_info(*t).replicas, *host_id)) {
            tokens.push_back(tmap.get_last_token(*t));
        }
        co_await coroutine::maybe_yield();
    }
    co_return tokens_to_json(std::move(tokens));
}

void set_token_metadata(http_context& ctx, routes& r, sharded<locator::shared_token_metadata>& tm, sharded<gms::gossiper>& g) {
    ss::local_hostid.set(r, [&tm](std::unique_ptr<http::request> req) {
        auto id = tm.local().get()->get_my_id();
        if (!bool(id)) {
            throw not_found_exception("local host ID is not yet set");
        }
        return make_ready_future<json::json_return_type>(id.to_sstring());
    });

    ss::get_tokens.set(r, [&ctx, &tm] (std::unique_ptr<http::request> req) {
        auto tmptr = tm.local().get();
        auto id = tmptr->get_my_id();
        return get_tokens_of(ctx, std::move(tmptr), id, std::move(req));
    });

    ss::get_node_tokens.set(r, [&ctx, &tm, &g] (std::unique_ptr<http::request> req) {
        gms::inet_address addr(req->get_path_param("endpoint"));
        std::optional<locator::host_id> host_id;
        try {
            host_id = g.local().get_host_id(addr);
        } catch (...) {}
        return get_tokens_of(ctx, tm.local().get(), host_id, std::move(req));
    });

    ss::get_leaving_nodes.set(r, [&tm, &g](const_req req) {
        const auto& local_tm = *tm.local().get();
        const auto& leaving_host_ids = local_tm.get_leaving_endpoints();
        std::unordered_set<gms::inet_address> eps;
        eps.reserve(leaving_host_ids.size());
        for (const auto host_id: leaving_host_ids) {
            eps.insert(g.local().get_address_map().get(host_id));
        }
        return eps | std::views::transform([] (auto& i) { return fmt::to_string(i); }) | std::ranges::to<std::vector>();
    });

    ss::get_moving_nodes.set(r, [](const_req req) {
        std::unordered_set<sstring> addr;
        return addr | std::ranges::to<std::vector>();
    });

    ss::get_excluded_nodes.set(r, [&tm](const_req req) {
        const auto& local_tm = *tm.local().get();
        std::vector<sstring> eps;
        local_tm.get_topology().for_each_node([&] (auto& node) {
            if (node.is_excluded()) {
                eps.push_back(node.host_id().to_sstring());
            }
        });
        return eps;
    });

    ss::get_joining_nodes.set(r, [&tm, &g](const_req req) {
        const auto& local_tm = *tm.local().get();
        const auto& points = local_tm.get_bootstrap_tokens();
        std::unordered_set<gms::inet_address> eps;
        eps.reserve(points.size());
        for (const auto& [token, host_id]: points) {
            eps.insert(g.local().get_address_map().get(host_id));
        }
        return eps | std::views::transform([] (auto& i) { return fmt::to_string(i); }) | std::ranges::to<std::vector>();
    });

    ss::get_host_id_map.set(r, [&tm, &g](const_req req) {
        if (!g.local().is_enabled()) {
            throw std::runtime_error("The gossiper is not ready yet");
        }
        return tm.local().get()->get_host_ids()
            | std::views::transform([&g] (locator::host_id id) {
                ss::mapper m;
                m.key = fmt::to_string(g.local().get_address_map().get(id));
                m.value = fmt::to_string(id);
                return m;
            })
            | std::ranges::to<std::vector<ss::mapper>>();
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
    ss::get_excluded_nodes.unset(r);
    ss::get_host_id_map.unset(r);
    httpd::endpoint_snitch_info_json::get_datacenter.unset(r);
    httpd::endpoint_snitch_info_json::get_rack.unset(r);
}

}
