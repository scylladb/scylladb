/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/token_metadata.hh"
#include "locator/snitch_base.hh"
#include "locator/production_snitch_base.hh"
#include "endpoint_snitch.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"
#include "api/api-doc/storage_service.json.hh"
#include "utils/fb_utilities.hh"

namespace api {
using namespace seastar::httpd;

void set_endpoint_snitch(http_context& ctx, routes& r, sharded<locator::snitch_ptr>& snitch) {
    static auto host_or_broadcast = [](const_req req) {
        auto host = req.get_query_param("host");
        return host.empty() ? gms::inet_address(utils::fb_utilities::get_broadcast_address()) : gms::inet_address(host);
    };

    httpd::endpoint_snitch_info_json::get_datacenter.set(r, [&ctx](const_req req) {
        auto& topology = ctx.shared_token_metadata.local().get()->get_topology();
        auto ep = host_or_broadcast(req);
        if (!topology.has_endpoint(ep)) {
            // Cannot return error here, nodetool status can race, request
            // info about just-left node and not handle it nicely
            return locator::endpoint_dc_rack::default_location.dc;
        }
        return topology.get_datacenter(ep);
    });

    httpd::endpoint_snitch_info_json::get_rack.set(r, [&ctx](const_req req) {
        auto& topology = ctx.shared_token_metadata.local().get()->get_topology();
        auto ep = host_or_broadcast(req);
        if (!topology.has_endpoint(ep)) {
            // Cannot return error here, nodetool status can race, request
            // info about just-left node and not handle it nicely
            return locator::endpoint_dc_rack::default_location.rack;
        }
        return topology.get_rack(ep);
    });

    httpd::endpoint_snitch_info_json::get_snitch_name.set(r, [&snitch] (const_req req) {
        return snitch.local()->get_name();
    });

    httpd::storage_service_json::update_snitch.set(r, [&snitch](std::unique_ptr<request> req) {
        locator::snitch_config cfg;
        cfg.name = req->get_query_param("ep_snitch_class_name");
        return locator::i_endpoint_snitch::reset_snitch(snitch, cfg).then([] {
            return make_ready_future<json::json_return_type>(json::json_void());
        });
    });

}

void unset_endpoint_snitch(http_context& ctx, routes& r) {
    httpd::endpoint_snitch_info_json::get_datacenter.unset(r);
    httpd::endpoint_snitch_info_json::get_rack.unset(r);
    httpd::endpoint_snitch_info_json::get_snitch_name.unset(r);
    httpd::storage_service_json::update_snitch.unset(r);
}

}
