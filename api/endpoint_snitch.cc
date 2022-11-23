/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/token_metadata.hh"
#include "locator/snitch_base.hh"
#include "endpoint_snitch.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"
#include "utils/fb_utilities.hh"

namespace api {

void set_endpoint_snitch(http_context& ctx, routes& r) {
    static auto host_or_broadcast = [](const_req req) {
        auto host = req.get_query_param("host");
        return host.empty() ? gms::inet_address(utils::fb_utilities::get_broadcast_address()) : gms::inet_address(host);
    };

    httpd::endpoint_snitch_info_json::get_datacenter.set(r, [&ctx](const_req req) {
        auto& topology = ctx.shared_token_metadata.local().get()->get_topology();
        return topology.get_datacenter(host_or_broadcast(req));
    });

    httpd::endpoint_snitch_info_json::get_rack.set(r, [&ctx](const_req req) {
        auto& topology = ctx.shared_token_metadata.local().get()->get_topology();
        return topology.get_rack(host_or_broadcast(req));
    });

    httpd::endpoint_snitch_info_json::get_snitch_name.set(r, [] (const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_name();
    });
}

}
