/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "locator/snitch_base.hh"
#include "endpoint_snitch.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"
#include "api/api-doc/storage_service.json.hh"

namespace api {
using namespace seastar::httpd;

void set_endpoint_snitch(http_context& ctx, routes& r, sharded<locator::snitch_ptr>& snitch) {
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
    httpd::endpoint_snitch_info_json::get_snitch_name.unset(r);
    httpd::storage_service_json::update_snitch.unset(r);
}

}
