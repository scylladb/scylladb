/*
 * Copyright 2015 Cloudius Systems
 */

#include "locator/snitch_base.hh"
#include "endpoint_snitch.hh"
#include "api/api-doc/endpoint_snitch_info.json.hh"

namespace api {

void set_endpoint_snitch(http_context& ctx, routes& r) {
    httpd::endpoint_snitch_info_json::get_datacenter.set(r, [] (const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(req.get_query_param("host"));
    });

    httpd::endpoint_snitch_info_json::get_rack.set(r, [] (const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(req.get_query_param("host"));
    });

    httpd::endpoint_snitch_info_json::get_snitch_name.set(r, [] (const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_name();
    });
}

}
