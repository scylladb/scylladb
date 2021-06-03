/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

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

    httpd::endpoint_snitch_info_json::get_datacenter.set(r, [](const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(host_or_broadcast(req));
    });

    httpd::endpoint_snitch_info_json::get_rack.set(r, [](const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_rack(host_or_broadcast(req));
    });

    httpd::endpoint_snitch_info_json::get_snitch_name.set(r, [] (const_req req) {
        return locator::i_endpoint_snitch::get_local_snitch_ptr()->get_name();
    });
}

}
