/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "service_levels.hh"
#include "api/api-doc/service_levels.json.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "seastar/json/json_elements.hh"
#include "transport/controller.hh"
#include <unordered_map>


namespace api {

namespace sl = httpd::service_levels_json;
using namespace json;
using namespace seastar::httpd;


void set_service_levels(http_context& ctx, routes& r, cql_transport::controller& ctl, sharded<cql3::query_processor>& qp) {
    sl::do_switch_tenants.set(r, [&ctl] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        co_await ctl.update_connections_scheduling_group();
        co_return json_void();
    });

    sl::count_connections.set(r, [&qp] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto connections = co_await qp.local().execute_internal(
            "SELECT username, scheduling_group FROM system.clients WHERE client_type='cql' ALLOW FILTERING",
            db::consistency_level::LOCAL_ONE,
            cql3::query_processor::cache_internal::no
        );

        using connections_per_user = std::unordered_map<sstring, uint64_t>;
        using connections_per_scheduling_group = std::unordered_map<sstring, connections_per_user>;
        connections_per_scheduling_group result;

        for (auto it = connections->begin(); it != connections->end(); it++) {
            auto user = it->get_as<sstring>("username");
            auto shg = it->get_as<sstring>("scheduling_group");

            if (result.contains(shg)) {
                result[shg][user]++;
            }
            else {
                result[shg] = {{user, 1}};
            }
        }

        co_return result;
    });

}




}