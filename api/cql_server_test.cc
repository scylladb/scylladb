/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "build_mode.hh"

#ifndef SCYLLA_BUILD_MODE_RELEASE

#include <seastar/core/coroutine.hh>

#include "api/api-doc/cql_server_test.json.hh"
#include "cql_server_test.hh"
#include "transport/controller.hh"
#include "transport/server.hh"
#include "service/qos/qos_common.hh"

namespace api {

namespace cst = httpd::cql_server_test_json;
using namespace json;
using namespace seastar::httpd;

struct connection_sl_params : public json::json_base {
    json::json_element<sstring> _role_name;
    json::json_element<sstring> _workload_type;
    json::json_element<sstring> _timeout;
    json::json_element<sstring> _scheduling_group;

    connection_sl_params(const sstring& role_name, const sstring& workload_type, const sstring& timeout, const sstring& scheduling_group) {
        _role_name = role_name;
        _workload_type = workload_type;
        _timeout = timeout;
        _scheduling_group = scheduling_group;
        register_params();
    }

    connection_sl_params(const connection_sl_params& params)
        : connection_sl_params(params._role_name(), params._workload_type(), params._timeout(), params._scheduling_group()) {}

    void register_params() {
        add(&_role_name, "role_name");
        add(&_workload_type, "workload_type");
        add(&_timeout, "timeout");
        add(&_scheduling_group, "scheduling_group");
    }    
};

void set_cql_server_test(http_context& ctx, seastar::httpd::routes& r, cql_transport::controller& ctl) {
    cst::connections_params.set(r, [&ctl] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto sl_params = co_await ctl.get_connections_service_level_params();

        std::vector<connection_sl_params> result;
        std::ranges::transform(std::move(sl_params), std::back_inserter(result), [] (const cql_transport::connection_service_level_params& params) {
            auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(params.timeout_config.read_timeout).count();
            return connection_sl_params(
                    std::move(params.role_name), 
                    sstring(qos::service_level_options::to_string(params.workload_type)), 
                    to_string(cql_duration(months_counter{0}, days_counter{0}, nanoseconds_counter{nanos})),
                    std::move(params.scheduling_group_name));
        });
        co_return result;
    });
}

void unset_cql_server_test(http_context& ctx, seastar::httpd::routes& r) {
    cst::connections_params.unset(r);
}

}

#endif
