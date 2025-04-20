/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <vector>

#include "hinted_handoff.hh"
#include "api/api.hh"
#include "api/api-doc/hinted_handoff.json.hh"

#include "gms/inet_address.hh"
#include "service/storage_proxy.hh"
#include "gms/gossiper.hh"

namespace api {

using namespace json;
using namespace seastar::httpd;
namespace hh = httpd::hinted_handoff_json;

void set_hinted_handoff(http_context& ctx, routes& r, sharded<service::storage_proxy>& proxy, sharded<gms::gossiper>& g) {
    hh::create_hints_sync_point.set(r, [&proxy, &g] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        auto parse_hosts_list = [&g] (sstring arg) {
            std::vector<sstring> hosts_str = split(arg, ",");
            std::vector<locator::host_id> hosts;
            hosts.reserve(hosts_str.size());

            for (const auto& host_str : hosts_str) {
                try {
                    gms::inet_address host;
                    host = gms::inet_address(host_str);
                    hosts.push_back(g.local().get_host_id(host));
                } catch (std::exception& e) {
                    throw httpd::bad_param_exception(format("Failed to parse host address {}: {}", host_str, e.what()));
                }
            }

            return hosts;
        };

        std::vector<locator::host_id> target_hosts = parse_hosts_list(req->get_query_param("target_hosts"));
        return proxy.local().create_hint_sync_point(std::move(target_hosts)).then([] (db::hints::sync_point sync_point) {
            return json::json_return_type(sync_point.encode());
        });
    });

    hh::get_hints_sync_point.set(r, [&proxy] (std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        db::hints::sync_point sync_point;
        const sstring encoded = req->get_query_param("id");
        try {
            sync_point = db::hints::sync_point::decode(encoded);
        } catch (std::exception& e) {
            throw httpd::bad_param_exception(format("Failed to parse the sync point description {}: {}", encoded, e.what()));
        }

        lowres_clock::time_point deadline;
        const sstring timeout_str = req->get_query_param("timeout");
        try {
            deadline = [&] {
                if (timeout_str.empty()) {
                    // Empty string - don't wait at all, just check the status
                    return lowres_clock::time_point::min();
                } else {
                    const auto timeout = std::stoll(timeout_str);
                    if (timeout >= 0) {
                        // Wait until the point is reached, or until `timeout` seconds elapse
                        return lowres_clock::now() + std::chrono::seconds(timeout);
                    } else {
                        // Negative value indicates infinite timeout
                        return lowres_clock::time_point::max();
                    }
                }
            } ();
        } catch (std::exception& e) {
            throw httpd::bad_param_exception(format("Failed to parse the timeout parameter {}: {}", timeout_str, e.what()));
        }

        using return_type = hh::ns_get_hints_sync_point::get_hints_sync_point_return_type;
        using return_type_wrapper = hh::ns_get_hints_sync_point::return_type_wrapper;

        return proxy.local().wait_for_hint_sync_point(std::move(sync_point), deadline).then([] {
            return json::json_return_type(return_type_wrapper(return_type::DONE));
        }).handle_exception_type([] (const timed_out_error&) {
            return json::json_return_type(return_type_wrapper(return_type::IN_PROGRESS));
        });
    });

    hh::list_endpoints_pending_hints.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    hh::truncate_all_hints.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::schedule_hint_delivery.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::pause_hints_delivery.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        sstring pause = req->get_query_param("pause");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::get_create_hint_count.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(0);
    });

    hh::get_not_stored_hints_count.set(r, [] (std::unique_ptr<http::request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(0);
    });
}

void unset_hinted_handoff(http_context& ctx, routes& r) {
    hh::create_hints_sync_point.unset(r);
    hh::get_hints_sync_point.unset(r);

    hh::list_endpoints_pending_hints.unset(r);
    hh::truncate_all_hints.unset(r);
    hh::schedule_hint_delivery.unset(r);
    hh::pause_hints_delivery.unset(r);
    hh::get_create_hint_count.unset(r);
    hh::get_not_stored_hints_count.unset(r);
}

}
