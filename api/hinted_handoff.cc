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

#include <algorithm>
#include <vector>

#include "hinted_handoff.hh"
#include "api/api-doc/hinted_handoff.json.hh"

#include "gms/inet_address.hh"
#include "gms/gossiper.hh"
#include "utils/UUID.hh"
#include "db/hints/sync_point_service.hh"
#include "seastar/core/coroutine.hh"

namespace api {

using namespace json;
namespace hh = httpd::hinted_handoff_json;

void set_hinted_handoff(http_context& ctx, routes& r, sharded<db::hints::sync_point_service>& svc) {
    hh::create_hints_waiting_point.set(r, [&svc] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        auto parse_hosts_list = [] (sstring arg) {
            std::vector<sstring> hosts_str = split(arg, ",");
            std::vector<gms::inet_address> hosts;
            hosts.reserve(hosts_str.size());
            if (hosts_str.empty()) {
                const auto members_set = gms::get_local_gossiper().get_live_members();
                std::copy(members_set.begin(), members_set.end(), std::back_inserter(hosts));
            } else {
                for (const auto& host_str : hosts_str) {
                    gms::inet_address host;
                    try {
                        host = gms::inet_address(host_str);
                    } catch (std::exception& e) {
                        throw httpd::bad_param_exception(e.what());
                    }
                    hosts.push_back(host);
                }
            }
            return hosts;
        };

        std::vector<gms::inet_address> source_hosts = parse_hosts_list(req->get_query_param("source_hosts"));
        std::vector<gms::inet_address> target_hosts = parse_hosts_list(req->get_query_param("target_hosts"));
        const auto waiting_point_id = co_await svc.local().create_sync_point(std::move(source_hosts), std::move(target_hosts));
        co_return json::json_return_type(waiting_point_id.to_sstring());
    });

    hh::get_hints_waiting_point.set(r, [&svc] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        utils::UUID waiting_point_id;
        lowres_clock::time_point deadline;
        try {
            waiting_point_id = utils::UUID(req->get_query_param("id"));

            const sstring timeout_str = req->get_query_param("timeout");
            deadline = lowres_clock::time_point::max();
            if (timeout_str.empty()) {
                deadline = lowres_clock::now();
            } else {
                const auto timeout = std::stoll(timeout_str);
                if (timeout >= 0) {
                    deadline = lowres_clock::now() + std::chrono::seconds(timeout);
                }
            }
        } catch (std::exception& e) {
            throw httpd::bad_param_exception(e.what());
        }

        using return_type = hh::ns_get_hints_waiting_point::get_hints_waiting_point_return_type;
        using return_type_wrapper = hh::ns_get_hints_waiting_point::return_type_wrapper;

        return_type ret;
        try {
            co_await svc.local().wait_for_sync_point(waiting_point_id, deadline);
            ret = return_type::DONE;
        } catch (timed_out_error&) {
            ret = return_type::IN_PROGRESS;
        } catch (...) {
            ret = return_type::FAILED;
        }

        co_return json::json_return_type(return_type_wrapper(ret));
    });

    hh::delete_hints_waiting_point.set(r, [&svc] (std::unique_ptr<request> req) -> future<json::json_return_type> {
        utils::UUID waiting_point_id;
        try {
            waiting_point_id = utils::UUID(req->get_query_param("id"));
        } catch (std::exception& e) {
            throw httpd::bad_param_exception(e.what());
        }

        co_await svc.local().delete_sync_point(waiting_point_id);
        co_return json_void();
    });

    hh::list_endpoints_pending_hints.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        std::vector<sstring> res;
        return make_ready_future<json::json_return_type>(res);
    });

    hh::truncate_all_hints.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::schedule_hint_delivery.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::pause_hints_delivery.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring pause = req->get_query_param("pause");
        return make_ready_future<json::json_return_type>(json_void());
    });

    hh::get_create_hint_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(0);
    });

    hh::get_not_stored_hints_count.set(r, [] (std::unique_ptr<request> req) {
        //TBD
        unimplemented();
        sstring host = req->get_query_param("host");
        return make_ready_future<json::json_return_type>(0);
    });
}

void unset_hinted_handoff(http_context& ctx, routes& r) {
    hh::create_hints_waiting_point.unset(r);
    hh::get_hints_waiting_point.unset(r);
    hh::delete_hints_waiting_point.unset(r);

    hh::list_endpoints_pending_hints.unset(r);
    hh::truncate_all_hints.unset(r);
    hh::schedule_hint_delivery.unset(r);
    hh::pause_hints_delivery.unset(r);
    hh::get_create_hint_count.unset(r);
    hh::get_not_stored_hints_count.unset(r);
}

}
