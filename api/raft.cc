/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "api/api-doc/raft.json.hh"

#include "service/raft/raft_group_registry.hh"
#include "log.hh"

using namespace seastar::httpd;

extern logging::logger apilog;

namespace api {

struct http_context;
namespace r = httpd::raft_json;
using namespace json;


namespace {

::service::raft_timeout get_request_timeout(const http::request& req) {
    return std::invoke([timeout_str = req.get_query_param("timeout")] {
        if (timeout_str.empty()) {
            return ::service::raft_timeout{};
        }
        auto dur = std::stoll(timeout_str);
        if (dur <= 0) {
            throw bad_param_exception{"Timeout must be a positive number."};
        }
        return ::service::raft_timeout{.value = lowres_clock::now() + std::chrono::seconds{dur}};
    });
}

}  // namespace


void set_raft(http_context&, httpd::routes& r, sharded<service::raft_group_registry>& raft_gr) {
    r::trigger_snapshot.set(r, [&raft_gr] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        raft::group_id gid{utils::UUID{req->get_path_param("group_id")}};
        auto timeout = get_request_timeout(*req);

        std::atomic<bool> found_srv{false};
        co_await raft_gr.invoke_on_all([gid, timeout, &found_srv] (service::raft_group_registry& raft_gr) -> future<> {
            if (!raft_gr.find_server(gid)) {
                co_return;
            }

            found_srv = true;
            apilog.info("Triggering Raft group {} snapshot", gid);
            auto srv = raft_gr.get_server_with_timeouts(gid);
            auto result = co_await srv.trigger_snapshot(nullptr, timeout);
            if (result) {
                apilog.info("New snapshot for Raft group {} created", gid);
            } else {
                apilog.info("Could not create new snapshot for Raft group {}, no new entries applied", gid);
            }
        });

        if (!found_srv) {
            throw bad_param_exception{fmt::format("Server for group ID {} not found", gid)};
        }

        co_return json_void{};
    });
    r::get_leader_host.set(r, [&raft_gr] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        return smp::submit_to(0, [&] {
            auto& srv = std::invoke([&] () -> raft::server& {
                if (req->query_parameters.contains("group_id")) {
                    raft::group_id id{utils::UUID{req->get_query_param("group_id")}};
                    return raft_gr.local().get_server(id);
                } else {
                    return raft_gr.local().group0();
                }
            });
            return json_return_type(srv.current_leader().to_sstring());
        });
    });
}

void unset_raft(http_context&, httpd::routes& r) {
    r::trigger_snapshot.unset(r);
    r::get_leader_host.unset(r);
}

}

