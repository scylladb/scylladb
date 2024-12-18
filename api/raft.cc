/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/coroutine.hh>

#include "api/api-doc/raft.json.hh"

#include "service/raft/raft_group_registry.hh"
#include "utils/log.hh"

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
        if (!req->query_parameters.contains("group_id")) {
            const auto leader_id = co_await raft_gr.invoke_on(0, [] (service::raft_group_registry& raft_gr) {
                auto& srv = raft_gr.group0();
                return srv.current_leader();
            });
            co_return json_return_type{leader_id.to_sstring()};
        }

        const raft::group_id gid{utils::UUID{req->get_query_param("group_id")}};

        std::atomic<bool> found_srv{false};
        std::atomic<raft::server_id> leader_id = raft::server_id::create_null_id();
        co_await raft_gr.invoke_on_all([gid, &found_srv, &leader_id] (service::raft_group_registry& raft_gr) {
            if (raft_gr.find_server(gid)) {
                found_srv = true;
                leader_id = raft_gr.get_server(gid).current_leader();
            }
            return make_ready_future<>();
        });

        if (!found_srv) {
            throw bad_param_exception{fmt::format("Server for group ID {} not found", gid)};
        }

        co_return json_return_type(leader_id.load().to_sstring());
    });
    r::read_barrier.set(r, [&raft_gr] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        auto timeout = get_request_timeout(*req);

        if (!req->query_parameters.contains("group_id")) {
            // Read barrier on group 0 by default
            co_await raft_gr.invoke_on(0, [timeout] (service::raft_group_registry& raft_gr) -> future<> {
                co_await raft_gr.group0_with_timeouts().read_barrier(nullptr, timeout);
            });
            co_return json_void{};
        }

        raft::group_id gid{utils::UUID{req->get_query_param("group_id")}};

        std::atomic<bool> found_srv{false};
        co_await raft_gr.invoke_on_all([gid, timeout, &found_srv] (service::raft_group_registry& raft_gr) -> future<> {
            if (!raft_gr.find_server(gid)) {
                co_return;
            }
            found_srv = true;
            co_await raft_gr.get_server_with_timeouts(gid).read_barrier(nullptr, timeout);
        });

        if (!found_srv) {
            throw bad_param_exception{fmt::format("Server for group ID {} not found", gid)};
        }

        co_return json_void{};
    });
    r::trigger_stepdown.set(r, [&raft_gr] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        auto timeout = get_request_timeout(*req);
        auto dur = timeout.value ? *timeout.value - lowres_clock::now() : std::chrono::seconds(60);
        const auto stepdown_timeout_ticks = dur / service::raft_tick_interval;
        auto timeout_dur = raft::logical_clock::duration(stepdown_timeout_ticks);

        if (!req->query_parameters.contains("group_id")) {
            // Stepdown on group 0 by default
            co_await raft_gr.invoke_on(0, [timeout_dur] (service::raft_group_registry& raft_gr) {
                apilog.info("Triggering stepdown for group0");
                return raft_gr.group0().stepdown(timeout_dur);
            });
            co_return json_void{};
        }
        raft::group_id gid{utils::UUID{req->get_path_param("group_id")}};

        std::atomic<bool> found_srv{false};
        co_await raft_gr.invoke_on_all([gid, timeout_dur, &found_srv] (service::raft_group_registry& raft_gr) -> future<> {
            auto* srv = raft_gr.find_server(gid);
            if (!srv) {
                co_return;
            }

            found_srv = true;
            apilog.info("Triggering stepdown for group {}", gid);
            co_await srv->stepdown(timeout_dur);
        });

        if (!found_srv) {
            throw std::runtime_error{fmt::format("Server for group ID {} not found", gid)};
        }

        co_return json_void{};
    });
}

void unset_raft(http_context&, httpd::routes& r) {
    r::trigger_snapshot.unset(r);
    r::get_leader_host.unset(r);
    r::read_barrier.unset(r);
    r::trigger_stepdown.unset(r);
}

}
