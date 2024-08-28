/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>

#include "api/api.hh"
#include "api/api-doc/raft.json.hh"

#include "service/raft/raft_group_registry.hh"

using namespace seastar::httpd;

extern logging::logger apilog;

namespace api {

namespace r = httpd::raft_json;
using namespace json;

void set_raft(http_context&, httpd::routes& r, sharded<service::raft_group_registry>& raft_gr) {
    r::trigger_snapshot.set(r, [&raft_gr] (std::unique_ptr<http::request> req) -> future<json_return_type> {
        raft::group_id gid{utils::UUID{req->get_path_param("group_id")}};
        auto timeout_dur = std::invoke([timeout_str = req->get_query_param("timeout")] {
            if (timeout_str.empty()) {
                return std::chrono::seconds{60};
            }
            auto dur = std::stoll(timeout_str);
            if (dur <= 0) {
                throw std::runtime_error{"Timeout must be a positive number."};
            }
            return std::chrono::seconds{dur};
        });

        std::atomic<bool> found_srv{false};
        co_await raft_gr.invoke_on_all([gid, timeout_dur, &found_srv] (service::raft_group_registry& raft_gr) -> future<> {
            auto* srv = raft_gr.find_server(gid);
            if (!srv) {
                co_return;
            }

            found_srv = true;
            abort_on_expiry aoe(lowres_clock::now() + timeout_dur);
            apilog.info("Triggering Raft group {} snapshot", gid);
            auto result = co_await srv->trigger_snapshot(&aoe.abort_source());
            if (result) {
                apilog.info("New snapshot for Raft group {} created", gid);
            } else {
                apilog.info("Could not create new snapshot for Raft group {}, no new entries applied", gid);
            }
        });

        if (!found_srv) {
            throw std::runtime_error{fmt::format("Server for group ID {} not found", gid)};
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
}

void unset_raft(http_context&, httpd::routes& r) {
    r::trigger_snapshot.unset(r);
    r::get_leader_host.unset(r);
}

}

