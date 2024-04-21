/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "stream_manager.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_result_future.hh"
#include "api/api.hh"
#include "api/api-doc/stream_manager.json.hh"
#include <vector>
#include <rapidjson/document.h>
#include "gms/gossiper.hh"

namespace api {
using namespace seastar::httpd;

namespace hs = httpd::stream_manager_json;

static void set_summaries(const std::vector<streaming::stream_summary>& from,
        json::json_list<hs::stream_summary>& to) {
    if (!from.empty()) {
        hs::stream_summary res;
        res.cf_id = fmt::to_string(from.front().cf_id);
        // For each stream_session, we pretend we are sending/receiving one
        // file, to make it compatible with nodetool.
        res.files = 1;
        // We can not estimate total number of bytes the stream_session will
        // send or recvieve since we don't know the size of the frozen_mutation
        // until we read it.
        res.total_size = 0;
        to.push(res);
    }
}

static hs::progress_info get_progress_info(const streaming::progress_info& info) {
    hs::progress_info res;
    res.current_bytes = info.current_bytes;
    res.direction = info.dir;
    res.file_name = info.file_name;
    res.peer = fmt::to_string(info.peer);
    res.session_index = 0;
    res.total_bytes = info.total_bytes;
    return res;
}

static void set_files(const std::map<sstring, streaming::progress_info>& from,
        json::json_list<hs::progress_info_mapper>& to) {
    for (auto i : from) {
        hs::progress_info_mapper m;
        m.key = i.first;
        m.value = get_progress_info(i.second);
        to.push(m);
    }
}

static hs::stream_state get_state(
        streaming::stream_result_future& result_future) {
    hs::stream_state state;
    state.description = result_future.description;
    state.plan_id = result_future.plan_id.to_sstring();
    for (auto info : result_future.get_coordinator().get()->get_all_session_info()) {
        hs::stream_info si;
        si.peer = fmt::to_string(info.peer);
        si.session_index = 0;
        si.state = info.state;
        si.connecting = si.peer;
        set_summaries(info.receiving_summaries, si.receiving_summaries);
        set_summaries(info.sending_summaries, si.sending_summaries);
        set_files(info.receiving_files, si.receiving_files);
        set_files(info.sending_files, si.sending_files);
        state.sessions.push(si);
    }
    return state;
}

void set_stream_manager(http_context& ctx, routes& r, sharded<streaming::stream_manager>& sm) {
    hs::get_current_streams.set(r,
            [&sm] (std::unique_ptr<request> req) {
                return sm.invoke_on_all([] (auto& sm) {
                    return sm.update_all_progress_info();
                }).then([&sm] {
                    return sm.map_reduce0([](streaming::stream_manager& stream) {
                        std::vector<hs::stream_state> res;
                        for (auto i : stream.get_initiated_streams()) {
                            res.push_back(get_state(*i.second.get()));
                        }
                        for (auto i : stream.get_receiving_streams()) {
                            res.push_back(get_state(*i.second.get()));
                        }
                        return res;
                    }, std::vector<hs::stream_state>(),concat<hs::stream_state>).
                    then([](const std::vector<hs::stream_state>& res) {
                        return make_ready_future<json::json_return_type>(res);
                    });
                });
            });

    hs::get_all_active_streams_outbound.set(r, [&sm](std::unique_ptr<request> req) {
        return sm.map_reduce0([](streaming::stream_manager& stream) {
            return stream.get_initiated_streams().size();
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_total_incoming_bytes.set(r, [&sm](std::unique_ptr<request> req) {
        gms::inet_address peer(req->get_path_param("peer"));
        return sm.map_reduce0([peer](streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards(peer).then([] (auto sbytes) {
                return sbytes.bytes_received;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_all_total_incoming_bytes.set(r, [&sm](std::unique_ptr<request> req) {
        return sm.map_reduce0([](streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards().then([] (auto sbytes) {
                return sbytes.bytes_received;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_total_outgoing_bytes.set(r, [&sm](std::unique_ptr<request> req) {
        gms::inet_address peer(req->get_path_param("peer"));
        return sm.map_reduce0([peer] (streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards(peer).then([] (auto sbytes) {
                return sbytes.bytes_sent;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_all_total_outgoing_bytes.set(r, [&sm](std::unique_ptr<request> req) {
        return sm.map_reduce0([](streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards().then([] (auto sbytes) {
                return sbytes.bytes_sent;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });
}

void unset_stream_manager(http_context& ctx, routes& r) {
    hs::get_current_streams.unset(r);
    hs::get_all_active_streams_outbound.unset(r);
    hs::get_total_incoming_bytes.unset(r);
    hs::get_all_total_incoming_bytes.unset(r);
    hs::get_total_outgoing_bytes.unset(r);
    hs::get_all_total_outgoing_bytes.unset(r);
}

}
