/*
 * Copyright (C) 2015 ScyllaDB
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

#include "stream_manager.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_result_future.hh"
#include "api/api-doc/stream_manager.json.hh"
#include <vector>
#include "gms/gossiper.hh"

namespace api {

namespace hs = httpd::stream_manager_json;

static void set_summaries(const std::vector<streaming::stream_summary>& from,
        json::json_list<hs::stream_summary>& to) {
    if (!from.empty()) {
        hs::stream_summary res;
        res.cf_id = boost::lexical_cast<std::string>(from.front().cf_id);
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
    res.peer = boost::lexical_cast<std::string>(info.peer);
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
        si.peer = boost::lexical_cast<std::string>(info.peer);
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

void set_stream_manager(http_context& ctx, routes& r) {
    hs::get_current_streams.set(r,
            [] (std::unique_ptr<request> req) {
                return streaming::get_stream_manager().invoke_on_all([] (auto& sm) {
                    return sm.update_all_progress_info();
                }).then([] {
                    return streaming::get_stream_manager().map_reduce0([](streaming::stream_manager& stream) {
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

    hs::get_all_active_streams_outbound.set(r, [](std::unique_ptr<request> req) {
        return streaming::get_stream_manager().map_reduce0([](streaming::stream_manager& stream) {
            return stream.get_initiated_streams().size();
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_total_incoming_bytes.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address peer(req->param["peer"]);
        return streaming::get_stream_manager().map_reduce0([peer](streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards(peer).then([] (auto sbytes) {
                return sbytes.bytes_received;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_all_total_incoming_bytes.set(r, [](std::unique_ptr<request> req) {
        return streaming::get_stream_manager().map_reduce0([](streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards().then([] (auto sbytes) {
                return sbytes.bytes_received;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_total_outgoing_bytes.set(r, [](std::unique_ptr<request> req) {
        gms::inet_address peer(req->param["peer"]);
        return streaming::get_stream_manager().map_reduce0([peer] (streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards(peer).then([] (auto sbytes) {
                return sbytes.bytes_sent;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });

    hs::get_all_total_outgoing_bytes.set(r, [](std::unique_ptr<request> req) {
        return streaming::get_stream_manager().map_reduce0([](streaming::stream_manager& sm) {
            return sm.get_progress_on_all_shards().then([] (auto sbytes) {
                return sbytes.bytes_sent;
            });
        }, 0, std::plus<int64_t>()).then([](int64_t res) {
            return make_ready_future<json::json_return_type>(res);
        });
    });
}

}
