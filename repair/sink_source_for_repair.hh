/*
 * Copyright (C) 2018 ScyllaDB
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

#pragma once

#include <functional>
#include <tuple>
#include <type_traits>
#include <vector>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/rpc/rpc_types.hh>
#include "message/messaging_service.hh"
#include "gms/inet_address.hh"
#include "repair/repair.hh"

// Wraps sink and source objects for repair master or repair follower nodes.
// For repair master, it stores sink and source pair for each of the followers.
// For repair follower, it stores one sink and source pair for repair master.
template<class SinkType, class SourceType>
class sink_source_for_repair {
    uint32_t _repair_meta_id;
    using get_sink_source_fn_type = std::function<future<std::tuple<rpc::sink<SinkType>, rpc::source<SourceType>>> (uint32_t repair_meta_id, netw::messaging_service::msg_addr addr)>;
    using sink_type  = std::reference_wrapper<rpc::sink<SinkType>>;
    using source_type = std::reference_wrapper<rpc::source<SourceType>>;
    // The vectors below store sink and source object for peer nodes.
    std::vector<std::optional<rpc::sink<SinkType>>> _sinks;
    std::vector<std::optional<rpc::source<SourceType>>> _sources;
    std::vector<bool> _sources_closed;
    get_sink_source_fn_type _fn;
public:
    sink_source_for_repair(uint32_t repair_meta_id, size_t nr_peer_nodes, get_sink_source_fn_type fn);

    void mark_source_closed(unsigned node_idx);

    future<std::tuple<sink_type, source_type>> get_sink_source(gms::inet_address remote_node, unsigned node_idx);

    future<> close();
};

using sink_source_for_get_full_row_hashes = sink_source_for_repair<repair_stream_cmd, repair_hash_with_cmd>;
using sink_source_for_get_row_diff = sink_source_for_repair<repair_hash_with_cmd, repair_row_on_wire_with_cmd>;
using sink_source_for_put_row_diff = sink_source_for_repair<repair_row_on_wire_with_cmd, repair_stream_cmd>;
