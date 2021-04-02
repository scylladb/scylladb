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

#include <optional>
#include <seastar/core/loop.hh>
#include "repair/sink_source_for_repair.hh"

template<class SinkType, class SourceType>
sink_source_for_repair<SinkType, SourceType>::sink_source_for_repair(uint32_t repair_meta_id, size_t nr_peer_nodes, sink_source_for_repair::get_sink_source_fn_type fn)
    : _repair_meta_id(repair_meta_id)
    , _sinks(nr_peer_nodes)
    , _sources(nr_peer_nodes)
    , _sources_closed(nr_peer_nodes, false)
    , _fn(std::move(fn)) {
}

template<class SinkType, class SourceType>
void sink_source_for_repair<SinkType, SourceType>::mark_source_closed(unsigned node_idx) {
    _sources_closed[node_idx] = true;
}

template<class SinkType, class SourceType>
future<std::tuple<typename sink_source_for_repair<SinkType, SourceType>::sink_type, typename sink_source_for_repair<SinkType, SourceType>::source_type>>
sink_source_for_repair<SinkType, SourceType>::get_sink_source(gms::inet_address remote_node, unsigned node_idx) {
    using value_type = std::tuple<sink_type, source_type>;
    if (_sinks[node_idx] && _sources[node_idx]) {
        return make_ready_future<value_type>(value_type(_sinks[node_idx].value(), _sources[node_idx].value()));
    }
    if (_sinks[node_idx] || _sources[node_idx]) {
        return make_exception_future<value_type>(std::runtime_error(format("sink or source is missing for node {}", remote_node)));
    }
    return _fn(_repair_meta_id, netw::messaging_service::msg_addr(remote_node)).then_unpack([this, node_idx] (rpc::sink<SinkType> sink, rpc::source<SourceType> source) mutable {
        _sinks[node_idx].emplace(std::move(sink));
        _sources[node_idx].emplace(std::move(source));
        return make_ready_future<value_type>(value_type(_sinks[node_idx].value(), _sources[node_idx].value()));
    });
}

template<class SinkType, class SourceType>
future<> sink_source_for_repair<SinkType, SourceType>::close() {
    return parallel_for_each(boost::irange(unsigned(0), unsigned(_sources.size())), [this] (unsigned node_idx) mutable {
        std::optional<rpc::sink<SinkType>>& sink_opt = _sinks[node_idx];
        auto f = sink_opt ? sink_opt->close() : make_ready_future<>();
        return f.finally([this, node_idx] {
            std::optional<rpc::source<SourceType>>& source_opt = _sources[node_idx];
            if (source_opt && !_sources_closed[node_idx]) {
                return repeat([&source_opt] () mutable {
                    // Keep reading source until end of stream
                    return (*source_opt)().then([] (std::optional<std::tuple<SourceType>> opt) mutable {
                        if (opt) {
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        } else {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                    }).handle_exception([] (std::exception_ptr ep) {
                        return make_ready_future<stop_iteration>(stop_iteration::yes);
                    });
                });
            }
            return make_ready_future<>();
        });
    });
}

template class sink_source_for_repair<repair_stream_cmd, repair_hash_with_cmd>;
template class sink_source_for_repair<repair_hash_with_cmd, repair_row_on_wire_with_cmd>;
template class sink_source_for_repair<repair_row_on_wire_with_cmd, repair_stream_cmd>;
