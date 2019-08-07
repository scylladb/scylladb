/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by ScyllaDB
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

#include "log.hh"
#include "streaming/stream_detail.hh"
#include "streaming/stream_transfer_task.hh"
#include "streaming/stream_session.hh"
#include "streaming/stream_manager.hh"
#include "streaming/stream_reason.hh"
#include "streaming/stream_mutation_fragments_cmd.hh"
#include "mutation_reader.hh"
#include "frozen_mutation.hh"
#include "mutation.hh"
#include "message/messaging_service.hh"
#include "range.hh"
#include "dht/i_partitioner.hh"
#include "service/priority_manager.hh"
#include <boost/range/irange.hpp>
#include "service/storage_service.hh"
#include <boost/icl/interval.hpp>
#include <boost/icl/interval_set.hpp>
#include "sstables/sstables.hh"

namespace streaming {

extern logging::logger sslog;

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session, UUID cf_id, dht::token_range_vector ranges, long total_size)
    : stream_task(session, cf_id)
    , _ranges(std::move(ranges))
    , _total_size(total_size) {
}

stream_transfer_task::~stream_transfer_task() = default;

dht::partition_range_vector to_partition_ranges(const dht::token_range_vector& ranges) {
    dht::partition_range_vector prs;
    prs.reserve(ranges.size());
    for (auto& range : ranges) {
        prs.push_back(dht::to_partition_range(range));
    }
    return prs;
}

struct send_info {
    database& db;
    utils::UUID plan_id;
    utils::UUID cf_id;
    netw::messaging_service::msg_addr id;
    uint32_t dst_cpu_id;
    stream_reason reason;
    size_t mutations_nr{0};
    semaphore mutations_done{0};
    bool error_logged = false;
    column_family& cf;
    dht::token_range_vector ranges;
    dht::partition_range_vector prs;
    flat_mutation_reader reader;
    send_info(database& db_, utils::UUID plan_id_, utils::UUID cf_id_,
              dht::token_range_vector ranges_, netw::messaging_service::msg_addr id_,
              uint32_t dst_cpu_id_, stream_reason reason_)
        : db(db_)
        , plan_id(plan_id_)
        , cf_id(cf_id_)
        , id(id_)
        , dst_cpu_id(dst_cpu_id_)
        , reason(reason_)
        , cf(db.find_column_family(cf_id))
        , ranges(std::move(ranges_))
        , prs(to_partition_ranges(ranges))
        , reader(cf.make_streaming_reader(cf.schema(), prs)) {
    }
    future<bool> has_relevant_range_on_this_shard() {
        return do_with(false, [this] (bool& found_relevant_range) {
            return do_for_each(ranges, [this, &found_relevant_range] (dht::token_range range) {
                if (!found_relevant_range) {
                    auto sharder = dht::selective_token_range_sharder(range, engine().cpu_id());
                    auto range_shard = sharder.next();
                    if (range_shard) {
                        found_relevant_range = true;
                    }
                }
            }).then([&found_relevant_range] {
                return found_relevant_range;
            });
        });
    }
    future<size_t> estimate_partitions() {
        return do_with(cf.get_sstables(), size_t(0), [this] (auto& sstables, size_t& partition_count) {
            return do_for_each(*sstables, [this, &partition_count] (auto& sst) {
                return do_for_each(ranges, [this, &sst, &partition_count] (auto& range) {
                    partition_count += sst->estimated_keys_for_range(range);
                });
            }).then([&partition_count] {
                return partition_count;
            });
        });
    }
};

future<stop_iteration> do_send_mutations(lw_shared_ptr<send_info> si, frozen_mutation fm, bool fragmented) {
    return get_local_stream_manager().mutation_send_limiter().wait().then([si, fragmented, fm = std::move(fm)] () mutable {
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION to {}, cf_id={}", si->plan_id, si->id, si->cf_id);
        auto fm_size = fm.representation().size();
        netw::get_local_messaging_service().send_stream_mutation(si->id, si->plan_id, std::move(fm), si->dst_cpu_id, fragmented, si->reason).then([si, fm_size] {
            sslog.debug("[Stream #{}] GOT STREAM_MUTATION Reply from {}", si->plan_id, si->id.addr);
            get_local_stream_manager().update_progress(si->plan_id, si->id.addr, progress_info::direction::OUT, fm_size);
            si->mutations_done.signal();
        }).handle_exception([si] (auto ep) {
            // There might be larger number of STREAM_MUTATION inflight.
            // Log one error per column_family per range
            if (!si->error_logged) {
                si->error_logged = true;
                sslog.warn("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION to {}: {}", si->plan_id, si->id, ep);
            }
            si->mutations_done.broken();
        }).finally([] {
            get_local_stream_manager().mutation_send_limiter().signal();
        });
        return stop_iteration::no;
    });
}

future<> send_mutations(lw_shared_ptr<send_info> si) {
    size_t fragment_size = default_frozen_fragment_size;
    // Mutations cannot be sent fragmented if the receiving side doesn't support that.
    if (!service::get_local_storage_service().cluster_supports_large_partitions()) {
        fragment_size = std::numeric_limits<size_t>::max();
    }
    return fragment_and_freeze(std::move(si->reader), [si] (auto fm, bool fragmented) {
        if (!si->db.column_family_exists(si->cf_id)) {
            return make_ready_future<stop_iteration>(stop_iteration::yes);
        }
        si->mutations_nr++;
        return do_send_mutations(si, std::move(fm), fragmented);
    }, fragment_size).then([si] {
        return si->mutations_done.wait(si->mutations_nr);
    });
}

future<> send_mutation_fragments(lw_shared_ptr<send_info> si) {
  return si->estimate_partitions().then([si] (size_t estimated_partitions) {
    sslog.info("[Stream #{}] Start sending ks={}, cf={}, estimated_partitions={}, with new rpc streaming", si->plan_id, si->cf.schema()->ks_name(), si->cf.schema()->cf_name(), estimated_partitions);
    return netw::get_local_messaging_service().make_sink_and_source_for_stream_mutation_fragments(si->reader.schema()->version(), si->plan_id, si->cf_id, estimated_partitions, si->reason, si->id).then([si] (rpc::sink<frozen_mutation_fragment, stream_mutation_fragments_cmd> sink, rpc::source<int32_t> source) mutable {
        auto got_error_from_peer = make_lw_shared<bool>(false);

        auto source_op = [source, got_error_from_peer, si] () mutable -> future<> {
            return repeat([source, got_error_from_peer, si] () mutable {
                return source().then([source, got_error_from_peer, si] (stdx::optional<std::tuple<int32_t>> status_opt) mutable {
                    if (status_opt) {
                        auto status = std::get<0>(*status_opt);
                        *got_error_from_peer = status == -1;
                        sslog.debug("Got status code from peer={}, plan_id={}, cf_id={}, status={}", si->id.addr, si->plan_id, si->cf_id, status);
                        // we've got an error from the other side, but we cannot just abandon rpc::source we
                        // need to continue reading until EOS since this will signal that no more work
                        // is left and rpc::source can be destroyed. The sender closes connection immediately
                        // after sending the status, so EOS should arrive shortly.
                        return stop_iteration::no;
                    } else {
                        return stop_iteration::yes;
                    }
                });
            });
        }();

        auto sink_op = [sink, si, got_error_from_peer] () mutable -> future<> {
            return do_with(std::move(sink), [si, got_error_from_peer] (rpc::sink<frozen_mutation_fragment, stream_mutation_fragments_cmd>& sink) {
                return repeat([&sink, si, got_error_from_peer] () mutable {
                    return si->reader(db::no_timeout).then([&sink, si, s = si->reader.schema(), got_error_from_peer] (mutation_fragment_opt mf) mutable {
                        if (mf && !(*got_error_from_peer)) {
                            frozen_mutation_fragment fmf = freeze(*s, *mf);
                            auto size = fmf.representation().size();
                            streaming::get_local_stream_manager().update_progress(si->plan_id, si->id.addr, streaming::progress_info::direction::OUT, size);
                            return sink(fmf, stream_mutation_fragments_cmd::mutation_fragment_data).then([] { return stop_iteration::no; });
                        } else {
                            return make_ready_future<stop_iteration>(stop_iteration::yes);
                        }
                    });
                }).then([&sink] () mutable {
                    return sink(frozen_mutation_fragment(bytes_ostream()), stream_mutation_fragments_cmd::end_of_stream);
                }).handle_exception([&sink] (std::exception_ptr ep) mutable {
                    // Notify the receiver the sender has failed
                    return sink(frozen_mutation_fragment(bytes_ostream()), stream_mutation_fragments_cmd::error).then([ep = std::move(ep)] () mutable {
                        return make_exception_future<>(std::move(ep));
                    });
                }).finally([&sink] () mutable {
                    return sink.close();
                });
            });
        }();

        return when_all_succeed(std::move(source_op), std::move(sink_op)).then([got_error_from_peer, si] {
            if (*got_error_from_peer) {
                throw std::runtime_error(sprint("Peer failed to process mutation_fragment peer=%s, plan_id=%s, cf_id=%s", si->id.addr, si->plan_id, si->cf_id));
            }
        });
    });
  });
}

future<> stream_transfer_task::execute() {
    auto plan_id = session->plan_id();
    auto cf_id = this->cf_id;
    auto dst_cpu_id = session->dst_cpu_id;
    auto id = netw::messaging_service::msg_addr{session->peer, session->dst_cpu_id};
    sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}", plan_id, cf_id);
    sort_and_merge_ranges();
    bool streaming_with_rpc_stream = service::get_local_storage_service().cluster_supports_stream_with_rpc_stream();
    auto reason = session->get_reason();
    return session->get_db().invoke_on_all([plan_id, cf_id, id, dst_cpu_id, ranges=this->_ranges, streaming_with_rpc_stream, reason] (database& db) {
        auto si = make_lw_shared<send_info>(db, plan_id, cf_id, std::move(ranges), id, dst_cpu_id, reason);
        return si->has_relevant_range_on_this_shard().then([si, plan_id, cf_id, streaming_with_rpc_stream] (bool has_relevant_range_on_this_shard) {
            if (!has_relevant_range_on_this_shard) {
                sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}: ignore ranges on shard={}",
                        plan_id, cf_id, engine().cpu_id());
                return make_ready_future<>();
            }
            if (streaming_with_rpc_stream) {
                return send_mutation_fragments(std::move(si));
            } else {
                return send_mutations(std::move(si));
            }
        });
    }).then([this, plan_id, cf_id, id, streaming_with_rpc_stream] {
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION_DONE to {}, cf_id={}", plan_id, id, cf_id);
        return session->ms().send_stream_mutation_done(id, plan_id, _ranges,
                cf_id, session->dst_cpu_id).handle_exception([plan_id, id, cf_id] (auto ep) {
            sslog.warn("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION_DONE to {}: {}", plan_id, id, ep);
            std::rethrow_exception(ep);
        });
    }).then([this, id, plan_id, cf_id] {
        sslog.debug("[Stream #{}] GOT STREAM_MUTATION_DONE Reply from {}", plan_id, id.addr);
    }).handle_exception([this, plan_id, id] (auto ep){
        sslog.warn("[Stream #{}] stream_transfer_task: Fail to send to {}: {}", plan_id, id, ep);
        std::rethrow_exception(ep);
    });
}

void stream_transfer_task::append_ranges(const dht::token_range_vector& ranges) {
    _ranges.insert(_ranges.end(), ranges.begin(), ranges.end());
}

void stream_transfer_task::sort_and_merge_ranges() {
    boost::icl::interval_set<dht::token> myset;
    dht::token_range_vector ranges;
    sslog.debug("cf_id = {}, before ranges = {}, size={}", cf_id, _ranges, _ranges.size());
    _ranges.swap(ranges);
    for (auto& range : ranges) {
        // TODO: We should convert range_to_interval and interval_to_range to
        // take nonwrapping_range ranges.
        myset += locator::token_metadata::range_to_interval(range);
    }
    ranges.clear();
    ranges.shrink_to_fit();
    for (auto& i : myset) {
        auto r = locator::token_metadata::interval_to_range(i);
        _ranges.push_back(dht::token_range(r));
    }
    sslog.debug("cf_id = {}, after  ranges = {}, size={}", cf_id, _ranges, _ranges.size());
}

} // namespace streaming
