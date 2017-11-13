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

namespace streaming {

extern logging::logger sslog;

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session, UUID cf_id, dht::token_range_vector ranges, long total_size)
    : stream_task(session, cf_id)
    , _ranges(std::move(ranges))
    , _total_size(total_size) {
}

stream_transfer_task::~stream_transfer_task() = default;

struct send_info {
    database& db;
    utils::UUID plan_id;
    utils::UUID cf_id;
    dht::partition_range_vector prs;
    netw::messaging_service::msg_addr id;
    uint32_t dst_cpu_id;
    size_t mutations_nr{0};
    semaphore mutations_done{0};
    bool error_logged = false;
    flat_mutation_reader reader;
    send_info(database& db_, utils::UUID plan_id_, utils::UUID cf_id_,
              dht::partition_range_vector prs_, netw::messaging_service::msg_addr id_,
              uint32_t dst_cpu_id_)
        : db(db_)
        , plan_id(plan_id_)
        , cf_id(cf_id_)
        , prs(std::move(prs_))
        , id(id_)
        , dst_cpu_id(dst_cpu_id_)
        , reader([&] {
            auto& cf = db.find_column_family(cf_id);
            return cf.make_streaming_reader(cf.schema(), prs);
        }())
    { }
};

future<stop_iteration> do_send_mutations(lw_shared_ptr<send_info> si, frozen_mutation fm, bool fragmented) {
    return get_local_stream_manager().mutation_send_limiter().wait().then([si, fragmented, fm = std::move(fm)] () mutable {
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION to {}, cf_id={}", si->plan_id, si->id, si->cf_id);
        auto fm_size = fm.representation().size();
        netw::get_local_messaging_service().send_stream_mutation(si->id, si->plan_id, std::move(fm), si->dst_cpu_id, fragmented).then([si, fm_size] {
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

void stream_transfer_task::start() {
    auto plan_id = session->plan_id();
    auto cf_id = this->cf_id;
    auto dst_cpu_id = session->dst_cpu_id;
    auto& schema = session->get_local_db().find_column_family(cf_id).schema();
    auto id = netw::messaging_service::msg_addr{session->peer, session->dst_cpu_id};
    sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}", plan_id, cf_id);
    sort_and_merge_ranges();
    _shard_ranges = dht::split_ranges_to_shards(_ranges, *schema);
    parallel_for_each(_shard_ranges, [this, dst_cpu_id, plan_id, cf_id, id] (auto& item) {
        auto& shard = item.first;
        auto& prs = item.second;
        return session->get_db().invoke_on(shard, [plan_id, cf_id, id, dst_cpu_id, prs = std::move(prs)] (database& db) mutable {
            auto si = make_lw_shared<send_info>(db, plan_id, cf_id, prs, id, dst_cpu_id);
            return send_mutations(std::move(si));
        });
    }).then([this, plan_id, cf_id, id] {
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION_DONE to {}, cf_id={}", plan_id, id, cf_id);
        return session->ms().send_stream_mutation_done(id, plan_id, _ranges,
                cf_id, session->dst_cpu_id).handle_exception([plan_id, id, cf_id] (auto ep) {
            sslog.warn("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION_DONE to {}: {}", plan_id, id, ep);
            std::rethrow_exception(ep);
        });
    }).then([this, id, plan_id, cf_id] {
        sslog.debug("[Stream #{}] GOT STREAM_MUTATION_DONE Reply from {}", plan_id, id.addr);
        session->start_keep_alive_timer();
        session->transfer_task_completed(cf_id);
    }).handle_exception([this, plan_id, id] (auto ep){
        sslog.warn("[Stream #{}] stream_transfer_task: Fail to send to {}: {}", plan_id, id, ep);
        this->session->on_error();
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
