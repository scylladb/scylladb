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
 * Modified by Cloudius Systems.
 * Copyright 2015 Cloudius Systems.
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

namespace streaming {

extern logging::logger sslog;

stream_transfer_task::stream_transfer_task(shared_ptr<stream_session> session, UUID cf_id, std::vector<range<dht::token>> ranges, long total_size)
    : stream_task(session, cf_id)
    , _ranges(std::move(ranges))
    , _total_size(total_size) {
}

stream_transfer_task::~stream_transfer_task() = default;

struct send_info {
    database& db;
    utils::UUID plan_id;
    utils::UUID cf_id;
    query::partition_range pr;
    net::messaging_service::msg_addr id;
    uint32_t dst_cpu_id;
    size_t mutations_nr{0};
    semaphore mutations_done{0};
    bool error_logged = false;
    send_info(database& db_, utils::UUID plan_id_, utils::UUID cf_id_,
              query::partition_range pr_, net::messaging_service::msg_addr id_,
              uint32_t dst_cpu_id_)
        : db(db_)
        , plan_id(plan_id_)
        , cf_id(cf_id_)
        , pr(pr_)
        , id(id_)
        , dst_cpu_id(dst_cpu_id_) {
    }
};

future<stop_iteration> do_send_mutations(auto si, auto fm) {
    return get_local_stream_manager().mutation_send_limiter().wait().then([si, fm = std::move(fm)] () mutable {
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION to {}, cf_id={}", si->plan_id, si->id, si->cf_id);
        auto fm_size = fm.representation().size();
        net::get_local_messaging_service().send_stream_mutation(si->id, si->plan_id, std::move(fm), si->dst_cpu_id).then([si, fm_size] {
            sslog.debug("[Stream #{}] GOT STREAM_MUTATION Reply from {}", si->plan_id, si->id.addr);
            get_local_stream_manager().update_progress(si->plan_id, si->id.addr, progress_info::direction::OUT, fm_size);
            si->mutations_done.signal();
        }).handle_exception([si] (auto ep) {
            // There might be larger number of STREAM_MUTATION inflight.
            // Log one error per column_family per range
            if (!si->error_logged) {
                si->error_logged = true;
                sslog.error("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION to {}: {}", si->plan_id, si->id, ep);
            }
            si->mutations_done.broken();
        }).finally([] {
            get_local_stream_manager().mutation_send_limiter().signal();
        });
        return stop_iteration::no;
    });
}

future<> send_mutations(auto si) {
    auto& cf = si->db.find_column_family(si->cf_id);
    auto& priority = service::get_local_mutation_stream_priority();
    return do_with(cf.make_reader(cf.schema(), si->pr, priority), [si] (auto& reader) {
        return repeat([si, &reader] () {
            return reader().then([si] (auto mopt) {
                if (mopt && si->db.column_family_exists(si->cf_id)) {
                    si->mutations_nr++;
                    auto fm = frozen_mutation(*mopt);
                    return do_send_mutations(si, std::move(fm));
                } else {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }
            });
        });
    }).then([si] {
        return si->mutations_done.wait(si->mutations_nr);
    });
}

void stream_transfer_task::start() {
    auto plan_id = session->plan_id();
    auto cf_id = this->cf_id;
    auto id = net::messaging_service::msg_addr{session->peer, session->dst_cpu_id};
    sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}", plan_id, cf_id);
    parallel_for_each(_ranges.begin(), _ranges.end(), [this, plan_id, cf_id, id] (auto range) {
        unsigned shard_begin = range.start() ? dht::shard_of(range.start()->value()) : 0;
        unsigned shard_end = range.end() ? dht::shard_of(range.end()->value()) + 1 : smp::count;
        auto cf_id = this->cf_id;
        auto dst_cpu_id = this->session->dst_cpu_id;
        auto pr = query::to_partition_range(range);
        auto shard_range = boost::irange<unsigned>(shard_begin, shard_end);
        sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}, shard_begin={} shard_end={}", plan_id, cf_id, shard_begin, shard_end);
        return parallel_for_each(shard_range.begin(), shard_range.end(),
                [this, plan_id, cf_id, id, dst_cpu_id, pr] (unsigned shard) {
            sslog.debug("[Stream #{}] stream_transfer_task: cf_id={}, invoke_on shard={}", plan_id, cf_id, shard);
            return this->session->get_db().invoke_on(shard, [plan_id, cf_id, id, dst_cpu_id, pr] (database& db) {
                // Send mutations on related shards, do not capture this
                auto si = make_lw_shared<send_info>(db, plan_id, cf_id, pr, id, dst_cpu_id);
                return send_mutations(si);
            });
        });
    }).then([this, plan_id, cf_id, id] {
        sslog.debug("[Stream #{}] SEND STREAM_MUTATION_DONE to {}, cf_id={}", plan_id, id, cf_id);
        return session->ms().send_stream_mutation_done(id, plan_id, _ranges,
                cf_id, session->dst_cpu_id).handle_exception([plan_id, id, cf_id] (auto ep) {
            sslog.error("[Stream #{}] stream_transfer_task: Fail to send STREAM_MUTATION_DONE to {}: {}", plan_id, id, ep);
            throw;
        });
    }).then([this, id, plan_id, cf_id] {
        sslog.debug("[Stream #{}] GOT STREAM_MUTATION_DONE Reply from {}", plan_id, id.addr);
        session->start_keep_alive_timer();
        session->transfer_task_completed(cf_id);
    }).handle_exception([this, plan_id, id] (auto ep){
        sslog.error("[Stream #{}] stream_transfer_task: Fail to send to {}: {}", plan_id, id, ep);
        this->session->on_error();
    });
}

} // namespace streaming
