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
 */

/*
 * Modified by ScyllaDB
 * Copyright 2015 ScyllaDB
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

#include "load_broadcaster.hh"
#include "cache_hitrate_calculator.hh"
#include "db/system_keyspace.hh"

namespace service {

constexpr std::chrono::milliseconds load_broadcaster::BROADCAST_INTERVAL;

logging::logger llogger("load_broadcaster");

void load_broadcaster::start_broadcasting() {
    _done = make_ready_future<>();

    // send the first broadcast "right away" (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
    // after that send every BROADCAST_INTERVAL.

    _timer.set_callback([this] {
        llogger.debug("Disseminating load info ...");
        _done = _db.map_reduce0([](database& db) {
            int64_t res = 0;
            for (auto i : db.get_column_families()) {
                res += i.second->get_stats().live_disk_space_used;
            }
            return res;
        }, int64_t(0), std::plus<int64_t>()).then([this] (int64_t size) {
            gms::versioned_value::factory value_factory;
            return _gossiper.add_local_application_state(gms::application_state::LOAD,
                value_factory.load(size)).then([this] {
                _timer.arm(BROADCAST_INTERVAL);
                return make_ready_future<>();
            });
        });
    });

    _timer.arm(2 * gms::gossiper::INTERVAL);
}

future<> load_broadcaster::stop_broadcasting() {
    _timer.cancel();
    return std::move(_done);
}


// cache_hitrate_calculator implementation
cache_hitrate_calculator::cache_hitrate_calculator(seastar::sharded<database>& db, seastar::sharded<cache_hitrate_calculator>& me) : _db(db), _me(me),
        _timer(std::bind(std::mem_fn(&cache_hitrate_calculator::recalculate_timer), this))
{}

void cache_hitrate_calculator::recalculate_timer() {
    recalculate_hitrates().then_wrapped([p = shared_from_this()] (future<lowres_clock::duration> f) {
        lowres_clock::duration d;
        if (f.failed()) {
            d = std::chrono::milliseconds(2000);
        } else {
            d = f.get0();
        }
        p->run_on((engine().cpu_id() + 1) % smp::count, d);
    });
}

void cache_hitrate_calculator::run_on(size_t master, lowres_clock::duration d) {
    if (!_stopped) {
        _me.invoke_on(master, [d] (cache_hitrate_calculator& local) {
            local._timer.arm(d);
        }).handle_exception_type([] (seastar::no_sharded_instance_exception&) { /* ignore */ });
    }
}

future<lowres_clock::duration> cache_hitrate_calculator::recalculate_hitrates() {
    struct stat {
        float h = 0;
        float m = 0;
        stat& operator+=(stat& o) {
            h += o.h;
            m += o.m;
            return *this;
        }
    };

    static auto non_system_filter = [] (const std::pair<utils::UUID, lw_shared_ptr<column_family>>& cf) {
        return cf.second->schema()->ks_name() != db::system_keyspace::NAME;
    };

    auto cf_to_cache_hit_stats = [] (database& db) {
        return boost::copy_range<std::unordered_map<utils::UUID, stat>>(db.get_column_families() | boost::adaptors::filtered(non_system_filter) |
                boost::adaptors::transformed([]  (const std::pair<utils::UUID, lw_shared_ptr<column_family>>& cf) {
            auto& stats = cf.second->get_row_cache().stats();
            return std::make_pair(cf.first, stat{float(stats.hits.rate().rates[0]), float(stats.misses.rate().rates[0])});
        }));
    };

    auto sum_stats_per_cf = [] (std::unordered_map<utils::UUID, stat> a, std::unordered_map<utils::UUID, stat> b) {
        for (auto& r : b) {
            a[r.first] += r.second;
        }
        return std::move(a);
    };

    return _db.map_reduce0(cf_to_cache_hit_stats, std::unordered_map<utils::UUID, stat>(), sum_stats_per_cf).then([this] (std::unordered_map<utils::UUID, stat> rates) mutable {
        _diff = 0;
        // set calculated rates on all shards
        return _db.invoke_on_all([this, rates = std::move(rates), cpuid = engine().cpu_id()] (database& db) {
            for (auto& cf : db.get_column_families() | boost::adaptors::filtered(non_system_filter)) {
                stat s = rates.at(cf.first);
                float rate = 0;
                if (s.h) {
                    rate = s.h / (s.h + s.m);
                }
                if (engine().cpu_id() == cpuid) {
                    // calculate max difference between old rate and new one for all cfs
                    _diff = std::max(_diff, std::abs(float(cf.second->get_global_cache_hit_rate()) - rate));
                }
                cf.second->set_global_cache_hit_rate(cache_temperature(rate));
            }
        });
    }).then([this] {
        // if max difference during this round is big schedule next recalculate earlier
        if (_diff < 0.01) {
            return std::chrono::milliseconds(2000);
        } else {
            return std::chrono::milliseconds(500);
        }
    });
}

future<> cache_hitrate_calculator::stop() {
    _timer.cancel();
    _stopped = true;
    return make_ready_future<>();
}

}
