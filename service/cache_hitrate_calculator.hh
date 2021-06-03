/*
 * Copyright (C) 2016-present ScyllaDB
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

#pragma  once

#include "database_fwd.hh"
#include "utils/UUID.hh"
#include <seastar/core/timer.hh>
#include <seastar/core/sharded.hh>

using namespace seastar;

namespace service {

class cache_hitrate_calculator : public seastar::async_sharded_service<cache_hitrate_calculator>, public seastar::peering_sharded_service<cache_hitrate_calculator> {
    struct stat {
        float h = 0;
        float m = 0;
        stat& operator+=(stat& o) {
            h += o.h;
            m += o.m;
            return *this;
        }
    };

    seastar::sharded<database>& _db;
    timer<lowres_clock> _timer;
    bool _stopped = false;
    float _diff = 0;
    std::unordered_map<utils::UUID, stat> _rates;
    size_t _slen = 0;
    std::string _gstate;
    future<> _done = make_ready_future();

    future<lowres_clock::duration> recalculate_hitrates();
    void recalculate_timer();
public:
    cache_hitrate_calculator(seastar::sharded<database>& db);
    void run_on(size_t master, lowres_clock::duration d = std::chrono::milliseconds(2000));

    future<> stop();
};

}
