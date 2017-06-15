/*
 * Copyright (C) 2016 ScyllaDB
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

#include "database.hh"
#include "core/timer.hh"
#include "core/sharded.hh"

namespace service {

class cache_hitrate_calculator : public seastar::async_sharded_service<cache_hitrate_calculator> {
    seastar::sharded<database>& _db;
    seastar::sharded<cache_hitrate_calculator>& _me;
    timer<lowres_clock> _timer;
    bool _stopped = false;
    float _diff = 0;

    future<lowres_clock::duration> recalculate_hitrates();
    void recalculate_timer();
public:
    cache_hitrate_calculator(seastar::sharded<database>& db, seastar::sharded<cache_hitrate_calculator>& me);
    void run_on(size_t master, lowres_clock::duration d = std::chrono::milliseconds(2000));

    future<> stop();
};

}
