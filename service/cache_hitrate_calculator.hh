/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma  once

#include "replica/database_fwd.hh"
#include "schema/schema_fwd.hh"
#include <seastar/core/timer.hh>
#include <seastar/core/sharded.hh>

using namespace seastar;

namespace gms { class gossiper; }

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

    seastar::sharded<replica::database>& _db;
    gms::gossiper& _gossiper;
    timer<lowres_clock> _timer;
    bool _stopped = false;
    float _diff = 0;
    std::unordered_map<table_id, stat> _rates;
    size_t _slen = 0;
    std::string _gstate;
    uint64_t _published_nr = 0;
    lowres_clock::time_point _published_time;
    future<> _done = make_ready_future();

    future<lowres_clock::duration> recalculate_hitrates();
    void recalculate_timer();
public:
    cache_hitrate_calculator(seastar::sharded<replica::database>& db, gms::gossiper& g);
    void run_on(size_t master, lowres_clock::duration d = std::chrono::milliseconds(2000));

    future<> stop();
};

}
