/*
 * Copyright (C) 2020 ScyllaDB
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

#include <seastar/core/seastar.hh>
#include <seastar/core/timer.hh>

#include "service/storage_proxy.hh"

namespace cdc::kafka {

class kafka_upload_service final {
    service::storage_proxy& _proxy;
    timer<seastar::lowres_clock> _timer;

    void on_timer() {
        arm_timer();

        // Logic goes here. Remember to wait for it to finish in
        // kafka_upload_service::stop
    }

    void arm_timer() {
        _timer.arm(seastar::lowres_clock::now() + std::chrono::seconds(10));
    }
public:
    kafka_upload_service(service::storage_proxy& proxy)
        : _proxy(proxy)
        , _timer([this] { on_timer(); })
    {
        _proxy.set_kafka_upload_service(this);
        arm_timer();
    }

    future<> stop() {
        _timer.cancel();
        return make_ready_future<>();
    }
};

} // namespace cdc::kafka
