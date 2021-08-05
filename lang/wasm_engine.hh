/*
 * Copyright (C) 2021-present ScyllaDB
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

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#ifdef SCYLLA_ENABLE_WASMTIME

#include "wasmtime.hh"

namespace wasm {

class engine {
    wasmtime::Engine _engine;
    uint64_t _initial_fuel_amount;
public:
    engine(uint64_t initial_fuel_amount)
            : _engine(make_config())
            , _initial_fuel_amount(initial_fuel_amount)
        {}
    wasmtime::Engine& get() { return _engine; }
    uint64_t initial_fuel_amount() { return _initial_fuel_amount; };
private:
    wasmtime::Config make_config() {
        wasmtime::Config cfg;
        cfg.consume_fuel(true);
        return cfg;
    }
};

}

#else

namespace wasm {
class engine {};
}

#endif

namespace wasm {
seastar::future<> init_sharded_engine(seastar::sharded<engine>& e);
}
