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

#include "wasm_engine.hh"

namespace wasm {

#ifdef SCYLLA_ENABLE_WASMTIME

seastar::future<> init_sharded_engine(seastar::sharded<engine>& e) {
    // Fuel defines more or less how many bytecode instructions
    // can be performed at once. Empirically, 20k units
    // allow for considerably less than 0.5ms of preemption-free execution time.
    // TODO: investigate other configuration variables.
    // We're particularly interested in limiting resource usage
    // and yielding in the middle of execution - which is possible
    // in the original wasmtime implementation for Rust and tightly
    // bound with its native async support, but not yet possible
    // in wasmtime.hh binding at the time of this writing.
    // It's highly probable that a more generic support for yielding
    // can be contributed to wasmtime.  
    const uint64_t initial_fuel_amount = 20*1024;
    return e.start(initial_fuel_amount);
}

#else

seastar::future<> init_sharded_engine(seastar::sharded<engine>& e) {
    return e.start();
}


#endif

} // namespace wasm
