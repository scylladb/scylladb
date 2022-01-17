/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

#ifdef SCYLLA_ENABLE_WASMTIME

#include "wasmtime.hh"

namespace wasm {

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
constexpr uint64_t default_initial_fuel_amount = 20*1024;

class engine {
    wasmtime::Engine _engine;
    uint64_t _initial_fuel_amount;
public:
    engine(uint64_t initial_fuel_amount = default_initial_fuel_amount)
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
