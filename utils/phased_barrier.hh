/*
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

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include "seastarx.hh"

namespace utils {

// Synchronizer which allows to track and wait for asynchronous operations
// which were in progress at the time of wait initiation.
class phased_barrier {
public:
    using phase_type = uint64_t;
private:
    using gate = seastar::gate;
    lw_shared_ptr<gate> _gate;
    phase_type _phase;
public:
    phased_barrier()
        : _gate(make_lw_shared<gate>())
        , _phase(0)
    { }

    class operation {
        lw_shared_ptr<gate> _gate;
    public:
        operation() : _gate() {}
        operation(lw_shared_ptr<gate> g) : _gate(std::move(g)) {}
        operation(const operation&) = delete;
        operation(operation&&) = default;
        operation& operator=(operation&&) = default;
        ~operation() {
            if (_gate) {
                _gate->leave();
            }
        }
    };

    // Starts new operation. The operation ends when the "operation" object is destroyed.
    // The operation may last longer than the life time of the phased_barrier.
    operation start() {
        _gate->enter();
        return { _gate };
    }

    // Starts a new phase and waits for all operations started in any of the earlier phases.
    // It is fine to start multiple awaits in parallel.
    // Strong exception guarantees.
    future<> advance_and_await() {
        auto new_gate = make_lw_shared<gate>();
        ++_phase;
        auto old_gate = std::exchange(_gate, std::move(new_gate));
        return old_gate->close().then([old_gate, op = start()] {});
    }

    // Returns current phase number. The smallest value returned is 0.
    phase_type phase() const {
        return _phase;
    }
};

}
