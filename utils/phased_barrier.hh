/*
 * Copyright 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
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
    using gate = seastar::named_gate;
    sstring _name;
    lw_shared_ptr<gate> _gate;
    phase_type _phase;
public:
    explicit phased_barrier(sstring name)
        : _name(std::move(name))
        , _gate(make_lw_shared<gate>(_name))
        , _phase(0)
    { }
    phased_barrier(phased_barrier&&) = default;
    phased_barrier& operator=(phased_barrier&&) = default;

    class operation {
        lw_shared_ptr<gate> _gate;
    public:
        operation() : _gate() {}
        operation(lw_shared_ptr<gate> g) : _gate(std::move(g)) {}
        operation(const operation&) = delete;
        operation(operation&&) = default;
        operation& operator=(operation&& o) noexcept {
            if (this != &o) {
                this->~operation();
                new (this) operation(std::move(o));
            }
            return *this;
        }
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

    future<> close() noexcept {
        return _gate->close();
    }

    bool is_closed() const noexcept {
        return _gate->is_closed();
    }

    // Starts a new phase and waits for all operations started in any of the earlier phases.
    // It is fine to start multiple awaits in parallel.
    // Cannot fail.
    future<> advance_and_await() noexcept {
        if (!operations_in_progress()) {
            ++_phase;
            return make_ready_future();
        }
        auto new_gate = [this] {
            seastar::memory::scoped_critical_alloc_section _;
            return make_lw_shared<gate>(_name);
        }();
        ++_phase;
        auto old_gate = std::exchange(_gate, std::move(new_gate));
        return old_gate->close().then([old_gate, op = start()] {});
    }

    // Returns current phase number. The smallest value returned is 0.
    phase_type phase() const {
        return _phase;
    }

    // Number of operations in current phase.
    size_t operations_in_progress() const {
        return _gate->get_count();
    }
};

}
