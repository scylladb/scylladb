/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/signal.hh>

#include "seastarx.hh"

// Must live in a seastar::thread
class stop_signal {
    bool _caught = false;
    condition_variable _cond;
    sharded<abort_source> _abort_sources;
    future<> _broadcasts_to_abort_sources_done = make_ready_future<>();
private:
    void signaled() {
        if (_caught) {
            return;
        }
        _caught = true;
        _cond.broadcast();
        _broadcasts_to_abort_sources_done = _broadcasts_to_abort_sources_done.then([this] {
            return _abort_sources.invoke_on_all(&abort_source::request_abort);
        });
    }
public:
    stop_signal() {
        _abort_sources.start().get();
        handle_signal(SIGINT, [this] { signaled(); });
        handle_signal(SIGTERM, [this] { signaled(); });
    }
    ~stop_signal() {
        // There's no way to unregister a handler yet, so register a no-op handler instead.
        handle_signal(SIGINT, [] {});
        handle_signal(SIGTERM, [] {});
        _broadcasts_to_abort_sources_done.get();
        _abort_sources.stop().get();
    }
    future<> wait() {
        return _cond.wait([this] { return _caught; });
    }
    bool stopping() const {
        return _caught;
    }
    abort_source& as_local_abort_source() { return _abort_sources.local(); }
    sharded<abort_source>& as_sharded_abort_source() { return _abort_sources; }
};
