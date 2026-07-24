/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

#include "db/timeout_clock.hh"
#include "timeout_config.hh"

class service_permit {
    struct impl {
        seastar::semaphore_units<> units;
        std::optional<db::timeout_clock::time_point> start_time;
        db::timeout_clock::time_point deadline = db::timeout_clock::time_point::max();
        timeout_context timeout_ctx;
    };
    seastar::lw_shared_ptr<impl> _permit;
    service_permit(seastar::semaphore_units<>&& u) : _permit(seastar::make_lw_shared<impl>()) {
        _permit->units = std::move(u);
    }
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit);
    friend service_permit empty_service_permit();
public:
    size_t count() const { return _permit ? _permit->units.count() : 0; };
    // Merge additional semaphore units into this permit.
    // Used to grow the permit after the actual resource cost is known.
    void adopt(seastar::semaphore_units<>&& units) {
        if (_permit) {
            _permit->units.adopt(std::move(units));
        }
    }
    void set_start_time(db::timeout_clock::time_point t) {
        if (_permit) {
            _permit->start_time = t;
        }
    }
    std::optional<db::timeout_clock::time_point> start_time() const {
        return _permit ? _permit->start_time : std::nullopt;
    }
    void set_deadline(db::timeout_clock::time_point d) {
        if (_permit) {
            _permit->deadline = d;
        }
    }
    db::timeout_clock::time_point deadline() const {
        return _permit ? _permit->deadline : db::timeout_clock::time_point::max();
    }
    void set_timeout_ctx(timeout_context ctx) {
        if (_permit) {
            _permit->timeout_ctx = ctx;
        }
    }
    timeout_context timeout_ctx() const {
        return _permit ? _permit->timeout_ctx : timeout_context{};
    }
};

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit) {
    return service_permit(std::move(permit));
}

inline service_permit empty_service_permit() {
    return make_service_permit(seastar::semaphore_units<>());
}
