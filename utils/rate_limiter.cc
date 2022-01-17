/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "rate_limiter.hh"

utils::rate_limiter::rate_limiter(size_t rate)
        : _units_per_s(rate) {
    if (_units_per_s != 0) {
        _timer.set_callback(std::bind(&rate_limiter::on_timer, this));
        _timer.arm(lowres_clock::now() + std::chrono::seconds(1),
                std::optional<lowres_clock::duration> {
                        std::chrono::seconds(1) });
    }
}

void utils::rate_limiter::on_timer() {
    _sem.signal(_units_per_s - _sem.current());
}

future<> utils::rate_limiter::reserve(size_t u) {
    if (_units_per_s == 0) {
        return make_ready_future<>();
    }
    if (u <= _units_per_s) {
        return _sem.wait(u);
    }
    auto n = std::min(u, _units_per_s);
    auto r = u - n;
    return _sem.wait(n).then([this, r] {
        return reserve(r);
    });
}
