/*
 * Copyright (C) 2015 ScyllaDB
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

#include "rate_limiter.hh"

utils::rate_limiter::rate_limiter(size_t rate)
        : _units_per_s(rate) {
    if (_units_per_s != 0) {
        _timer.set_callback(std::bind(&rate_limiter::on_timer, this));
        _timer.arm(lowres_clock::now() + std::chrono::seconds(1),
                std::experimental::optional<lowres_clock::duration> {
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
