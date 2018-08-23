/*
 * Copyright (C) 2018 ScyllaDB
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

#include <seastar/core/sleep.hh>

template<typename EventuallySucceedingFunction>
void eventually(EventuallySucceedingFunction&& f, size_t max_attempts = 12) {
    size_t attempts = 0;
    while (true) {
        try {
            f();
            break;
        } catch (...) {
            if (++attempts < max_attempts) {
                sleep(std::chrono::milliseconds(1 << attempts)).get0();
            } else {
                throw;
            }
        }
    }
}

template<typename EventuallySucceedingFunction>
bool eventually_true(EventuallySucceedingFunction&& f) {
    const unsigned max_attempts = 10;
    unsigned attempts = 0;
    while (true) {
        if (f()) {
            return true;
        }

        if (++attempts < max_attempts) {
            seastar::sleep(std::chrono::milliseconds(1 << attempts)).get0();
        } else {
            return false;
        }
    }

    return false;
}

#define REQUIRE_EVENTUALLY_EQUAL(a, b) BOOST_REQUIRE(eventually_true([&] { return a == b; }))
