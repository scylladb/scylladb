/*
 * Copyright (C) 2018-present ScyllaDB
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

#define BOOST_TEST_MODULE partition_data
#include <boost/test/unit_test.hpp>

#include <random>

#include <boost/range/irange.hpp>
#include <boost/range/algorithm/generate.hpp>

#include "test/lib/random_utils.hh"
#include "utils/disk-error-handler.hh"
#include "atomic_cell.hh"
#include "types.hh"

BOOST_AUTO_TEST_CASE(test_atomic_cell) {
    struct test_case {
        bool live;
        bool fixed_size;
        bytes value;
        bool expiring;
        bool counter_update;
    };

    auto cases = std::vector<test_case> {
        // Live, fixed-size, empty cell
        { true, true, bytes(), false },
        // Live, small cell
        { true, false, tests::random::get_bytes(1024), false, false },
        // Live, large cell
        { true, false, tests::random::get_bytes(129 * 1024), false, false },
        // Live, empty cell
        { true, false, bytes(), false, false },
        // Live, expiring cell
        { true, false, tests::random::get_bytes(1024), true, false },
        // Dead cell
        { false, false, bytes(), false, false },
        // Counter update cell
        { true, false, bytes(), false, true },
    };

    for (auto tc : cases) {
        auto& live = tc.live;
        auto& fixed_size = tc.fixed_size;
        auto& value = tc.value;
        auto& expiring = tc.expiring;
        auto& counter_update = tc.counter_update;
        auto timestamp = tests::random::get_int<api::timestamp_type>();
        auto ttl = gc_clock::duration(tests::random::get_int<int32_t>(1, std::numeric_limits<int32_t>::max()));
        auto expiry_time = gc_clock::time_point(gc_clock::duration(tests::random::get_int<int32_t>(1, std::numeric_limits<int32_t>::max())));
        auto deletion_time = expiry_time;
        auto counter_update_value = tests::random::get_int<int64_t>();

        auto test_cell = [&] (auto cell) {
            auto verify_cell = [&] (auto view) {
                if (!live) {
                    BOOST_CHECK(!view.is_live());
                    BOOST_CHECK(view.deletion_time() == deletion_time);
                    return;
                }
                BOOST_CHECK(view.is_live());
                BOOST_CHECK_EQUAL(view.timestamp(), timestamp);
                if (counter_update) {
                    BOOST_CHECK(view.is_counter_update());
                    BOOST_CHECK_EQUAL(view.counter_update_value(), counter_update_value);
                } else {
                    BOOST_CHECK(!view.is_counter_update());
                    BOOST_CHECK(view.value() == managed_bytes_view(bytes_view(value)));
                }
                BOOST_CHECK_EQUAL(view.is_live_and_has_ttl(), expiring);
                if (expiring) {
                    BOOST_CHECK(view.ttl() == ttl);
                    BOOST_CHECK(view.expiry() == expiry_time);
                }
            };

            auto view = atomic_cell_view(cell);
            verify_cell(view);
        };

        if (live) {
            if (counter_update) {
                test_cell(atomic_cell::make_live_counter_update(timestamp, counter_update_value));
            } else if (expiring) {
                test_cell(atomic_cell::make_live(*bytes_type, timestamp, value, expiry_time, ttl));
            } else {
                test_cell(atomic_cell::make_live(*bytes_type, timestamp, value));
            }
        } else {
            test_cell(atomic_cell::make_dead(timestamp, deletion_time));
        }
    }
}
