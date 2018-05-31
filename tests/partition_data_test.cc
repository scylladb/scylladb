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

#define BOOST_TEST_MODULE partition_data
#include <boost/test/unit_test.hpp>

#include <random>

#include <boost/range/irange.hpp>
#include <boost/range/algorithm/generate.hpp>

#include "data/cell.hh"

#include "random-utils.hh"
#include "disk-error-handler.hh"

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
        // Live, fixed-size cell
        { true, true, tests::random::get_bytes(data::cell::maximum_internal_storage_length / 2), false, false },
        // Live, variable-size (small), cell
        { true, false, tests::random::get_bytes(data::cell::maximum_internal_storage_length / 2), false, false },
        // Live, variable-size (large), cell
        { true, false, tests::random::get_bytes(data::cell::maximum_external_chunk_length * 5), false, false },
        // Live, variable-size, empty cell
        { true, false, bytes(), false, false },
        // Live, expiring, variable-size cell
        { true, false, tests::random::get_bytes(data::cell::maximum_internal_storage_length / 2), true, false },
        // Dead cell
        { false, false, bytes(), false, false },
        // Counter update cell
        { true, false, bytes(), false, true },
    };

    for (auto tc : cases) {
        auto [live, fixed_size, value, expiring, counter_update] = tc;
        auto timestamp = tests::random::get_int<api::timestamp_type>();
        auto ti = [&] {
            if (fixed_size) {
                return data::type_info::make_fixed_size(value.size());
            } else {
                return data::type_info::make_variable_size();
            }
        }();
        auto ttl = gc_clock::duration(tests::random::get_int<int32_t>(1, std::numeric_limits<int32_t>::max()));
        auto expiry_time = gc_clock::time_point(gc_clock::duration(tests::random::get_int<int32_t>(1, std::numeric_limits<int32_t>::max())));
        auto deletion_time = expiry_time;
        auto counter_update_value = tests::random::get_int<int64_t>();

        std::optional<imr::alloc::object_allocator> allocator;
        allocator.emplace();

        auto test_cell = [&] (auto builder) {
            auto expected_size = data::cell::size_of(builder, *allocator);
            if (fixed_size) {
                BOOST_CHECK_GE(expected_size, value.size());
            }

            allocator->allocate_all();

            auto buffer = std::make_unique<uint8_t[]>(expected_size);
            BOOST_CHECK_EQUAL(data::cell::serialize(buffer.get(), builder, *allocator), expected_size);

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
                    BOOST_CHECK(view.value() == value);
                }
                BOOST_CHECK_EQUAL(view.is_expiring(), expiring);
                if (expiring) {
                    BOOST_CHECK(view.ttl() == ttl);
                    BOOST_CHECK(view.expiry() == expiry_time);
                }
            };

            auto view = data::cell::make_atomic_cell_view(ti, buffer.get());
            verify_cell(view);

            allocator.emplace();

            auto copier = data::cell::copy_fn(ti, buffer.get());
            BOOST_CHECK_EQUAL(data::cell::size_of(copier, *allocator), expected_size);

            allocator->allocate_all();

            auto copied = std::make_unique<uint8_t[]>(expected_size);
            BOOST_CHECK_EQUAL(data::cell::serialize(copied.get(), copier, *allocator), expected_size);

            auto view2 = data::cell::make_atomic_cell_view(ti, copied.get());
            verify_cell(view2);

            auto ctx = data::cell::context(buffer.get(), ti);
            BOOST_CHECK_EQUAL(data::cell::structure::serialized_object_size(buffer.get(), ctx), expected_size);
            auto moved = std::make_unique<uint8_t[]>(expected_size);
            std::copy_n(buffer.get(), expected_size, moved.get());
            imr::methods::move<data::cell::structure>(moved.get());

            auto view3 = data::cell::make_atomic_cell_view(ti, moved.get());
            verify_cell(view3);

            imr::methods::destroy<data::cell::structure>(moved.get());
            imr::methods::destroy<data::cell::structure>(copied.get());
        };

        if (live) {
            if (counter_update) {
                test_cell(data::cell::make_live_counter_update(timestamp, counter_update_value));
            } else if (expiring) {
                test_cell(data::cell::make_live(ti, timestamp, value, expiry_time, ttl));
            } else {
                test_cell(data::cell::make_live(ti, timestamp, value));
            }
        } else {
            test_cell(data::cell::make_dead(timestamp, deletion_time));
        }
    }
}



