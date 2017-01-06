/*
 * Copyright (C) 2017 ScyllaDB
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

#include "counters.hh"

#include <seastar/core/thread.hh>

#include <boost/range/algorithm/sort.hpp>

#include "tests/test-utils.hh"
#include "tests/test_services.hh"
#include "disk-error-handler.hh"
#include "schema_builder.hh"
#include "keys.hh"
#include "mutation.hh"
#include "frozen_mutation.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

std::vector<counter_id> generate_ids(unsigned count) {
    std::vector<counter_id> id;
    std::generate_n(std::back_inserter(id), count, counter_id::generate_random);
    boost::range::sort(id);
    return id;
}

SEASTAR_TEST_CASE(test_counter_cell) {
    return seastar::async([] {
        auto id = generate_ids(3);

        counter_cell_builder b1;
        b1.add_shard(counter_shard(id[0], 5, 1));
        b1.add_shard(counter_shard(id[1], -4, 1));
        auto c1 = atomic_cell_or_collection(b1.build(0));

        auto cv = counter_cell_view(c1.as_atomic_cell());
        BOOST_REQUIRE_EQUAL(cv.total_value(), 1);

        counter_cell_builder b2;
        b2.add_shard(counter_shard(*cv.get_shard(id[0])).update(2, 1));
        b2.add_shard(counter_shard(id[2], 1, 1));
        auto c2 = atomic_cell_or_collection(b2.build(0));

        cv = counter_cell_view(c2.as_atomic_cell());
        BOOST_REQUIRE_EQUAL(cv.total_value(), 8);

        counter_cell_view::apply_reversibly(c1, c2);
        cv = counter_cell_view(c1.as_atomic_cell());
        BOOST_REQUIRE_EQUAL(cv.total_value(), 4);
    });
}
