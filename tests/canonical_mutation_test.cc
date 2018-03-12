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


#include <boost/test/unit_test.hpp>

#include "canonical_mutation.hh"
#include "mutation_source_test.hh"
#include "mutation_assertions.hh"

#include "tests/test_services.hh"
#include "tests/test-utils.hh"

#include <seastar/core/thread.hh>

SEASTAR_TEST_CASE(test_conversion_back_and_forth) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for_each_mutation([] (const mutation& m) {
            canonical_mutation cm(m);
            assert_that(cm.to_mutation(m.schema())).is_equal_to(m);
        });
    });
}

SEASTAR_TEST_CASE(test_reading_with_different_schemas) {
    return seastar::async([] {
        storage_service_for_tests ssft;
        for_each_mutation_pair([] (const mutation& m1, const mutation& m2, are_equal eq) {
            if (m1.schema() == m2.schema()) {
                return;
            }

            canonical_mutation cm1(m1);
            canonical_mutation cm2(m2);

            if (can_upgrade_schema(m1.schema(), m2.schema())) {
                auto m = cm1.to_mutation(m1.schema());
                m.upgrade(m2.schema());
                assert_that(cm1.to_mutation(m2.schema())).is_equal_to(m);
            }

            if (can_upgrade_schema(m2.schema(), m1.schema())) {
                auto m = cm2.to_mutation(m2.schema());
                m.upgrade(m1.schema());
                assert_that(cm2.to_mutation(m1.schema())).is_equal_to(m);
            }
        });
    });
}
