/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>

#include "mutation/canonical_mutation.hh"
#include "mutation/async_utils.hh"
#include "test/lib/mutation_source_test.hh"
#include "test/lib/mutation_assertions.hh"

#include "test/lib/scylla_test_case.hh"

#include <seastar/core/thread.hh>

SEASTAR_THREAD_TEST_CASE(test_conversion_back_and_forth) {
    for (auto do_make_canonical_mutation_gently : {false, true}) {
        for (auto do_make_mutation_gently : {false, true}) {
            for_each_mutation([&] (const mutation& m) {
                auto cm = do_make_canonical_mutation_gently ? make_canonical_mutation_gently(m).get() : canonical_mutation{m};
                auto m2 = do_make_mutation_gently ? to_mutation_gently(cm, m.schema()).get() : cm.to_mutation(m.schema());
                assert_that(m2).is_equal_to(m);
            });
        }
    }
}

SEASTAR_TEST_CASE(test_reading_with_different_schemas) {
    return seastar::async([] {
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
