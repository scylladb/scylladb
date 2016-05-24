/*
 * Copyright (C) 2016 ScyllaDB
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

#define BOOST_TEST_DYN_LINK

#include <seastar/core/thread.hh>
#include <seastar/tests/test-utils.hh>

#include "mutation_source_test.hh"
#include "streamed_mutation.hh"

#include "disk-error-handler.hh"

thread_local disk_error_signal_type commit_error;
thread_local disk_error_signal_type general_disk_error;

void check_order_of_fragments(streamed_mutation sm)
{
    stdx::optional<position_in_partition> previous;
    position_in_partition::less_compare cmp(*sm.schema());
    auto mf = sm().get0();
    while (mf) {
        if (previous) {
            BOOST_REQUIRE(cmp(*previous, *mf));
        }
        previous = mf->position();
        mf = sm().get0();
    }
}

SEASTAR_TEST_CASE(test_mutation_from_streamed_mutation_from_mutation) {
    return seastar::async([] {
        for_each_mutation([&] (const mutation& m) {
            auto get_sm = [&] {
                return streamed_mutation_from_mutation(mutation(m));
            };

            check_order_of_fragments(get_sm());
            auto mopt = mutation_from_streamed_mutation(get_sm()).get0();
            BOOST_REQUIRE(mopt);
            BOOST_REQUIRE_EQUAL(m, *mopt);
        });
    });
}

SEASTAR_TEST_CASE(test_mutation_merger) {
    return seastar::async([] {
        for_each_mutation_pair([&] (const mutation& m1, const mutation& m2, are_equal) {
            if (m1.schema()->version() != m2.schema()->version()) {
                return;
            }

            auto m12 = m1;
            m12.apply(m2);

            auto get_sm = [&] {
                std::vector<streamed_mutation> sms;
                sms.emplace_back(streamed_mutation_from_mutation(mutation(m1)));
                sms.emplace_back(streamed_mutation_from_mutation(mutation(m2.schema(), m1.decorated_key(), m2.partition())));
                return merge_mutations(std::move(sms));
            };

            check_order_of_fragments(get_sm());
            auto mopt = mutation_from_streamed_mutation(get_sm()).get0();
            BOOST_REQUIRE(mopt);
            BOOST_REQUIRE(m12.partition().difference(m1.schema(), mopt->partition()).empty());
            BOOST_REQUIRE(mopt->partition().difference(m1.schema(), m12.partition()).empty());
        });
    });
}

