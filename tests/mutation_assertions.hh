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

#pragma once

#include "mutation.hh"

class mutation_assertion {
    mutation _m;
public:
    mutation_assertion(mutation m)
        : _m(std::move(m))
    { }

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    mutation_assertion& is_equal_to(const mutation& other, const stdx::optional<query::clustering_row_ranges>& ck_ranges = {}) {
        if (ck_ranges) {
            mutation_assertion(_m.sliced(*ck_ranges)).is_equal_to(other.sliced(*ck_ranges));
            return *this;
        }
        if (_m != other) {
            BOOST_FAIL(sprint("Mutations differ, expected %s\n ...but got: %s", other, _m));
        }
        if (other != _m) {
            BOOST_FAIL(sprint("Mutation inequality is not symmetric for %s\n ...and: %s", other, _m));
        }
        return *this;
    }

    mutation_assertion& is_not_equal_to(const mutation& other) {
        if (_m == other) {
            BOOST_FAIL(sprint("Mutations equal but expected to differ: %s\n ...and: %s", other, _m));
        }
        return *this;
    }

    mutation_assertion& has_schema(schema_ptr s) {
        if (_m.schema() != s) {
            BOOST_FAIL(sprint("Expected mutation of schema %s, but got %s", *s, *_m.schema()));
        }
        return *this;
    }

    mutation_assertion& has_same_continuity(const mutation& other) {
        if (!_m.partition().equal_continuity(*_m.schema(), other.partition())) {
            BOOST_FAIL(sprint("Continuity doesn't match: %s\n ...and: %s", other, _m));
        }
        return *this;
    }

    // Verifies that mutation data remains unchanged when upgraded to the new schema
    void is_upgrade_equivalent(schema_ptr new_schema) {
        mutation m2 = _m;
        m2.upgrade(new_schema);
        BOOST_REQUIRE(m2.schema() == new_schema);
        mutation_assertion(m2).is_equal_to(_m);

        mutation m3 = m2;
        m3.upgrade(_m.schema());
        BOOST_REQUIRE(m3.schema() == _m.schema());
        mutation_assertion(m3).is_equal_to(_m);
        mutation_assertion(m3).is_equal_to(m2);
    }
};

static inline
mutation_assertion assert_that(mutation m) {
    return { std::move(m) };
}

static inline
mutation_assertion assert_that(streamed_mutation sm) {
    auto mo = mutation_from_streamed_mutation(std::move(sm)).get0();
    return { std::move(*mo) };
}

class mutation_opt_assertions {
    mutation_opt _mo;
public:
    mutation_opt_assertions(mutation_opt mo) : _mo(std::move(mo)) {}

    mutation_assertion has_mutation() {
        if (!_mo) {
            BOOST_FAIL("Expected engaged mutation_opt, but found not");
        }
        return { *_mo };
    }

    void has_no_mutation() {
        if (_mo) {
            BOOST_FAIL("Expected disengaged mutation_opt");
        }
    }
};

static inline
mutation_opt_assertions assert_that(mutation_opt mo) {
    return { std::move(mo) };
}

static inline
mutation_opt_assertions assert_that(streamed_mutation_opt smo) {
    auto mo = mutation_from_streamed_mutation(std::move(smo)).get0();
    return { std::move(mo) };
}

class streamed_mutation_assertions {
    streamed_mutation _sm;
    clustering_key::equality _ck_eq;
public:
    streamed_mutation_assertions(streamed_mutation sm)
        : _sm(std::move(sm)), _ck_eq(*_sm.schema()) { }

    streamed_mutation_assertions& produces_static_row() {
        auto mfopt = _sm().get0();
        if (!mfopt) {
            BOOST_FAIL("Expected static row, got end of stream");
        }
        if (mfopt->mutation_fragment_kind() != mutation_fragment::kind::static_row) {
            BOOST_FAIL(sprint("Expected static row, got: %s", mfopt->mutation_fragment_kind()));
        }
        return *this;
    }

    streamed_mutation_assertions& produces(mutation_fragment::kind k, std::vector<int> ck_elements) {
        std::vector<bytes> ck_bytes;
        for (auto&& e : ck_elements) {
            ck_bytes.emplace_back(int32_type->decompose(e));
        }
        auto ck = clustering_key_prefix::from_exploded(*_sm.schema(), std::move(ck_bytes));

        auto mfopt = _sm().get0();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected mutation fragment %s, got end of stream", ck));
        }
        if (mfopt->mutation_fragment_kind() != k) {
            BOOST_FAIL(sprint("Expected mutation fragment kind %s, got: %s", k, mfopt->mutation_fragment_kind()));
        }
        if (!_ck_eq(mfopt->key(), ck)) {
            BOOST_FAIL(sprint("Expected key %s, got: %s", ck, mfopt->key()));
        }
        return *this;
    }

    streamed_mutation_assertions& produces(mutation_fragment mf) {
        auto mfopt = _sm().get0();
        if (!mfopt) {
            BOOST_FAIL(sprint("Expected mutation fragment %s, got end of stream", mf));
        }
        if (!mfopt->equal(*_sm.schema(), mf)) {
            BOOST_FAIL(sprint("Expected %s, but got %s", mf, *mfopt));
        }
        return *this;
    }

    streamed_mutation_assertions& produces(const mutation& m) {
        assert_that(mutation_from_streamed_mutation(_sm).get0()).is_equal_to(m);
        return *this;
    }

    streamed_mutation_assertions& produces_only(const std::deque<mutation_fragment>& fragments) {
        for (auto&& f : fragments) {
            produces(f);
        }
        produces_end_of_stream();
        return *this;
    }

    streamed_mutation_assertions& produces_row_with_key(const clustering_key& ck) {
        BOOST_TEST_MESSAGE(sprint("Expect %s", ck));
        auto mfo = _sm().get0();
        if (!mfo) {
            BOOST_FAIL(sprint("Expected row with key %s, but got end of stream", ck));
        }
        if (!mfo->is_clustering_row()) {
            BOOST_FAIL(sprint("Expected row with key %s, but got %s", ck, *mfo));
        }
        auto& actual = mfo->as_clustering_row().key();
        if (!actual.equal(*_sm.schema(), ck)) {
            BOOST_FAIL(sprint("Expected row with key %s, but key is %s", ck, actual));
        }
        return *this;
    }

    // If ck_ranges is passed, verifies only that information relevant for ck_ranges matches.
    streamed_mutation_assertions& produces_range_tombstone(const range_tombstone& rt, const query::clustering_row_ranges& ck_ranges = {}) {
        BOOST_TEST_MESSAGE(sprint("Expect %s", rt));
        auto mfo = _sm().get0();
        if (!mfo) {
            BOOST_FAIL(sprint("Expected range tombstone %s, but got end of stream", rt));
        }
        if (!mfo->is_range_tombstone()) {
            BOOST_FAIL(sprint("Expected range tombstone %s, but got %s", rt, *mfo));
        }
        auto& actual = mfo->as_range_tombstone();
        const schema& s = *_sm.schema();
        if (!ck_ranges.empty()) {
            range_tombstone_list actual_list(s);
            range_tombstone_list expected_list(s);
            actual_list.apply(s, actual);
            expected_list.apply(s, rt);
            actual_list.trim(s, ck_ranges);
            expected_list.trim(s, ck_ranges);
            if (!actual_list.equal(s, expected_list)) {
                BOOST_FAIL(sprint("Expected %s, but got %s", expected_list, actual_list));
            }
        } else if (!actual.equal(s, rt)) {
            BOOST_FAIL(sprint("Expected range tombstone %s, but got %s", rt, actual));
        }
        return *this;
    }

    streamed_mutation_assertions& fwd_to(const clustering_key& ck1, const clustering_key& ck2) {
        return fwd_to(position_range{
            position_in_partition(position_in_partition::clustering_row_tag_t(), ck1),
            position_in_partition(position_in_partition::clustering_row_tag_t(), ck2)
        });
    }

    streamed_mutation_assertions& fwd_to(position_range range) {
        BOOST_TEST_MESSAGE(sprint("Forwarding to %s", range));
        _sm.fast_forward_to(std::move(range)).get();
        return *this;
    }

    streamed_mutation_assertions& produces_end_of_stream() {
        auto mfopt = _sm().get0();
        if (mfopt) {
            BOOST_FAIL(sprint("Expected end of stream, got: %s", *mfopt));
        }
        return *this;
    }

    void has_monotonic_positions() {
        position_in_partition::less_compare less(*_sm.schema());
        mutation_fragment_opt prev;
        for (;;) {
            mutation_fragment_opt mfo = _sm().get0();
            if (!mfo) {
                break;
            }
            if (prev) {
                if (!less(prev->position(), mfo->position())) {
                    BOOST_FAIL(sprint("previous fragment has greater position: prev=%s, current=%s", *prev, *mfo));
                }
            }
            prev = std::move(mfo);
        }
    }
};

static inline streamed_mutation_assertions assert_that_stream(streamed_mutation sm)
{
    return streamed_mutation_assertions(std::move(sm));
}
