// Copyright (C) 2025-present ScyllaDB
// SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0

#include "mutation_assertions.hh"
#include "clustering_interval_set.hh"

mutation_partition_assertion&
mutation_partition_assertion::is_equal_to(const schema& s, const mutation_partition& other,
        const std::optional<query::clustering_row_ranges>& ck_ranges) {
    if (ck_ranges) {
        mutation_partition_assertion(_schema, _m.sliced(*_schema, *ck_ranges))
            .is_equal_to(s, other.sliced(s, *ck_ranges));
        return *this;
    }
    if (!_m.equal(*_schema, other, s)) {
        BOOST_FAIL(format("Mutations differ, expected {}\n ...but got: {}",
                            mutation_partition::printer(s, other), mutation_partition::printer(*_schema, _m)));
    }
    if (!other.equal(s, _m, *_schema)) {
        BOOST_FAIL(format("Mutation inequality is not symmetric for {}\n ...and: {}",
                            mutation_partition::printer(s, other), mutation_partition::printer(*_schema, _m)));
    }
    return *this;
}

mutation_partition_assertion&
mutation_partition_assertion::is_not_equal_to(const schema& s, const mutation_partition& other) {
    if (_m.equal(*_schema, other, s)) {
        BOOST_FAIL(format("Mutations equal but expected to differ: {}\n ...and: {}",
                            mutation_partition::printer(s, other), mutation_partition::printer(*_schema, _m)));
    }
    return *this;
}

mutation_partition_assertion&
mutation_partition_assertion::has_same_continuity(const mutation_partition& other) {
    if (!_m.equal_continuity(*_schema, other)) {
        auto expected = other.get_continuity(*_schema, is_continuous::yes);
        auto got = _m.get_continuity(*_schema, is_continuous::yes);
        BOOST_FAIL(format("Continuity doesn't match, expected: {}\nbut got: {}, mutation before: {}\n ...and after: {}",
                            expected, got, mutation_partition::printer(*_schema, other), mutation_partition::printer(*_schema, _m)));
    }
    return *this;
}

mutation_partition_assertion&
mutation_partition_assertion::is_continuous(const position_range& r, ::is_continuous cont) {
    if (!_m.check_continuity(*_schema, r, cont)) {
        BOOST_FAIL(format("Expected range {} to be {} in {}", r, cont ? "continuous" : "discontinuous", mutation_partition::printer(*_schema, _m)));
    }
    return *this;
}

mutation_assertion&
mutation_assertion::is_equal_to(const mutation& other, const std::optional<query::clustering_row_ranges>& ck_ranges) {
    if (ck_ranges) {
        mutation_assertion(_m.sliced(*ck_ranges)).is_equal_to(other.sliced(*ck_ranges));
        return *this;
    }
    if (_m != other) {
        BOOST_FAIL(format("Mutations differ, expected {}\n ...but got: {}", other, _m));
    }
    if (other != _m) {
        BOOST_FAIL(format("Mutation inequality is not symmetric for {}\n ...and: {}", other, _m));
    }
    return *this;
}

mutation_assertion&
mutation_assertion::is_equal_to_compacted(const mutation& other, const std::optional<query::clustering_row_ranges>& ck_ranges) {
    mutation_assertion(_m.compacted()).is_equal_to(other.compacted(), ck_ranges);
    return *this;
}

mutation_assertion&
mutation_assertion::is_not_equal_to(const mutation& other) {
    if (_m == other) {
        BOOST_FAIL(format("Mutations equal but expected to differ: {}\n ...and: {}", other, _m));
    }
    return *this;
}

mutation_assertion&
mutation_assertion::has_schema(schema_ptr s) {
    if (_m.schema() != s) {
        BOOST_FAIL(format("Expected mutation of schema {}, but got {}", *s, *_m.schema()));
    }
    return *this;
}

mutation_assertion&
mutation_assertion::has_same_continuity(const mutation& other) {
    assert_that(_m.schema(), _m.partition()).has_same_continuity(other.partition());
    return *this;
}

mutation_assertion&
mutation_assertion::is_continuous(const position_range& r, ::is_continuous cont) {
    assert_that(_m.schema(), _m.partition()).is_continuous(r, cont);
    return *this;
}

// Verifies that mutation data remains unchanged when upgraded to the new schema
void
mutation_assertion::is_upgrade_equivalent(schema_ptr new_schema) {
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

mutation_assertion
mutation_opt_assertions::has_mutation() {
    if (!_mo) {
        BOOST_FAIL("Expected engaged mutation_opt, but found not");
    }
    return { *_mo };
}

void
mutation_opt_assertions::has_no_mutation() {
    if (_mo) {
        BOOST_FAIL("Expected disengaged mutation_opt");
    }
}
