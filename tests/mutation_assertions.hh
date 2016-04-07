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

    mutation_assertion& is_equal_to(const mutation& other) {
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
