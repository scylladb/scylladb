/*
 * Copyright 2015 Cloudius Systems
 */

#pragma once

#include "mutation.hh"

class mutation_assertion {
    const mutation& _m;
public:
    mutation_assertion(const mutation& m)
        : _m(m)
    { }

    void is_equal_to(const mutation& other) {
        if (_m != other) {
            BOOST_FAIL(sprint("Mutations differ, expected %s\n ...but got: %s", other, _m));
        }
    }
};

static inline
mutation_assertion assert_that(const mutation& m) {
    return { m };
}
