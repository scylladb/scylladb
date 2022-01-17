/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/allocation_strategy.hh"

//
// ~~ Definitions ~~
//
// Mergeable type is a type which has an associated "apply" binary operation (T x T -> T)
// which forms a commutative semigroup with instances of that type.
//
// ReversiblyMergeable type is a Mergeable type which has two binary operations associated,
// "apply_reversibly" and "revert", both working on objects of that type (T x T -> T x T)
// with the following properties:
//
//   apply_reversibly(x, y) = (x', y')
//   revert(x', y') = (x'', y'')
//
//   x'  = apply(x, y)
//   x'' = x
//   apply(x'', y'') = apply(x, y)
//
// Note that it is not guaranteed that y'' = y and the state of y' is unspecified.
//
// ~~ API ~~
//
// "apply_reversibly" and "revert" are usually implemented as instance methods or functions
// mutating both arguments to store the result of the operation in them.
//
// "revert" is not allowed to throw. If "apply_reversibly" throws the objects on which it operates
// are left in valid states, with guarantees the same as if a successful apply_reversibly() was
// followed by revert().
//


template<typename T>
struct default_reversible_applier {
    void operator()(T& dst, T& src) const {
        dst.apply_reversibly(src);
    }
};

template<typename T>
struct default_reverter {
    void operator()(T& dst, T& src) const noexcept {
        dst.revert(src);
    }
};
