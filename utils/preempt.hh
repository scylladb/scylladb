/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/bool_class.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/preempt.hh>

#include "seastarx.hh"

class is_preemptible_tag;
using is_preemptible = bool_class<is_preemptible_tag>;

/// A function which decides when to preempt.
/// If it returns true then the algorithm will be interrupted.
using preemption_check = noncopyable_function<bool() noexcept>;

inline
preemption_check default_preemption_check() {
    return [] () noexcept {
        return seastar::need_preempt();
    };
}

inline
preemption_check never_preempt() {
    return [] () noexcept {
        return false;
    };
}

inline
preemption_check always_preempt() {
    return [] () noexcept {
        return true;
    };
}
