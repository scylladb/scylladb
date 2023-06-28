/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/util/later.hh>
#include <seastar/core/future.hh>
#include <seastar/core/preempt.hh>

/// A helper struct that helps force a preemption every given number of calls.
/// The main use case for it is limiting the number of recursive calls
/// that occur when a function calls itself indirectly via .then().
template<unsigned int PreemptEvery>
struct preemptor {
    static_assert(PreemptEvery > 0);

private:
    unsigned int _counter = 0;

public:
    /// Increments the internal counter and either calls given function immediately,
    /// or after a yield, depending on the value of the internal counter.
    template<typename F, typename... Args>
    requires std::invocable<F, Args...>
    seastar::futurize_t<std::invoke_result_t<F>> maybe_after_preemption(F&& f, Args&&... args) {
        if (++_counter == PreemptEvery) {
            _counter = 0;
            return seastar::yield().then(std::forward<F>(f), std::forward<Args>(args)...);
        } else {
            return futurize_invoke<F>(std::forward<F>(f), std::forward<Args>(args)...);
        }
    }
};
