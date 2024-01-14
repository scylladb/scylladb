/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/future-util.hh>
#include <seastar/util/noncopyable_function.hh>

#include "seastarx.hh"

namespace utils {

// Represents a deferring operation which defers cooperatively with the caller.
//
// The operation is started and resumed by calling run(), which returns
// with stop_iteration::no whenever the operation defers and is not completed yet.
// When the operation is finally complete, run() returns with stop_iteration::yes.
// After that, run() should not be invoked any more.
//
// This allows the caller to:
//   1) execute some post-defer and pre-resume actions atomically
//   2) have control over when the operation is resumed and in which context,
//      in particular the caller can cancel the operation at deferring points.
//
// One simple way to drive the operation to completion:
//
//   coroutine c;
//   while (c.run() == stop_iteartion::no) {}
//
class coroutine final {
public:
    coroutine() = default;
    coroutine(noncopyable_function<stop_iteration()> f) : _run(std::move(f)) {}
    stop_iteration run() { return _run(); }
    explicit operator bool() const { return bool(_run); }
private:
    noncopyable_function<stop_iteration()> _run;
};

// Makes a coroutine which does nothing.
inline
coroutine make_empty_coroutine() {
    return coroutine([] { return stop_iteration::yes; });
}

}
