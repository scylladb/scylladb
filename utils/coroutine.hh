/*
 * Copyright (C) 2018 ScyllaDB
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

#include <functional>
#include <seastar/core/future-util.hh>
#include <seastar/util/noncopyable_function.hh>

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
