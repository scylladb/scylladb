/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once

#include <utility>
#include <memory>

// do_with() holds an object alive for the duration until a future
// completes, and allow the code involved in making the future
// complete to have easy access to this object.
//
// do_with() takes two arguments: The first is an temporary object (rvalue),
// the second is a function returning a future (a so-called "promise").
// The function is given (a moved copy of) this temporary object, by
// reference, and it is ensured that the object will not be destructed until
// the completion of the future returned by the function.
//
// do_with() returns a future which resolves to whatever value the given future
// (returned by the given function) resolves to. This returned value must not
// contain references to the temporary object, as at that point the temporary
// is destructed.

template<typename T, typename F>
inline
auto do_with(T&& rvalue, F&& f) {
    auto obj = std::make_unique<T>(std::forward<T>(rvalue));
    auto fut = f(*obj);
    return fut.finally([obj = std::move(obj)] () {});
}
