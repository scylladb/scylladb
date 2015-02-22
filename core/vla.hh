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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef VLA_HH_
#define VLA_HH_

#include <memory>
#include <new>
#include <assert.h>
#include <type_traits>

// Some C APIs have a structure with a variable length array at the end.
// This is a helper function to help allocate it.
//
// for a structure
//
//   struct xx { int a; float b[0]; };
//
// use
//
//   make_struct_with_vla(&xx::b, number_of_bs);
//
// to allocate it.
//
template <class S, typename E>
inline
std::unique_ptr<S>
make_struct_with_vla(E S::*last, size_t nr) {
    auto fake = reinterpret_cast<S*>(0);
    size_t offset = reinterpret_cast<uintptr_t>(&(fake->*last));
    size_t element_size = sizeof((fake->*last)[0]);
    assert(offset == sizeof(S));
    auto p = ::operator new(offset + element_size * nr);
    return std::unique_ptr<S>(new (p) S());
}



#endif /* VLA_HH_ */
