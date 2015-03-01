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

/*
 * This header file defines a hash function for enum types, using the
 * standard hash function of the underlying type (such as int). This makes
 * it possible to inherit from this type to
 */

#include <type_traits>
#include <functional>
#include <cstddef>

template <typename T>
class enum_hash {
    static_assert(std::is_enum<T>::value, "must be an enum");
public:
    std::size_t operator()(const T& e) const {
        using utype = typename std::underlying_type<T>::type;
        return std::hash<utype>()(static_cast<utype>(e));
    }
};
