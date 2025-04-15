/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstring>
#include <type_traits>

template <class T> concept Trivial = std::is_trivial_v<T>;
template <class T> concept TriviallyCopyable = std::is_trivially_copyable_v<T>;

template <TriviallyCopyable To>
To read_unaligned(const void* src) {
    To dst;
    std::memcpy(&dst, src, sizeof(To));
    return dst;
}

template <TriviallyCopyable From>
void write_unaligned(void* dst, const From& src) {
    std::memcpy(dst, &src, sizeof(From));
}
