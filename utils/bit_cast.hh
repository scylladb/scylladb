/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstring>
#include <type_traits>
#include <span>

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

template <TriviallyCopyable From>
std::byte* write_unaligned(std::byte* dst, const From& src) {
    std::memcpy(dst, &src, sizeof(From));
    return dst + sizeof(From);
}

std::span<const std::byte> object_representation(const TriviallyCopyable auto& x) {
    return {reinterpret_cast<const std::byte*>(&x), sizeof(x)};
}
