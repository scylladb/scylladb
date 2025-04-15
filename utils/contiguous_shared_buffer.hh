/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <concepts>
#include <cstddef>

// A contiguous buffer of char objects which can be trimmed and
// supports zero-copy sharing of its underlying memory.
template<typename T>
concept ContiguousSharedBuffer = std::movable<T>
    && std::default_initializable<T>
    && requires(T& obj, size_t pos, size_t len) {

        // Creates a new buffer that shares the memory of the original buffer.
        // The lifetime of the new buffer is independent of the original buffer.
        { obj.share() } -> std::same_as<T>;

        // Like share() but the new buffer represents a sub-range of the original buffer.
        { obj.share(pos, len) } -> std::same_as<T>;

        // Trims the suffix of a buffer so that 'len' is the index of the first removed byte.
        { obj.trim(len) } -> std::same_as<void>;

        // Trims the prefix of the buffer so that `pos` is the index of the first byte after the trim.
        { obj.trim_front(pos) } -> std::same_as<void>;

        { obj.begin() } -> std::same_as<const char*>;
        { obj.get() } -> std::same_as<const char*>;
        { obj.get_write() } -> std::same_as<char*>;
        { obj.end() } -> std::same_as<const char*>;
        { obj.size() } -> std::same_as<size_t>;
        { obj.empty() } -> std::same_as<bool>;
};
