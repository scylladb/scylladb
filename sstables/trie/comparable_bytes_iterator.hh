/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <concepts>
#include <iterator>
#include <span>

template <typename T>
concept bytespan_ref = std::same_as<T, std::span<std::byte>&&>
    || std::same_as<T, std::span<const std::byte>&&>;

// Converting keys from the regular Scylla format to the BTI byte-comparable format
// can be expensive, so we allow the conversion to happen lazily.
// To support that, the traversal routines take the key as an iterator/generator
// of fragments (byte spans).
//
// Note: a std::generator<const_bytes>::iterator fulfils this concept.
template <typename T>
concept comparable_bytes_iterator = requires (T it) {
    // Users of comparable_bytes_iterator want to modify the contents of `*it`
    // (for their convenience, not due to necessity), so operator* returns a reference.
    { *it } -> bytespan_ref;
    // Allowed to destroy `*it`;
    { ++it };
    // Since this concept is designed for lazy evaluation,
    // the "end" of the sequence is usually more naturally encoded
    // as something in the iterator itself, rather than some external "end" object.
    //
    // So we don't bother with iterator pairs and we instead
    // require the iterator itself to know if it's done.
    { it == std::default_sentinel } -> std::same_as<bool>;
    { it != std::default_sentinel } -> std::same_as<bool>;
};