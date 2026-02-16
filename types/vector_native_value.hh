/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>
#include "types/types.hh"

/// Native in-memory representation for CQL vector values.
///
/// Two storage strategies, selected at construction time:
///
///   **Fixed-size elements** (float, int32, double, etc.):
///     Stores N values in a contiguous byte buffer using std::vector<std::byte>.
///     Values are in native endianness, enabling zero-copy typed access via as_span<T>().
///
///   **Variable-size elements** (text, blob, frozen UDTs, etc.):
///     Stores N fully-deserialized data_value objects in a std::vector<data_value>.
///     Same as the old approach but wrapped in a unified container.
///
class vector_native_value {
public:
    /// Default constructor: creates a zero-dimension fixed-size vector.
    /// Required to satisfy emptyable<> constraints even though maybe_empty
    /// resolves to vector_native_value directly (has_empty concept is satisfied).
    vector_native_value() : _dimension(0), _element_size(0) {}

    /// Construct a fixed-size vector from a contiguous byte buffer.
    /// The buffer must contain exactly `dimension * element_size` bytes,
    /// with each element in native endianness.
    vector_native_value(size_t dimension, size_t element_size, const std::byte* data)
        : _dimension(dimension)
        , _element_size(element_size)
        , _data(std::vector<std::byte>(data, data + dimension * element_size)) {
    }

    /// Construct a fixed-size vector with a pre-built vector buffer.
    vector_native_value(size_t dimension, size_t element_size, std::vector<std::byte> data)
        : _dimension(dimension)
        , _element_size(element_size)
        , _data(std::move(data)) {
    }

    /// Construct a variable-size vector from deserialized data_values.
    vector_native_value(size_t dimension, std::vector<data_value> values)
        : _dimension(dimension)
        , _element_size(0)
        , _data(std::move(values)) {
    }

    vector_native_value(const vector_native_value&) = default;
    vector_native_value(vector_native_value&&) noexcept = default;
    vector_native_value& operator=(const vector_native_value&) = default;
    vector_native_value& operator=(vector_native_value&&) noexcept = default;
    ~vector_native_value() = default;

    /// Number of elements in the vector.
    size_t size() const { return _dimension; }

    /// True if elements are stored as contiguous fixed-size native values.
    bool is_fixed_size() const { return _element_size > 0; }

    /// For fixed-size vectors: returns the byte size of each element.
    /// For variable-size vectors: returns 0.
    size_t element_size() const { return _element_size; }

    /// For fixed-size vectors: returns a typed span over the contiguous buffer.
    /// Precondition: is_fixed_size() && sizeof(T) == element_size().
    template <typename T>
    std::span<const T> as_span() const {
        const auto& fixed = std::get<std::vector<std::byte>>(_data);
        return std::span<const T>(reinterpret_cast<const T*>(fixed.data()), _dimension);
    }

    /// For fixed-size vectors: returns element i by value.
    /// Precondition: is_fixed_size() && sizeof(T) == element_size().
    template <typename T>
    T typed_element(size_t i) const {
        const auto& fixed = std::get<std::vector<std::byte>>(_data);
        T val;
        std::memcpy(&val, fixed.data() + i * sizeof(T), sizeof(T));
        return val;
    }

    /// For variable-size vectors: returns a reference to element i.
    /// Precondition: !is_fixed_size().
    const data_value& element_as_data_value(size_t i) const {
        return std::get<std::vector<data_value>>(_data)[i];
    }

    /// For variable-size vectors: returns a reference to the underlying vector
    /// of data_values. Precondition: !is_fixed_size().
    const std::vector<data_value>& as_data_values() const {
        return std::get<std::vector<data_value>>(_data);
    }

    /// For fixed-size vectors: returns a view over the raw bytes.
    std::span<const std::byte> raw_bytes() const {
        const auto& fixed = std::get<std::vector<std::byte>>(_data);
        return std::span<const std::byte>(fixed.data(), fixed.size());
    }

    /// Provides maybe_empty compatible empty() method.
    /// A vector_native_value is never "empty" in the CQL sense;
    /// CQL empty-value semantics are handled by maybe_empty<> wrapper.
    bool empty() const { return false; }

    bool operator==(const vector_native_value& other) const;

private:
    size_t _dimension;
    // For fixed-size elements: byte size of each element. 0 for variable-size.
    size_t _element_size;
    // Type-safe storage:
    // - index 0: std::vector<std::byte> for fixed-size elements (contiguous native-endian values)
    // - index 1: std::vector<data_value> for variable-size elements
    std::variant<std::vector<std::byte>, std::vector<data_value>> _data;
};
