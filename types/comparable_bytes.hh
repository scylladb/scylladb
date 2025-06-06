/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/managed_bytes.hh"

class data_value;
class abstract_type;
class comparable_bytes;
using comparable_bytes_opt = std::optional<comparable_bytes>;

class comparable_bytes {
    // encoded data in byte comparable format
    managed_bytes _encoded_bytes;

public:
    // Constructor to initialise comparable_bytes object from an already encoded bytes
    // TODO : add conversion methods to managed_bytes once implementation is complete
    comparable_bytes(managed_bytes&& encoded_bytes)
        : _encoded_bytes(std::move(encoded_bytes)) {
    }

    // Methods to convert serialized bytes to comparable bytes
    comparable_bytes(const abstract_type& type, managed_bytes_view serialized_bytes_view);
    static comparable_bytes_opt from_serialized_bytes(const abstract_type& type, managed_bytes_opt serialized_bytes) {
        if (!serialized_bytes) {
            return comparable_bytes_opt();
        }

        return comparable_bytes(type, serialized_bytes.value());
    }
    // Method to convert data_value to comparable bytes
    static comparable_bytes_opt from_data_value(const data_value& value);

    // Methods to convert comparable bytes to serialized bytes and data_value
    managed_bytes_opt to_serialized_bytes(const abstract_type& type) const;
    data_value to_data_value(const shared_ptr<const abstract_type>& type) const;

    managed_bytes::size_type size() const { return _encoded_bytes.size(); }
    bool empty() const { return _encoded_bytes.empty(); }

    managed_bytes_view as_managed_bytes_view() const { return managed_bytes_view(_encoded_bytes); }

    auto operator<=>(const comparable_bytes& other) const {
        return compare_unsigned(managed_bytes_view(_encoded_bytes), managed_bytes_view(other._encoded_bytes));
    }

    bool operator==(const comparable_bytes& other) const {
        // optimised == overload that checks the length first
        return this->size() == other.size() && (*this <=> other) == std::strong_ordering::equal;
    }
};

// formatters for debugging and testcases
template <>
struct fmt::formatter<comparable_bytes> : fmt::formatter<managed_bytes_view> {
    template <typename FormatContext>
    auto format(const comparable_bytes& b, FormatContext& ctx) const {
        return fmt::formatter<managed_bytes_view>::format(b.as_managed_bytes_view(), ctx);
    }
};

template <>
struct fmt::formatter<comparable_bytes_opt> : fmt::formatter<managed_bytes_view> {
    template <typename FormatContext>
    auto format(const comparable_bytes_opt& opt, FormatContext& ctx) const {
        if (opt) {
            return fmt::formatter<managed_bytes_view>::format(opt->as_managed_bytes_view(), ctx);
        }
        return fmt::format_to(ctx.out(), "null");
    }
};
