/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "types/comparable_bytes.hh"

#include <seastar/core/on_internal_error.hh>

#include "bytes_ostream.hh"
#include "concrete_types.hh"

logging::logger cblogger("comparable_bytes");

// to_comparable_bytes_visitor provides methods to
// convert serialized bytes into byte comparable format.
struct to_comparable_bytes_visitor {
    managed_bytes_view& serialized_bytes_view;
    bytes_ostream& out;

    // TODO: Handle other types

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

comparable_bytes::comparable_bytes(const abstract_type& type, managed_bytes_view serialized_bytes_view) {
    bytes_ostream encoded_bytes_ostream;
    visit(type, to_comparable_bytes_visitor{serialized_bytes_view, encoded_bytes_ostream});
    _encoded_bytes = std::move(encoded_bytes_ostream).to_managed_bytes();
}

comparable_bytes_opt comparable_bytes::from_data_value(const data_value& value) {
    if (value.is_null()) {
        return comparable_bytes_opt();
    }

    auto mb = value.serialize_nonnull();
    return comparable_bytes(*value.type(), managed_bytes_view(mb));
}

// from_comparable_bytes_visitor provides methods to
// convert byte comparable format into standard serialized bytes.
struct from_comparable_bytes_visitor {
    managed_bytes_view& comparable_bytes_view;
    bytes_ostream& out;

    // TODO: Handle other types

    void operator()(const abstract_type& type) {
        // Unimplemented
        on_internal_error(cblogger, fmt::format("byte comparable format not supported for type {}", type.name()));
    }
};

managed_bytes_opt comparable_bytes::to_serialized_bytes(const abstract_type& type) const {
    if (_encoded_bytes.empty()) {
        return managed_bytes_opt();
    }

    managed_bytes_view comparable_bytes_view(_encoded_bytes);
    bytes_ostream serialized_bytes_ostream;
    visit(type, from_comparable_bytes_visitor{comparable_bytes_view, serialized_bytes_ostream});
    return std::move(serialized_bytes_ostream).to_managed_bytes();
}

data_value comparable_bytes::to_data_value(const data_type& type) const {
    auto decoded_bytes = to_serialized_bytes(*type);
    if (!decoded_bytes) {
        return data_value::make_null(type);
    }

    return type->deserialize(decoded_bytes.value());
}
