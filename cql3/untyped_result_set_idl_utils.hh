/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string_view>
#include <type_traits>

#include "cql3/untyped_result_set.hh"
#include "serializer_impl.hh"

namespace cql3::ser {

inline auto blob_as_input_stream(const untyped_result_set_row& row, std::string_view name) {
    return ::ser::as_input_stream(row.get_view(name));
}

template<typename T>
T deserialize_blob_as(const untyped_result_set_row& row, std::string_view name) {
    auto in = blob_as_input_stream(row, name);
    return ::ser::deserialize(in, std::type_identity<T>());
}

} // namespace cql3::ser
