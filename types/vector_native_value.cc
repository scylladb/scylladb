/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "types/vector_native_value.hh"

bool vector_native_value::operator==(const vector_native_value& other) const {
    if (_dimension != other._dimension || _element_size != other._element_size) {
        return false;
    }
    if (is_fixed_size()) {
        const auto& fixed1 = std::get<std::vector<std::byte>>(_data);
        const auto& fixed2 = std::get<std::vector<std::byte>>(other._data);
        return fixed1.size() == fixed2.size()
            && std::memcmp(fixed1.data(), fixed2.data(), fixed1.size()) == 0;
    } else {
        return std::get<std::vector<data_value>>(_data) ==
               std::get<std::vector<data_value>>(other._data);
    }
}
