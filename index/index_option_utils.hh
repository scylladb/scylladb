/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <string_view>
#include <vector>
#include <seastar/core/sstring.hh>

namespace secondary_index::util {

inline const std::vector<seastar::sstring> boolean_values = {"false", "true"};

void validate_enumerated_option(const std::vector<seastar::sstring>& supported_values, std::string_view index_type_name, const seastar::sstring& value_name,
        const seastar::sstring& value);

void validate_positive_option(int max, std::string_view index_type_name, const seastar::sstring& value_name, const seastar::sstring& value);

void validate_factor_option(float min, float max, std::string_view index_type_name, const seastar::sstring& value_name, const seastar::sstring& value);

} // namespace secondary_index::util
