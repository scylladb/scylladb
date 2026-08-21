/*
 * Copyright 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "index/index_option_utils.hh"
#include "exceptions/exceptions.hh"
#include <boost/algorithm/string.hpp>
#include <fmt/ranges.h>
#include <seastar/core/format.hh>

namespace secondary_index::util {

void validate_enumerated_option(
        const std::vector<sstring>& supported_values, std::string_view index_type_name, const sstring& value_name, const sstring& value) {
    bool is_valid = std::any_of(supported_values.begin(), supported_values.end(), [&](const std::string& v) {
        return boost::iequals(value, v);
    });

    if (!is_valid) {
        throw exceptions::invalid_request_exception(seastar::format("Invalid value in option '{}' for {} index: '{}'."
                                                                    " Supported are case-insensitive: {}",
                value_name, index_type_name, value, fmt::join(supported_values, ", ")));
    }
}

void validate_positive_option(int max, std::string_view index_type_name, const sstring& value_name, const sstring& value) {
    int num_value;
    size_t len;
    try {
        num_value = std::stoi(value, &len);
    } catch (...) {
        throw exceptions::invalid_request_exception(
                seastar::format("Invalid value in option '{}' for {} index: '{}' is not an integer", value_name, index_type_name, value));
    }
    if (len != value.size()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Invalid value in option '{}' for {} index: '{}' is not an integer", value_name, index_type_name, value));
    }

    if (num_value <= 0 || num_value > max) {
        throw exceptions::invalid_request_exception(
                seastar::format("Invalid value in option '{}' for {} index: '{}' is out of valid range [1 - {}]", value_name, index_type_name, value, max));
    }
}

void validate_factor_option(float min, float max, std::string_view index_type_name, const sstring& value_name, const sstring& value) {
    float num_value;
    size_t len;
    try {
        num_value = std::stof(value, &len);
    } catch (...) {
        throw exceptions::invalid_request_exception(
                seastar::format("Invalid value in option '{}' for {} index: '{}' is not a float", value_name, index_type_name, value));
    }
    if (len != value.size()) {
        throw exceptions::invalid_request_exception(
                seastar::format("Invalid value in option '{}' for {} index: '{}' is not a float", value_name, index_type_name, value));
    }

    if (!(num_value >= min && num_value <= max)) {
        throw exceptions::invalid_request_exception(seastar::format(
                "Invalid value in option '{}' for {} index: '{}' is out of valid range [{} - {}]", value_name, index_type_name, value, min, max));
    }
}

} // namespace secondary_index::util
