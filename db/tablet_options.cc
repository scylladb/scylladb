/*
 * Copyright 2025-present ScyllaDB
 */
/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <cstdlib>

#include "exceptions/exceptions.hh"
#include "db/tablet_options.hh"
#include "gms/feature_service.hh"
#include <seastar/core/bitops.hh>
#include "utils/log.hh"

extern logging::logger dblog;

namespace db {

static
bool parse_bool_option(const sstring& value) {
    if (strcasecmp(value.c_str(), "true") == 0 || strcasecmp(value.c_str(), "yes") == 0 || value == "1") {
        return true;
    }
    if (strcasecmp(value.c_str(), "false") == 0 || strcasecmp(value.c_str(), "no") == 0 || value == "0") {
        return false;
    }
    throw std::invalid_argument(format("Invalid boolean value: {}", value));
}

tablet_options::tablet_options(const map_type& map) {
    for (auto& [key, value_str] : map) {
        switch (tablet_options::from_string(key)) {
        case tablet_option_type::min_tablet_count:
            if (auto value = std::atol(value_str.c_str())) {
                min_tablet_count.emplace(value);
            }
            break;
        case tablet_option_type::max_tablet_count:
            if (auto value = std::atol(value_str.c_str())) {
                max_tablet_count.emplace(value);
            }
            break;
        case tablet_option_type::min_per_shard_tablet_count:
            if (auto value = std::atof(value_str.c_str())) {
                min_per_shard_tablet_count.emplace(value);
            }
            break;
        case tablet_option_type::expected_data_size_in_gb:
            if (auto value = std::atol(value_str.c_str())) {
                expected_data_size_in_gb.emplace(value);
            }
            break;
        case tablet_option_type::pow2_count:
            pow2_count = parse_bool_option(value_str);
            break;
        }
    }
}

sstring tablet_options::to_string(tablet_option_type hint) {
    switch (hint) {
    case tablet_option_type::min_tablet_count: return "min_tablet_count";
    case tablet_option_type::max_tablet_count: return "max_tablet_count";
    case tablet_option_type::min_per_shard_tablet_count: return "min_per_shard_tablet_count";
    case tablet_option_type::expected_data_size_in_gb: return "expected_data_size_in_gb";
    case tablet_option_type::pow2_count: return "pow2_count";
    }
}

tablet_option_type tablet_options::from_string(sstring hint_desc) {
    if (hint_desc == "min_tablet_count") {
        return tablet_option_type::min_tablet_count;
    } else if (hint_desc == "max_tablet_count") {
        return tablet_option_type::max_tablet_count;
    } else if (hint_desc == "min_per_shard_tablet_count") {
        return tablet_option_type::min_per_shard_tablet_count;
    } else if (hint_desc == "expected_data_size_in_gb") {
        return tablet_option_type::expected_data_size_in_gb;
    } else if (hint_desc == "pow2_count") {
        return tablet_option_type::pow2_count;
    } else {
        throw exceptions::syntax_exception(fmt::format("Unknown tablet hint '{}'", hint_desc));
    }
}

std::map<sstring, sstring> tablet_options::to_map() const {
    std::map<sstring, sstring> res;
    if (min_tablet_count) {
        res[to_string(tablet_option_type::min_tablet_count)] = fmt::to_string(*min_tablet_count);
    }
    if (max_tablet_count) {
        res[to_string(tablet_option_type::max_tablet_count)] = fmt::to_string(*max_tablet_count);
    }
    if (min_per_shard_tablet_count) {
        res[to_string(tablet_option_type::min_per_shard_tablet_count)] = fmt::to_string(*min_per_shard_tablet_count);
    }
    if (expected_data_size_in_gb) {
        res[to_string(tablet_option_type::expected_data_size_in_gb)] = fmt::to_string(*expected_data_size_in_gb);
    }
    if (pow2_count) {
        res[to_string(tablet_option_type::pow2_count)] = fmt::to_string(*pow2_count);
    }
    return res;
}

void tablet_options::validate(const map_type& map, const gms::feature_service& features) {
    std::optional<ssize_t> min_tablets;
    std::optional<ssize_t> max_tablets;
    bool pow2_count = features.arbitrary_tablet_boundaries ? default_pow2_count : true;

    for (auto& [key, value_str] : map) {
        switch (tablet_options::from_string(key)) {
        case tablet_option_type::min_tablet_count:
            if (auto value = std::atol(value_str.c_str()); value < 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for min_tablet_count", value));
            } else {
                min_tablets = value;
            }
            break;
        case tablet_option_type::max_tablet_count:
            if (auto value = std::atol(value_str.c_str()); value <= 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for max_tablet_count", value));
            } else {
                max_tablets = value;
            }
            break;
        case tablet_option_type::min_per_shard_tablet_count:
            if (auto value = std::atof(value_str.c_str()); value < 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for min_per_shard_tablet_count", value));
            }
            break;
        case tablet_option_type::expected_data_size_in_gb:
            if (auto value = std::atol(value_str.c_str()); value < 0) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for expected_data_size_in_gb", value));
            }
            break;
        case tablet_option_type::pow2_count:
            try {
                pow2_count = parse_bool_option(value_str);
            } catch (const std::invalid_argument& e) {
                throw exceptions::configuration_exception(format("Invalid value '{}' for pow2_count", value_str));
            }
            if (!pow2_count && !features.arbitrary_tablet_boundaries) {
                throw exceptions::configuration_exception(
                        "pow2_count cannot be set to false until the arbitrary_tablet_boundaries feature is enabled");
            }
            break;
        }
    }

    if (min_tablets && max_tablets) {
        auto effective_min = pow2_count ? 1u << log2ceil(static_cast<size_t>(*min_tablets)) : static_cast<size_t>(*min_tablets);
        auto effective_max = pow2_count ? 1u << log2floor(static_cast<size_t>(*max_tablets)) : static_cast<size_t>(*max_tablets);

        if (effective_min > effective_max) {
            throw exceptions::configuration_exception(
                    format("Invalid tablet count range: min_tablet_count={} (effective: {}) and max_tablet_count={} (effective: {}) "
                           "result in conflicting constraints after rounding to powers of 2. "
                           "Since tablet counts must be powers of 2, min_tablet_count rounds up and max_tablet_count rounds down"
                           "Please adjust the values so that the smallest power of 2 greater than min_tablet_count is <= the largest power of 2 <= max_tablet_count.",
                            *min_tablets, effective_min, *max_tablets, effective_max));
        }
    }
}

} // namespace db
