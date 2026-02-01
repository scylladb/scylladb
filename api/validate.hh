/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "locator/host_id.hh"
#include "locator/token_metadata.hh"
#include <seastar/core/sstring.hh>
#include <stdexcept>

inline
locator::host_id validate_host_id(const sstring& param) {
    auto hoep = locator::host_id_or_endpoint(param, locator::host_id_or_endpoint::param_type::host_id);
    return hoep.id();
}

inline
bool validate_bool(const sstring& param) {
    if (param == "true") {
        return true;
    } else if (param == "false") {
        return false;
    } else {
        throw std::runtime_error("Parameter must be either 'true' or 'false'");
    }
}

// converts string value of boolean parameter into bool
// maps (case insensitively)
//     "true", "yes" and "1" into true
//     "false", "no" and "0" into false
// otherwise throws runtime_error
inline
bool validate_bool_x(const sstring& param, bool default_value) {
    if (param.empty()) {
        return default_value;
    }

    if (strcasecmp(param.c_str(), "true") == 0 || strcasecmp(param.c_str(), "yes") == 0 || param == "1") {
        return true;
    }
    if (strcasecmp(param.c_str(), "false") == 0 || strcasecmp(param.c_str(), "no") == 0 || param == "0") {
        return false;
    }

    throw std::runtime_error("Invalid boolean parameter value");
}

inline
int64_t validate_int(const sstring& param) {
    return std::atoll(param.c_str());
}
