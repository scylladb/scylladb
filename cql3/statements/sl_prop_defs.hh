/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "cql3/statements/property_definitions.hh"
#include "service/qos/qos_common.hh"

namespace cql3 {

namespace statements {

class sl_prop_defs : public property_definitions {
    qos::service_level_options _slo;
public:
    void validate();

    static constexpr auto KW_SHARES = "shares";
    static constexpr int SHARES_DEFAULT_VAL = 1000;
    static constexpr int SHARES_MIN_VAL = 1;
    static constexpr int SHARES_MAX_VAL = 1000;

    qos::service_level_options get_service_level_options() const;
};

}

}
