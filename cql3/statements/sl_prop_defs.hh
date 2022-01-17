/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
    qos::service_level_options get_service_level_options() const;
};

}

}
