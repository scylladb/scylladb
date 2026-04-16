/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>

#include "gms/inet_address.hh"
#include "locator/types.hh"

namespace gms {

struct loaded_endpoint_state {
    inet_address endpoint;
    std::optional<locator::endpoint_dc_rack> opt_dc_rack;
};

} // namespace gms
