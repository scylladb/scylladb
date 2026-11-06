/*
 *
 * Modified by ScyllaDB
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include <optional>
#include "gms/inet_address.hh"
#include "locator/types.hh"

namespace gms {

struct loaded_endpoint_state {
    gms::inet_address endpoint;
    std::optional<locator::endpoint_dc_rack> opt_dc_rack;
};

} // namespace gms
