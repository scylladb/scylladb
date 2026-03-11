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
#include <unordered_set>
#include "gms/inet_address.hh"
#include "locator/types.hh"
#include "dht/token.hh"

namespace gms {

struct loaded_endpoint_state {
    gms::inet_address endpoint;
    std::unordered_set<dht::token> tokens;
    std::optional<locator::endpoint_dc_rack> opt_dc_rack;
};

} // namespace gms
