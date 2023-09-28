/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "locator/types.hh"

#include <unordered_set>

namespace dht {
class token;
}

namespace service {

struct replacement_info {
    std::unordered_set<dht::token> tokens;
    locator::endpoint_dc_rack dc_rack;
    locator::host_id host_id;
    gms::inet_address address;
};

}
