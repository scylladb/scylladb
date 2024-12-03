/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/lowres_clock.hh>

#include "service/address_map.hh"

namespace gms {

using gossip_address_map = service::address_map_t<seastar::lowres_clock>;

} // end of namespace service
