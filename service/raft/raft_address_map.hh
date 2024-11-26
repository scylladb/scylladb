/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/core/lowres_clock.hh>
#include <chrono>

#include "service/address_map.hh"

namespace service {

using raft_address_map = address_map_t<seastar::lowres_clock>;

} // end of namespace service
