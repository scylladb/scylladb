/*
 * Copyright (C) 2015-present ScyllaDB.
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General License for more details.
 *
 * You should have received a copy of the GNU General License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <vector>
#include <seastar/core/sstring.hh>

#include "seastarx.hh"

namespace dht {
struct endpoint_details {
    sstring _host;
    sstring _datacenter;
    sstring _rack;
};

struct token_range_endpoints {
    sstring _start_token;
    sstring _end_token;
    std::vector<sstring> _endpoints;
    std::vector<sstring> _rpc_endpoints;
    std::vector<endpoint_details> _endpoint_details;
};

}
