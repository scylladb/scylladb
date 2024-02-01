/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
 
#pragma once

#include "dht/token_range_endpoints.hh"

namespace replica {
    class database;
}

namespace gms {
    class gossiper;
}

namespace locator {
    future<std::vector<dht::token_range_endpoints>> describe_ring(const replica::database& db, const gms::gossiper& gossiper, const sstring& keyspace, bool include_only_local_dc = false);
    future<std::vector<dht::token_range_endpoints>> describe_ring_for_table(
        const replica::database& db,
        const gms::gossiper& gossiper,
        std::string_view ks_name,
        std::string_view cf_name);
}
