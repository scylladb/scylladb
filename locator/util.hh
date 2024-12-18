/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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
}