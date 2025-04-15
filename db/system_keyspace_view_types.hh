/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <utility>
#include <optional>
#include "dht/token.hh"
#include "seastarx.hh"

namespace dht {

class token;

}

namespace db {

using system_keyspace_view_name = std::pair<sstring, sstring>;

struct system_keyspace_view_build_progress {
    system_keyspace_view_name view;
    dht::token first_token;
    std::optional<dht::token> next_token;
    shard_id cpu_id;
};

}
