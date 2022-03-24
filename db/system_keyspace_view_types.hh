/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/smp.hh>
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
