/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */



#pragma once

#include "restrictions/restrictions_config.hh"

namespace db { class config; }

namespace cql3 {

struct cql_config {
    restrictions::restrictions_config restrictions;
    explicit cql_config(const db::config& cfg) : restrictions(cfg) {}
    struct default_tag{};
    cql_config(default_tag) : restrictions(restrictions::restrictions_config::default_tag{}) {}
};

extern const cql_config default_cql_config;

}
