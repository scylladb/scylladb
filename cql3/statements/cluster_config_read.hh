/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <map>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "types/types.hh"

namespace cql3 {
class query_processor;
}

namespace cql3::statements {

// Reads and parses the `configs` map<text,text> column for a single
// cluster-config row. `cql` is a "SELECT configs FROM ..." query and `params`
// binds its WHERE clause. A missing row or NULL/absent configs column yields an
// empty map. Centralizing this keeps every scope reader (and ALTER TABLE's
// read-modify-write) in agreement about what "no stored overrides" means.
//
// `params` is taken by value so it outlives the internal query: query_data_params
// is a non-owning span, so the bound values must stay alive across the co_await.
seastar::future<std::map<seastar::sstring, seastar::sstring>>
read_configs_column(query_processor& qp, seastar::sstring cql, std::vector<data_value_or_unset> params);

}
