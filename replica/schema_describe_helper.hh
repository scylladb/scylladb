/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "schema/schema.hh"
#include "replica/global_table_ptr.hh"
#include <seastar/core/sstring.hh>
#include <optional>

namespace replica {

::schema_describe_helper make_schema_describe_helper(schema_ptr schema, const data_dictionary::database& db);
::schema_describe_helper make_schema_describe_helper(const global_table_ptr& table_shards);

} // namespace replica
