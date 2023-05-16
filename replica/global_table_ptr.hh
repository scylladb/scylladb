/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include "schema/schema_fwd.hh"

namespace replica {
class database;
class table;

using global_table_ptr = std::vector<foreign_ptr<lw_shared_ptr<table>>>;
future<global_table_ptr> get_table_on_all_shards(sharded<database>& db, table_id uuid);

} // replica namespace
