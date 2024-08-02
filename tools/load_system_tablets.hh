/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <map>
#include <seastar/core/future.hh>

#include "reader_permit.hh"
#include "dht/token.hh"
#include "locator/tablets.hh"
#include "seastarx.hh"

namespace db {
class config;
}

namespace tools {

using tablets_t = std::map<dht::token, locator::tablet_replica_set>;

/// Load the rows of given table in "system.tablets" from its sstables
///
/// @param cfg the db config
/// @param scylla_data_path path to the scylla data directory, which is usually
///        /var/lib/scylla/data
/// @param keyspace_name the keyspace name of the table
/// @param table_name the table name of the table
/// @param permit the permit for performing read ops
/// @returns a map from last token to the replica set
future<tablets_t> load_system_tablets(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      std::string_view keyspace_name,
                                      std::string_view tablet_name,
                                      reader_permit permit);

}
