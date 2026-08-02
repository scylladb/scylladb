/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <filesystem>
#include <map>
#include <seastar/core/future.hh>

#include "reader_permit.hh"
#include "dht/token.hh"
#include "locator/tablets_fwd.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace db {
class config;
}

namespace tools {

using tablets_t = std::map<dht::token, locator::tablet_replica_set>;

/// Load the rows of given table in "system.tablets" from its sstables
///
/// @param dbcfg the db config
/// @param scylla_data_path path to the scylla data directory, which is usually
///        /var/lib/scylla/data
/// @param table the ID of the table whose tablet rows should be loaded
/// @param permit the permit for performing read ops
/// @returns a map from last token to the replica set
future<tablets_t> load_system_tablets(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      reader_permit permit);

}
