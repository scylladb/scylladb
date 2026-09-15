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
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace db {
class config;
}

namespace tools {

using tablets_t = std::map<dht::token, locator::tablet_replica_set>;

/// The identity of the node the data directory belongs to, as recorded in "system.local"
struct local_node_info {
    locator::host_id host_id;
    std::optional<unsigned> shard_count;
    std::optional<unsigned> ignore_msb_bits;
};

/// Load the rows of given table in "system.tablets" from its sstables
///
/// @param dbcfg the db config
/// @param scylla_data_path path to the scylla data directory, which is usually
///        /var/lib/scylla/data
/// @param table the ID of the table whose tablet rows should be loaded
/// @param permit the permit for performing read ops
/// @param tablets_directory the directory holding the sstables of
///        "system.tablets", when disengaged it is looked up under
///        \p scylla_data_path
/// @returns a map from last token to the replica set
future<tablets_t> load_system_tablets(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      reader_permit permit,
                                      std::optional<std::filesystem::path> tablets_directory = std::nullopt);

/// Load the identity of the local node from "system.local" and "system.topology"
///
/// @returns the identity of the node owning \p scylla_data_path, or nothing if
///          "system.local" is unavailable or doesn't identify the node. The
///          sharding parameters are disengaged if "system.topology" has no row
///          for the node
future<std::optional<local_node_info>> load_local_node_info(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      reader_permit permit);

}
