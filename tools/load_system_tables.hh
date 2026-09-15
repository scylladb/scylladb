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

#include "data_dictionary/storage_options.hh"
#include "reader_permit.hh"
#include "dht/token.hh"
#include "locator/host_id.hh"
#include "locator/tablets.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"
#include "sstables/open_info.hh"
#include "sstables/sstables_registry.hh"

namespace db {
class config;
}

namespace tools {

using tablets_t = std::map<dht::token, locator::tablet_replica_set>;

/// A row of "system.sstables_registry": one sstable of a table on object storage
struct sstables_registry_entry {
    sstring status;
    sstables::sstable_state state;
    sstables::entry_descriptor desc;
};

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

/// Load the storage options of a keyspace from "system_schema.scylla_keyspaces"
///
/// Says where the sstables of the tables of \p keyspace live: a local
/// directory, or a bucket of an object store.
///
/// @returns the storage options, or nothing when the keyspace has no row there,
///          which is how a keyspace on local storage is recorded
future<std::optional<data_dictionary::storage_options>> load_keyspace_storage_options(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      std::string_view keyspace,
                                      reader_permit permit);

/// Load the sstables a node owns of a table on object storage, from
/// "system.sstables_registry"
///
/// The sstables of such a table are not listable: the bucket is shared by the
/// whole cluster, so which of the objects in it make up the table on this node
/// is only recorded in the registry.
///
/// @returns the registry entries of \p table owned by \p node_owner
future<std::vector<sstables_registry_entry>> load_system_sstables_registry(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      table_id table,
                                      locator::host_id node_owner,
                                      reader_permit permit);

/// The sstables registry of a node which is not running
///
/// "system.sstables_registry" records which objects of a bucket make up a table
/// on a node, which is the only way to enumerate the sstables of a table living
/// in object storage: the bucket is shared by the whole cluster, and nothing in
/// it says which sstable belongs to which table. This reads the registry from
/// the sstables of the data dir, where a running node reads it through CQL.
///
/// Only listing is supported. The other operations throw: a tool must not write
/// to the registry of a node.
std::unique_ptr<sstables::sstables_registry> make_offline_sstables_registry(const db::config& dbcfg,
                                      std::filesystem::path scylla_data_path,
                                      reader_permit permit);

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
