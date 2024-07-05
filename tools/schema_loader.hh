/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "schema/schema.hh"

namespace db {
class config;
}

namespace tools {

/// Load the schema(s) from the specified string
///
/// The schema string is expected to contain everything that is needed to
/// create the table(s): keyspace, UDTs, etc. Definitions are expected to be
/// separated by `;`. A keyspace will be automatically generated if missing.
/// Tables whose name ends in "_scylla_cdc_log" are interpreted as CDC tables,
/// meaning they will be configured with the CDC partitioner.
/// Loading the schema(s) has no side-effect [1]. Nothing is written to disk,
/// it is all in memory, kept alive by the returned `schema_ptr`.
/// This is intended to be used by tools, which don't want to meddle with the
/// scylla home directory.
///
/// [1] Currently some global services has to be instantiated (snitch) to
/// be able to load the schema(s), these survive the call.
future<std::vector<schema_ptr>> load_schemas(const db::config& dbcfg, std::string_view schema_str);

/// Load exactly one schema from the specified path
///
/// If the file at the specified path contains more or less than one schema,
/// an exception will be thrown. See \ref load_schemas().
future<schema_ptr> load_one_schema_from_file(const db::config& dbcfg, std::filesystem::path path);

/// Load the system schema, with the given keyspace and table
///
/// Note that only schemas from builtin system tables are supported, i.e.,
/// from the following keyspaces:
/// * system
/// * system_schema
/// * system_distributed
/// * system_distributed_everywhere
///
/// Any table from said keyspaces can be loaded. The keyspaces are created with
/// all schema and experimental features enabled.
schema_ptr load_system_schema(const db::config& dbcfg, std::string_view keyspace, std::string_view table);

/// Load the schema of the table with the designated keyspace and table name,
/// from the system schema table sstables.
///
/// The schema table sstables are accessed for read only. In general this method
/// tries very hard to have no side-effects.
/// The \p scylla_data_path parameter is expected to point to the scylla data
/// directory, which is usually /var/lib/scylla/data.
future<schema_ptr> load_schema_from_schema_tables(const db::config& dbcfg, std::filesystem::path scylla_data_path, std::string_view keyspace, std::string_view table);

/// Load the schema from the sstable's serialization header.
///
/// This schema is incomplete:
/// * The names of the partition key and clustering key columns are not known
///   and are given placeholder names when the schema is loaded.
/// * Only the compression options are contained in the sstable, other options
///   like compaction, etc. are missing and defaults are used instead in the
///   returned schema object.
/// * Encryption options are likewise not contained in the sstable, if the
///   sstable is encrypted, the schema load will fail.
///
/// That said, this schema is enough to parse and display the sstable's content.
///
/// The keyspace and table parameters are optional. If known, provide them so
/// the schema is created with the proper names, otherwise placeholder names
/// will be used.
future<schema_ptr> load_schema_from_sstable(const db::config& dbcfg, std::filesystem::path sstable_path, std::string_view keyspace = "", std::string_view table = "");

} // namespace tools
