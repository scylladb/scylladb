/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <filesystem>
#include <seastar/core/future.hh>

#include "seastarx.hh"
#include "schema.hh"

namespace tools {

/// Load the schema(s) from the specified string
///
/// The schema string is expected to contain everything that is needed to
/// create the table(s): keyspace, UDTs, etc. Definitions are expected to be
/// separated by `;`. A keyspace will be automatically generated if missing.
/// TODO:
/// * dropped columns
/// * dropped collections
///
/// Loading the schema(s) has no side-effect [1]. Nothing is written to disk,
/// it is all in memory, kept alive by the returned `schema_ptr`.
/// This is intended to be used by tools, which don't want to meddle with the
/// scylla home directory.
///
/// [1] Currently some global services has to be instantiated (snitch) to
/// be able to load the schema(s), these survive the call.
future<std::vector<schema_ptr>> load_schemas(std::string_view schema_str);

/// Load exactly one schema from the specified string
///
/// If the string contains more or less then one schema, an exception will be
/// thrown. See \ref load_schemas().
future<schema_ptr> load_one_schema(std::string_view schema_str);

/// Load the schema(s) from the specified path
///
/// Same as \ref load_schemas() except it loads the schema from
/// the file at the specified path.
future<std::vector<schema_ptr>> load_schemas_from_file(std::filesystem::path path);

/// Load exactly one schema from the specified path
///
/// Same as \ref load_one_schema() except it loads the schema from
/// the file at the specified path.
future<schema_ptr> load_one_schema_from_file(std::filesystem::path path);

} // namespace tools
