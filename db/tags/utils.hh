/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <functional>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

#include "schema/schema.hh"
#include "service/client_state.hh"
#include "service/migration_manager.hh"

namespace db {

// get_tags_of_table() returns all tags associated with the given table, or
// nullptr if the table is missing the tags extension.
// Returned value is a non-owning pointer, which refers to the inner fields of the
// schema. It should not outlive the schema_ptr argument.
const std::map<sstring, sstring>* get_tags_of_table(schema_ptr schema);

// find_tag() returns the value of a specific tag, or nothing if it doesn't
// exist. If the table is missing the tags extension (e.g., is not an
// Alternator table) it's not an error - we return nothing, as in the case that
// tags exist but not this tag.
std::optional<std::string> find_tag(const schema& s, const sstring& tag);

// modify_tags() atomically modifies the tags on a given table: It reads the
// existing tags, passes them as a map to the given function which can modify
// the map, and finally writes the modified tags. This read-modify-write
// operation is atomic - isolated from other concurrent schema operations.
//
// The isolation requirement is also why modify_tags() takes the table's name
// ks,cf and not a schema object - the current schema may not be relevant by
// the time the tags are modified, due to some other concurrent modification.
// If a table (ks, cf) doesn't exist, no_such_column_family is thrown.
//
// If the table didn't have the tags schema extension, it's fine: The function
// is passed an empty map, and the tags it adds will be added to the table.
future<> modify_tags(service::migration_manager& mm, sstring ks, sstring cf,
                     std::function<void(std::map<sstring, sstring>&)> modify_func);
}
