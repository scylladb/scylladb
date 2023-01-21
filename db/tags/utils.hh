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

#include "schema.hh"
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

// FIXME: Updating tags currently relies on updating schema, which may be subject
// to races during concurrent updates of the same table. Once Scylla schema updates
// are fixed, this issue will automatically get fixed as well.
future<> update_tags(service::migration_manager& mm, schema_ptr schema, std::map<sstring, sstring>&& tags_map);

}
