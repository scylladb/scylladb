/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "schema/schema_fwd.hh"

using namespace seastar;

class partition_key_view;

namespace data_dictionary {
class database;
}

namespace validation {

constexpr size_t max_key_size = std::numeric_limits<uint16_t>::max();

// Returns an error string if key is invalid, a disengaged optional otherwise.
std::optional<sstring> is_cql_key_invalid(const schema& schema, partition_key_view key);
void validate_cql_key(const schema& schema, partition_key_view key);
schema_ptr validate_column_family(data_dictionary::database db, const sstring& keyspace_name, const sstring& cf_name);
void validate_keyspace(data_dictionary::database db, const sstring& keyspace_name);

}
