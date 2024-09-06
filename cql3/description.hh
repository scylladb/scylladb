/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "bytes.hh"

#include <vector>

using namespace seastar;

namespace data_dictionary {
class keyspace_element;
} // namespace data_dictionary

namespace replica {
class database;
} // namespace replica

namespace cql3 {


// Represents description of keyspace_element
struct description {
    sstring _keyspace;
    sstring _type;
    sstring _name;
    std::optional<sstring> _create_statement;

    // Description without create_statement
    description(replica::database& db, const data_dictionary::keyspace_element& element);

    // Description with create_statement
    description(replica::database& db, const data_dictionary::keyspace_element& element, bool with_internals);

    description(replica::database& db, const data_dictionary::keyspace_element& element, sstring create_statement);

    std::vector<bytes_opt> serialize() const;
};

} // namespace cql3
