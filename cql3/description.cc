/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/description.hh"

#include "cql3/util.hh"
#include "data_dictionary/keyspace_element.hh"
#include "replica/database.hh"

namespace cql3 {

description::description(replica::database& db, const data_dictionary::keyspace_element& element)
    : _keyspace(util::maybe_quote(element.keypace_name()))
    , _type(element.element_type(db))
    , _name(util::maybe_quote(element.element_name()))
    , _create_statement(std::nullopt) {}

description::description(replica::database& db, const data_dictionary::keyspace_element& element, bool with_internals)
    : _keyspace(util::maybe_quote(element.keypace_name()))
    , _type(element.element_type(db))
    , _name(util::maybe_quote(element.element_name()))
{
    std::ostringstream os;
    element.describe(db, os, with_internals);
    _create_statement = os.str();
}

description::description(replica::database& db, const data_dictionary::keyspace_element& element, sstring create_statement)
    : _keyspace(util::maybe_quote(element.keypace_name()))
    , _type(element.element_type(db))
    , _name(util::maybe_quote(element.element_name()))
    , _create_statement(std::move(create_statement)) {}

std::vector<bytes_opt> description::serialize() const {
    auto desc = std::vector<bytes_opt>{
        {to_bytes(_keyspace)},
        {to_bytes(_type)},
        {to_bytes(_name)}
    };

    if (_create_statement) {
        desc.push_back({to_bytes(*_create_statement)});
    }

    return desc;
}

} // namespace cql3
