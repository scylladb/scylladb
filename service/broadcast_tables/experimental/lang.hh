/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <optional>
#include <variant>

#include "bytes.hh"


namespace service::broadcast_tables {

using namespace std::string_literals;

// Represents 'SELECT value WHERE key = {key} FROM system.broadcast_kv_store;'.
struct select_query {
    bytes key;
};

// Represents 'UPDATE system.broadcast_kv_store SET value = {new_value} WHERE key = {key} [IF value = {value_condition}];'.
// If value_condition is nullopt, the update is unconditional.
struct update_query {
    bytes key;
    bytes new_value;
    std::optional<bytes_opt> value_condition;
};

struct query {
    std::variant<select_query, update_query> q;
};

} // namespace service::broadcast_tables
