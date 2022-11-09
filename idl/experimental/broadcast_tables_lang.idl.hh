/*
 * Copyright 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

namespace service {
namespace broadcast_tables {

struct select_query {
    bytes key;
};

struct update_query {
    bytes key;
    bytes new_value;
    std::optional<bytes_opt> value_condition;
};

struct query {
    std::variant<service::broadcast_tables::select_query, service::broadcast_tables::update_query> q;
};

} // namespace broadcast_tables
} // namespace service
