/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/ut_name.hh"
#include "cql3/column_identifier.hh"

namespace cql3 {

ut_name::ut_name(shared_ptr<column_identifier> ks_name, ::shared_ptr<column_identifier> ut_name)
    : _ks_name{!ks_name ? std::nullopt : std::optional<sstring>{ks_name->to_string()}}
    , _ut_name{ut_name}
{ }

bool ut_name::has_keyspace() const {
    return bool(_ks_name);
}

void ut_name::set_keyspace(sstring keyspace) {
    _ks_name = std::optional<sstring>{keyspace};
}

const sstring& ut_name::get_keyspace() const {
    return _ks_name.value();
}

bytes ut_name::get_user_type_name() const {
    return _ut_name->bytes_;
}

sstring ut_name::get_string_type_name() const
{
    return _ut_name->to_string();
}

sstring ut_name::to_cql_string() const {
    return (has_keyspace() ? (_ks_name.value() + ".") : "") + _ut_name->to_cql_string();
}

}
