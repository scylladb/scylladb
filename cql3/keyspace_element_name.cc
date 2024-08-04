/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/keyspace_element_name.hh"

namespace cql3 {

void keyspace_element_name::set_keyspace(std::string_view ks, bool keep_case)
{
    _ks_name = to_internal_name(ks, keep_case);
}

bool keyspace_element_name::has_keyspace() const
{
    return bool(_ks_name);
}

const sstring& keyspace_element_name::get_keyspace() const
{
    SCYLLA_ASSERT(_ks_name);
    return *_ks_name;
}

sstring keyspace_element_name::to_internal_name(std::string_view view, bool keep_case)
{
    sstring name(view);
    if (!keep_case) {
        std::transform(name.begin(), name.end(), name.begin(), ::tolower);
    }
    return name;
}

sstring keyspace_element_name::to_string() const
{
    return has_keyspace() ? (get_keyspace() + ".") : "";    
}

}
