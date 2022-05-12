/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/cf_name.hh"

namespace cql3 {

void cf_name::set_column_family(const sstring& cf, bool keep_case)
{
    _cf_name = to_internal_name(cf, keep_case);
}

const sstring& cf_name::get_column_family() const
{
    return _cf_name;
}

sstring cf_name::to_string() const
{
    return keyspace_element_name::to_string() + _cf_name;
}

}
