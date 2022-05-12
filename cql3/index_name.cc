/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "cql3/index_name.hh"
#include "cql3/cf_name.hh"

namespace cql3 {

void index_name::set_index(const sstring& idx, bool keep_case)
{
    _idx_name = to_internal_name(idx, keep_case);
}

const sstring& index_name::get_idx() const
{
    return _idx_name;
}

cf_name index_name::get_cf_name() const
{
    cf_name cf;
    if (has_keyspace()) {
        cf.set_keyspace(get_keyspace(), true);
    }
    return cf;
}

sstring index_name::to_string() const
{
    return keyspace_element_name::to_string() + _idx_name;
}

}
