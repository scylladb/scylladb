/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/column_specification.hh"

namespace cql3 {

column_specification::column_specification(std::string_view ks_name_, std::string_view cf_name_, ::shared_ptr<column_identifier> name_, data_type type_)
        : ks_name(ks_name_)
        , cf_name(cf_name_)
        , name(name_)
        , type(type_)
    { }


bool column_specification::all_in_same_table(const std::vector<lw_shared_ptr<column_specification>>& names)
{
    SCYLLA_ASSERT(!names.empty());

    auto first = names.front();
    return std::all_of(std::next(names.begin()), names.end(), [first] (auto&& spec) {
        return spec->ks_name == first->ks_name && spec->cf_name == first->cf_name;
    });
}

}
