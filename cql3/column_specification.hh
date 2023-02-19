/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "types/types.hh"

namespace cql3 {

class column_identifier;

class column_specification final {
public:
    const sstring ks_name;
    const sstring cf_name;
    const ::shared_ptr<column_identifier> name;
    const data_type type;

    column_specification(std::string_view ks_name_, std::string_view cf_name_, ::shared_ptr<column_identifier> name_, data_type type_);

    /**
     * Returns a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     *
     * @param alias the column alias
     * @return a new <code>ColumnSpecification</code> for the same column but with the specified alias.
     */
    lw_shared_ptr<column_specification> with_alias(::shared_ptr<column_identifier> alias) {
        return make_lw_shared<column_specification>(ks_name, cf_name, alias, type);
    }
    
    bool is_reversed_type() const {
        return ::dynamic_pointer_cast<const reversed_type_impl>(type) != nullptr;
    }

    static bool all_in_same_table(const std::vector<lw_shared_ptr<column_specification>>& names);
};

}
