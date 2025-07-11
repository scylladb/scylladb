/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/view_prop_defs.hh"

namespace cql3::statements {

void view_prop_defs::validate_raw(op_type, const data_dictionary::database db, sstring ks_name,
        const schema::extensions_map& exts) const
{
    cf_properties::validate(db, std::move(ks_name), exts);
}

void view_prop_defs::apply_to_builder(op_type, schema_builder& builder, schema::extensions_map exts,
        const data_dictionary::database db, sstring ks_name, bool is_colocated) const
{
    _properties->apply_to_builder(builder, exts, db, std::move(ks_name), !is_colocated);
}

} // namespace cql3::statements
