/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/statements/view_prop_defs.hh"

namespace cql3::statements {

void view_prop_defs::validate_raw(op_type op, const data_dictionary::database db, sstring ks_name,
        const schema::extensions_map& exts) const
{
    cf_properties::validate(db, std::move(ks_name), exts);

    if (use_compact_storage()) {
        throw exceptions::invalid_request_exception(format("Cannot use 'COMPACT STORAGE' when defining a materialized view"));
    }

    if (properties()->get_cdc_options(exts)) {
        throw exceptions::invalid_request_exception("Cannot enable CDC for a materialized view");
    }

    if (op == op_type::create) {
        const auto maybe_id = properties()->get_id();
        if (maybe_id && db.try_find_table(*maybe_id)) {
            const auto schema_ptr = db.find_schema(*maybe_id);
            const auto& ks_name = schema_ptr->ks_name();
            const auto& cf_name = schema_ptr->cf_name();

            throw exceptions::invalid_request_exception(seastar::format("Table with ID {} already exists: {}.{}", *maybe_id, ks_name, cf_name));
        }
    }
}

void view_prop_defs::apply_to_builder(op_type op, schema_builder& builder, schema::extensions_map exts,
        const data_dictionary::database db, sstring ks_name, bool is_colocated) const
{
    _properties->apply_to_builder(builder, exts, db, std::move(ks_name), !is_colocated);

    if (op == op_type::create) {
        const auto maybe_id = properties()->get_id();
        if (maybe_id) {
            builder.set_uuid(*maybe_id);
        }
    }

    if (op == op_type::alter) {
        if (builder.get_gc_grace_seconds() == 0) {
            throw exceptions::invalid_request_exception(
                    "Cannot alter gc_grace_seconds of a materialized view to 0, since this "
                    "value is used to TTL undelivered updates. Setting gc_grace_seconds too "
                    "low might cause undelivered updates to expire before being replayed.");
        }
    }

    if (builder.default_time_to_live().count() > 0) {
        throw exceptions::invalid_request_exception(
                "Cannot set or alter default_time_to_live for a materialized view. "
                "Data in a materialized view always expire at the same time than "
                "the corresponding data in the parent table.");
    }
}

} // namespace cql3::statements
