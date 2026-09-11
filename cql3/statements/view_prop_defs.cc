/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/view_prop_defs.hh"

#include <ranges>

#include <fmt/ranges.h>

#include "gms/feature_service.hh"

namespace cql3::statements {

void view_prop_defs::validate_raw(op_type op, const data_dictionary::database db, sstring ks_name,
        const schema::extensions_map& exts) const
{
    cf_properties::validate(db, std::move(ks_name), exts);

    // Registry-backed cluster-config properties (e.g. auto_repair_enabled) are recognized at
    // scope::table by cf_prop_defs::validate(), which cf_properties::validate() above delegates
    // to, so they pass keyword validation on the view paths too. But a materialized view has no
    // config write path yet: neither create_view_statement nor alter_view_statement writes the
    // out-of-band `configs` column of system_schema.scylla_tables, and apply_to_builder() below
    // never reads these keys. Accepting them would therefore report success and silently discard
    // the setting, so reject them explicitly for now -- mirroring the error ALTER TABLE already
    // raises when it is pointed at a view. If per-view config overrides are added later, this
    // check is what should be replaced by an actual write path.
    if (properties()->has_table_config_properties(db.features())) {
        const auto names = properties()->get_config_updates(db.features()) | std::views::keys;
        throw exceptions::invalid_request_exception(seastar::format(
                "Cluster-config properties are not supported on materialized views: {}",
                fmt::join(names, ", ")));
    }

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
