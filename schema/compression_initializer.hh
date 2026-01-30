/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <functional>

#include "db/config.hh"
#include "db/extensions.hh"
#include "schema/schema.hh"
#include "schema/schema_builder.hh"

bool is_internal_keyspace(std::string_view name);

namespace {

/**
 * Registers a schema initializer that applies default compression parameters
 * to user and system tables.
 *
 * System tables default to the LZ4 compressor.
 * User tables default to the configuration option: `sstable_compression_user_table_options`.
 *
 * User tables are all tables not belonging to internal keyspaces, namely
 * CQL base tables, materialized views, secondary indexes, CDC log tables,
 * Alternator base tables, Alternator GSIs, Alternator LSIs and Alternator Streams.
 */
inline void register_compression_initializer(db::config& cfg, std::function<bool()> dicts_feature_enabled_fn) {
    schema_builder::register_schema_initializer([&cfg, dicts_feature_enabled_fn = std::move(dicts_feature_enabled_fn)](schema_builder& builder) {

        if (is_internal_keyspace(builder.ks_name()) || cfg.extensions().is_extension_internal_keyspace(builder.ks_name())) {
            builder.set_compressor_params(compression_parameters::algorithm::lz4);
        } else {
            builder.set_compressor_params(cfg.get_sstable_compression_user_table_options(dicts_feature_enabled_fn()));
        }
    });
}

inline void register_compression_initializer(db::config& cfg, bool dicts_feature_enabled) {
    register_compression_initializer(cfg, [dicts_feature_enabled] { return dicts_feature_enabled; });
}

}