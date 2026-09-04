/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

namespace db {
class config;
}

/**
 * Registers a schema initializer that applies the configured default
 * speculative_retry value to user tables.
 *
 * When the `speculative_retry_user_table_default` configuration option is not
 * set, all tables keep the built-in default. System tables keep the built-in
 * default in any case.
 *
 * The option is live-updatable: the current value applies to tables created
 * after an update, and the parsed value is cached per shard. An invalid
 * initial value throws configuration_exception here, failing startup. An
 * invalid value set by a configuration update is ignored, keeping the last
 * valid value (or the built-in default if there is none).
 *
 * User tables are all tables not belonging to internal keyspaces, namely
 * CQL base tables, materialized views, secondary indexes, CDC log tables,
 * Alternator base tables, Alternator GSIs, Alternator LSIs and Alternator Streams.
 */
void register_speculative_retry_initializer(db::config& cfg);
