/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later)
 */

#pragma once

#include "db/timeout_clock.hh"
#include "query-result.hh"

namespace replica::mutation_dump {

schema_ptr generate_output_schema_from_underlying_schema(schema_ptr underlying_schema);

future<foreign_ptr<lw_shared_ptr<query::result>>> dump_mutations(
        sharded<database>& db,
        schema_ptr output_schema, // must have been generated from `underlying_schema`, with `generate_output_schema_from_underlying_schema()`
        schema_ptr underlying_schema,
        const dht::partition_range_vector& pr,
        const query::read_command& cmd,
        db::timeout_clock::time_point timeout);

} // namespace replica::mutation_dump
