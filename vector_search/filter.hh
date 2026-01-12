/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/rjson.hh"

namespace cql3 {

class query_options;

namespace restrictions {

class statement_restrictions;

/// Converts CQL statement restrictions to JSON format for the Vector Store service.
/// This function extracts primary key restrictions from the statement_restrictions
/// and serializes them to JSON compatible to Vector Store service filtering API.
rjson::value to_json(const statement_restrictions& restrictions, const query_options& options, bool allow_filtering);

} // namespace restrictions
} // namespace cql3
