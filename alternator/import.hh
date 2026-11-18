/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <seastar/core/sstring.hh>
#include "alternator/executor.hh"
#include "alternator/export.hh"
#include "alternator/node_incarnation.hh"
#include "utils/rjson.hh"

namespace db { struct alternator_import_row; }

namespace alternator {

// A row of the imports table, as stored (see db/system_distributed_keyspace.hh).
using import_row = db::alternator_import_row;

// The InputFormat values ImportTable supports so far.
enum class import_input_format {
    dynamodb_json,
};

// The InputCompressionType values ImportTable supports so far.
enum class import_input_compression_type {
    none,
    gzip,
};

// A validated ImportTable request.
struct import_table_request {
    std::string client_token;
    std::string s3_bucket;
    std::string s3_key_prefix;
    import_input_format input_format;
    import_input_compression_type input_compression_type;
    // Only the TableCreationParameters members ImportTable accepts, kept
    // here so there's no need to re-serialize for the response.
    rjson::value table_creation_parameters;
    create_table_params validated_table_params;
};

// Validate an ImportTable request, throwing api_error::validation on the
// first problem. Only parses: looks up neither imports nor tables.
import_table_request parse_import_table_request(const rjson::value& request, const gms::feature_service& feat, db::tablets_mode_t::mode tablets_mode);

// The ImportTableDescription of an import, as ImportTable (and, in the future, DescribeImport)
// reports it. Throws std::runtime_error if the stored request of the row cannot be parsed - a
// corrupt row.
rjson::value make_import_table_description(const import_row& row);

} // namespace alternator
