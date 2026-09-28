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
#include "alternator/executor.hh"
#include "utils/rjson.hh"

namespace alternator {

// A validated ImportTable request.
struct import_table_request {
    std::string client_token;
    std::string s3_bucket;
    std::string s3_key_prefix;
    // Only DYNAMODB_JSON so far.
    std::string input_format;
    // NONE or GZIP.
    std::string input_compression_type;
    // Only the TableCreationParameters members ImportTable accepts, kept
    // here so there's no need to re-serialize for the response.
    rjson::value table_creation_parameters;
    create_table_params validated_table_params;
};

// Validate an ImportTable request, throwing api_error::validation on the
// first problem. Only parses: looks up neither imports nor tables.
import_table_request parse_import_table_request(const rjson::value& request, const gms::feature_service& feat, db::tablets_mode_t::mode tablets_mode);

// The ImportTableDescription of an import which has just been accepted:
// FAILED with NotImplemented for now, with zero counters and a fresh ImportArn.
rjson::value make_import_table_description(const import_table_request& parsed);

// FIXME: stub. Imports are not persisted yet, so there is nothing to look the token up.
// Once import state is stored, this needs to consult it and honour the 8-hour window and
// describe the existing import or return an error when parameters don't match.
future<std::optional<executor::request_return_type>> get_in_progress_import_description(std::string_view client_token);

} // namespace alternator
