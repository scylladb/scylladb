/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include <seastar/core/sstring.hh>
#include <seastar/http/request.hh>

#include "api/api_init.hh"
#include "schema/schema_fwd.hh"
#include "seastarx.hh"

namespace api {

// Lookups in the data dictionary made on behalf of a request. A keyspace or
// table the request names but the schema does not know is reported with
// httpd::bad_param_exception, so that the client gets a 400 rather than the
// 500 any other exception escaping a handler would produce.

// verify that the keyspace is found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective keyspace error.
sstring validate_keyspace(const http_context& ctx, sstring ks_name);

// verify that the keyspace parameter is found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective keyspace error.
sstring validate_keyspace(const http_context& ctx, const std::unique_ptr<http::request>& req);
sstring validate_keyspace(const http_context& ctx, const http::request& req);

// verify that the keyspace:table is found, otherwise a bad_param_exception exception is thrown
// returns the table_id of the table if found
table_id validate_table(data_dictionary::database db, sstring ks_name, sstring table_name);

// for the endpoints that are per-table once a keyspace replicates by tablets:
// throws bad_param_exception naming api_endpoint_path if ks_name is such a keyspace.
void ensure_tablets_disabled(const http_context& ctx, const sstring& ks_name, const sstring& api_endpoint_path);

// whether any keyspace at all replicates by tablets
bool any_of_keyspaces_use_tablets(const http_context& ctx);

// splits a request parameter assumed to hold a comma-separated list of table names
// verify that the tables are found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective no_such_column_family error.
// Returns a vector of all table infos given by the parameter, or
// if the parameter is not found or is empty, returns a list of all table infos in the keyspace.

std::vector<table_info> parse_table_infos(const sstring& ks_name, const http_context& ctx, sstring value);

std::pair<sstring, std::vector<table_info>> parse_table_infos(const http_context& ctx, const http::request& req, sstring cf_param_name = "cf");

} // namespace api
