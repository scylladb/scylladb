/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <algorithm>

#include <seastar/http/exception.hh>

#include "api/api.hh"
#include "api/data_dictionary.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "schema/schema.hh"

namespace api {

sstring validate_keyspace(const http_context& ctx, sstring ks_name) {
    if (ctx.data_dict().has_keyspace(ks_name)) {
        return ks_name;
    }
    throw httpd::bad_param_exception(data_dictionary::no_such_keyspace(ks_name).what());
}

sstring validate_keyspace(const http_context& ctx, const std::unique_ptr<http::request>& req) {
    return validate_keyspace(ctx, req->get_path_param("keyspace"));
}

sstring validate_keyspace(const http_context& ctx, const http::request& req) {
    return validate_keyspace(ctx, req.get_path_param("keyspace"));
}

table_id validate_table(data_dictionary::database db, sstring ks_name, sstring table_name) {
    try {
        return db.find_schema(ks_name, table_name)->id();
    } catch (data_dictionary::no_such_column_family& e) {
        throw httpd::bad_param_exception(e.what());
    }
}

void ensure_tablets_disabled(const http_context& ctx, const sstring& ks_name, const sstring& api_endpoint_path) {
    if (ctx.data_dict().find_keyspace(ks_name).uses_tablets()) {
        throw httpd::bad_param_exception{fmt::format("{} is per-table in keyspace '{}'. Please provide table name using 'cf' parameter.", api_endpoint_path, ks_name)};
    }
}

bool any_of_keyspaces_use_tablets(const http_context& ctx) {
    auto db = ctx.data_dict();
    auto uses_tablets = [&db](const auto& ks_name) {
        return db.find_keyspace(ks_name).uses_tablets();
    };

    auto keyspaces = db.get_all_keyspaces();
    return std::any_of(std::begin(keyspaces), std::end(keyspaces), uses_tablets);
}

std::vector<table_info> parse_table_infos(const sstring& ks_name, const http_context& ctx, sstring value) {
    std::vector<table_info> res;
    try {
        if (value.empty()) {
            const auto& cf_meta_data = ctx.data_dict().find_keyspace(ks_name).metadata().get()->cf_meta_data();
            res.reserve(cf_meta_data.size());
            for (const auto& [name, schema] : cf_meta_data) {
                res.emplace_back(table_info{name, schema->id()});
            }
        } else {
            std::vector<sstring> names = split(value, ",");
            res.reserve(names.size());
            const auto db = ctx.data_dict();
            for (const auto& table_name : names) {
                res.emplace_back(table_info{table_name, db.find_schema(ks_name, table_name)->id()});
            }
        }
    } catch (const data_dictionary::no_such_keyspace& e) {
        throw httpd::bad_param_exception(e.what());
    } catch (const data_dictionary::no_such_column_family& e) {
        throw httpd::bad_param_exception(e.what());
    }
    return res;
}

std::pair<sstring, std::vector<table_info>> parse_table_infos(const http_context& ctx, const http::request& req, sstring cf_param_name) {
    auto keyspace = validate_keyspace(ctx, req);
    auto tis = parse_table_infos(keyspace, ctx, req.get_query_param(cf_param_name));
    return std::make_pair(std::move(keyspace), std::move(tis));
}

} // namespace api
