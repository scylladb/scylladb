/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/json/json_elements.hh>
#include "api/api_init.hh"
#include "db/data_listeners.hh"

namespace cql_transport { class controller; }
namespace db {
class snapshot_ctl;
namespace view {
class view_builder;
}
class system_keyspace;
}
namespace netw { class messaging_service; }
class repair_service;
class sstables_loader;

namespace gms {

class gossiper;

}

namespace api {

// verify that the keyspace is found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective keyspace error.
sstring validate_keyspace(const http_context& ctx, sstring ks_name);

// verify that the keyspace parameter is found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective keyspace error.
sstring validate_keyspace(const http_context& ctx, const std::unique_ptr<http::request>& req);

// verify that the table parameter is found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective table error.
void validate_table(const http_context& ctx, sstring ks_name, sstring table_name);

// splits a request parameter assumed to hold a comma-separated list of table names
// verify that the tables are found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective no_such_column_family error.
// Returns an empty vector if no parameter was found.
// If the parameter is found and empty, returns a list of all table names in the keyspace.
std::vector<sstring> parse_tables(const sstring& ks_name, const http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name);

// splits a request parameter assumed to hold a comma-separated list of table names
// verify that the tables are found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective no_such_column_family error.
// Returns a vector of all table infos given by the parameter, or
// if the parameter is not found or is empty, returns a list of all table infos in the keyspace.
std::vector<table_info> parse_table_infos(const sstring& ks_name, const http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name);

std::vector<table_info> parse_table_infos(const sstring& ks_name, const http_context& ctx, sstring value);

struct scrub_info {
    sstables::compaction_type_options::scrub opts;
    sstring keyspace;
    std::vector<sstring> column_families;
};

future<scrub_info> parse_scrub_options(const http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl, std::unique_ptr<http::request> req);

void set_storage_service(http_context& ctx, httpd::routes& r, sharded<service::storage_service>& ss, service::raft_group0_client&);
void unset_storage_service(http_context& ctx, httpd::routes& r);
void set_sstables_loader(http_context& ctx, httpd::routes& r, sharded<sstables_loader>& sst_loader);
void unset_sstables_loader(http_context& ctx, httpd::routes& r);
void set_view_builder(http_context& ctx, httpd::routes& r, sharded<db::view::view_builder>& vb);
void unset_view_builder(http_context& ctx, httpd::routes& r);
void set_repair(http_context& ctx, httpd::routes& r, sharded<repair_service>& repair);
void unset_repair(http_context& ctx, httpd::routes& r);
void set_transport_controller(http_context& ctx, httpd::routes& r, cql_transport::controller& ctl);
void unset_transport_controller(http_context& ctx, httpd::routes& r);
void set_thrift_controller(http_context& ctx, httpd::routes& r);
void unset_thrift_controller(http_context& ctx, httpd::routes& r);
void set_snapshot(http_context& ctx, httpd::routes& r, sharded<db::snapshot_ctl>& snap_ctl);
void unset_snapshot(http_context& ctx, httpd::routes& r);
void set_load_meter(http_context& ctx, httpd::routes& r, service::load_meter& lm);
void unset_load_meter(http_context& ctx, httpd::routes& r);
seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, http_context &ctx, bool legacy_request = false);

} // namespace api
