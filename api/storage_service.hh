/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"
#include "db/data_listeners.hh"

namespace cql_transport { class controller; }
class thrift_controller;
namespace db {
class snapshot_ctl;
namespace view {
class view_builder;
}
class system_keyspace;
}
namespace netw { class messaging_service; }
class repair_service;
namespace cdc { class generation_service; }
class sstables_loader;

namespace gms {

class gossiper;

}

namespace api {

// verify that the keyspace parameter is found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective keyspace error.
sstring validate_keyspace(http_context& ctx, const parameters& param);

// splits a request parameter assumed to hold a comma-separated list of table names
// verify that the tables are found, otherwise a bad_param_exception exception is thrown
// containing the description of the respective no_such_column_family error.
std::vector<sstring> parse_tables(const sstring& ks_name, http_context& ctx, const std::unordered_map<sstring, sstring>& query_params, sstring param_name);

void set_storage_service(http_context& ctx, routes& r, sharded<service::storage_service>& ss, sharded<gms::gossiper>& g, sharded<cdc::generation_service>& cdc_gs, sharded<db::system_keyspace>& sys_ls);
void set_sstables_loader(http_context& ctx, routes& r, sharded<sstables_loader>& sst_loader);
void unset_sstables_loader(http_context& ctx, routes& r);
void set_view_builder(http_context& ctx, routes& r, sharded<db::view::view_builder>& vb);
void unset_view_builder(http_context& ctx, routes& r);
void set_repair(http_context& ctx, routes& r, sharded<repair_service>& repair);
void unset_repair(http_context& ctx, routes& r);
void set_transport_controller(http_context& ctx, routes& r, cql_transport::controller& ctl);
void unset_transport_controller(http_context& ctx, routes& r);
void set_rpc_controller(http_context& ctx, routes& r, thrift_controller& ctl);
void unset_rpc_controller(http_context& ctx, routes& r);
void set_snapshot(http_context& ctx, routes& r, sharded<db::snapshot_ctl>& snap_ctl);
void unset_snapshot(http_context& ctx, routes& r);
seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, http_context &ctx, bool legacy_request = false);

}
