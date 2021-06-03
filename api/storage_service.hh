/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/sharded.hh>
#include "api.hh"
#include "db/data_listeners.hh"

namespace cql_transport { class controller; }
class thrift_controller;
namespace db { class snapshot_ctl; }
namespace netw { class messaging_service; }
class repair_service;

namespace api {

void set_storage_service(http_context& ctx, routes& r);
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
