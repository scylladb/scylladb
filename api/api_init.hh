/*
 * Copyright 2016 ScylaDB
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

#include <seastar/http/httpd.hh>
#include <seastar/core/future.hh>

#include "database_fwd.hh"
#include "seastarx.hh"

namespace service {

class load_meter;
class storage_proxy;

} // namespace service

namespace locator {

class token_metadata;
class shared_token_metadata;

} // namespace locator

namespace cql_transport { class controller; }
class thrift_controller;
namespace db { class snapshot_ctl; }
namespace netw { class messaging_service; }
class repair_service;

namespace api {

struct http_context {
    sstring api_dir;
    sstring api_doc;
    httpd::http_server_control http_server;
    distributed<database>& db;
    distributed<service::storage_proxy>& sp;
    service::load_meter& lmeter;
    const sharded<locator::shared_token_metadata>& shared_token_metadata;

    http_context(distributed<database>& _db,
            distributed<service::storage_proxy>& _sp,
            service::load_meter& _lm, const sharded<locator::shared_token_metadata>& _stm)
            : db(_db), sp(_sp), lmeter(_lm), shared_token_metadata(_stm) {
    }

    const locator::token_metadata& get_token_metadata();
};

future<> set_server_init(http_context& ctx);
future<> set_server_config(http_context& ctx);
future<> set_server_snitch(http_context& ctx);
future<> set_server_storage_service(http_context& ctx);
future<> set_server_repair(http_context& ctx, sharded<repair_service>& repair);
future<> unset_server_repair(http_context& ctx);
future<> set_transport_controller(http_context& ctx, cql_transport::controller& ctl);
future<> unset_transport_controller(http_context& ctx);
future<> set_rpc_controller(http_context& ctx, thrift_controller& ctl);
future<> unset_rpc_controller(http_context& ctx);
future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl);
future<> unset_server_snapshot(http_context& ctx);
future<> set_server_gossip(http_context& ctx);
future<> set_server_load_sstable(http_context& ctx);
future<> set_server_messaging_service(http_context& ctx, sharded<netw::messaging_service>& ms);
future<> unset_server_messaging_service(http_context& ctx);
future<> set_server_storage_proxy(http_context& ctx);
future<> set_server_stream_manager(http_context& ctx);
future<> set_hinted_handoff(http_context& ctx);
future<> unset_hinted_handoff(http_context& ctx);
future<> set_server_gossip_settle(http_context& ctx);
future<> set_server_cache(http_context& ctx);
future<> set_server_done(http_context& ctx);

}
