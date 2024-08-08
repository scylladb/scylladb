/*
 * Copyright 2016 ScylaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

#include <seastar/http/httpd.hh>
#include <seastar/core/future.hh>

#include "replica/database_fwd.hh"
#include "tasks/task_manager.hh"
#include "seastarx.hh"

using request = http::request;
using reply = http::reply;

namespace service {

class load_meter;
class storage_proxy;
class storage_service;
class raft_group0_client;
class raft_group_registry;

} // namespace service

class sstables_loader;

namespace streaming {
class stream_manager;
}

namespace gms {
    class inet_address;
}

namespace locator {

class token_metadata;
class shared_token_metadata;
class snitch_ptr;

} // namespace locator

namespace cql_transport { class controller; }
namespace db {
class snapshot_ctl;
class config;
namespace view {
class view_builder;
}
class system_keyspace;
}
namespace netw { class messaging_service; }
class repair_service;

namespace gms {

class gossiper;

}

namespace auth { class service; }

namespace tasks {
class task_manager;
}

namespace api {

struct http_context {
    sstring api_dir;
    sstring api_doc;
    httpd::http_server_control http_server;
    distributed<replica::database>& db;

    http_context(distributed<replica::database>& _db)
            : db(_db)
    {
    }
};

future<> set_server_init(http_context& ctx);
future<> set_server_config(http_context& ctx, const db::config& cfg);
future<> unset_server_config(http_context& ctx);
future<> set_server_snitch(http_context& ctx, sharded<locator::snitch_ptr>& snitch);
future<> unset_server_snitch(http_context& ctx);
future<> set_server_storage_service(http_context& ctx, sharded<service::storage_service>& ss, service::raft_group0_client&);
future<> unset_server_storage_service(http_context& ctx);
future<> set_server_sstables_loader(http_context& ctx, sharded<sstables_loader>& sst_loader);
future<> unset_server_sstables_loader(http_context& ctx);
future<> set_server_view_builder(http_context& ctx, sharded<db::view::view_builder>& vb);
future<> unset_server_view_builder(http_context& ctx);
future<> set_server_repair(http_context& ctx, sharded<repair_service>& repair);
future<> unset_server_repair(http_context& ctx);
future<> set_transport_controller(http_context& ctx, cql_transport::controller& ctl);
future<> unset_transport_controller(http_context& ctx);
future<> set_thrift_controller(http_context& ctx);
future<> unset_thrift_controller(http_context& ctx);
future<> set_server_authorization_cache(http_context& ctx, sharded<auth::service> &auth_service);
future<> unset_server_authorization_cache(http_context& ctx);
future<> set_server_snapshot(http_context& ctx, sharded<db::snapshot_ctl>& snap_ctl);
future<> unset_server_snapshot(http_context& ctx);
future<> set_server_token_metadata(http_context& ctx, sharded<locator::shared_token_metadata>& tm);
future<> unset_server_token_metadata(http_context& ctx);
future<> set_server_gossip(http_context& ctx, sharded<gms::gossiper>& g);
future<> unset_server_gossip(http_context& ctx);
future<> set_server_column_family(http_context& ctx, sharded<db::system_keyspace>& sys_ks);
future<> unset_server_column_family(http_context& ctx);
future<> set_server_messaging_service(http_context& ctx, sharded<netw::messaging_service>& ms);
future<> unset_server_messaging_service(http_context& ctx);
future<> set_server_storage_proxy(http_context& ctx, sharded<service::storage_proxy>& proxy);
future<> unset_server_storage_proxy(http_context& ctx);
future<> set_server_stream_manager(http_context& ctx, sharded<streaming::stream_manager>& sm);
future<> unset_server_stream_manager(http_context& ctx);
future<> set_hinted_handoff(http_context& ctx, sharded<service::storage_proxy>& p);
future<> unset_hinted_handoff(http_context& ctx);
future<> set_server_cache(http_context& ctx);
future<> unset_server_cache(http_context& ctx);
future<> set_server_compaction_manager(http_context& ctx);
future<> set_server_done(http_context& ctx);
future<> set_server_task_manager(http_context& ctx, sharded<tasks::task_manager>& tm, lw_shared_ptr<db::config> cfg);
future<> unset_server_task_manager(http_context& ctx);
future<> set_server_task_manager_test(http_context& ctx, sharded<tasks::task_manager>& tm);
future<> unset_server_task_manager_test(http_context& ctx);
future<> set_server_tasks_compaction_module(http_context& ctx, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>& snap_ctl);
future<> unset_server_tasks_compaction_module(http_context& ctx);
future<> set_server_raft(http_context&, sharded<service::raft_group_registry>&);
future<> unset_server_raft(http_context&);
future<> set_load_meter(http_context& ctx, service::load_meter& lm);
future<> unset_load_meter(http_context& ctx);
future<> set_server_cql_server_test(http_context& ctx, cql_transport::controller& ctl);
future<> unset_server_cql_server_test(http_context& ctx);

}
