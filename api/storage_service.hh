/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/sharded.hh>
#include <seastar/json/json_elements.hh>
#include "api/api_init.hh"
#include "db/data_listeners.hh"
#include "db/snapshot-ctl.hh"
#include "compaction/compaction_descriptor.hh"
#include "gms/gossip_address_map.hh"

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

struct scrub_info {
    compaction::compaction_type_options::scrub opts;
    sstring keyspace;
    std::vector<sstring> column_families;
    sstring snapshot_tag;
};

// TTL is given as a number with an optional a suffix of:
// s - seconds (default)
// m - minutes
// h - hours
// d - days
std::optional<std::chrono::seconds> validate_ttl(const std::string& value);

void set_storage_service(http_context& ctx, httpd::routes& r, sharded<service::storage_service>& ss, sharded<db::snapshot_ctl>&, service::raft_group0_client&);
void unset_storage_service(http_context& ctx, httpd::routes& r);
void set_sstables_loader(http_context& ctx, httpd::routes& r, sharded<sstables_loader>& sst_loader);
void unset_sstables_loader(http_context& ctx, httpd::routes& r);
void set_view_builder(http_context& ctx, httpd::routes& r, sharded<db::view::view_builder>& vb, sharded<gms::gossiper>& g);
void unset_view_builder(http_context& ctx, httpd::routes& r);
void set_repair(http_context& ctx, httpd::routes& r, sharded<repair_service>& repair, sharded<gms::gossip_address_map>& am);
void unset_repair(http_context& ctx, httpd::routes& r);
void set_transport_controller(http_context& ctx, httpd::routes& r, cql_transport::controller& ctl);
void unset_transport_controller(http_context& ctx, httpd::routes& r);
void set_thrift_controller(http_context& ctx, httpd::routes& r);
void unset_thrift_controller(http_context& ctx, httpd::routes& r);
void set_snapshot(http_context& ctx, httpd::routes& r, sharded<db::snapshot_ctl>& snap_ctl);
// Streams the response body of GET /storage_service/snapshots to `out`, closing it.
future<> get_snapshot_details(std::unordered_map<sstring, db::snapshot_ctl::db_snapshot_details> result, output_stream<char>&& out);
void unset_snapshot(http_context& ctx, httpd::routes& r);
void set_load_meter(http_context& ctx, httpd::routes& r, service::load_meter& lm);
void unset_load_meter(http_context& ctx, httpd::routes& r);
seastar::future<json::json_return_type> run_toppartitions_query(db::toppartitions_query& q, bool legacy_request = false);

} // namespace api
