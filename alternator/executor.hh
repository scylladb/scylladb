/*
 * Copyright 2019 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include "seastarx.hh"
#include <seastar/json/json_elements.hh>
#include <seastar/core/sharded.hh>

#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "db/timeout_clock.hh"

#include "alternator/error.hh"
#include "stats.hh"
#include "utils/rjson.hh"

namespace db {
    class system_distributed_keyspace;
}

namespace query {
class partition_slice;
class result;
}

namespace cql3::selection {
    class selection;
}

namespace service {
    class storage_service;
}

namespace alternator {

class rmw_operation;

struct make_jsonable : public json::jsonable {
    rjson::value _value;
public:
    explicit make_jsonable(rjson::value&& value);
    std::string to_json() const override;
};
struct json_string : public json::jsonable {
    std::string _value;
public:
    explicit json_string(std::string&& value);
    std::string to_json() const override;
};

class executor : public peering_sharded_service<executor> {
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;
    db::system_distributed_keyspace& _sdks;
    service::storage_service& _ss;
    // An smp_service_group to be used for limiting the concurrency when
    // forwarding Alternator request between shards - if necessary for LWT.
    smp_service_group _ssg;

public:
    using client_state = service::client_state;
    using request_return_type = std::variant<json::json_return_type, api_error>;
    stats _stats;
    static constexpr auto ATTRS_COLUMN_NAME = ":attrs";
    static constexpr auto KEYSPACE_NAME_PREFIX = "alternator_";
    static constexpr std::string_view INTERNAL_TABLE_PREFIX = ".scylla.alternator.";

    executor(service::storage_proxy& proxy, service::migration_manager& mm, db::system_distributed_keyspace& sdks, service::storage_service& ss, smp_service_group ssg)
        : _proxy(proxy), _mm(mm), _sdks(sdks), _ss(ss), _ssg(ssg) {}

    future<request_return_type> create_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> update_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> put_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> update_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> list_tables(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> scan(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_endpoints(client_state& client_state, service_permit permit, rjson::value request, std::string host_header);
    future<request_return_type> batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> query(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request);
    future<request_return_type> tag_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> untag_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> list_tags_of_resource(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> list_streams(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> describe_stream(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> get_shard_iterator(client_state& client_state, service_permit permit, rjson::value request);
    future<request_return_type> get_records(client_state& client_state, tracing::trace_state_ptr, service_permit permit, rjson::value request);

    future<> start();
    future<> stop() { return make_ready_future<>(); }

    future<> create_keyspace(std::string_view keyspace_name);

    static tracing::trace_state_ptr maybe_trace_query(client_state& client_state, sstring_view op, sstring_view query);

    static sstring table_name(const schema&);
    static db::timeout_clock::time_point default_timeout();
    static schema_ptr find_table(service::storage_proxy&, const rjson::value& request);

private:
    friend class rmw_operation;

    static bool is_alternator_keyspace(const sstring& ks_name);
    static sstring make_keyspace_name(const sstring& table_name);
    static void describe_key_schema(rjson::value& parent, const schema&, std::unordered_map<std::string,std::string> * = nullptr);
    static void describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string,std::string>&);
    
public:    
    static std::optional<rjson::value> describe_single_item(schema_ptr,
        const query::partition_slice&,
        const cql3::selection::selection&,
        const query::result&,
        const std::unordered_set<std::string>&);

    static void describe_single_item(const cql3::selection::selection&,
        const std::vector<bytes_opt>&,
        const std::unordered_set<std::string>&,
        rjson::value&,
        bool = false);



    void add_stream_options(const rjson::value& stream_spec, schema_builder&) const;
    void supplement_table_info(rjson::value& descr, const schema& schema) const;
    void supplement_table_stream_info(rjson::value& descr, const schema& schema) const;
};

}
