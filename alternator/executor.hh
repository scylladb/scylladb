/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/future.hh>
#include "audit/audit.hh"
#include "seastarx.hh"
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/noncopyable_function.hh>

#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "service_permit.hh"
#include "db/timeout_clock.hh"
#include "db/config.hh"

#include "alternator/error.hh"
#include "alternator/attribute_path.hh"
#include "alternator/stats.hh"
#include "alternator/executor_util.hh"

#include "utils/rjson.hh"
#include "utils/updateable_value.hh"

#include "tracing/trace_state.hh"
#include "cdc/cdc_options.hh"

namespace db {
    class system_distributed_keyspace;
    class system_keyspace;
}

namespace audit {
class audit_info_alternator;
}

namespace query {
class partition_slice;
class result;
}

namespace cql3::selection {
    class selection;
}

namespace service {
    class storage_proxy;
    class cas_shard;
    class storage_service;
}

namespace vector_search {
    class vector_store_client;
}

namespace cdc {
    class metadata;
}

namespace gms {

class gossiper;

}

class schema_builder;


namespace alternator {

enum class table_status;
class rmw_operation;
class put_or_delete_item;

namespace parsed {
class expression_cache;
}

class executor : public peering_sharded_service<executor> {
    gms::gossiper& _gossiper;
    service::storage_service& _ss;
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;
    db::system_distributed_keyspace& _sdks;
    db::system_keyspace& _system_keyspace;
    cdc::metadata& _cdc_metadata;
    vector_search::vector_store_client& _vsc;
    utils::updateable_value<bool> _enforce_authorization;
    utils::updateable_value<bool> _warn_authorization;
    seastar::sharded<audit::audit>& _audit;
    // An smp_service_group to be used for limiting the concurrency when
    // forwarding Alternator request between shards - if necessary for LWT.
    smp_service_group _ssg;

    std::unique_ptr<parsed::expression_cache> _parsed_expression_cache;

    struct describe_table_info_manager;
    std::unique_ptr<describe_table_info_manager> _describe_table_info_manager;

    future<> cache_newly_calculated_size_on_all_shards(schema_ptr schema, std::uint64_t size_in_bytes, std::chrono::nanoseconds ttl);
    future<> fill_table_size(rjson::value &table_description, schema_ptr schema, bool deleting);
public:
    using client_state = service::client_state;

    // Controls whether Alternator returns a routing-hint response header when
    // a request was sent to the "wrong" coordinator. The header, if returned,
    // tells the driver the token range of the partition and which nodes in the
    // local DC are the natural replicas for it.
    //
    // none        - no header is returned (default, current behavior)
    // node_aware  - return header if this node is not one of the RF replicas;
    //               works for both tablets and vnodes
    // shard_aware - return header if this shard is not the right shard;
    //               only meaningful for tablets (vnodes fall back to
    //               node_aware behavior)
    //
    // This is currently a compile-time constant. In the future it may be
    // configurable per port.
    enum class node_awareness {
        none,
        node_aware,
        shard_aware,
    };

    // request_return_type is the return type of the executor methods.
    // It holds one of three response payloads:
    // 1. A std::string, which is the response body for the request.
    // 2. A body_writer, an asynchronous function (returning future<>) that
    //    takes an output_stream and writes the response body into it.
    //    This is used for streaming large responses without needing to
    //    allocate a contiguous std::string for the entire response.
    // 3. An api_error, which is an error response returned to the client.
    // Additionally, extra_headers carries optional HTTP response headers
    // that will be added to the reply (e.g., ROUTING_HEADER_NAME).
    struct request_return_type {
        std::variant<std::string, body_writer, api_error> response;
        std::vector<std::pair<sstring, sstring>> extra_headers;
        // Implicit conversion constructors that make it easy for functions
        // that return a response (without extra headers) to just return a
        // std::string, body_writer or api_error.
        request_return_type(std::string s) : response(std::move(s)) {}
        request_return_type(body_writer bw) : response(std::move(bw)) {}
        request_return_type(api_error err) : response(std::move(err)) {}
        // Default constructor: response holds a default-constructed std::string
        // (the first variant alternative) and extra_headers is empty.
        // This matches the behavior of the old bare std::variant default constructor.
        request_return_type() = default;
    };

    // The current node-awareness mode. When not none, single-partition
    // operations include a routing-hint header in the response when the
    // request was sent to the wrong coordinator.
    static constexpr node_awareness routing_awareness = node_awareness::node_aware;

    // Name of the HTTP response header carrying the routing hint.
    static constexpr std::string_view ROUTING_HEADER_NAME = "X-Scylla-Alternator-Routing-V1";

    // If routing awareness is enabled and this node is not the right coordinator
    // for the given schema and token, adds the routing-hint header to ret.
    void add_routing_header_if_needed(request_return_type& ret, const schema_ptr& schema, const dht::token& token, const client_state& cs);

    stats _stats;
    // The metric_groups object holds this stat object's metrics registered
    // as long as the stats object is alive.
    seastar::metrics::metric_groups _metrics;
    static constexpr auto ATTRS_COLUMN_NAME = ":attrs";
    static constexpr auto KEYSPACE_NAME_PREFIX = "alternator_";
    static constexpr std::string_view INTERNAL_TABLE_PREFIX = ".scylla.alternator.";

    executor(gms::gossiper& gossiper,
             service::storage_proxy& proxy,
             service::storage_service& ss,
             service::migration_manager& mm,
             db::system_distributed_keyspace& sdks,
             db::system_keyspace& system_keyspace,
             cdc::metadata& cdc_metadata,
             vector_search::vector_store_client& vsc,
             smp_service_group ssg,
             utils::updateable_value<uint32_t> default_timeout_in_ms);
    ~executor();

    future<request_return_type> create_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> update_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> put_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> update_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> list_tables(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> scan(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> describe_endpoints(client_state& client_state, service_permit permit, rjson::value request, std::string host_header, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> query(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> tag_resource(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> untag_resource(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> list_tags_of_resource(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> update_time_to_live(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> describe_time_to_live(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> list_streams(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> describe_stream(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> get_shard_iterator(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> get_records(client_state& client_state, tracing::trace_state_ptr, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);
    future<request_return_type> describe_continuous_backups(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info);

    future<> start();
    future<> stop();

    static db::timeout_clock::time_point default_timeout();
private:
    static thread_local utils::updateable_value<uint32_t> s_default_timeout_in_ms;
    friend class rmw_operation;

    // Helper to set up auditing for an Alternator operation. Checks whether
    // the operation should be audited (via will_log()) and if so, allocates
    // and populates audit_info. No allocation occurs when auditing is disabled.
    void maybe_audit(std::unique_ptr<audit::audit_info_alternator>& audit_info,
                     audit::statement_category category,
                     std::string_view ks_name,
                     std::string_view table_name,
                     std::string_view operation_name,
                     const rjson::value& request,
                     std::optional<db::consistency_level> cl = std::nullopt,
                     std::optional<audit::audit_table_set> alternator_batch_tables = std::nullopt);

    future<rjson::value> fill_table_description(schema_ptr schema, table_status tbl_status, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit);
    future<executor::request_return_type> create_table_on_shard0(service::client_state&& client_state, tracing::trace_state_ptr trace_state, rjson::value request, bool enforce_authorization,
            bool warn_authorization, const db::tablets_mode_t::mode tablets_mode, std::unique_ptr<audit::audit_info_alternator>& audit_info);

    future<> do_batch_write(
        std::vector<std::pair<schema_ptr, put_or_delete_item>> mutation_builders,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit);

    future<> cas_write(schema_ptr schema, service::cas_shard cas_shard, const dht::decorated_key& dk,
        const std::vector<put_or_delete_item>& mutation_builders, service::client_state& client_state,
        tracing::trace_state_ptr trace_state, service_permit permit);

public:
    static bool add_stream_options(const rjson::value& stream_spec, schema_builder&, service::storage_proxy& sp, const cdc::options& existing_cdc_opts = {});
    static void supplement_table_info(rjson::value& descr, const schema& schema, service::storage_proxy& sp);
    static void supplement_table_stream_info(rjson::value& descr, const schema& schema, const service::storage_proxy& sp);
};

// returns table creation time in seconds since epoch for `db_clock`
double get_table_creation_time(const schema &schema);

// result of parsing ARN (Amazon Resource Name)
// ARN format is `arn:<partition>:<service>:<region>:<account-id>:<resource-type>/<resource-id>/<postfix>`
// we ignore partition, service and account-id
// resource-type must be string "table"
// resource-id will be returned as table_name
// region will be returned as keyspace_name
// postfix is a string after resource-id and will be returned as is (whole), including separator.
struct arn_parts {
    std::string_view keyspace_name;
    std::string_view table_name;
    std::string_view postfix;
};
// arn - arn to parse
// arn_field_name - identifier of the ARN, used only when reporting an error (in error messages), for example "Incorrect resource identifier `<arn_field_name>`"
// type_name - used only when reporting an error (in error messages), for example "... is not a valid <type_name> ARN ..."
// expected_postfix - optional filter of postfix value (part of ARN after resource-id, including separator, see comments for struct arn_parts).
//    If is empty - then postfix value must be empty as well
//    if not empty - postfix value must start with expected_postfix, but might be longer
arn_parts parse_arn(std::string_view arn, std::string_view arn_field_name, std::string_view type_name, std::string_view expected_postfix);

// The format is table1|table2|table3...
sstring print_names_for_audit(const audit::audit_table_set& names);
}
