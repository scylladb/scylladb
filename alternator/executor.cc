/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <fmt/ranges.h>
#include <seastar/core/on_internal_error.hh>
#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "alternator/consumed_capacity.hh"
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "cdc/log.hh"
#include "cdc/cdc_options.hh"
#include "cdc/cdc_extension.hh"
#include "auth/service.hh"
#include "cql3/cql3_type.hh"
#include "db/config.hh"
#include "db/view/view_build_status.hh"
#include "locator/tablets.hh"
#include "mutation/tombstone.hh"
#include "locator/abstract_replication_strategy.hh"
#include "utils/log.hh"
#include "schema/schema_builder.hh"
#include "exceptions/exceptions.hh"
#include "service/client_state.hh"
#include "mutation/timestamp.hh"
#include "types/map.hh"
#include "schema/schema.hh"
#include "query/query-request.hh"
#include "query/query-result-reader.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "bytes.hh"
#include "service/pager/query_pagers.hh"
#include <functional>
#include "error.hh"
#include "serialization.hh"
#include "expressions.hh"
#include "conditions.hh"
#include <optional>
#include "utils/assert.hh"
#include "utils/overloaded_functor.hh"
#include "mutation/collection_mutation.hh"
#include "schema/schema.hh"
#include "db/tags/extension.hh"
#include "db/tags/utils.hh"
#include "replica/database.hh"
#include "alternator/rmw_operation.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/loop.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <boost/range/algorithm/find_end.hpp>
#include <unordered_set>
#include "service/storage_proxy.hh"
#include "gms/feature_service.hh"
#include "gms/gossiper.hh"
#include "utils/error_injection.hh"
#include "db/schema_tables.hh"
#include "utils/rjson.hh"
#include "alternator/extract_from_attrs.hh"
#include "types/types.hh"
#include "db/system_keyspace.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "cql3/statements/index_target.hh"
#include "index/secondary_index.hh"
#include "alternator/ttl_tag.hh"
#include "vector_search/vector_store_client.hh"
#include "utils/simple_value_with_expiry.hh"

using namespace std::chrono_literals;

namespace std {
    template <> struct hash<std::pair<sstring, sstring>> {
        size_t operator () (const std::pair<sstring, sstring>& p) const {
            return std::hash<sstring>()(p.first) * 1009 + std::hash<sstring>()(p.second) * 3;
        }
    };
}

namespace alternator {

logging::logger elogger("alternator-executor");

// Alternator-specific table properties stored as hidden table tags:
//
// Alternator doesn't keep its own records of which Alternator tables exist
// or how each one was configured. Instead, an Alternator table is created
// as a CQL table - and that CQL table stores its own name, schema, views,
// and so on. However, there are some Alternator-specific properties of a
// table which are not part of a CQL schema but which we need to remember.
// We store those extra properties as hidden "tags" on the CQL table, using
// the schema's "tags extension". This extension provides a map<string,string>
// for the table that is stored persistently on disk, but also readable
// quickly from memory.
// Alternator also uses the tags extension to store user-defined tags on
// tables (the TagResource, UntagResource and ListTagsOfResource requests).
// So the internal tags are kept hidden from the user by using the prefix
// "system:" in their name (see tag_key_is_internal()).
// The following is the list of these Alternator-specific hidden tags that
// Alternator adds to tables:
//
// Tags storing the "ReadCapacityUnits" and "WriteCapacityUnits"
// configured for a table with BillingMode=PROVISIONED.
const sstring RCU_TAG_KEY("system:provisioned_rcu");
const sstring WCU_TAG_KEY("system:provisioned_wcu");
// Tag storing the table's original creation time, in milliseconds since the
// Unix epoch. All tables get this tag when they are created, but it may be
// missing in old tables created before this tag was introduced.
const sstring TABLE_CREATION_TIME_TAG_KEY("system:table_creation_time");
// If this tag is present, it stores the name of the attribute that was
// configured by UpdateTimeToLive to be the expiration-time attribute for
// this table.
extern const sstring TTL_TAG_KEY("system:ttl_attribute");
// This will be set to 1 in a case, where user DID NOT specify a range key.
// The way GSI / LSI is implemented by Alternator assumes user specified keys will come first
// in materialized view's key list. Then, if needed missing keys are added (current implementation
// of materialized views requires that all base hash / range keys were added to the view as well).
// Alternator allows only a single range key attribute to be specified by the user. So if
// the SPURIOUS_RANGE_KEY_ADDED_TO_GSI_AND_USER_DIDNT_SPECIFY_RANGE_KEY_TAG_KEY is set the user didn't specify any key and
// base table's keys were added as range keys. In all other cases either the first key is the user specified key,
// following ones are base table's keys added as needed or range key list will be empty.
extern const sstring SPURIOUS_RANGE_KEY_ADDED_TO_GSI_AND_USER_DIDNT_SPECIFY_RANGE_KEY_TAG_KEY("system:spurious_range_key_added_to_gsi_and_user_didnt_specify_range_key");

// The following tags also have the "system:" prefix but are NOT used
// by Alternator to store table properties - only the user ever writes to
// them, as a way to configure the table. As such, these tags are writable
// (and readable) by the user, and not hidden by tag_key_is_internal().
// The reason why both hidden (internal) and user-configurable tags share the
// same "system:" prefix is historic.

// Setting the tag with a numeric value will enable a specific initial number
// of tablets (setting the value to 0 means enabling tablets with
// an automatic selection of the best number of tablets).
// Setting this tag to any non-numeric value (e.g., an empty string or the
// word "none") will ask to disable tablets.
static constexpr auto INITIAL_TABLETS_TAG_KEY = "system:initial_tablets";


enum class table_status {
    active = 0,
    creating,
    updating,
    deleting
};

static std::string_view table_status_to_sstring(table_status tbl_status) {
    switch (tbl_status) {
        case table_status::active:
            return "ACTIVE";
        case table_status::creating:
            return "CREATING";
        case table_status::updating:
            return "UPDATING";
        case table_status::deleting:
            return "DELETING";
    }
    return "UNKNOWN";
}

void executor::maybe_audit(
    std::unique_ptr<audit::audit_info_alternator>& audit_info,
    audit::statement_category category,
    std::string_view ks_name,
    std::string_view table_name,
    std::string_view operation_name,
    const rjson::value& request,
    std::optional<db::consistency_level> cl)
{
    if (_audit.local_is_initialized() && _audit.local().will_log(category, ks_name, table_name)) {
        audit_info = std::make_unique<audit::audit_info_alternator>(
            category, sstring(ks_name), sstring(table_name), cl);
        // FIXME: rjson::print(request) serializes the entire JSON request body, which
        // can be up to 16 MB for BatchWriteItem.
        audit_info->set_query_string(sstring(rjson::print(request)), sstring(operation_name));
    }
}

static lw_shared_ptr<keyspace_metadata> create_keyspace_metadata(std::string_view keyspace_name, service::storage_proxy& sp, gms::gossiper& gossiper, api::timestamp_type,
        const std::map<sstring, sstring>& tags_map, const gms::feature_service& feat, const db::tablets_mode_t::mode tablets_mode);

static const column_definition& attrs_column(const schema& schema) {
    const column_definition* cdef = schema.get_column_definition(bytes(executor::ATTRS_COLUMN_NAME));
    throwing_assert(cdef);
    return *cdef;
}

// This function throws api_error::validation if input value is not an object.
static void validate_is_object(const rjson::value& value, const char* caller) {
    if (!value.IsObject()) {
        throw api_error::validation(fmt::format("{} must be an object", caller));
    }
}

// This function assumes the given value is an object and returns requested member value.
// If it is not possible, an api_error::validation is thrown.
static const rjson::value& get_member(const rjson::value& obj, const char* member_name, const char* caller) {
    validate_is_object(obj, caller);
    const rjson::value* ret = rjson::find(obj, member_name);
    if (!ret) {
       throw api_error::validation(fmt::format("{} is missing a mandatory member {}", caller, member_name));
    }
    return *ret;
}


// This function assumes the given value is an object with a single member, and returns this member.
// In case the requirements are not met, an api_error::validation is thrown.
static const rjson::value::Member& get_single_member(const rjson::value& v, const char* caller) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error::validation(format("{}: expected an object with a single member.", caller));
    }
    return *(v.MemberBegin());
}

class executor::describe_table_info_manager : public service::migration_listener::empty_listener {
    executor &_executor;

    struct table_info {
        utils::simple_value_with_expiry<std::uint64_t> size_in_bytes;
    };
    std::unordered_map<std::pair<sstring, sstring>, table_info> info_for_tables;
    bool active = false;

public:
    describe_table_info_manager(executor& executor) : _executor(executor) {
        _executor._proxy.data_dictionary().real_database_ptr()->get_notifier().register_listener(this);
        active = true;
    }
    describe_table_info_manager(const describe_table_info_manager &) = delete;
    describe_table_info_manager(describe_table_info_manager&&) = delete;
    ~describe_table_info_manager() {
        if (active) {
            on_fatal_internal_error(elogger, "describe_table_info_manager was not stopped before destruction");
        }
    }

    describe_table_info_manager &operator = (const describe_table_info_manager &) = delete;
    describe_table_info_manager &operator = (describe_table_info_manager&&) = delete;

    static std::chrono::high_resolution_clock::time_point now() {
        return std::chrono::high_resolution_clock::now();
    }

    std::optional<std::uint64_t> get_cached_size_in_bytes(const sstring &ks_name, const sstring &cf_name) const {
        auto it = info_for_tables.find({ks_name, cf_name});
        if (it != info_for_tables.end()) {
            return it->second.size_in_bytes.get();
        }
        return std::nullopt;
    }
    void cache_size_in_bytes(sstring ks_name, sstring cf_name, std::uint64_t size_in_bytes, std::chrono::high_resolution_clock::time_point expiry) {
        info_for_tables[{std::move(ks_name), std::move(cf_name)}].size_in_bytes.set_if_longer_expiry(size_in_bytes, expiry);
    }
    future<> stop() {
        co_await _executor._proxy.data_dictionary().real_database_ptr()->get_notifier().unregister_listener(this);
        active = false;
        co_return;
    }
    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        if (!ks_name.starts_with(executor::KEYSPACE_NAME_PREFIX)) return;
        info_for_tables.erase({ks_name, cf_name});
    }
};

executor::executor(gms::gossiper& gossiper,
         service::storage_proxy& proxy,
         service::storage_service& ss,
         service::migration_manager& mm,
         db::system_distributed_keyspace& sdks,
         db::system_keyspace& system_keyspace,
         cdc::metadata& cdc_metadata,
         vector_search::vector_store_client& vsc,
         smp_service_group ssg,
         utils::updateable_value<uint32_t> default_timeout_in_ms)
    : _gossiper(gossiper),
      _ss(ss),
      _proxy(proxy),
      _mm(mm),
      _sdks(sdks),
      _system_keyspace(system_keyspace),
      _cdc_metadata(cdc_metadata),
      _vsc(vsc),
      _enforce_authorization(_proxy.data_dictionary().get_config().alternator_enforce_authorization),
      _warn_authorization(_proxy.data_dictionary().get_config().alternator_warn_authorization),
      _audit(audit::audit::audit_instance()),
      _ssg(ssg),
      _parsed_expression_cache(std::make_unique<parsed::expression_cache>(
        parsed::expression_cache::config{_proxy.data_dictionary().get_config().alternator_max_expression_cache_entries_per_shard},
        _stats))
{
    s_default_timeout_in_ms = std::move(default_timeout_in_ms);
    _describe_table_info_manager = std::make_unique<describe_table_info_manager>(*this);
    register_metrics(_metrics, _stats);
}

executor::~executor() = default;

static void set_table_creation_time(std::map<sstring, sstring>& tags_map, db_clock::time_point creation_time) {
    auto tm = std::chrono::duration_cast<std::chrono::milliseconds>(creation_time.time_since_epoch()).count();
    tags_map[TABLE_CREATION_TIME_TAG_KEY] = std::to_string(tm);
}

double get_table_creation_time(const schema &schema) {
    auto time = db::find_tag(schema, TABLE_CREATION_TIME_TAG_KEY);
    if (time) {
        try {
            auto val = std::stoll(*time);
            if (val > 0) {
                return val / 1000.0;
            }
        }
        catch(...) {}
    }
    return 0.0;
}

void executor::supplement_table_info(rjson::value& descr, const schema& schema, service::storage_proxy& sp) {
    auto creation_time = get_table_creation_time(schema);

    rjson::add(descr, "CreationDateTime", rjson::value(creation_time));
    rjson::add(descr, "TableStatus", "ACTIVE");
    rjson::add(descr, "TableId", rjson::from_string(schema.id().to_sstring()));

    executor::supplement_table_stream_info(descr, schema, sp);
}

// get_table_for_write() is similar to get_table(), but additionally, if the
// configuration allows this, may also allow writing to system table with
// prefix INTERNAL_TABLE_PREFIX. See also get_table_or_view() in
// executor_read.cc which allows *reading* internal tables by the Query
// operation.
static schema_ptr get_table_for_write(service::storage_proxy& proxy, const rjson::value& request) {
    std::string table_name = get_table_name(request);
    if (schema_ptr s = try_get_internal_table(proxy.data_dictionary(), table_name)) {
        if (!proxy.data_dictionary().get_config().alternator_allow_system_table_write()) {
            throw api_error::resource_not_found(fmt::format(
                "Table {} is an internal table, and writing to it is forbidden"
                " by the alternator_allow_system_table_write configuration",
                table_name));
        }
        return s;
    }
    return find_table(proxy, table_name);
}

static rjson::value generate_arn_for_table(const schema& schema) {
    return rjson::from_string(format("arn:scylla:alternator:{}:scylla:table/{}", schema.ks_name(), schema.cf_name()));
}

static rjson::value generate_arn_for_index(const schema& schema, std::string_view index_name) {
    return rjson::from_string(fmt::format(
        "arn:scylla:alternator:{}:scylla:table/{}/index/{}",
        schema.ks_name(), schema.cf_name(), index_name));
}

// The following function checks if a given view has finished building.
// We need this for describe_table() to know if a view is still backfilling,
// or active.
//
// Currently we don't have in view_ptr the knowledge whether a view finished
// building long ago - so checking this involves a somewhat inefficient, but
// still node-local, process:
// We need a table that can accurately tell that all nodes have finished
// building this view. system.built_views is not good enough because it only
// knows the view building status in the current node. In recent versions,
// after PR #19745, we have a local table system.view_build_status_v2 with
// global information, replacing the old system_distributed.view_build_status.
// In theory, there can be a period during upgrading an old cluster when this
// table is not yet available. However, since the IndexStatus is a new feature
// too, it is acceptable that it doesn't yet work in the middle of the update.
static future<bool> is_view_built(
        view_ptr view,
        service::storage_proxy& proxy,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit) {
    auto schema = proxy.data_dictionary().find_table(
        "system", db::system_keyspace::VIEW_BUILD_STATUS_V2).schema();
    // The table system.view_build_status_v2 has "keyspace_name" and
    // "view_name" as the partition key, and each clustering row has
    // "host_id" as clustering key and a string "status". We need to
    // read a single partition:
    partition_key pk = partition_key::from_exploded(*schema,
        {utf8_type->decompose(view->ks_name()),
         utf8_type->decompose(view->cf_name())});
    dht::partition_range_vector partition_ranges{
        dht::partition_range(dht::decorate_key(*schema, pk))};
    auto selection = cql3::selection::selection::wildcard(schema); // only for get_query_options()!
    auto partition_slice = query::partition_slice(
        {query::clustering_range::make_open_ended_both_sides()},
        {}, // static columns
        {schema->get_column_definition("status")->id}, // regular columns
        selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(
        schema->id(), schema->version(), partition_slice,
        proxy.get_max_result_size(partition_slice),
        query::tombstone_limit(proxy.get_tombstone_limit()));
    service::storage_proxy::coordinator_query_result qr =
        co_await proxy.query(
            schema, std::move(command), std::move(partition_ranges),
            db::consistency_level::LOCAL_ONE,
            service::storage_proxy::coordinator_query_options(
                executor::default_timeout(), std::move(permit), client_state, trace_state));
    query::result_set rs = query::result_set::from_raw_result(
        schema, partition_slice, *qr.query_result);
    std::unordered_map<locator::host_id, sstring> statuses;
    for (auto&& r : rs.rows()) {
        auto host_id = r.get<utils::UUID>("host_id");
        auto status = r.get<sstring>("status");
        if (host_id && status) {
            statuses.emplace(locator::host_id(*host_id), *status);
        }
    }
    // A view is considered "built" if all nodes reported SUCCESS in having
    // built this view. Note that we need this "SUCCESS" for all nodes in the
    // cluster - even those that are temporarily down (their success is known
    // by this node, even if they are down). Conversely, we don't care what is
    // the recorded status for any node which is no longer in the cluster - it
    // is possible we forgot to erase the status of nodes that left the
    // cluster, but here we just ignore them and look at the nodes actually
    // in the topology.
    bool all_built = true;
    auto token_metadata = proxy.get_token_metadata_ptr();
    token_metadata->get_topology().for_each_node(
        [&] (const locator::node& node) {
            // Note: we could skip nodes in DCs which have no replication of
            // this view. However, in practice even those nodes would run
            // the view building (and just see empty content) so we don't
            // need to bother with this skipping.
            auto it = statuses.find(node.host_id());
            if (it == statuses.end() || it->second != "SUCCESS") {
                all_built = false;
            }
        });
    co_return all_built;

}

future<> executor::cache_newly_calculated_size_on_all_shards(schema_ptr schema, std::uint64_t size_in_bytes, std::chrono::nanoseconds ttl) {
    auto expiry = describe_table_info_manager::now() + ttl;
    return container().invoke_on_all(
        [schema, size_in_bytes, expiry] (executor& exec) {
            exec._describe_table_info_manager->cache_size_in_bytes(schema->ks_name(), schema->cf_name(), size_in_bytes, expiry);
        });
}

future<> executor::fill_table_size(rjson::value &table_description, schema_ptr schema, bool deleting) {
    auto cached_size = _describe_table_info_manager->get_cached_size_in_bytes(schema->ks_name(), schema->cf_name());
    std::uint64_t total_size = 0;
    if (cached_size) {
        total_size = *cached_size;
    } else {
        // there's no point in trying to estimate value of table that is being deleted, as other nodes more often than not might
        // move forward with deletion faster than we calculate the size
        if (!deleting) {
            total_size = co_await _ss.estimate_total_sstable_volume(schema->id(), service::storage_service::ignore_errors::yes);
            const auto expiry = std::chrono::seconds{ _proxy.data_dictionary().get_config().alternator_describe_table_info_cache_validity_in_seconds() };
            // Note: we don't care when the notification of other shards will finish, as long as it will be done
            // it's possible to get into race condition (next DescribeTable comes to other shard, that new shard doesn't have
            // the size yet, so it will calculate it again) - this is not a problem, because it will call cache_newly_calculated_size_on_all_shards
            // with expiry, which is extremely unlikely to be exactly the same as the previous one, all shards will keep the size coming with expiry that is further into the future.
            // In case of the same expiry, some shards will have different size, which means DescribeTable will return different values depending on the shard
            // which is also fine, as the specification doesn't give precision guarantees of any kind.
            co_await cache_newly_calculated_size_on_all_shards(schema, total_size, expiry);
        }
    }
    rjson::add(table_description, "TableSizeBytes", total_size);
}

future<rjson::value> executor::fill_table_description(schema_ptr schema, table_status tbl_status, service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit)
{
    rjson::value table_description = rjson::empty_object();
    auto tags_ptr = db::get_tags_of_table(schema);

    rjson::add(table_description, "TableName", rjson::from_string(schema->cf_name()));
    co_await fill_table_size(table_description, schema, tbl_status == table_status::deleting);

    auto creation_timestamp = get_table_creation_time(*schema);

    // FIXME: In DynamoDB the CreateTable implementation is asynchronous, and
    // the table may be in "Creating" state until creating is finished.
    // We don't currently do this in Alternator - instead CreateTable waits
    // until the table is really available. So/ DescribeTable returns either
    // ACTIVE or doesn't exist at all (and DescribeTable returns an error).
    // The states CREATING and UPDATING are not currently returned.
    rjson::add(table_description, "TableStatus", rjson::from_string(table_status_to_sstring(tbl_status)));
    rjson::add(table_description, "TableArn", generate_arn_for_table(*schema));
    rjson::add(table_description, "TableId", rjson::from_string(schema->id().to_sstring()));
    rjson::add(table_description, "BillingModeSummary", rjson::empty_object());
    rjson::add(table_description["BillingModeSummary"], "LastUpdateToPayPerRequestDateTime", rjson::value(creation_timestamp));
    // In PAY_PER_REQUEST billing mode, provisioned capacity should return 0
    int rcu = 0;
    int wcu = 0;
    bool is_pay_per_request = true;

    if (tags_ptr) {
        auto rcu_tag = tags_ptr->find(RCU_TAG_KEY);
        auto wcu_tag = tags_ptr->find(WCU_TAG_KEY);
        if (rcu_tag != tags_ptr->end() && wcu_tag != tags_ptr->end()) {
            try {
                rcu = std::stoi(rcu_tag->second);
                wcu = std::stoi(wcu_tag->second);
                is_pay_per_request = false;
            } catch (...) {
                rcu = 0;
                wcu = 0;
            }
        }
    }
    if (is_pay_per_request) {
        rjson::add(table_description["BillingModeSummary"], "BillingMode", "PAY_PER_REQUEST");
    } else {
        rjson::add(table_description["BillingModeSummary"], "BillingMode", "PROVISIONED");
    }
    rjson::add(table_description, "ProvisionedThroughput", rjson::empty_object());
    rjson::add(table_description["ProvisionedThroughput"], "ReadCapacityUnits", rcu);
    rjson::add(table_description["ProvisionedThroughput"], "WriteCapacityUnits", wcu);
    rjson::add(table_description["ProvisionedThroughput"], "NumberOfDecreasesToday", 0);

    data_dictionary::table t = _proxy.data_dictionary().find_column_family(schema);

    if (tbl_status != table_status::deleting) {
        rjson::add(table_description, "CreationDateTime", rjson::value(creation_timestamp));
        std::unordered_map<std::string,std::string> key_attribute_types;
        // Add base table's KeySchema and collect types for AttributeDefinitions:
        describe_key_schema(table_description, *schema, &key_attribute_types, tags_ptr);
        if (!t.views().empty()) {
            rjson::value gsi_array = rjson::empty_array();
            rjson::value lsi_array = rjson::empty_array();
            for (const view_ptr& vptr : t.views()) {
                rjson::value view_entry = rjson::empty_object();
                const sstring& cf_name = vptr->cf_name();
                size_t delim_it = cf_name.find(':');
                if (delim_it == sstring::npos) {
                    elogger.error("Invalid internal index table name: {}", cf_name);
                    continue;
                }
                sstring index_name = cf_name.substr(delim_it + 1);
                rjson::add(view_entry, "IndexName", rjson::from_string(index_name));
                rjson::add(view_entry, "IndexArn", generate_arn_for_index(*schema, index_name));
                // Add index's KeySchema and collect types for AttributeDefinitions:
                describe_key_schema(view_entry, *vptr, &key_attribute_types, db::get_tags_of_table(vptr));
                // Add projection type
                rjson::value projection = rjson::empty_object();
                rjson::add(projection, "ProjectionType", "ALL");
                // FIXME: we have to get ProjectionType from the schema when it is added
                rjson::add(view_entry, "Projection", std::move(projection));
                // Local secondary indexes are marked by an extra '!' sign occurring before the ':' delimiter
                bool is_lsi = (delim_it > 1 && cf_name[delim_it-1] == '!');
                // Add IndexStatus and Backfilling flags, but only for GSIs -
                // LSIs can only be created with the table itself and do not
                // have a status. Alternator schema operations are synchronous
                // so only two combinations of these flags are possible: ACTIVE
                // (for a built view) or CREATING+Backfilling (if view building
                // is in progress).
                if (!is_lsi) {
                    if (co_await is_view_built(vptr, _proxy, client_state, trace_state, permit)) {
                        rjson::add(view_entry, "IndexStatus", "ACTIVE");
                    } else {
                        rjson::add(view_entry, "IndexStatus", "CREATING");
                        rjson::add(view_entry, "Backfilling", rjson::value(true));
                    }
                }
                rjson::value& index_array = is_lsi ? lsi_array : gsi_array;
                rjson::push_back(index_array, std::move(view_entry));
            }
            if (!lsi_array.Empty()) {
                rjson::add(table_description, "LocalSecondaryIndexes", std::move(lsi_array));
            }
            if (!gsi_array.Empty()) {
                rjson::add(table_description, "GlobalSecondaryIndexes", std::move(gsi_array));
            }
        }
        // List vector indexes, if this table has any:
        rjson::value vector_index_array = rjson::empty_array();
        abort_on_expiry vector_index_status_aoe(executor::default_timeout());
        for (const index_metadata& im : schema->indices()) {
            const auto& opts = im.options();
            auto class_it = opts.find(db::index::secondary_index::custom_class_option_name);
            if (class_it == opts.end() || class_it->second != "vector_index") {
                continue;
            }
            rjson::value entry = rjson::empty_object();
            rjson::add(entry, "IndexName", rjson::from_string(im.name()));
            rjson::value vector_attribute = rjson::empty_object();
            auto target_it = opts.find(cql3::statements::index_target::target_option_name);
            if (target_it != opts.end()) {
                rjson::add(vector_attribute, "AttributeName", rjson::from_string(target_it->second));
            }
            auto dims_it = opts.find("dimensions");
            if (dims_it != opts.end()) {
                try {
                    rjson::add(vector_attribute, "Dimensions", std::stoi(dims_it->second));
                } catch (const std::logic_error&) {
                    // This should never happen, because the dimensions option
                    // is validated on index creation
                    on_internal_error(elogger, fmt::format("Unexpected non-integer dimensions value '{}' for vector index '{}'", dims_it->second, im.name()));
                }
            }
            rjson::add(entry, "VectorAttribute", std::move(vector_attribute));
            // Always return a Projection. Currently only KEYS_ONLY is
            // supported, so we always return that.
            rjson::value projection = rjson::empty_object();
            rjson::add(projection, "ProjectionType", "KEYS_ONLY");
            rjson::add(entry, "Projection", std::move(projection));
            // Report IndexStatus and Backfilling based on the vector store's
            // reported state: SERVING -> ACTIVE, BOOTSTRAPPING -> CREATING+Backfilling,
            // anything else (INITIALIZING, unreachable, etc.) -> CREATING.
            auto vstatus = co_await _vsc.get_index_status(
                    schema->ks_name(), im.name(), vector_index_status_aoe.abort_source());
            using index_status = vector_search::vector_store_client::index_status;
            if (vstatus == index_status::serving) {
                rjson::add(entry, "IndexStatus", "ACTIVE");
            } else {
                rjson::add(entry, "IndexStatus", "CREATING");
                if (vstatus == index_status::backfilling) {
                    rjson::add(entry, "Backfilling", rjson::value(true));
                }
            }
            rjson::push_back(vector_index_array, std::move(entry));
        }
        if (!vector_index_array.Empty()) {
            rjson::add(table_description, "VectorIndexes", std::move(vector_index_array));
        }

        // Use map built by describe_key_schema() for base and indexes to produce
        // AttributeDefinitions for all key columns:
        rjson::value attribute_definitions = rjson::empty_array();
        for (auto& type : key_attribute_types) {
            rjson::value key = rjson::empty_object();
            rjson::add(key, "AttributeName", rjson::from_string(type.first));
            rjson::add(key, "AttributeType", rjson::from_string(type.second));
            rjson::push_back(attribute_definitions, std::move(key));
        }
        rjson::add(table_description, "AttributeDefinitions", std::move(attribute_definitions));
    }
    executor::supplement_table_stream_info(table_description, *schema, _proxy);

    co_return table_description;
}

future<executor::request_return_type> executor::describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_table++;
    elogger.trace("Describing table {}", request);

    schema_ptr schema = get_table(_proxy, request);

    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(), schema->cf_name(), "DescribeTable", request);

    get_stats_from_schema(_proxy, *schema)->api_operations.describe_table++;
    tracing::add_alternator_table_name(trace_state, schema->cf_name());

    rjson::value table_description = co_await fill_table_description(schema, table_status::active, client_state, trace_state, permit);
    rjson::value response = rjson::empty_object();
    rjson::add(response, "Table", std::move(table_description));
    elogger.trace("returning {}", response);
    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.delete_table++;
    elogger.trace("Deleting table {}", request);

    std::string table_name = get_table_name(request);
    std::string keyspace_name = executor::KEYSPACE_NAME_PREFIX + table_name;

    maybe_audit(audit_info, audit::statement_category::DDL, keyspace_name, table_name, "DeleteTable", request);

    tracing::add_alternator_table_name(trace_state, table_name);
    auto& p = _proxy.container();

    schema_ptr schema = get_table(_proxy, request);
    rjson::value table_description = co_await fill_table_description(schema, table_status::deleting, client_state, trace_state, permit);
    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::DROP, _stats);
    co_await _mm.container().invoke_on(0, [&, cs = client_state.move_to_other_shard()] (service::migration_manager& mm) -> future<> {
        size_t retries = mm.get_concurrent_ddl_retries();
        for (;;) {
            auto group0_guard = co_await mm.start_group0_operation();

            std::optional<data_dictionary::table> tbl = p.local().data_dictionary().try_find_table(keyspace_name, table_name);
            if (!tbl) {
                // DynamoDB returns validation error even when table does not exist
                // and the table name is invalid.
                validate_table_name(table_name);
                throw api_error::resource_not_found(fmt::format("Requested resource not found: Table: {} not found", table_name));
            }

            auto m = co_await service::prepare_column_family_drop_announcement(p.local(), keyspace_name, table_name, group0_guard.write_timestamp(), service::drop_views::yes);
            auto m2 = co_await service::prepare_keyspace_drop_announcement(p.local(), keyspace_name, group0_guard.write_timestamp());

            std::move(m2.begin(), m2.end(), std::back_inserter(m));

            // When deleting a table and its views, we need to remove this role's
            // special permissions in those tables (undoing the "auto-grant" done
            // by CreateTable). If we didn't do this, if a second role later
            // recreates a table with the same name, the first role would still
            // have permissions over the new table.
            // To make things more robust we just remove *all* permissions for
            // the deleted table (CQL's drop_table_statement also does this).
            // Unfortunately, there is an API mismatch between this code (which
            // uses separate group0_guard and vector<mutation>) and the function
            // revoke_all() which uses a combined "group0_batch" structure - so
            // we need to do some ugly back-and-forth conversions between the pair
            // to the group0_batch and back to the pair :-(
            service::group0_batch mc(std::move(group0_guard));
            mc.add_mutations(std::move(m));
            auto resource = auth::make_data_resource(schema->ks_name(), schema->cf_name());
            co_await auth::revoke_all(*cs.get().get_auth_service(), resource, mc);
            for (const view_ptr& v : tbl->views()) {
                resource = auth::make_data_resource(v->ks_name(), v->cf_name());
                co_await auth::revoke_all(*cs.get().get_auth_service(), resource, mc);
            }
            std::tie(m, group0_guard) = co_await std::move(mc).extract();

            try {
                co_await mm.announce(std::move(m), std::move(group0_guard), fmt::format("alternator-executor: delete {} table", table_name));
                break;
            } catch (const service::group0_concurrent_modification& ex) {
                elogger.info("Failed to execute DeleteTable {} due to concurrent schema modifications. {}.",
                        table_name, retries ? "Retrying" : "Number of retries exceeded, giving up");
                if (retries--) {
                    continue;
                }
                throw;
            }
        }
    });

    rjson::value response = rjson::empty_object();
    rjson::add(response, "TableDescription", std::move(table_description));
    elogger.trace("returning {}", response);
    co_return rjson::print(std::move(response));
}

static data_type parse_key_type(std::string_view type) {
    // Note that keys are only allowed to be string, blob or number (S/B/N).
    // The other types: boolean and various lists or sets - are not allowed.
    if (type.length() == 1) {
        switch (type[0]) {
        case 'S': return utf8_type;
        case 'B': return bytes_type;
        case 'N': return decimal_type; // FIXME: use a specialized Alternator type, not the general "decimal_type".
        }
    }
    throw api_error::validation(
            fmt::format("Invalid key type '{}', can only be S, B or N.", type));
}


static void add_column(schema_builder& builder, const std::string& name, const rjson::value& attribute_definitions, column_kind kind, bool computed_column=false) {
    // FIXME: Currently, the column name ATTRS_COLUMN_NAME is not allowed
    // because we use it for our untyped attribute map, and we can't have a
    // second column with the same name. We should fix this, by renaming
    // some column names which we want to reserve.
    if (name == executor::ATTRS_COLUMN_NAME) {
        throw api_error::validation(fmt::format("Column name '{}' is currently reserved. FIXME.", name));
    }
    for (auto it = attribute_definitions.Begin(); it != attribute_definitions.End(); ++it) {
        const rjson::value& attribute_info = *it;
        if (rjson::to_string_view(attribute_info["AttributeName"]) == name) {
            std::string_view type = rjson::to_string_view(attribute_info["AttributeType"]);
            data_type dt = parse_key_type(type);
            if (computed_column) {
                // Computed column for GSI (doesn't choose a real column as-is
                // but rather extracts a single value from the ":attrs" map)
                alternator_type at = type_info_from_string(type).atype;
                builder.with_computed_column(to_bytes(name), dt, kind,
                    std::make_unique<extract_from_attrs_column_computation>(to_bytes(name), at));
            } else {
                builder.with_column(to_bytes(name), dt, kind);
            }
            return;
        }
    }
    throw api_error::validation(
            fmt::format("KeySchema key '{}' missing in AttributeDefinitions", name));
}

// Parse the KeySchema request attribute, which specifies the column names
// for a key. A KeySchema must include up to two elements, the first must be
// the HASH key name, and the second one, if exists, must be a RANGE key name.
// The function returns the two column names - the first is the hash key
// and always present, the second is the range key and may be an empty string.
static std::pair<std::string, std::string> parse_key_schema(const rjson::value& obj, std::string_view supplementary_context) {
    const rjson::value *key_schema;
    if (!obj.IsObject() || !(key_schema = rjson::find(obj, "KeySchema"))) {
        throw api_error::validation("Missing KeySchema member");
    }
    if (!key_schema->IsArray() || key_schema->Size() < 1 || key_schema->Size() > 2) {
        throw api_error::validation("KeySchema must list exactly one or two key columns");
    }
    if (!(*key_schema)[0].IsObject()) {
        throw api_error::validation("First element of KeySchema must be an object");
    }
    const rjson::value *v = rjson::find((*key_schema)[0], "KeyType");
    if (!v || !v->IsString() || rjson::to_string_view(*v) != "HASH") {
        throw api_error::validation("First key in KeySchema must be a HASH key");
    }
    v = rjson::find((*key_schema)[0], "AttributeName");
    if (!v || !v->IsString()) {
        throw api_error::validation("First key in KeySchema must have string AttributeName");
    }
    validate_attr_name_length(supplementary_context, v->GetStringLength(), true, "HASH key in KeySchema - ");
    std::string hash_key = rjson::to_string(*v);
    std::string range_key;
    if (key_schema->Size() == 2) {
        if (!(*key_schema)[1].IsObject()) {
            throw api_error::validation("Second element of KeySchema must be an object");
        }
        v = rjson::find((*key_schema)[1], "KeyType");
        if (!v || !v->IsString() || rjson::to_string_view(*v) != "RANGE") {
            throw api_error::validation("Second key in KeySchema must be a RANGE key");
        }
        v = rjson::find((*key_schema)[1], "AttributeName");
        if (!v || !v->IsString()) {
            throw api_error::validation("Second key in KeySchema must have string AttributeName");
        }
        validate_attr_name_length(supplementary_context, v->GetStringLength(), true, "RANGE key in KeySchema - ");
        range_key = v->GetString();
    }
    return {hash_key, range_key};
}

arn_parts parse_arn(std::string_view arn, std::string_view arn_field_name, std::string_view type_name, std::string_view expected_postfix) {
    // Expected ARN format arn:partition:service:region:account-id:resource-type/resource-id
    // So (old arn produced by scylla for internal purposes) arn:scylla:alternator:${KEYSPACE_NAME}:scylla:table/${TABLE_NAME}
    // or (KCL ready new arn) arn:aws:dynamodb:us-east-1:797456418907:table/${KEYSPACE_NAME}@${TABLE_NAME}/stream/2025-12-18T17:38:48.952
    // According to Amazon account-id must be a number, but we don't check for it here.
    // postfix is part of the string after table name, including the separator, e.g. "/stream/2025-12-18T17:38:48.952" in the second example above
    // if expected_postfix is empty, then we reject ARN with any postfix
    // otherwise we require that postfix starts with expected_postfix and we return it

    if (!arn.starts_with("arn:")) {
        throw api_error::access_denied(fmt::format("{}: Invalid {} ARN `{}`  - missing `arn:` prefix", arn_field_name, type_name, arn));
    }

    // skip to the resource part
    // we don't require ARNs to follow KCL requirements, except for number of parts.
    auto index = arn.find(':'); // skip arn
    bool is_scylla_arn = false;
    std::string_view keyspace_name;
    if (index != std::string_view::npos) {
        auto start = index + 1;
        index = arn.find(':', index + 1); // skip partition
        if (index != std::string_view::npos) {
            if (arn.substr(start, index - start) == "scylla") {
                is_scylla_arn = true;
            }
            index = arn.find(':', index + 1); // skip service
            if (index != std::string_view::npos) {
                start = index + 1;
                index = arn.find(':', index + 1); // skip region
                if (index != std::string_view::npos) {
                    if (is_scylla_arn) {
                        keyspace_name = arn.substr(start, index - start);
                    }
                    index = arn.find(':', index + 1); // skip account-id
                }
            }
        }
    }
    if (index == std::string_view::npos) {
        throw api_error::access_denied(fmt::format("{}: Invalid {} ARN `{}` - missing arn:<partition>:<service>:<region>:<account-id> prefix", arn_field_name, type_name, arn));
    }

    // index is at last `:` before `table/<table-name>`
    // this looks like a valid ARN up to this point
    // as a sanity check make sure that what follows is a resource-type "table/"
    if (arn.substr(index + 1, 6) != "table/") {
        throw api_error::validation(fmt::format("{}: Invalid {} ARN `{}` - resource-type is not `table/`", arn_field_name, type_name, arn));
    }
    index += 7; // move past ":table/"

    // Amazon's spec allows both ':' and '/' as resource separators
    auto end_index = arn.substr(index).find_first_of("/:");
    if (end_index == std::string_view::npos) {
        end_index = arn.length();
    }
    else {
        end_index += index; // adjust end_index to be relative to the start of the string, not to the start of the table name
    }
    auto table_name = arn.substr(index, end_index - index);
    if (!is_scylla_arn) {
        auto separator_pos = table_name.find_first_of("@");
        if (separator_pos == std::string_view::npos) {
            throw api_error::validation(fmt::format("{}: Invalid {} ARN `{}` - missing separator `@`", arn_field_name, type_name, arn));
        }
        keyspace_name = table_name.substr(0, separator_pos);
        table_name = table_name.substr(separator_pos + 1);
    }
    index = end_index;

    // caller might require a specific postfix after the table name
    // so for example in arn:aws:dynamodb:us-east-1:797456418907:table/dynamodb_streams_verification_table_rc/stream/2025-12-18T17:38:48.952
    // specific postfix could be "/stream/" to make sure ARN is for a stream, not for the table itself
    // postfix will contain leading separator
    std::string_view postfix = arn.substr(index);

    if (postfix.empty() == expected_postfix.empty() && postfix.starts_with(expected_postfix)) {
        // we will end here if
        // - postfix and expected_postfix are both empty (thus `starts_with` will check against empty string and return true)
        // - both are not empty and postfix starts with expected_postfix
        return { keyspace_name, table_name, postfix };
    }
    throw api_error::validation(fmt::format("{}: Invalid {} ARN `{}` - expected `{}` after table name `{}`", arn_field_name, type_name, arn, expected_postfix, table_name));
}

static schema_ptr get_table_from_arn(service::storage_proxy& proxy, std::string_view arn) {
    // NOTE: This code returns AccessDeniedException if it's problematic to parse or recognize an arn.
    // Technically, a properly formatted, but nonexistent arn *should* return AccessDeniedException,
    // while an incorrectly formatted one should return ValidationException.
    // Unfortunately, the rules are really uncertain, since DynamoDB
    // states that arns are of the form arn:partition:service:region:account-id:resource-type/resource-id
    // or similar - yet, for some arns that do not fit that pattern (e.g. "john"),
    // it still returns AccessDeniedException rather than ValidationException.
    // Consequently, this code simply falls back to AccessDeniedException,
    // concluding that an error is an error and code which uses tagging
    // must be ready for handling AccessDeniedException instances anyway.
    try {
        auto parts = parse_arn(arn, "ResourceArn", "table", "");
        return proxy.data_dictionary().find_schema(parts.keyspace_name, parts.table_name);
    } catch (const data_dictionary::no_such_column_family& e) {
        throw api_error::resource_not_found(fmt::format("ResourceArn: Invalid table ARN `{}` - not found", arn));
    } catch (const std::out_of_range& e) {
        throw api_error::access_denied(fmt::format("ResourceArn: Invalid table ARN `{}` - {}", arn, e.what()));
    }
}

static bool is_legal_tag_char(char c) {
    // FIXME: According to docs, unicode strings should also be accepted.
    // Alternator currently uses a simplified ASCII approach
    return std::isalnum(c) || std::isspace(c)
            || c == '+' || c == '-' || c == '=' || c == '.' || c == '_' || c == ':' || c == '/' ;
}

static bool validate_legal_tag_chars(std::string_view tag) {
    return std::all_of(tag.begin(), tag.end(), &is_legal_tag_char);
}

static const std::unordered_set<std::string_view> allowed_write_isolation_values = {
    "f", "forbid", "forbid_rmw",
    "a", "always", "always_use_lwt",
    "o", "only_rmw_uses_lwt",
    "u", "unsafe", "unsafe_rmw",
};

static void validate_tags(const std::map<sstring, sstring>& tags) {
    auto it = tags.find(rmw_operation::WRITE_ISOLATION_TAG_KEY);
    if (it != tags.end()) {
        std::string_view value = it->second;
        if (!allowed_write_isolation_values.contains(value)) {
            throw api_error::validation(
                    fmt::format("Incorrect write isolation tag {}. Allowed values: {}", value, allowed_write_isolation_values));
        }
    }
}

static rmw_operation::write_isolation parse_write_isolation(std::string_view value) {
    if (!value.empty()) {
        switch (value[0]) {
        case 'f':
            return rmw_operation::write_isolation::FORBID_RMW;
        case 'a':
            return rmw_operation::write_isolation::LWT_ALWAYS;
        case 'o':
            return rmw_operation::write_isolation::LWT_RMW_ONLY;
        case 'u':
            return rmw_operation::write_isolation::UNSAFE_RMW;
        }
    }
    // Shouldn't happen as validate_tags() / set_default_write_isolation()
    // verify allow only a closed set of values.
    return rmw_operation::default_write_isolation;

}
// This default_write_isolation is always overwritten in main.cc, which calls
// set_default_write_isolation().
rmw_operation::write_isolation rmw_operation::default_write_isolation =
        rmw_operation::write_isolation::LWT_ALWAYS;
void rmw_operation::set_default_write_isolation(std::string_view value) {
    if (value.empty()) {
        throw std::runtime_error("When Alternator is enabled, write "
                "isolation policy must be selected, using the "
                "'--alternator-write-isolation' option. "
                "See docs/alternator/alternator.md for instructions.");
    }
    if (!allowed_write_isolation_values.contains(value)) {
        throw std::runtime_error(fmt::format("Invalid --alternator-write-isolation "
                "setting '{}'. Allowed values: {}.",
                value, allowed_write_isolation_values));
    }
    default_write_isolation = parse_write_isolation(value);
}

// Alternator uses tags whose keys start with the "system:" prefix for
// internal purposes. Those should not be readable by ListTagsOfResource,
// nor writable with TagResource or UntagResource (see #24098).
// Only a few specific system tags, currently only "system:write_isolation"
// and "system:initial_tablets", are deliberately intended to be set and read
// by the user, so are not considered "internal".
static bool tag_key_is_internal(std::string_view tag_key) {
    return tag_key.starts_with("system:")
        && tag_key != rmw_operation::WRITE_ISOLATION_TAG_KEY
        && tag_key != INITIAL_TABLETS_TAG_KEY;
}

enum class update_tags_action { add_tags, delete_tags };
static void update_tags_map(const rjson::value& tags, std::map<sstring, sstring>& tags_map, update_tags_action action) {
    if (action == update_tags_action::add_tags) {
        for (auto it = tags.Begin(); it != tags.End(); ++it) {
            if (!it->IsObject()) {
                throw api_error::validation("invalid tag object");
            }
            const rjson::value* key = rjson::find(*it, "Key");
            const rjson::value* value = rjson::find(*it, "Value");
            if (!key || !key->IsString() || !value || !value->IsString()) {
                throw api_error::validation("string Key and Value required");
            }
            auto tag_key = rjson::to_string_view(*key);
            auto tag_value = rjson::to_string_view(*value);

            if (tag_key.empty()) {
                throw api_error::validation("A tag Key cannot be empty");
            }
            if (tag_key.size() > 128) {
                throw api_error::validation("A tag Key is limited to 128 characters");
            }
            if (!validate_legal_tag_chars(tag_key)) {
                throw api_error::validation("A tag Key can only contain letters, spaces, and [+-=._:/]");
            }
            if (tag_key_is_internal(tag_key)) {
                throw api_error::validation(fmt::format("Tag key '{}' is reserved for internal use", tag_key));
            }
            // Note tag values are limited similarly to tag keys, but have a
            // longer length limit, and *can* be empty.
            if (tag_value.size() > 256) {
                throw api_error::validation("A tag Value is limited to 256 characters");
            }
            if (!validate_legal_tag_chars(tag_value)) {
                throw api_error::validation("A tag Value can only contain letters, spaces, and [+-=._:/]");
            }
            tags_map[sstring(tag_key)] = sstring(tag_value);
        }
    } else if (action == update_tags_action::delete_tags) {
        for (auto it = tags.Begin(); it != tags.End(); ++it) {
            auto tag_key = rjson::to_string_view(*it);
            if (tag_key_is_internal(tag_key)) {
                throw api_error::validation(fmt::format("Tag key '{}' is reserved for internal use", tag_key));
            }
            tags_map.erase(sstring(tag_key));
        }
    }

    if (tags_map.size() > 50) {
        throw api_error::validation("Number of Tags exceed the current limit for the provided ResourceArn");
    }
    validate_tags(tags_map);
}

future<executor::request_return_type> executor::tag_resource(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.tag_resource++;

    const rjson::value* arn = rjson::find(request, "ResourceArn");
    if (!arn || !arn->IsString()) {
        co_return api_error::access_denied("Incorrect resource identifier");
    }
    schema_ptr schema = get_table_from_arn(_proxy, rjson::to_string_view(*arn));

    maybe_audit(audit_info, audit::statement_category::DDL, schema->ks_name(), schema->cf_name(), "TagResource", request);

    get_stats_from_schema(_proxy, *schema)->api_operations.tag_resource++;
    const rjson::value* tags = rjson::find(request, "Tags");
    if (!tags || !tags->IsArray()) {
        co_return api_error::validation("Cannot parse tags");
    }
    if (tags->Size() < 1) {
        co_return api_error::validation("The number of tags must be at least 1") ;
    }
    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::ALTER, _stats);
    co_await db::modify_tags(_mm, schema->ks_name(), schema->cf_name(), [tags](std::map<sstring, sstring>& tags_map) {
        update_tags_map(*tags, tags_map, update_tags_action::add_tags);
    });
    co_return ""; // empty response
}

future<executor::request_return_type> executor::untag_resource(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.untag_resource++;

    const rjson::value* arn = rjson::find(request, "ResourceArn");
    if (!arn || !arn->IsString()) {
        co_return api_error::access_denied("Incorrect resource identifier");
    }

    const rjson::value* tags = rjson::find(request, "TagKeys");
    if (!tags || !tags->IsArray()) {
        co_return api_error::validation(format("Cannot parse tag keys"));
    }

    schema_ptr schema = get_table_from_arn(_proxy, rjson::to_string_view(*arn));
    maybe_audit(audit_info, audit::statement_category::DDL, schema->ks_name(), schema->cf_name(), "UntagResource", request);

    get_stats_from_schema(_proxy, *schema)->api_operations.untag_resource++;
    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::ALTER, _stats);
    co_await db::modify_tags(_mm, schema->ks_name(), schema->cf_name(), [tags](std::map<sstring, sstring>& tags_map) {
        update_tags_map(*tags, tags_map, update_tags_action::delete_tags);
    });
    co_return ""; // empty response
}

future<executor::request_return_type> executor::list_tags_of_resource(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_tags_of_resource++;
    const rjson::value* arn = rjson::find(request, "ResourceArn");
    if (!arn || !arn->IsString()) {
        return make_ready_future<request_return_type>(api_error::access_denied("Incorrect resource identifier"));
    }
    schema_ptr schema = get_table_from_arn(_proxy, rjson::to_string_view(*arn));

    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(), schema->cf_name(), "ListTagsOfResource", request);

    get_stats_from_schema(_proxy, *schema)->api_operations.list_tags_of_resource++;
    auto tags_map = get_tags_of_table_or_throw(schema);
    rjson::value ret = rjson::empty_object();
    rjson::add(ret, "Tags", rjson::empty_array());

    rjson::value& tags = ret["Tags"];
    for (auto& tag_entry : tags_map) {
        if (tag_key_is_internal(tag_entry.first)) {
            continue;
        }
        rjson::value new_entry = rjson::empty_object();
        rjson::add(new_entry, "Key", rjson::from_string(tag_entry.first));
        rjson::add(new_entry, "Value", rjson::from_string(tag_entry.second));
        rjson::push_back(tags, std::move(new_entry));
    }

    return make_ready_future<executor::request_return_type>(rjson::print(std::move(ret)));
}

struct billing_mode_type {
    bool provisioned = false;
    int rcu;
    int wcu;
};

static billing_mode_type verify_billing_mode(const rjson::value& request) {
    // Alternator does not yet support billing or throughput limitations, but
    // let's verify that BillingMode is at least legal.
    std::string billing_mode = get_string_attribute(request, "BillingMode", "PROVISIONED");
    if (billing_mode == "PAY_PER_REQUEST") {
        if (rjson::find(request, "ProvisionedThroughput")) {
            throw api_error::validation("When BillingMode=PAY_PER_REQUEST, ProvisionedThroughput cannot be specified.");
        }
    } else if (billing_mode == "PROVISIONED") {
        const rjson::value *provisioned_throughput = rjson::find(request, "ProvisionedThroughput");
        if (!provisioned_throughput) {
            throw api_error::validation("When BillingMode=PROVISIONED, ProvisionedThroughput must be specified.");
        }
        const rjson::value& throughput = *provisioned_throughput;
        auto rcu = get_int_attribute(throughput, "ReadCapacityUnits");
        auto wcu = get_int_attribute(throughput, "WriteCapacityUnits");
        if (!rcu.has_value()) {
            throw api_error::validation("provisionedThroughput.readCapacityUnits is missing, when BillingMode=PROVISIONED, ProvisionedThroughput must be specified.");
        }
        if (!wcu.has_value()) {
            throw api_error::validation("provisionedThroughput.writeCapacityUnits is missing, when BillingMode=PROVISIONED, ProvisionedThroughput must be specified.");
        }
        return billing_mode_type{true, *rcu, *wcu};
    } else {
        throw api_error::validation("Unknown BillingMode={}. Must be PAY_PER_REQUEST or PROVISIONED.");
    }
    return billing_mode_type();
}

// Validate that a AttributeDefinitions parameter in CreateTable is valid, and
// throws user-facing api_error::validation if it's not.
// In particular, verify that the same AttributeName doesn't appear more than
// once (Issue #13870).
// Return the set of attribute names defined in AttributeDefinitions - this
// set is useful for later verifying that all of them are used by some
// KeySchema (issue #19784)
static std::unordered_set<std::string> validate_attribute_definitions(std::string_view supplementary_context, const rjson::value& attribute_definitions) {
    if (!attribute_definitions.IsArray()) {
        throw api_error::validation("AttributeDefinitions must be an array");
    }
    std::unordered_set<std::string> seen_attribute_names;
    for (auto it = attribute_definitions.Begin(); it != attribute_definitions.End(); ++it) {
        const rjson::value* attribute_name = rjson::find(*it, "AttributeName");
        if (!attribute_name) {
            throw api_error::validation("AttributeName missing in AttributeDefinitions");
        }
        if (!attribute_name->IsString()) {
            throw api_error::validation("AttributeName in AttributeDefinitions must be a string");
        }
        validate_attr_name_length(supplementary_context, attribute_name->GetStringLength(), true, "in AttributeDefinitions - ");
        auto [it2, added] = seen_attribute_names.emplace(rjson::to_string_view(*attribute_name));
        if (!added) {
            throw api_error::validation(fmt::format("Duplicate AttributeName={} in AttributeDefinitions",
                rjson::to_string_view(*attribute_name)));
        }
        const rjson::value* attribute_type = rjson::find(*it, "AttributeType");
        if (!attribute_type) {
            throw api_error::validation("AttributeType missing in AttributeDefinitions");
        }
        if (!attribute_type->IsString()) {
            throw api_error::validation("AttributeType in AttributeDefinitions must be a string");
        }
    }
    return seen_attribute_names;
}

// The following "extract_from_attrs_column_computation" implementation is
// what allows Alternator GSIs and LSIs to use in a materialized view's key a
// member from the ":attrs" map instead of a real column in the schema:

const bytes extract_from_attrs_column_computation::MAP_NAME = executor::ATTRS_COLUMN_NAME;

column_computation_ptr extract_from_attrs_column_computation::clone() const {
    return std::make_unique<extract_from_attrs_column_computation>(*this);
}

// Serialize the *definition* of this column computation into a JSON
// string with a unique "type" string - TYPE_NAME - which then causes
// column_computation::deserialize() to create an object from this class.
bytes extract_from_attrs_column_computation::serialize() const {
    rjson::value ret = rjson::empty_object();
    rjson::add(ret, "type", TYPE_NAME);
    rjson::add(ret, "attr_name", rjson::from_string(to_string_view(_attr_name)));
    rjson::add(ret, "desired_type", represent_type(_desired_type).ident);
    return to_bytes(rjson::print(ret));
}

// Construct an extract_from_attrs_column_computation object based on the
// saved output of serialize(). Calls on_internal_error() if the string
// doesn't match the expected output format of serialize(). "type" is not
// checked - we assume the caller (column_computation::deserialize()) won't
// call this constructor if "type" doesn't match.
extract_from_attrs_column_computation::extract_from_attrs_column_computation(const rjson::value &v) {
    const rjson::value* attr_name = rjson::find(v, "attr_name");
    if (attr_name->IsString()) {
        _attr_name = bytes(to_bytes_view(rjson::to_string_view(*attr_name)));
        const rjson::value* desired_type = rjson::find(v, "desired_type");
        if (desired_type->IsString()) {
            _desired_type = type_info_from_string(rjson::to_string_view(*desired_type)).atype;
            switch (_desired_type) {
            case alternator_type::S:
            case alternator_type::B:
            case alternator_type::N:
                // We're done
                return;
            default:
                // Fall through to on_internal_error below.
                break;
            }
        }
    }
    on_internal_error(elogger, format("Improperly formatted alternator::extract_from_attrs_column_computation computed column definition: {}", v));
}

regular_column_transformation::result extract_from_attrs_column_computation::compute_value(
        const schema& schema,
        const partition_key& key,
        const db::view::clustering_or_static_row& row) const
{
    const column_definition* attrs_col = schema.get_column_definition(MAP_NAME);
    if (!attrs_col || !attrs_col->is_regular() || !attrs_col->is_multi_cell()) {
        on_internal_error(elogger, "extract_from_attrs_column_computation::compute_value() on a table without an attrs map");
    }
    // Look for the desired attribute _attr_name in the attrs_col map in row:
    const atomic_cell_or_collection* attrs = row.cells().find_cell(attrs_col->id);
    if (!attrs) {
        return regular_column_transformation::result();
    }
    collection_mutation_view cmv = attrs->as_collection_mutation();
    return cmv.with_deserialized(*attrs_col->type, [this] (const collection_mutation_view_description& cmvd) {
        for (auto&& [key, cell] : cmvd.cells) {
            if (key == _attr_name) {
                return regular_column_transformation::result(cell,
                    std::bind(serialized_value_if_type, std::placeholders::_1, _desired_type));
            }
        }
        return regular_column_transformation::result();
    });
}

// extract_from_attrs_column_computation needs the whole row to compute
// value, it can't use just the partition key.
bytes extract_from_attrs_column_computation::compute_value(const schema&, const partition_key&) const {
    on_internal_error(elogger, "extract_from_attrs_column_computation::compute_value called without row");
}

// Because `CreateTable` request creates GSI/LSI together with the base table (so the base table is empty),
// we can skip view building process and immediately mark the view as built on all nodes.
//
// However, we can do this only for tablet-based views because `view_building_worker` will automatically propagate
// this information to `system.built_views` table (see `view_building_worker::update_built_views()`).
// For vnode-based views, `view_builder` will process the view and mark it as built.
static future<> mark_view_schemas_as_built(utils::chunked_vector<mutation>& out, std::vector<schema_ptr> schemas, api::timestamp_type ts, service::storage_proxy& sp) {
    auto token_metadata = sp.get_token_metadata_ptr();
    for (auto& schema: schemas) {
        if (schema->is_view()) {
            for (auto& host_id: token_metadata->get_topology().get_all_host_ids()) {
                auto view_status_mut = co_await sp.system_keyspace().make_view_build_status_mutation(ts, {schema->ks_name(), schema->cf_name()}, host_id, db::view::build_status::SUCCESS);
                out.push_back(std::move(view_status_mut));
            }
        }
    }
}

// When Alternator Streams are requested on a tablet table, convert the
// already-configured enabled=true CDC options into a deferred state:
// - enabled=false: don't create the CDC log table yet
// - enable_requested=true: persist the user's intent for the topology
//   coordinator to finalize later
// - tablet_merge_blocked=true: suppress tablet merges that would produce
//   2-parent shards incompatible with the DynamoDB Streams API
// The topology coordinator will finalize enablement once all pending merges
// complete (see maybe_finalize_pending_stream_enables in cdc/generation.cc).
//
// Callers must ensure the builder already has CDC options with enabled=true.
static void defer_enabling_streams_block_tablet_merges(schema_builder& builder) {
    auto& exts = builder.get_extensions();
    auto it = exts.find(cdc::cdc_extension::NAME);
    throwing_assert(it != exts.end());
    auto opts = dynamic_pointer_cast<cdc::cdc_extension>(it->second)->get_options();
    throwing_assert(opts.enabled());
    opts.enabled(false);
    opts.enable_requested(true);
    opts.tablet_merge_blocked(true);
    builder.with_cdc_options(opts);
}

// Returns true if the given attribute name is already the target of any vector
// index on the schema. Analogous to schema::has_index(), but looks up by the
// indexed attribute name rather than the index name.
static bool has_vector_index_on_attribute(const schema& s, std::string_view attribute_name) {
    for (const index_metadata& im : s.indices()) {
        // No need to check if the secondary index is a vector index, because
        // Alternator doesn't use secondary indexes for anything else (GSI and
        // LSI are implemented as materialized views, not secondary indexes).
        const auto& opts = im.options();
        auto target_it = opts.find(cql3::statements::index_target::target_option_name);
        if (target_it != opts.end() && target_it->second == attribute_name) {
            return true;
        }
    }
    return false;
}

// Returns the validated "Dimensions" value from a VectorAttribute JSON object
// or throws api_error::validation if invalid. The "source" parameter is used
// in error messages (e.g., "VectorIndexes" or "VectorIndexUpdates").
static int get_dimensions(const rjson::value& vector_attribute, std::string_view source) {
    const rjson::value* dimensions_v = rjson::find(vector_attribute, "Dimensions");
    if (!dimensions_v || !dimensions_v->IsInt() || dimensions_v->GetInt() <= 0 || (vector_dimension_t)dimensions_v->GetInt() > cql3::cql3_type::MAX_VECTOR_DIMENSION) {
        throw api_error::validation(fmt::format("{} Dimensions must be an integer between 1 and {}.", source, cql3::cql3_type::MAX_VECTOR_DIMENSION));
    }
    return dimensions_v->GetInt();
}

future<executor::request_return_type> executor::create_table_on_shard0(service::client_state&& client_state, tracing::trace_state_ptr trace_state, rjson::value request, bool enforce_authorization, bool warn_authorization,
            const db::tablets_mode_t::mode tablets_mode, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    throwing_assert(this_shard_id() == 0);

    // We begin by parsing and validating the content of the CreateTable
    // command. We can't inspect the current database schema at this point
    // (e.g., verify that this table doesn't already exist) - we can only
    // do this further down - after taking group0_guard.
    std::string table_name = get_table_name(request);
    validate_table_name(table_name);

    if (table_name.find(executor::INTERNAL_TABLE_PREFIX) == 0) {
        co_return api_error::validation(fmt::format("Prefix {} is reserved for accessing internal tables", executor::INTERNAL_TABLE_PREFIX));
    }
    std::string keyspace_name = executor::KEYSPACE_NAME_PREFIX + table_name;

    maybe_audit(audit_info, audit::statement_category::DDL, keyspace_name, table_name, "CreateTable", request);

    const rjson::value* attribute_definitions = rjson::find(request, "AttributeDefinitions");
    if (attribute_definitions == nullptr) {
        co_return api_error::validation("Missing AttributeDefinitions in CreateTable request");
    }
    // Save the list of AttributeDefinitions in unused_attribute_definitions,
    // and below remove each one as we see it in a KeySchema of the table or
    // any of its GSIs or LSIs. If anything remains in this set at the end of
    // this function, it's an error.
    std::unordered_set<std::string> unused_attribute_definitions =
        validate_attribute_definitions("", *attribute_definitions);

    tracing::add_alternator_table_name(trace_state, table_name);

    schema_builder builder(keyspace_name, table_name);
    auto [hash_key, range_key] = parse_key_schema(request, "");
    add_column(builder, hash_key, *attribute_definitions, column_kind::partition_key);
    unused_attribute_definitions.erase(hash_key);
    if (!range_key.empty()) {
        add_column(builder, range_key, *attribute_definitions, column_kind::clustering_key);
        unused_attribute_definitions.erase(range_key);
    }
    builder.with_column(bytes(executor::ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);

    billing_mode_type bm = verify_billing_mode(request);

    schema_ptr partial_schema = builder.build();

    // Parse Local/GlobalSecondaryIndexes parameters before creating the
    // base table, so if we have a parse errors we can fail without creating
    // any table.
    std::vector<schema_builder> view_builders;
    std::unordered_set<std::string> index_names;

    const rjson::value* lsi = rjson::find(request, "LocalSecondaryIndexes");
    if (lsi) {
        if (!lsi->IsArray()) {
            throw api_error::validation("LocalSecondaryIndexes must be an array.");
        }
        for (const rjson::value& l : lsi->GetArray()) {
            const rjson::value* index_name_v = rjson::find(l, "IndexName");
            if (!index_name_v || !index_name_v->IsString()) {
                throw api_error::validation("LocalSecondaryIndexes IndexName must be a string.");
            }
            std::string_view index_name = rjson::to_string_view(*index_name_v);
            auto [it, added] = index_names.emplace(index_name);
            if (!added) {
                co_return api_error::validation(fmt::format("Duplicate IndexName '{}', ", index_name));
            }
            std::string vname(lsi_name(table_name, index_name));
            elogger.trace("Adding LSI {}", index_name);
            if (range_key.empty()) {
                co_return api_error::validation("LocalSecondaryIndex requires that the base table have a range key");
            }
            // FIXME: read and handle "Projection" parameter. This will
            // require the MV code to copy just parts of the attrs map.
            schema_builder view_builder(keyspace_name, vname);
            auto [view_hash_key, view_range_key] = parse_key_schema(l, "Local Secondary Index");
            if (view_hash_key != hash_key) {
                co_return api_error::validation("LocalSecondaryIndex hash key must match the base table hash key");
            }
            add_column(view_builder, view_hash_key, *attribute_definitions, column_kind::partition_key);
            unused_attribute_definitions.erase(view_hash_key);
            if (view_range_key.empty()) {
                co_return api_error::validation("LocalSecondaryIndex must specify a sort key");
            }
            unused_attribute_definitions.erase(view_range_key);
            if (view_range_key == hash_key) {
                co_return api_error::validation("LocalSecondaryIndex sort key cannot be the same as hash key");
            }
            add_column(view_builder, view_range_key, *attribute_definitions, column_kind::clustering_key, view_range_key != range_key);
            // Base key columns which aren't part of the index's key need to
            // be added to the view nonetheless, as (additional) clustering
            // key(s).
            if (!range_key.empty() && view_range_key != range_key) {
                add_column(view_builder, range_key, *attribute_definitions, column_kind::clustering_key);
            }
            view_builder.with_column(bytes(executor::ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);
            // Note above we don't need to add virtual columns, as all
            // base columns were copied to view. TODO: reconsider the need
            // for virtual columns when we support Projection.
            // LSIs have no tags, but Scylla's "synchronous_updates" feature
            // (which an LSIs need), is actually implemented as a tag so we
            // need to add it here:
            std::map<sstring, sstring> tags_map = {{db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY, "true"}};
            view_builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));
            view_builders.emplace_back(std::move(view_builder));
        }
    }

    const rjson::value* gsi = rjson::find(request, "GlobalSecondaryIndexes");
    if (gsi) {
        if (!gsi->IsArray()) {
            co_return api_error::validation("GlobalSecondaryIndexes must be an array.");
        }
        for (const rjson::value& g : gsi->GetArray()) {
            const rjson::value* index_name_v = rjson::find(g, "IndexName");
            if (!index_name_v || !index_name_v->IsString()) {
                co_return api_error::validation("GlobalSecondaryIndexes IndexName must be a string.");
            }
            std::string_view index_name = rjson::to_string_view(*index_name_v);
            auto [it, added] = index_names.emplace(index_name);
            if (!added) {
                co_return api_error::validation(fmt::format("Duplicate IndexName '{}', ", index_name));
            }
            std::string vname(view_name(table_name, index_name));
            elogger.trace("Adding GSI {}", index_name);
            // FIXME: read and handle "Projection" parameter. This will
            // require the MV code to copy just parts of the attrs map.
            schema_builder view_builder(keyspace_name, vname);
            auto [view_hash_key, view_range_key] = parse_key_schema(g, "GlobalSecondaryIndexes");

            // If an attribute is already a real column in the base table
            // (i.e., a key attribute), we can use it directly as a view key.
            // Otherwise, we need to add it as a "computed column", which
            // extracts and deserializes the attribute from the ":attrs" map.
            bool view_hash_key_real_column = partial_schema->get_column_definition(to_bytes(view_hash_key));
            add_column(view_builder, view_hash_key, *attribute_definitions, column_kind::partition_key, !view_hash_key_real_column);
            unused_attribute_definitions.erase(view_hash_key);
            if (!view_range_key.empty()) {
                bool view_range_key_real_column = partial_schema->get_column_definition(to_bytes(view_range_key));
                add_column(view_builder, view_range_key, *attribute_definitions, column_kind::clustering_key, !view_range_key_real_column);
                if (!partial_schema->get_column_definition(to_bytes(view_range_key)) &&
                    !partial_schema->get_column_definition(to_bytes(view_hash_key))) {
                    // FIXME: This warning should go away. See issue #6714
                    elogger.warn("Only 1 regular column from the base table should be used in the GSI key in order to ensure correct liveness management without assumptions");
                }
                unused_attribute_definitions.erase(view_range_key);
            }

            // Base key columns which aren't part of the index's key need to
            // be added to the view nonetheless, as (additional) clustering
            // key(s).
            // NOTE: DescribeTable's implementation depends on those keys being added AFTER user specified keys.
            bool spurious_base_key_added_as_range_key = false;
            if (hash_key != view_hash_key && hash_key != view_range_key) {
                add_column(view_builder, hash_key, *attribute_definitions, column_kind::clustering_key);
                spurious_base_key_added_as_range_key = true;
            }
            if (!range_key.empty() && range_key != view_hash_key && range_key != view_range_key) {
                add_column(view_builder, range_key, *attribute_definitions, column_kind::clustering_key);
                spurious_base_key_added_as_range_key = true;
            }
            std::map<sstring, sstring> tags;
            if (view_range_key.empty() && spurious_base_key_added_as_range_key) {
                tags[SPURIOUS_RANGE_KEY_ADDED_TO_GSI_AND_USER_DIDNT_SPECIFY_RANGE_KEY_TAG_KEY] = "true";
            }
            view_builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(std::move(tags)));
            view_builders.emplace_back(std::move(view_builder));
        }
    }
    if (!unused_attribute_definitions.empty()) {
        co_return api_error::validation(fmt::format(
            "AttributeDefinitions defines spurious attributes not used by any KeySchema: {}",
            unused_attribute_definitions));
    }

    // Parse VectorIndexes parameters and apply them to "builder". This all
    // happens before we actually create the table, so if we have a parse
    // errors we can still fail without creating any table.
    const rjson::value* vector_indexes = rjson::find(request, "VectorIndexes");
    if (vector_indexes) {
        if (!vector_indexes->IsArray()) {
            co_return api_error::validation("VectorIndexes must be an array.");
        }
        std::unordered_set<std::string> seen_attribute_names;
        for (const rjson::value& v : vector_indexes->GetArray()) {
            const rjson::value* index_name_v = rjson::find(v, "IndexName");
            if (!index_name_v || !index_name_v->IsString()) {
                co_return api_error::validation("VectorIndexes IndexName must be a string.");
            }
            std::string_view index_name = rjson::to_string_view(*index_name_v);
            // Limit the length and character choice of a vector index's
            // name to the same rules as table names. This is slightly
            // different from GSI/LSI names, where we limit not the length
            // of the index name but its sum with the base table name.
            validate_table_name(index_name, "VectorIndexes IndexName");
            if (!index_names.emplace(index_name).second) {
                co_return api_error::validation(fmt::format("Duplicate IndexName '{}', ", index_name));
            }
            const rjson::value* vector_attribute_v = rjson::find(v, "VectorAttribute");
            if (!vector_attribute_v || !vector_attribute_v->IsObject()) {
                co_return api_error::validation("VectorIndexes VectorAttribute must be an object.");
            }
            const rjson::value* attribute_name_v = rjson::find(*vector_attribute_v, "AttributeName");
            if (!attribute_name_v || !attribute_name_v->IsString()) {
                co_return api_error::validation("VectorIndexes AttributeName must be a string.");
            }
            std::string_view attribute_name = rjson::to_string_view(*attribute_name_v);
            validate_attr_name_length("VectorIndexes", attribute_name.size(), /*is_key=*/false, "AttributeName ");
            if (!seen_attribute_names.emplace(attribute_name).second) {
                co_return api_error::validation(fmt::format("Duplicate vector index on the same AttributeName '{}'", attribute_name));
            }
            // attribute_name must not be one of the key columns of the base
            // or GSIs or LSIs, because those have mandatory types (defined in
            // AttributeDefinitions) which will never be a vector.
            for (auto it = attribute_definitions->Begin(); it != attribute_definitions->End(); ++it) {
                if (rjson::to_string_view((*it)["AttributeName"]) == attribute_name) {
                    co_return api_error::validation(fmt::format(
                        "VectorIndexes AttributeName '{}' is a key column of type {} so cannot be used as a vector index target.", attribute_name, rjson::to_string_view((*it)["AttributeType"])));
                }
            }
            int dimensions = get_dimensions(*vector_attribute_v, "VectorIndexes");
            // The optional Projection parameter is only supported with
            // ProjectionType=KEYS_ONLY. Other values are not yet supported.
            const rjson::value* projection_v = rjson::find(v, "Projection");
            if (projection_v) {
                if (!projection_v->IsObject()) {
                    co_return api_error::validation("VectorIndexes Projection must be an object.");
                }
                const rjson::value* projection_type_v = rjson::find(*projection_v, "ProjectionType");
                if (!projection_type_v || !projection_type_v->IsString() ||
                        rjson::to_string_view(*projection_type_v) != "KEYS_ONLY") {
                    co_return api_error::validation("VectorIndexes Projection: only ProjectionType=KEYS_ONLY is currently supported.");
                }
            }
            // Add a vector index metadata entry to the base table schema.
            index_options_map index_options;
            index_options[db::index::secondary_index::custom_class_option_name] = "vector_index";
            index_options[cql3::statements::index_target::target_option_name] = sstring(attribute_name);
            index_options["dimensions"] = std::to_string(dimensions);
            builder.with_index(index_metadata{sstring(index_name), index_options,
                    index_metadata_kind::custom, index_metadata::is_local_index(false)});
        }
        // If we have any vector indexes, we will use CDC and the CDC log
        // name will need to fit our length limits, so validate it now.
        if (vector_indexes->Size() > 0) {
            validate_cdc_log_name_length(builder.cf_name());
        }
    }

    // We don't yet support configuring server-side encryption (SSE) via the
    // SSESpecifiction attribute, but an SSESpecification with Enabled=false
    // is simply the default, and should be accepted:
    rjson::value* sse_specification = rjson::find(request, "SSESpecification");
    if (sse_specification && sse_specification->IsObject()) {
        rjson::value* enabled = rjson::find(*sse_specification, "Enabled");
        if (!enabled || !enabled->IsBool()) {
            co_return api_error("ValidationException", "SSESpecification needs boolean Enabled");
        }
        if (enabled->GetBool()) {
            // TODO: full support for SSESpecification
            co_return api_error("ValidationException", "SSESpecification: configuring encryption-at-rest is not yet supported.");
        }
    }

    rjson::value* stream_specification = rjson::find(request, "StreamSpecification");
    if (stream_specification && stream_specification->IsObject()) {
        if (executor::add_stream_options(*stream_specification, builder, _proxy)) {
            validate_cdc_log_name_length(builder.cf_name());
        }
    }

    // Parse the "Tags" parameter early, so we can avoid creating the table
    // at all if this parsing failed.
    const rjson::value* tags = rjson::find(request, "Tags");
    std::map<sstring, sstring> tags_map;
    if (tags && tags->IsArray()) {
        update_tags_map(*tags, tags_map, update_tags_action::add_tags);
    }
    if (bm.provisioned) {
        tags_map[RCU_TAG_KEY] = std::to_string(bm.rcu);
        tags_map[WCU_TAG_KEY] = std::to_string(bm.wcu);
    }
    set_table_creation_time(tags_map, db_clock::now());
    builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));

    co_await verify_create_permission(enforce_authorization, warn_authorization, client_state, _stats);

    schema_ptr schema = builder.build();
    for (auto& view_builder : view_builders) {
        // Note below we don't need to add virtual columns, as all
        // base columns were copied to view. TODO: reconsider the need
        // for virtual columns when we support Projection.
        for (const column_definition& regular_cdef : schema->regular_columns()) {
            if (!view_builder.has_column(*cql3::to_identifier(regular_cdef))) {
                view_builder.with_column(regular_cdef.name(), regular_cdef.type, column_kind::regular_column);
            }
        }
        const bool include_all_columns = true;
        view_builder.with_view_info(schema, include_all_columns, ""/*where clause*/);
    }

    size_t retries = _mm.get_concurrent_ddl_retries();
    for (;;) {
        auto group0_guard = co_await _mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        utils::chunked_vector<mutation> schema_mutations;
        auto ksm = create_keyspace_metadata(keyspace_name, _proxy, _gossiper, ts, tags_map, _proxy.features(), tablets_mode);
        locator::replication_strategy_params params(ksm->strategy_options(), ksm->initial_tablets(), ksm->consistency_option());
        const auto& topo = _proxy.local_db().get_token_metadata().get_topology();
        auto rs = locator::abstract_replication_strategy::create_replication_strategy(ksm->strategy_name(), params, topo);

        // Vector indexes is a new feature that we decided to only support
        // on tablets.
        if (vector_indexes && vector_indexes->Size() > 0) {
            if (!rs->uses_tablets()) {
                co_return api_error::validation("Vector indexes are not supported on tables using vnodes.");
            }
        }
        // Creating an index in tablets mode requires the keyspace to be RF-rack-valid.
        // GSI and LSI indexes are based on materialized views which require RF-rack-validity to avoid consistency issues.
        if (!view_builders.empty() || _proxy.data_dictionary().get_config().rf_rack_valid_keyspaces()) {
            try {
                locator::assert_rf_rack_valid_keyspace(keyspace_name, _proxy.local_db().get_token_metadata_ptr(), *rs);
            } catch (const std::invalid_argument& ex) {
                if (!view_builders.empty()) {
                    co_return api_error::validation(fmt::format("GlobalSecondaryIndexes and LocalSecondaryIndexes on a table "
                        "using tablets require the number of racks in the cluster to be either 1 or 3"));
                } else {
                    co_return api_error::validation(fmt::format("Cannot create table '{}' with tablets: the configuration "
                        "option 'rf_rack_valid_keyspaces' is enabled, which enforces that tables using tablets can only be created in clusters "
                        "that have either 1 or 3 racks", table_name));
                }
            }
        }
        try {
            schema_mutations = service::prepare_new_keyspace_announcement(_proxy.local_db(), ksm, ts);
        } catch (exceptions::already_exists_exception&) {
            if (_proxy.data_dictionary().has_schema(keyspace_name, table_name)) {
                co_return api_error::resource_in_use(fmt::format("Table {} already exists", table_name));
            }
        }
        if (_proxy.data_dictionary().try_find_table(schema->id())) {
            // This should never happen, the ID is supposed to be unique
            co_return api_error::internal(format("Table with ID {} already exists", schema->id()));
        }
        std::vector<schema_ptr> schemas;
        schemas.push_back(schema);
        for (schema_builder& view_builder : view_builders) {
            schemas.push_back(view_builder.build());
        }
        co_await service::prepare_new_column_families_announcement(schema_mutations, _proxy, *ksm, schemas, ts);
        if (ksm->uses_tablets()) {
            co_await mark_view_schemas_as_built(schema_mutations, schemas, ts, _proxy);
        }

        // If a role is allowed to create a table, we must give it permissions to
        // use (and eventually delete) the specific table it just created (and
        // also the view tables). This is known as "auto-grant".
        // Unfortunately, there is an API mismatch between this code (which uses
        // separate group0_guard and vector<mutation>) and the function
        // grant_applicable_permissions() which uses a combined "group0_batch"
        // structure - so we need to do some ugly back-and-forth conversions
        // between the pair to the group0_batch and back to the pair :-(
        service::group0_batch mc(std::move(group0_guard));
        mc.add_mutations(std::move(schema_mutations));
        if (client_state.user()) {
            auto resource = auth::make_data_resource(schema->ks_name(), schema->cf_name());
            co_await auth::grant_applicable_permissions(
                *client_state.get_auth_service(), *client_state.user(), resource, mc);
            for (const schema_builder& view_builder : view_builders) {
                resource = auth::make_data_resource(view_builder.ks_name(), view_builder.cf_name());
                co_await auth::grant_applicable_permissions(
                    *client_state.get_auth_service(), *client_state.user(), resource, mc);
            }
        }
        std::tie(schema_mutations, group0_guard) = co_await std::move(mc).extract();
        try {
            co_await _mm.announce(std::move(schema_mutations), std::move(group0_guard), fmt::format("alternator-executor: create {} table", table_name));
            break;
        }  catch (const service::group0_concurrent_modification& ex) {
            elogger.info("Failed to execute CreateTable {} due to concurrent schema modifications. {}.",
                    table_name, retries ? "Retrying" : "Number of retries exceeded, giving up");
            if (retries--) {
                continue;
            }
            throw;
        }
    }

    co_await _mm.wait_for_schema_agreement(_proxy.local_db(), db::timeout_clock::now() + 10s, nullptr);
    rjson::value status = rjson::empty_object();
    executor::supplement_table_info(request, *schema, _proxy);
    rjson::add(status, "TableDescription", std::move(request));
    co_return rjson::print(std::move(status));
}

future<executor::request_return_type> executor::create_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.create_table++;
    elogger.trace("Creating table {}", request);

    // Note: audit_info is captured by reference into the invoke_on() lambda and written on shard 0.
    // This is safe because co_await keeps the caller's coroutine frame (and audit_info) alive for
    // the entire duration of invoke_on(). Only the unique_ptr itself is read/written cross-shard —
    // no concurrent access occurs since the caller is suspended during invoke_on().
    co_return co_await _mm.container().invoke_on(0, [&, tr = tracing::global_trace_state_ptr(trace_state), request = std::move(request), &e = this->container(), client_state_other_shard = client_state.move_to_other_shard(), enforce_authorization = bool(_enforce_authorization), warn_authorization = bool(_warn_authorization)]
                                        (service::migration_manager& mm) mutable -> future<executor::request_return_type> {
        const db::tablets_mode_t::mode tablets_mode = _proxy.data_dictionary().get_config().tablets_mode_for_new_keyspaces(); // type cast
        // `invoke_on` hopped us to shard 0, but `this` points to `executor` is from 'old' shard, we need to hop it too.
        co_return co_await e.local().create_table_on_shard0(client_state_other_shard.get(), tr, std::move(request), enforce_authorization, warn_authorization, std::move(tablets_mode), audit_info);
    });
}

// When UpdateTable adds a GSI, the type of its key columns must be specified
// in a AttributeDefinitions. If one of these key columns are *already* key
// columns of the base table or any of its prior GSIs or LSIs, the type
// given in AttributeDefinitions must match the type of the existing key -
// otherwise Alternator will not know which type to enforce in new writes.
// Also, if the table already has vector indexes, their key attributes cannot
// be redefined in AttributeDefinitions with a non-vector type.
// This function checks for such conflicts. It assumes that the structure of
// the given attribute_definitions was already validated (with
// validate_attribute_definitions()).
// This function should be called multiple times - once for the base schema
// and once for each of its views (existing GSIs and LSIs on this table).
 static void check_attribute_definitions_conflicts(const rjson::value& attribute_definitions, const schema& schema) {
    for (auto it = attribute_definitions.Begin(); it != attribute_definitions.End(); ++it) {
        const rjson::value& attribute_info = *it;
        std::string_view attribute_name = rjson::to_string_view(attribute_info["AttributeName"]);
        for (auto& def : schema.primary_key_columns()) {
            if (attribute_name == def.name_as_text()) {
                auto def_type = type_to_string(def.type);
                std::string_view type = rjson::to_string_view(attribute_info["AttributeType"]);
                if (type != def_type) {
                    throw api_error::validation(fmt::format("AttributeDefinitions redefined {} to {} already a key attribute of type {} in this table", def.name_as_text(), type, def_type));
                }
                break;
            }
        }
        // Additionally, if we have a vector index, its key attribute is
        // required to have a vector type, and cannot be listed in
        // AttributeDefinitions with a non-vector key type.
        if (has_vector_index_on_attribute(schema, attribute_name)) {
            throw api_error::validation(fmt::format("AttributeDefinitions redefines {} but already a key of a vector index in this table", attribute_name));
        }
    }
}

future<executor::request_return_type> executor::update_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.update_table++;
    elogger.trace("Updating table {}", request);

    static const std::vector<sstring> unsupported = {
        "ProvisionedThroughput",
        "ReplicaUpdates",
        "SSESpecification",
    };

    for (auto& s : unsupported) {
        if (rjson::find(request, s)) {
            co_await coroutine::return_exception(api_error::validation(s + " not supported"));
        }
    }

    bool empty_request = true;

    if (rjson::find(request, "BillingMode")) {
        empty_request = false;
        verify_billing_mode(request);
    }

    // Note: audit_info is captured by reference into the invoke_on() lambda and written on shard 0.
    // This is safe because co_await keeps the caller's coroutine frame (and audit_info) alive for
    // the entire duration of invoke_on(). Only the unique_ptr itself is read/written cross-shard —
    // no concurrent access occurs since the caller is suspended during invoke_on().
    co_return co_await _mm.container().invoke_on(0, [&p = _proxy.container(), request = std::move(request), gt = tracing::global_trace_state_ptr(std::move(trace_state)), enforce_authorization = bool(_enforce_authorization),
                warn_authorization = bool(_warn_authorization), client_state_other_shard = client_state.move_to_other_shard(), empty_request, &e = this->container(), &audit_info]
                                                (service::migration_manager& mm) mutable -> future<executor::request_return_type> {
        schema_ptr schema;
        size_t retries = mm.get_concurrent_ddl_retries();
        for (;;) {
            auto group0_guard = co_await mm.start_group0_operation();

            schema_ptr tab = get_table(p.local(), request);

            e.local().maybe_audit(audit_info, audit::statement_category::DDL, tab->ks_name(), tab->cf_name(), "UpdateTable", request);

            tracing::add_alternator_table_name(gt, tab->cf_name());

            // the ugly but harmless conversion to string_view here is because
            // Seastar's sstring is missing a find(std::string_view) :-()
            if (std::string_view(tab->cf_name()).find(INTERNAL_TABLE_PREFIX) == 0) {
                co_await coroutine::return_exception(api_error::validation(fmt::format("Prefix {} is reserved for accessing internal tables", INTERNAL_TABLE_PREFIX)));
            }

            schema_builder builder(tab);

            rjson::value* stream_specification = rjson::find(request, "StreamSpecification");
            if (stream_specification && stream_specification->IsObject()) {
                empty_request = false;
                if (add_stream_options(*stream_specification, builder, p.local())) {
                    validate_cdc_log_name_length(builder.cf_name());
                    // On tablet tables, defer stream enablement and block
                    // tablet merges (see defer_enabling_streams_block_tablet_merges).
                    bool uses_tablets = p.local().local_db().find_keyspace(tab->ks_name()).get_replication_strategy().uses_tablets();
                    if (uses_tablets) {
                        defer_enabling_streams_block_tablet_merges(builder);
                    }
                }
                auto stream_enabled = rjson::find(*stream_specification, "StreamEnabled");
                if (stream_enabled && stream_enabled->IsBool()) {
                    if (stream_enabled->GetBool()) {
                        if (tab->cdc_options().enabled() || tab->cdc_options().enable_requested()) {
                            co_return api_error::validation("Table already has an enabled stream: TableName: " + tab->cf_name());
                        }
                    }
                    else if (!tab->cdc_options().enabled() && !tab->cdc_options().enable_requested()) {
                        co_return api_error::validation("Table has no stream to disable: TableName: " + tab->cf_name());
                    }
                }
            }

            // Support VectorIndexUpdates to add or delete a vector index,
            // similar to GlobalSecondaryIndexUpdates above. We handle this
            // before builder.build() so we can use builder directly.
            rjson::value* vector_index_updates = rjson::find(request, "VectorIndexUpdates");
            if (vector_index_updates) {
                if (!vector_index_updates->IsArray()) {
                    co_return api_error::validation("VectorIndexUpdates must be an array");
                }
                if (vector_index_updates->Size() > 1) {
                    // VectorIndexUpdates mirrors GlobalSecondaryIndexUpdates.
                    // Since DynamoDB artificially limits the latter to just a
                    // single operation (one Create or one Delete), we also
                    // place the same artificial limit on VectorIndexUpdates,
                    // and throw the same LimitExceeded error if the client
                    // tries to pass more than one operation.
                    co_return api_error::limit_exceeded("VectorIndexUpdates only allows one index creation or deletion");
                }
            }
            if (vector_index_updates && vector_index_updates->Size() == 1) {
                empty_request = false;
                if (!(*vector_index_updates)[0].IsObject() || (*vector_index_updates)[0].MemberCount() != 1) {
                    co_return api_error::validation("VectorIndexUpdates array must contain one object with a Create or Delete operation");
                }
                auto it = (*vector_index_updates)[0].MemberBegin();
                const std::string_view op = rjson::to_string_view(it->name);
                if (!it->value.IsObject()) {
                    co_return api_error::validation("VectorIndexUpdates entries must be objects");
                }
                const rjson::value* index_name_v = rjson::find(it->value, "IndexName");
                if (!index_name_v || !index_name_v->IsString()) {
                    co_return api_error::validation("VectorIndexUpdates operation must have IndexName");
                }
                sstring index_name = rjson::to_sstring(*index_name_v);
                if (op == "Create") {
                    if (!p.local().local_db().find_keyspace(tab->ks_name()).get_replication_strategy().uses_tablets()) {
                        co_return api_error::validation("Vector indexes are not supported on tables using vnodes.");
                    }
                    validate_table_name(index_name, "VectorIndexUpdates IndexName");
                    // Check for duplicate index name against existing vector indexes, GSIs and LSIs.
                    if (tab->has_index(index_name)) {
                        // Alternator only uses a secondary index for vector
                        // search (GSI and LSI are implemented as materialized
                        // views, not secondary indexes), so the error message
                        // can refer to a "Vector index".
                        co_return api_error::validation(fmt::format(
                            "Vector index {} already exists in table {}", index_name, tab->cf_name()));
                    }
                    if (p.local().data_dictionary().has_schema(tab->ks_name(), gsi_name(tab->cf_name(), index_name, false)) ||
                        p.local().data_dictionary().has_schema(tab->ks_name(), lsi_name(tab->cf_name(), index_name, false))) {
                        co_return api_error::validation(fmt::format(
                            "GSI or LSI {} already exists in table {}, cannot reuse the name for a vector index", index_name, tab->cf_name()));
                    }
                    const rjson::value* vector_attribute_v = rjson::find(it->value, "VectorAttribute");
                    if (!vector_attribute_v || !vector_attribute_v->IsObject()) {
                        co_return api_error::validation("VectorIndexUpdates Create VectorAttribute must be an object.");
                    }
                    const rjson::value* attribute_name_v = rjson::find(*vector_attribute_v, "AttributeName");
                    if (!attribute_name_v || !attribute_name_v->IsString()) {
                        co_return api_error::validation("VectorIndexUpdates Create AttributeName must be a string.");
                    }
                    std::string_view attribute_name = rjson::to_string_view(*attribute_name_v);
                    validate_attr_name_length("VectorIndexUpdates", attribute_name.size(), /*is_key=*/false, "AttributeName ");
                    // attribute_name must not be a key column of the base
                    // table or any of its GSIs or LSIs, because those have
                    // mandatory types (defined in AttributeDefinitions) which
                    // will never be a vector.
                    for (const column_definition& cdef : tab->primary_key_columns()) {
                        if (cdef.name_as_text() == attribute_name) {
                            co_return api_error::validation(fmt::format(
                                "VectorIndexUpdates AttributeName '{}' is a key column and cannot be used as a vector index target.", attribute_name));
                        }
                    }
                    for (const auto& view : p.local().data_dictionary().find_column_family(tab).views()) {
                        for (const column_definition& cdef : view->primary_key_columns()) {
                            if (cdef.name_as_text() == attribute_name) {
                                co_return api_error::validation(fmt::format(
                                    "VectorIndexUpdates AttributeName '{}' is a key column of a GSI or LSI and cannot be used as a vector index target.", attribute_name));
                            }
                        }
                    }
                    // attribute_name must not already be the target of an
                    // existing vector index.
                    if (has_vector_index_on_attribute(*tab, attribute_name)) {
                        co_return api_error::validation(fmt::format(
                            "VectorIndexUpdates AttributeName '{}' is already the target of an existing vector index.", attribute_name));
                    }
                    int dimensions = get_dimensions(*vector_attribute_v, "VectorIndexUpdates");
                    // The optional Projection parameter is only supported with
                    // ProjectionType=KEYS_ONLY. Other values are not yet supported.
                    const rjson::value* projection_v = rjson::find(it->value, "Projection");
                    if (projection_v) {
                        if (!projection_v->IsObject()) {
                            co_return api_error::validation("VectorIndexUpdates Projection must be an object.");
                        }
                        const rjson::value* projection_type_v = rjson::find(*projection_v, "ProjectionType");
                        if (!projection_type_v || !projection_type_v->IsString() ||
                                rjson::to_string_view(*projection_type_v) != "KEYS_ONLY") {
                            co_return api_error::validation("VectorIndexUpdates Projection: only ProjectionType=KEYS_ONLY is currently supported.");
                        }
                    }
                    // A vector index will use CDC on this table, so the CDC
                    // log table name will need to fit our length limits
                    validate_cdc_log_name_length(builder.cf_name());
                    index_options_map index_options;
                    index_options[db::index::secondary_index::custom_class_option_name] = "vector_index";
                    index_options[cql3::statements::index_target::target_option_name] = sstring(attribute_name);
                    index_options["dimensions"] = std::to_string(dimensions);
                    builder.with_index(index_metadata{index_name, index_options,
                            index_metadata_kind::custom, index_metadata::is_local_index(false)});
                } else if (op == "Delete") {
                    if (!tab->has_index(index_name)) {
                        co_return api_error::resource_not_found(fmt::format(
                            "No vector index {} in table {}", index_name, tab->cf_name()));
                    }
                    builder.without_index(index_name);
                } else {
                    // Update operation not yet supported, as we don't yet
                    // have any updatable properties of vector indexes.
                    co_return api_error::validation(fmt::format(
                        "VectorIndexUpdates supports a Create or Delete operation, saw '{}'", op));
                }
            }

            schema = builder.build();
            std::vector<view_ptr> new_views;
            std::vector<std::string> dropped_views;

            rjson::value* gsi_updates = rjson::find(request, "GlobalSecondaryIndexUpdates");
            if (gsi_updates) {
                if (!gsi_updates->IsArray()) {
                    co_return api_error::validation("GlobalSecondaryIndexUpdates must be an array");
                }
                if (gsi_updates->Size() > 1) {
                    // Although UpdateTable takes an array of operations and could
                    // support multiple Create and/or Delete operations in one
                    // command, DynamoDB doesn't actually allows this, and throws
                    // a LimitExceededException if this is attempted.
                    co_return api_error::limit_exceeded("GlobalSecondaryIndexUpdates only allows one index creation or deletion");
                }
                if (vector_index_updates && vector_index_updates->IsArray() &&
                    vector_index_updates->Size() && gsi_updates->Size()) {
                    co_return api_error::limit_exceeded("UpdateTable cannot have both VectorIndexUpdates and GlobalSecondaryIndexUpdates in the same request");
                }
                if (gsi_updates->Size() == 1) {
                    empty_request = false;
                    if (!(*gsi_updates)[0].IsObject() || (*gsi_updates)[0].MemberCount() != 1) {
                        co_return api_error::validation("GlobalSecondaryIndexUpdates array must contain one object with a Create, Delete or Update operation");
                    }
                    auto it = (*gsi_updates)[0].MemberBegin();
                    const std::string_view op = rjson::to_string_view(it->name);
                    if (!it->value.IsObject()) {
                        co_return api_error::validation("GlobalSecondaryIndexUpdates entries must be objects");
                    }
                    const rjson::value* index_name_v = rjson::find(it->value, "IndexName");
                    if (!index_name_v || !index_name_v->IsString()) {
                        co_return api_error::validation("GlobalSecondaryIndexUpdates operation must have IndexName");
                    }
                    std::string_view index_name = rjson::to_string_view(*index_name_v);
                    std::string_view table_name = schema->cf_name();
                    std::string_view keyspace_name = schema->ks_name();
                    std::string vname(view_name(table_name, index_name));
                    if (op == "Create") {
                        const rjson::value* attribute_definitions = rjson::find(request, "AttributeDefinitions");
                        if (!attribute_definitions) {
                            co_return api_error::validation("GlobalSecondaryIndexUpdates Create needs AttributeDefinitions");
                        }
                        std::unordered_set<std::string> unused_attribute_definitions =
                            validate_attribute_definitions("GlobalSecondaryIndexUpdates", *attribute_definitions);
                        check_attribute_definitions_conflicts(*attribute_definitions, *schema);
                        for (auto& view : p.local().data_dictionary().find_column_family(tab).views()) {
                            check_attribute_definitions_conflicts(*attribute_definitions, *view);
                        }

                        if (p.local().data_dictionary().has_schema(keyspace_name, vname)) {
                            // Surprisingly, DynamoDB uses validation error here, not resource_in_use
                            co_return api_error::validation(fmt::format(
                                "GSI {} already exists in table {}", index_name, table_name));
                        }
                        if (p.local().data_dictionary().has_schema(keyspace_name, lsi_name(table_name, index_name, false))) {
                            co_return api_error::validation(fmt::format(
                                "LSI {} already exists in table {}, can't use same name for GSI", index_name, table_name));
                        }
                        if (tab->has_index(sstring(index_name))) {
                            co_return api_error::validation(fmt::format(
                                "Vector index {} already exists in table {}, cannot reuse the name for a GSI", index_name, table_name));
                        }
                        try {
                            locator::assert_rf_rack_valid_keyspace(keyspace_name, p.local().local_db().get_token_metadata_ptr(),
                                    p.local().local_db().find_keyspace(keyspace_name).get_replication_strategy());
                        } catch (const std::invalid_argument& ex) {
                            co_return api_error::validation(fmt::format("GlobalSecondaryIndexes on a table "
                                "using tablets require the number of racks in the cluster to be either 1 or 3"));
                        }

                        elogger.trace("Adding GSI {}", index_name);
                        // FIXME: read and handle "Projection" parameter. This will
                        // require the MV code to copy just parts of the attrs map.
                        schema_builder view_builder(keyspace_name, vname);
                        auto [view_hash_key, view_range_key] = parse_key_schema(it->value, "GlobalSecondaryIndexUpdates");
                        // If an attribute is already a real column in the base
                        // table (i.e., a key attribute in the base table),
                        // we can use it directly as a view key. Otherwise, we
                        // need to add it as a "computed column", which extracts
                        // and deserializes the attribute from the ":attrs" map.
                        bool view_hash_key_real_column =
                            schema->get_column_definition(to_bytes(view_hash_key));
                        add_column(view_builder, view_hash_key, *attribute_definitions, column_kind::partition_key, !view_hash_key_real_column);
                        unused_attribute_definitions.erase(view_hash_key);
                        if (!view_range_key.empty()) {
                            bool view_range_key_real_column =
                                schema->get_column_definition(to_bytes(view_range_key));
                            add_column(view_builder, view_range_key, *attribute_definitions, column_kind::clustering_key, !view_range_key_real_column);
                            if (!schema->get_column_definition(to_bytes(view_range_key)) &&
                                !schema->get_column_definition(to_bytes(view_hash_key))) {
                                // FIXME: This warning should go away. See issue #6714
                                elogger.warn("Only 1 regular column from the base table should be used in the GSI key in order to ensure correct liveness management without assumptions");
                            }
                            unused_attribute_definitions.erase(view_range_key);
                        }
                        // Surprisingly, although DynamoDB checks for unused
                        // AttributeDefinitions in CreateTable, it does not
                        // check it in UpdateTable. We decided to check anyway.
                        if (!unused_attribute_definitions.empty()) {
                            co_return api_error::validation(fmt::format(
                                "AttributeDefinitions defines spurious attributes not used by any KeySchema: {}",
                                unused_attribute_definitions));
                        }
                        // Base key columns which aren't part of the index's key need to
                        // be added to the view nonetheless, as (additional) clustering
                        // key(s).
                        for (auto& def : schema->primary_key_columns()) {
                            if  (def.name_as_text() != view_hash_key && def.name_as_text() != view_range_key) {
                                view_builder.with_column(def.name(), def.type, column_kind::clustering_key);
                            }
                        }
                        // GSIs have no tags:
                        view_builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>());
                        // Note below we don't need to add virtual columns, as all
                        // base columns were copied to view. TODO: reconsider the need
                        // for virtual columns when we support Projection.
                        for (const column_definition& regular_cdef : schema->regular_columns()) {
                            if (!view_builder.has_column(*cql3::to_identifier(regular_cdef))) {
                                view_builder.with_column(regular_cdef.name(), regular_cdef.type, column_kind::regular_column);
                            }
                        }
                        const bool include_all_columns = true;
                        view_builder.with_view_info(schema, include_all_columns, ""/*where clause*/);
                        new_views.emplace_back(view_builder.build());
                    } else if (op == "Delete") {
                        elogger.trace("Deleting GSI {}", index_name);
                        if (!p.local().data_dictionary().has_schema(keyspace_name, vname)) {
                            co_return api_error::resource_not_found(fmt::format("No GSI {} in table {}", index_name, table_name));
                        }
                        dropped_views.emplace_back(vname);
                    } else if (op == "Update") {
                        co_return api_error::validation("GlobalSecondaryIndexUpdates Update not yet supported");
                    } else {
                        co_return api_error::validation(fmt::format("GlobalSecondaryIndexUpdates supports a Create, Delete or Update operation, saw '{}'", op));
                    }
                }
            }

            if (empty_request) {
                co_return api_error::validation("UpdateTable requires one of GlobalSecondaryIndexUpdates, VectorIndexUpdates, StreamSpecification or BillingMode to be specified");
            }

            co_await verify_permission(enforce_authorization, warn_authorization, client_state_other_shard.get(), schema, auth::permission::ALTER, e.local()._stats);
            auto m = co_await service::prepare_column_family_update_announcement(p.local(), schema, std::vector<view_ptr>(), group0_guard.write_timestamp());
            for (view_ptr view : new_views) {
                auto m2 = co_await service::prepare_new_view_announcement(p.local(), view, group0_guard.write_timestamp());
                std::move(m2.begin(), m2.end(), std::back_inserter(m));
            }
            for (const std::string& view_name : dropped_views) {
                auto m2 = co_await service::prepare_view_drop_announcement(p.local(), schema->ks_name(), view_name, group0_guard.write_timestamp());
                std::move(m2.begin(), m2.end(), std::back_inserter(m));
            }
            // If a role is allowed to create a GSI, we should give it permissions
            // to read the GSI it just created. This is known as "auto-grant".
            // Also, when we delete a GSI we should revoke any permissions set on
            // it - so if it's ever created again the old permissions wouldn't be
            // remembered for the new GSI. This is known as "auto-revoke"
            if (client_state_other_shard.get().user() && (!new_views.empty() || !dropped_views.empty())) {
                service::group0_batch mc(std::move(group0_guard));
                mc.add_mutations(std::move(m));
                for (view_ptr view : new_views) {
                    auto resource = auth::make_data_resource(view->ks_name(), view->cf_name());
                    co_await auth::grant_applicable_permissions(
                        *client_state_other_shard.get().get_auth_service(), *client_state_other_shard.get().user(), resource, mc);
                }
                for (const auto& view_name : dropped_views) {
                    auto resource = auth::make_data_resource(schema->ks_name(), view_name);
                    co_await auth::revoke_all(*client_state_other_shard.get().get_auth_service(), resource, mc);
                }
                std::tie(m, group0_guard) = co_await std::move(mc).extract();
            }
            try {
                co_await mm.announce(std::move(m), std::move(group0_guard), format("alternator-executor: update {} table", tab->cf_name()));
                break;
            } catch (const service::group0_concurrent_modification& ex) {
                elogger.info("Failed to execute UpdateTable {} due to concurrent schema modifications. {}.",
                        tab->cf_name(), retries ? "Retrying" : "Number of retries exceeded, giving up");
                if (retries--) {
                    continue;
                }
                throw;
            }
        }
        co_await mm.wait_for_schema_agreement(p.local().local_db(), db::timeout_clock::now() + 10s, nullptr);

        rjson::value status = rjson::empty_object();
        supplement_table_info(request, *schema, p.local());
        rjson::add(status, "TableDescription", std::move(request));
        co_return rjson::print(std::move(status));
    });
}

// attribute_collector is a helper class used to accept several attribute
// puts or deletes, and collect them as single collection mutation.
// The implementation is somewhat complicated by the need of cells in a
// collection to be sorted by key order.
class attribute_collector {
    std::map<bytes, atomic_cell, serialized_compare> collected;
    void add(bytes&& name, atomic_cell&& cell) {
        collected.emplace(std::move(name), std::move(cell));
    }
    void add(const bytes& name, atomic_cell&& cell) {
        collected.emplace(name, std::move(cell));
    }
public:
    attribute_collector() : collected(attrs_type()->get_keys_type()->as_less_comparator()) { }
    void put(bytes&& name, const bytes& val, api::timestamp_type ts) {
        add(std::move(name), atomic_cell::make_live(*bytes_type, ts, val, atomic_cell::collection_member::yes));

    }
    void put(const bytes& name, const bytes& val, api::timestamp_type ts) {
        add(name, atomic_cell::make_live(*bytes_type, ts, val, atomic_cell::collection_member::yes));
    }
    void del(bytes&& name, api::timestamp_type ts) {
        add(std::move(name), atomic_cell::make_dead(ts, gc_clock::now()));
    }
    void del(const bytes& name, api::timestamp_type ts) {
        add(name, atomic_cell::make_dead(ts, gc_clock::now()));
    }
    collection_mutation_description to_mut() {
        collection_mutation_description ret;
        for (auto&& e : collected) {
            ret.cells.emplace_back(e.first, std::move(e.second));
        }
        return ret;
    }
    bool empty() const {
        return collected.empty();
    }
};

// Verify that a value parsed from the user input is legal. In particular,
// we check that the value is not an empty set, string or bytes - which is
// (somewhat artificially) forbidden by DynamoDB.
void validate_value(const rjson::value& v, const char* caller) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error::validation(format("{}: improperly formatted value '{}'", caller, v));
    }
    auto it = v.MemberBegin();
    const std::string_view type = rjson::to_string_view(it->name);
    if (type == "SS" || type == "BS" || type == "NS") {
        if (!it->value.IsArray()) {
            throw api_error::validation(format("{}: improperly formatted set '{}'", caller, v));
        }
        if (it->value.Size() == 0) {
            throw api_error::validation(format("{}: empty set not allowed", caller));
        }
    } else if (type == "S" || type == "B") {
        if (!it->value.IsString()) {
            throw api_error::validation(format("{}: improperly formatted value '{}'", caller, v));
        }
    } else if (type == "N") {
        if (!it->value.IsString()) {
            // DynamoDB uses a SerializationException in this case, not ValidationException.
            throw api_error::serialization(format("{}: number value must be encoded as string '{}'", caller, v));
        }
    } else if (type != "L" && type != "M" && type != "BOOL" && type != "NULL") {
        // TODO: can do more sanity checks on the content of the above types.
        throw api_error::validation(fmt::format("{}: unknown type {} for value {}", caller, type, v));
    }
}

// The put_or_delete_item class builds the mutations needed by the PutItem and
// DeleteItem operations - either as stand-alone commands or part of a list
// of commands in BatchWriteItem.
// put_or_delete_item splits each operation into two stages: Constructing the
// object parses and validates the user input (throwing exceptions if there
// are input errors). Later, build() generates the actual mutation, with a
// specified timestamp. This split is needed because of the peculiar needs of
// BatchWriteItem and LWT. BatchWriteItem needs all parsing to happen before
// any writing happens (if one of the commands has an error, none of the
// writes should be done). LWT makes it impossible for the parse step to
// generate "mutation" objects, because the timestamp still isn't known.
class put_or_delete_item {
private:
    partition_key _pk;
    clustering_key _ck;
    struct cell {
        bytes column_name;
        bytes value;
    };
    // PutItem: engaged _cells, write these cells to item (_pk, _ck).
    // DeleteItem: disengaged _cells, delete the entire item (_pk, _ck).
    std::optional<std::vector<cell>> _cells;
    // WCU calculation takes into account some length in bytes,
    // that length can have different meaning depends on the operation but the
    // the calculation of length in bytes to WCU is the same.
    uint64_t _length_in_bytes = 0;
public:
    struct delete_item {};
    struct put_item {};
    put_or_delete_item(const rjson::value& key, schema_ptr schema, delete_item);
    put_or_delete_item(const rjson::value& item, schema_ptr schema, put_item, std::unordered_map<bytes, std::string> key_attributes);
    // put_or_delete_item doesn't keep a reference to schema (so it can be
    // moved between shards for LWT) so it needs to be given again to build():
    mutation build(schema_ptr schema, api::timestamp_type ts) const;
    const partition_key& pk() const { return _pk; }
    const clustering_key& ck() const { return _ck; }
    uint64_t length_in_bytes() const noexcept {
        return _length_in_bytes;
    }
    void set_length_in_bytes(uint64_t length) noexcept {
        _length_in_bytes = length;
    }
    bool is_put_item() noexcept {
        return _cells.has_value();
    }
};

put_or_delete_item::put_or_delete_item(const rjson::value& key, schema_ptr schema, delete_item)
        : _pk(pk_from_json(key, schema)), _ck(ck_from_json(key, schema)) {
    check_key(key, schema);
}

// find_attribute() checks whether the named attribute is stored in the
// schema as a real column (we do this for key attribute, and for a GSI key)
// and if so, returns that column. If not, the function returns nullptr,
// telling the caller that the attribute is stored serialized in the
// ATTRS_COLUMN_NAME map - not in a stand-alone column in the schema.
static inline const column_definition* find_attribute(const schema& schema, const bytes& attribute_name) {
    const column_definition* cdef = schema.get_column_definition(attribute_name);
    // Although ATTRS_COLUMN_NAME exists as an actual column, when used as an
    // attribute name it should refer to an attribute inside ATTRS_COLUMN_NAME
    // not to ATTRS_COLUMN_NAME itself. This if() is needed for #5009.
    if (cdef && cdef->name() == executor::ATTRS_COLUMN_NAME) {
        return nullptr;
    }
    return cdef;
}


// Get a list of all attributes that serve as a key attributes for any of the
// GSIs or LSIs of this table, and the declared type for each (can be only
// "S", "B", or "N"). The implementation below will also list the base table's
// key columns (they are the views' clustering keys).
std::unordered_map<bytes, std::string> si_key_attributes(data_dictionary::table t) {
    std::unordered_map<bytes, std::string> ret;
    for (const view_ptr& v : t.views()) {
        for (const column_definition& cdef : v->partition_key_columns()) {
            ret[cdef.name()] = type_to_string(cdef.type);
        }
        for (const column_definition& cdef : v->clustering_key_columns()) {
            ret[cdef.name()] = type_to_string(cdef.type);
        }
    }
    return ret;
}

// Get a map from attribute name (bytes) to dimensions (int) for all vector
// indexes defined on this table's schema. Used to validate written values
// against the vector index constraints.
static std::unordered_map<bytes, int> vector_index_attributes(const schema& s) {
    std::unordered_map<bytes, int> ret;
    for (const index_metadata& im : s.indices()) {
        const auto& opts = im.options();
        auto class_it = opts.find(db::index::secondary_index::custom_class_option_name);
        if (class_it == opts.end() || class_it->second != "vector_index") {
            continue;
        }
        auto target_it = opts.find(cql3::statements::index_target::target_option_name);
        auto dims_it = opts.find("dimensions");
        if (target_it == opts.end() || dims_it == opts.end()) {
            continue;
        }
        try {
            ret[to_bytes(target_it->second)] = std::stoi(dims_it->second);
        } catch (...) {}
    }
    return ret;
}

// When an attribute is a key (hash or sort) of one of the GSIs or LSIs on a
// table, DynamoDB refuses an update to that attribute with an unsuitable
// value. Unsuitable values are:
//   1. An empty string (those are normally allowed as values, but not allowed
//      as keys, including GSI keys).
//   2. A value with a type different than that declared for the GSI key.
//      Normally non-key attributes can take values of any type (DynamoDB is
//      schema-less), but as soon as an attribute is used as a GSI key, it
//      must be set only to the specific type declared for that key.
//   (Note that a missing value for an GSI key attribute is fine - the update
//   will happen on the base table, but won't reach the view table. In this
//   case, this function simply won't be called for this attribute.)
//
// This function checks if the given attribute update is an update to some
// GSI's key, and if the value is unsuitable, an api_error::validation is
// thrown. The checking here is similar to the checking done in
// get_key_from_typed_value() for the base table's key columns.
//
// validate_value_if_index_key() should only be called after validate_value()
// already validated that the value itself has a valid form.
static void validate_value_if_index_key(
        std::unordered_map<bytes, std::string> key_attributes,
        const bytes& attribute,
        const rjson::value& value) {
    auto it = key_attributes.find(attribute);
    if (it == key_attributes.end()) {
        // Given attribute is not a key column with a fixed type, so no
        // more validation to do.
        return;
    }
    const std::string& expected_type = it->second;
    // We assume that validate_value() was previously called on this value,
    // so value is known to be of the proper format (an object with one
    // member, whose key and value are strings)
    std::string_view value_type = rjson::to_string_view(value.MemberBegin()->name);
    if (expected_type != value_type) {
        throw api_error::validation(fmt::format(
            "Type mismatch: expected type {} for GSI or LSI key attribute {}, got type {}",
            expected_type, to_string_view(attribute), value_type));
    }
    std::string_view value_content = rjson::to_string_view(value.MemberBegin()->value);
    if (value_content.empty()) {
        throw api_error::validation(fmt::format(
            "GSI or LSI key attribute {} cannot be set to an empty string", to_string_view(attribute)));
    }
}

// When an attribute is the target of a vector index on the table, a write
// to that attribute is rejected unless the value is a DynamoDB list (type
// "L") of exactly the declared number of numeric (type "N") elements, where
// each number can be represented as a 32-bit float.
//
// validate_value_if_vector_index_attribute() should only be called after
// validate_value() already confirmed the value has a valid DynamoDB form.
static void validate_value_if_vector_index_attribute(
        const std::unordered_map<bytes, int>& vector_attrs,
        const bytes& attribute,
        const rjson::value& value) {
    auto it = vector_attrs.find(attribute);
    if (it == vector_attrs.end()) {
        return;
    }
    int dimensions = it->second;
    std::string_view attr_name = to_string_view(attribute);
    // value is a DynamoDB typed value: an object with one member whose key
    // is the type tag. validate_value() already checked the overall shape.
    std::string_view value_type = rjson::to_string_view(value.MemberBegin()->name);
    if (value_type != "L") {
        throw api_error::validation(fmt::format(
            "Vector index attribute '{}' must be a list of {} numbers, got type {}",
            attr_name, dimensions, value_type));
    }
    const rjson::value& list = value.MemberBegin()->value;
    if (!list.IsArray() || (int)list.Size() != dimensions) {
        throw api_error::validation(fmt::format(
            "Vector index attribute '{}' must be a list of exactly {} numbers, got {} elements",
            attr_name, dimensions, list.IsArray() ? (int)list.Size() : -1));
    }
    for (const rjson::value& elem : list.GetArray()) {
        if (!elem.IsObject() || elem.MemberCount() != 1 ||
                rjson::to_string_view(elem.MemberBegin()->name) != "N") {
            throw api_error::validation(fmt::format(
                "Vector index attribute '{}' must contain only numbers", attr_name));
        }
        std::string_view num_str = rjson::to_string_view(elem.MemberBegin()->value);
        float f;
        auto [ptr, ec] = std::from_chars(num_str.data(), num_str.data() + num_str.size(), f);
        if (ec != std::errc{} || ptr != num_str.data() + num_str.size() || !std::isfinite(f)) {
            throw api_error::validation(fmt::format(
                "Vector index attribute '{}' element '{}' cannot be represented as a 32-bit float",
                attr_name, num_str));
        }
    }
}

put_or_delete_item::put_or_delete_item(const rjson::value& item, schema_ptr schema, put_item, std::unordered_map<bytes, std::string> key_attributes)
        : _pk(pk_from_json(item, schema)), _ck(ck_from_json(item, schema)) {
    _cells = std::vector<cell>();
    _cells->reserve(item.MemberCount());
    auto vec_attrs = vector_index_attributes(*schema);
    for (auto it = item.MemberBegin(); it != item.MemberEnd(); ++it) {
        bytes column_name = to_bytes(rjson::to_string_view(it->name));
        validate_value(it->value, "PutItem");
        const column_definition* cdef = find_attribute(*schema, column_name);
        validate_attr_name_length("", column_name.size(), cdef && cdef->is_primary_key());
        _length_in_bytes += column_name.size();
        if (!cdef) {
            // This attribute may be a key column of one of the GSI or LSI,
            // in which case there are some limitations on the value.
            if (!key_attributes.empty()) {
                validate_value_if_index_key(key_attributes, column_name, it->value);
            }
            // This attribute may also be a vector index target column,
            // in which case it must be a list of the right number of floats.
            if (!vec_attrs.empty()) {
                validate_value_if_vector_index_attribute(vec_attrs, column_name, it->value);
            }
            bytes value = serialize_item(it->value);
            if (value.size()) {
                // ScyllaDB uses one extra byte compared to DynamoDB for the bytes length
                _length_in_bytes += value.size() - 1;
            }
            _cells->push_back({std::move(column_name), serialize_item(it->value)});
        } else if (!cdef->is_primary_key()) {
            // Fixed-type regular columns were used for LSIs and also (in the
            // slightly more distant past) GSIs, before they were moved to
            // :attrs. We keep this branch for backward compatibility.
            bytes value = get_key_from_typed_value(it->value, *cdef);
            _cells->push_back({std::move(column_name), value});
            if (value.size()) {
                // ScyllaDB uses one extra byte compared to DynamoDB for the bytes length
                _length_in_bytes += value.size() - 1;
            }
        }
    }
    if (_pk.representation().size() > 2) {
        // ScyllaDB uses two extra bytes compared to DynamoDB for the key bytes length
        _length_in_bytes += _pk.representation().size() - 2;
    }
    if (_ck.representation().size() > 2) {
        // ScyllaDB uses two extra bytes compared to DynamoDB for the key bytes length
        _length_in_bytes += _ck.representation().size() - 2;
    }
}

mutation put_or_delete_item::build(schema_ptr schema, api::timestamp_type ts) const {
    mutation m(schema, _pk);
    // If there's no clustering key, a tombstone should be created directly
    // on a partition, not on a clustering row - otherwise it will look like
    // an open-ended range tombstone, which will crash on KA/LA sstable format.
    // Ref: #6035
    const bool use_partition_tombstone = schema->clustering_key_size() == 0;
    if (!_cells) {
        if (use_partition_tombstone) {
            m.partition().apply(tombstone(ts, gc_clock::now()));
        } else {
            // a DeleteItem operation:
            m.partition().clustered_row(*schema, _ck).apply(tombstone(ts, gc_clock::now()));
        }
        return m;
    }
    // else, a PutItem operation:
    auto& row = m.partition().clustered_row(*schema, _ck);
    attribute_collector attrs_collector;
    for (auto& c : *_cells) {
        const column_definition* cdef = find_attribute(*schema, c.column_name);
        if (!cdef) {
            attrs_collector.put(c.column_name, c.value, ts);
        } else {
            row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, std::move(c.value)));
        }
    }
    auto attrs = attrs_column(*schema);
    if (!attrs_collector.empty()) {
        auto serialized_map = attrs_collector.to_mut().serialize(*attrs_type());
        row.cells().apply(attrs, std::move(serialized_map));
    }
    // To allow creation of an item with no attributes, we need a row marker.
    row.apply(row_marker(ts));
    // PutItem is supposed to completely replace the old item, so we need to
    // also have a tombstone removing old cells. Important points:
    // 1) Alternator's schema is dynamic, therefore we store data in a map
    //    in column :attrs. Since we're replacing a row, invalidating only
    //    :attrs is enough. Alternator base tables also had columns for LSI
    //    keys and GSI keys. New tables no longer have such columns, but old
    //    tables created in the past may still have them.
    // 2) We use a collection tombstone for the :attrs column instead of a row
    //    tombstone. While a row tombstone would also replace the data, it has
    //    an undesirable side effect for CDC, which would report it as a
    //    separate deletion event. To model PutItem's "replace" semantic, we
    //    leverage a corner case: a collection tombstone at ts-1 paired with an
    //    upsert at ts is not reported by CDC as a separate REMOVE event. We
    //    can't use the timestamp ts, because when data and tombstone tie on
    //    timestamp, the tombstone wins. These tricks were introduced in
    //    Scylla to handle collection replacements in CQL (see #6084, PR #6491,
    //    e.g. cql3::maps::setter::execute()) and we utilize it to avoid
    //    emitting the REMOVE event (resolving #6930).
    row.cells().apply(attrs, collection_mutation_description{tombstone{ts - 1, gc_clock::now()}}.serialize(*attrs.type));
    // Note that for old tables created with regular LSI and GSI key columns,
    // we must also delete the regular columns that are not part of the new
    // schema consisting of pk, ck, and :attrs.
    for (const auto& cdef : schema->regular_columns()) {
        if (cdef.name_as_text() != executor::ATTRS_COLUMN_NAME) {
            row.cells().apply(cdef, atomic_cell::make_dead(ts - 1, gc_clock::now()));
        }
    }
    return m;
}

// The DynamoDB API doesn't let the client control the server's timeout, so
// we have a global default_timeout() for Alternator requests. The value of
// s_default_timeout_ms is overwritten in alternator::controller::start_server()
// based on the "alternator_timeout_in_ms" configuration parameter.
thread_local utils::updateable_value<uint32_t> executor::s_default_timeout_in_ms{10'000};
db::timeout_clock::time_point executor::default_timeout() {
    return db::timeout_clock::now() + std::chrono::milliseconds(s_default_timeout_in_ms);
}

static lw_shared_ptr<query::read_command> previous_item_read_command(service::storage_proxy& proxy,
        schema_ptr schema,
        const clustering_key& ck,
        shared_ptr<cql3::selection::selection> selection) {
    std::vector<query::clustering_range> bounds;
    if (schema->clustering_key_size() == 0) {
        bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    } else {
        bounds.push_back(query::clustering_range::make_singular(ck));
    }
    // FIXME: We pretend to take a selection (all callers currently give us a
    // wildcard selection...) but here we read the entire item anyway. We
    // should take the column list from selection instead of building it here.
    auto regular_columns =
            schema->regular_columns() | std::views::transform(&column_definition::id)
            |  std::ranges::to<query::column_id_vector>();
    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    return ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, proxy.get_max_result_size(partition_slice),
            query::tombstone_limit(proxy.get_tombstone_limit()));
}

static dht::partition_range_vector to_partition_ranges(const schema& schema, const partition_key& pk) {
    return dht::partition_range_vector{dht::partition_range(dht::decorate_key(schema, pk))};
}
static dht::partition_range_vector to_partition_ranges(const dht::decorated_key& pk) {
    return dht::partition_range_vector{dht::partition_range(pk)};
}

// Parse the different options for the ReturnValues parameter. We parse all
// the known options, but only UpdateItem actually supports all of them. The
// other operations (DeleteItem and PutItem) will refuse some of them.
rmw_operation::returnvalues rmw_operation::parse_returnvalues(const rjson::value& request) {
    const rjson::value* attribute_value = rjson::find(request, "ReturnValues");
    if (!attribute_value) {
        return rmw_operation::returnvalues::NONE;
    }
    if (!attribute_value->IsString()) {
        throw api_error::validation(format("Expected string value for ReturnValues, got: {}", *attribute_value));
    }
    auto s = rjson::to_string_view(*attribute_value);
    if (s == "NONE") {
        return rmw_operation::returnvalues::NONE;
    } else if (s == "ALL_OLD") {
        return rmw_operation::returnvalues::ALL_OLD;
    } else if (s == "UPDATED_OLD") {
        return rmw_operation::returnvalues::UPDATED_OLD;
    } else if (s == "ALL_NEW") {
        return rmw_operation::returnvalues::ALL_NEW;
    } else if (s == "UPDATED_NEW") {
        return rmw_operation::returnvalues::UPDATED_NEW;
    } else {
        throw api_error::validation(fmt::format("Unrecognized value for ReturnValues: {}", s));
    }
}

rmw_operation::returnvalues_on_condition_check_failure
rmw_operation::parse_returnvalues_on_condition_check_failure(const rjson::value& request) {
    const rjson::value* attribute_value = rjson::find(request, "ReturnValuesOnConditionCheckFailure");
    if (!attribute_value) {
        return rmw_operation::returnvalues_on_condition_check_failure::NONE;
    }
    if (!attribute_value->IsString()) {
        throw api_error::validation(format("Expected string value for ReturnValuesOnConditionCheckFailure, got: {}", *attribute_value));
    }
    auto s = rjson::to_string_view(*attribute_value);
    if (s == "NONE") {
        return rmw_operation::returnvalues_on_condition_check_failure::NONE;
    } else if (s == "ALL_OLD") {
        return rmw_operation::returnvalues_on_condition_check_failure::ALL_OLD;
    } else {
        throw api_error::validation(fmt::format("Unrecognized value for ReturnValuesOnConditionCheckFailure: {}", s));
    }
}

rmw_operation::rmw_operation(service::storage_proxy& proxy, rjson::value&& request)
    : _request(std::move(request))
    , _schema(get_table_for_write(proxy, _request))
    , _write_isolation(get_write_isolation_for_schema(_schema))
    , _consumed_capacity(_request)
    , _returnvalues(parse_returnvalues(_request))
    , _returnvalues_on_condition_check_failure(parse_returnvalues_on_condition_check_failure(_request))
{
    // _pk and _ck will be assigned later, by the subclass's constructor
    // (each operation puts the key in a slightly different location in
    // the request).
}

std::optional<mutation> rmw_operation::apply(foreign_ptr<lw_shared_ptr<query::result>> qr, const query::partition_slice& slice, api::timestamp_type ts, cdc::per_request_options& cdc_opts) {
    if (qr->row_count()) {
        auto selection = cql3::selection::selection::wildcard(_schema);
        uint64_t item_length = 0;
        auto previous_item = describe_single_item(_schema, slice, *selection, *qr, {}, &item_length);
        if (_consumed_capacity._total_bytes < item_length) {
            _consumed_capacity._total_bytes = item_length;
        }
        if (previous_item) {
            if (should_fill_preimage()) {
                cdc_opts.preimage = make_lw_shared<cql3::untyped_result_set>(*_schema, std::move(qr), *selection, slice);
            }
            return apply(std::make_unique<rjson::value>(std::move(*previous_item)), ts, cdc_opts);
        }
    }
    return apply(std::unique_ptr<rjson::value>(), ts, cdc_opts);
}

rmw_operation::write_isolation rmw_operation::get_write_isolation_for_schema(schema_ptr schema) {
    const auto tags_ptr = db::get_tags_of_table(schema);
    if (!tags_ptr) {
        // Tags missing entirely from this table. This can't happen for a
        // normal Alternator table, but can happen if get_table_for_write()
        // allowed writing to a non-Alternator table (e.g., an internal table).
        // If it is a system table, LWT will not work (and is also pointless
        // for non-distributed tables), so use UNSAFE_RMW.
        if(is_internal_keyspace(schema->ks_name())) {
            return write_isolation::UNSAFE_RMW;
        } else {
            return default_write_isolation;
        }
    }
    auto it = tags_ptr->find(WRITE_ISOLATION_TAG_KEY);
    if (it == tags_ptr->end() || it->second.empty()) {
        return default_write_isolation;
    }
    return parse_write_isolation(it->second);
}

// shard_for_execute() checks whether execute() must be called on a specific
// other shard. Running execute() on a specific shard is necessary only if it
// will use LWT (storage_proxy::cas()). This is because cas() can only be
// called on the specific shard owning (as per get_cas_shard()) _pk's token.
// Knowing if execute() will call cas() or not may depend on whether there is
// a read-before-write, but not just on it - depending on configuration,
// execute() may unconditionally use cas() for every write. Unfortunately,
// this requires duplicating here a bit of logic from execute().
// The returned cas_shard must be passed to execute() to ensure
// the tablet shard won't change. The caller must hold the returned object for
// the duration of execution, even if we were already on the right shard - so it doesn't move.
std::optional<service::cas_shard> rmw_operation::shard_for_execute(bool needs_read_before_write) {
    if (_write_isolation == write_isolation::FORBID_RMW ||
        (_write_isolation == write_isolation::LWT_RMW_ONLY && !needs_read_before_write) ||
        _write_isolation == write_isolation::UNSAFE_RMW) {
        return {};
    }
    // If we're still here, cas() *will* be called by execute(), so let's
    // find the appropriate shard to run it on:
    const auto token = dht::get_token(*_schema, _pk);
    return service::cas_shard(*_schema, token);
}

// Build the return value from the different RMW operations (UpdateItem,
// PutItem, DeleteItem). All these return nothing by default, but can
// optionally return Attributes if requested via the ReturnValues option.
static executor::request_return_type rmw_operation_return(rjson::value&& attributes, const consumed_capacity_counter& consumed_capacity, uint64_t& metric) {
    rjson::value ret = rjson::empty_object();
    consumed_capacity.add_consumed_capacity_to_response_if_needed(ret);
    metric += consumed_capacity.get_consumed_capacity_units();
    if (!attributes.IsNull()) {
        rjson::add(ret, "Attributes", std::move(attributes));
    }
    return rjson::print(std::move(ret));
}

static future<std::unique_ptr<rjson::value>> get_previous_item(
            service::storage_proxy& proxy,
            service::client_state& client_state,
            schema_ptr schema,
            const partition_key& pk,
            const clustering_key& ck,
            service_permit permit,
            db::consistency_level cl,
            uint64_t& item_length)
    {
        auto selection = cql3::selection::selection::wildcard(schema);
        auto command = previous_item_read_command(proxy, schema, ck, selection);
        command->allow_limit = db::allow_per_partition_rate_limit::yes;
        return proxy.query(schema, command, to_partition_ranges(*schema, pk), cl, service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state)).then(
            [schema, command, selection = std::move(selection), &item_length] (service::storage_proxy::coordinator_query_result qr) {
        auto previous_item = describe_single_item(schema, command->slice, *selection, *qr.query_result, {}, &item_length);
        if (previous_item) {
            return make_ready_future<std::unique_ptr<rjson::value>>(std::make_unique<rjson::value>(std::move(*previous_item)));
        } else {
            return make_ready_future<std::unique_ptr<rjson::value>>();
        }
    });
}

static future<std::unique_ptr<rjson::value>> get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const partition_key& pk,
        const clustering_key& ck,
        service_permit permit,
        alternator::stats& global_stats,
        alternator::stats& per_table_stats,
        uint64_t& item_length)
{
    global_stats.reads_before_write++;
    per_table_stats.reads_before_write++;
    return get_previous_item(proxy, client_state, schema, pk, ck, permit, db::consistency_level::LOCAL_QUORUM, item_length);
}

static future<uint64_t> get_previous_item_size(
            service::storage_proxy& proxy,
            service::client_state& client_state,
            schema_ptr schema,
            const partition_key& pk,
            const clustering_key& ck,
            service_permit permit) {
    uint64_t item_length = 0;
    // The use of get_previous_item here is for DynamoDB calculation compatibility mode,
    // and the actual value is ignored. For performance reasons, we use CL_LOCAL_ONE.
    co_await  get_previous_item(proxy, client_state, schema, pk, ck, permit, db::consistency_level::LOCAL_ONE, item_length);
    co_return item_length;
}

future<executor::request_return_type> rmw_operation::execute(service::storage_proxy& proxy,
        std::optional<service::cas_shard> cas_shard,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit,
        bool needs_read_before_write,
        stats& global_stats,
        stats& per_table_stats,
        uint64_t& wcu_total) {
    auto cdc_opts = cdc::per_request_options{
        .alternator = true,
        .alternator_streams_increased_compatibility = schema()->cdc_options().enabled() && proxy.data_dictionary().get_config().alternator_streams_increased_compatibility(),
    };
    if (needs_read_before_write) {
        if (_write_isolation == write_isolation::FORBID_RMW) {
            throw api_error::validation("Read-modify-write operations are disabled by 'forbid_rmw' write isolation policy. Refer to https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more information.");
        }
        global_stats.reads_before_write++;
        per_table_stats.reads_before_write++;
        if (_write_isolation == write_isolation::UNSAFE_RMW) {
            // This is the old, unsafe, read before write which does first
            // a read, then a write. TODO: remove this mode entirely.
            return get_previous_item(proxy, client_state, schema(), _pk, _ck, permit, global_stats, per_table_stats, _consumed_capacity._total_bytes).then(
                    [this, &proxy, &wcu_total, trace_state, permit = std::move(permit), cdc_opts = std::move(cdc_opts)] (std::unique_ptr<rjson::value> previous_item) mutable {
                std::optional<mutation> m = apply(std::move(previous_item), api::new_timestamp(), cdc_opts);
                if (!m) {
                    return make_ready_future<executor::request_return_type>(api_error::conditional_check_failed("The conditional request failed", std::move(_return_attributes)));
                }
                return proxy.mutate(utils::chunked_vector<mutation>{std::move(*m)}, db::consistency_level::LOCAL_QUORUM, executor::default_timeout(), trace_state, std::move(permit), db::allow_per_partition_rate_limit::yes, false, std::move(cdc_opts)).then([this,&wcu_total] () mutable {
                    return rmw_operation_return(std::move(_return_attributes), _consumed_capacity, wcu_total);
                });
            });
        }
    } else if (_write_isolation != write_isolation::LWT_ALWAYS) {
        std::optional<mutation> m = apply(nullptr, api::new_timestamp(), cdc_opts);
        throwing_assert(m); // !needs_read_before_write, so apply() did not check a condition
        return proxy.mutate(utils::chunked_vector<mutation>{std::move(*m)}, db::consistency_level::LOCAL_QUORUM, executor::default_timeout(), trace_state, std::move(permit), db::allow_per_partition_rate_limit::yes, false, std::move(cdc_opts)).then([this, &wcu_total] () mutable {
            return rmw_operation_return(std::move(_return_attributes), _consumed_capacity, wcu_total);
        });
    }
    throwing_assert(cas_shard);
    // If we're still here, we need to do this write using LWT:
    global_stats.write_using_lwt++;
    per_table_stats.write_using_lwt++;
    auto timeout = executor::default_timeout();
    auto selection = cql3::selection::selection::wildcard(schema());
    auto read_command = needs_read_before_write ?
            previous_item_read_command(proxy, schema(), _ck, selection) :
            nullptr;
    return proxy.cas(schema(), std::move(*cas_shard), *this, read_command, to_partition_ranges(*schema(), _pk),
            {timeout, std::move(permit), client_state, trace_state},
            db::consistency_level::LOCAL_SERIAL, db::consistency_level::LOCAL_QUORUM, timeout, timeout, true, std::move(cdc_opts)).then([this, read_command, &wcu_total] (bool is_applied) mutable {
        if (!is_applied) {
            return make_ready_future<executor::request_return_type>(api_error::conditional_check_failed("The conditional request failed", std::move(_return_attributes)));
        }
        return make_ready_future<executor::request_return_type>(rmw_operation_return(std::move(_return_attributes), _consumed_capacity, wcu_total));
    });
}

static parsed::condition_expression get_parsed_condition_expression(parsed::expression_cache& parsed_expression_cache, rjson::value& request) {
    rjson::value* condition_expression = rjson::find(request, "ConditionExpression");
    if (!condition_expression) {
        // Returning an empty() condition_expression means no condition.
        return parsed::condition_expression{};
    }
    if (!condition_expression->IsString()) {
        throw api_error::validation("ConditionExpression must be a string");
    }
    if (condition_expression->GetStringLength() == 0) {
        throw api_error::validation("ConditionExpression must not be empty");
    }
    try {
        return parsed_expression_cache.parse_condition_expression(rjson::to_string_view(*condition_expression), "ConditionExpression");
    } catch(expressions_syntax_error& e) {
        throw api_error::validation(e.what());
    }
}

static bool check_needs_read_before_write(const parsed::condition_expression& condition_expression) {
    // Theoretically, a condition expression may exist but not refer to the
    // item at all. But this is not a useful case and there is no point in
    // optimizing for it.
    return !condition_expression.empty();
}

class put_item_operation : public rmw_operation {
private:
    put_or_delete_item _mutation_builder;
public:
    parsed::condition_expression _condition_expression;
    put_item_operation(parsed::expression_cache& parsed_expression_cache, service::storage_proxy& proxy, rjson::value&& request)
        : rmw_operation(proxy, std::move(request))
        , _mutation_builder(rjson::get(_request, "Item"), schema(), put_or_delete_item::put_item{},
            si_key_attributes(proxy.data_dictionary().find_table(schema()->ks_name(), schema()->cf_name()))) {
        _pk = _mutation_builder.pk();
        _ck = _mutation_builder.ck();
        if (_returnvalues != returnvalues::NONE && _returnvalues != returnvalues::ALL_OLD) {
            throw api_error::validation(format("PutItem supports only NONE or ALL_OLD for ReturnValues"));
        }
        _condition_expression = get_parsed_condition_expression(parsed_expression_cache, _request);
        const rjson::value* expression_attribute_names = rjson::find(_request, "ExpressionAttributeNames");
        const rjson::value* expression_attribute_values = rjson::find(_request, "ExpressionAttributeValues");
        if (!_condition_expression.empty()) {
            std::unordered_set<std::string> used_attribute_names;
            std::unordered_set<std::string> used_attribute_values;
            resolve_condition_expression(_condition_expression,
                    expression_attribute_names, expression_attribute_values,
                    used_attribute_names, used_attribute_values);
            verify_all_are_used(expression_attribute_names, used_attribute_names,"ExpressionAttributeNames", "PutItem");
            verify_all_are_used(expression_attribute_values, used_attribute_values,"ExpressionAttributeValues", "PutItem");
        } else {
            if (expression_attribute_names) {
                throw api_error::validation("ExpressionAttributeNames cannot be used without ConditionExpression");
            }
            if (expression_attribute_values) {
                throw api_error::validation("ExpressionAttributeValues cannot be used without ConditionExpression");
            }
        }
        _consumed_capacity += _mutation_builder.length_in_bytes();
    }
    bool needs_read_before_write() const {
        return _request.HasMember("Expected") ||
               check_needs_read_before_write(_condition_expression) ||
               _returnvalues == returnvalues::ALL_OLD;
    }
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts, cdc::per_request_options& cdc_opts) const override {
        if (!verify_expected(_request, previous_item.get()) ||
            !verify_condition_expression(_condition_expression, previous_item.get())) {
            if (previous_item && _returnvalues_on_condition_check_failure ==
                returnvalues_on_condition_check_failure::ALL_OLD) {
                _return_attributes = std::move(*previous_item);
            }
            // If the update is to be cancelled because of an unfulfilled Expected
            // condition, return an empty optional mutation, which is more
            // efficient than throwing an exception.
            return {};
        }
        if (_returnvalues == returnvalues::ALL_OLD && previous_item) {
            _return_attributes = std::move(*previous_item);
        } else {
            _return_attributes = {};
        }
        return _mutation_builder.build(_schema, ts);
    }
    virtual ~put_item_operation() = default;
};

future<executor::request_return_type> executor::put_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.put_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("put_item {}", request);

    auto op = make_shared<put_item_operation>(*_parsed_expression_cache, _proxy, std::move(request));

    if (!audit_info) {
        // On LWT shard bounce, audit_info is already set on the originating shard.
        // The reference captured in the bounce lambda points back to the original
        // coroutine frame, which remains alive for the entire cross-shard call.
        // Only reads of this pointer occur on the target shard — no writes or frees.
        maybe_audit(audit_info, audit::statement_category::DML, op->schema()->ks_name(),
                    op->schema()->cf_name(), "PutItem", op->request(), db::consistency_level::LOCAL_QUORUM);
    }

    tracing::add_alternator_table_name(trace_state, op->schema()->cf_name());
    const bool needs_read_before_write = op->needs_read_before_write();

    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, op->schema(), auth::permission::MODIFY, _stats);

    auto cas_shard = op->shard_for_execute(needs_read_before_write);

    if (cas_shard && !cas_shard->this_shard()) {
        _stats.api_operations.put_item--; // uncount on this shard, will be counted in other shard
        _stats.shard_bounce_for_lwt++;
        co_return co_await container().invoke_on(cas_shard->shard(), _ssg,
                [request = std::move(*op).move_request(), cs = client_state.move_to_other_shard(), gt = tracing::global_trace_state_ptr(trace_state), permit = std::move(permit), &audit_info]
                (executor& e) mutable {
            return do_with(cs.get(), [&e, request = std::move(request), trace_state = tracing::trace_state_ptr(gt), &audit_info]
                                     (service::client_state& client_state) mutable {
                //FIXME: Instead of passing empty_service_permit() to the background operation,
                // the current permit's lifetime should be prolonged, so that it's destructed
                // only after all background operations are finished as well.
                return e.put_item(client_state, std::move(trace_state), empty_service_permit(), std::move(request), audit_info);
            });
        });
    }
    lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *(op->schema()));
    per_table_stats->api_operations.put_item++;
    uint64_t wcu_total = 0;
    auto res = co_await op->execute(_proxy, std::move(cas_shard), client_state, trace_state, std::move(permit), needs_read_before_write, _stats, *per_table_stats, wcu_total);
    per_table_stats->operation_sizes.put_item_op_size_kb.add(bytes_to_kb_ceil(op->consumed_capacity()._total_bytes));
    per_table_stats->wcu_total[stats::wcu_types::PUT_ITEM] += wcu_total;
    _stats.wcu_total[stats::wcu_types::PUT_ITEM] += wcu_total;
    per_table_stats->api_operations.put_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    _stats.api_operations.put_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    co_return res;
}

class delete_item_operation : public rmw_operation {
private:
    put_or_delete_item _mutation_builder;
public:
    parsed::condition_expression _condition_expression;
    delete_item_operation(parsed::expression_cache& parsed_expression_cache, service::storage_proxy& proxy, rjson::value&& request)
        : rmw_operation(proxy, std::move(request))
        , _mutation_builder(rjson::get(_request, "Key"), schema(), put_or_delete_item::delete_item{}) {
        _pk = _mutation_builder.pk();
        _ck = _mutation_builder.ck();
        if (_returnvalues != returnvalues::NONE && _returnvalues != returnvalues::ALL_OLD) {
            throw api_error::validation(format("DeleteItem supports only NONE or ALL_OLD for ReturnValues"));
        }
        _condition_expression = get_parsed_condition_expression(parsed_expression_cache, _request);
        const rjson::value* expression_attribute_names = rjson::find(_request, "ExpressionAttributeNames");
        const rjson::value* expression_attribute_values = rjson::find(_request, "ExpressionAttributeValues");
        if (!_condition_expression.empty()) {
            std::unordered_set<std::string> used_attribute_names;
            std::unordered_set<std::string> used_attribute_values;
            resolve_condition_expression(_condition_expression,
                    expression_attribute_names, expression_attribute_values,
                    used_attribute_names, used_attribute_values);
            verify_all_are_used(expression_attribute_names, used_attribute_names,"ExpressionAttributeNames", "DeleteItem");
            verify_all_are_used(expression_attribute_values, used_attribute_values, "ExpressionAttributeValues", "DeleteItem");
        } else {
            if (expression_attribute_names) {
                throw api_error::validation("ExpressionAttributeNames cannot be used without ConditionExpression");
            }
            if (expression_attribute_values) {
                throw api_error::validation("ExpressionAttributeValues cannot be used without ConditionExpression");
            }
        }
    }
    bool needs_read_before_write() const {
        return _request.HasMember("Expected") ||
                check_needs_read_before_write(_condition_expression) ||
                _returnvalues == returnvalues::ALL_OLD;
    }
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts, cdc::per_request_options& cdc_opts) const override {
        if (!verify_expected(_request, previous_item.get()) ||
            !verify_condition_expression(_condition_expression, previous_item.get())) {
            if (previous_item && _returnvalues_on_condition_check_failure ==
                returnvalues_on_condition_check_failure::ALL_OLD) {
                _return_attributes = std::move(*previous_item);
            }
            // If the update is to be cancelled because of an unfulfilled Expected
            // condition, return an empty optional mutation, which is more
            // efficient than throwing an exception.
            return {};
        }
        if (_returnvalues == returnvalues::ALL_OLD && previous_item) {
            _return_attributes = std::move(*previous_item);
        } else {
            _return_attributes = {};
        }
        if (_consumed_capacity._total_bytes == 0) {
            _consumed_capacity._total_bytes = 1;
        }
        return _mutation_builder.build(_schema, ts);
    }
    virtual ~delete_item_operation() = default;
};

future<executor::request_return_type> executor::delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.delete_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("delete_item {}", request);

    auto op = make_shared<delete_item_operation>(*_parsed_expression_cache, _proxy, std::move(request));

    if (!audit_info) {
        // On LWT shard bounce, audit_info is already set on the originating shard.
        // The reference captured in the bounce lambda points back to the original
        // coroutine frame, which remains alive for the entire cross-shard call.
        // Only reads of this pointer occur on the target shard — no writes or frees.
        maybe_audit(audit_info, audit::statement_category::DML, op->schema()->ks_name(),
                    op->schema()->cf_name(), "DeleteItem", op->request(), db::consistency_level::LOCAL_QUORUM);
    }

    lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *(op->schema()));
    tracing::add_alternator_table_name(trace_state, op->schema()->cf_name());
    const bool needs_read_before_write = _proxy.data_dictionary().get_config().alternator_force_read_before_write() || op->needs_read_before_write();

    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, op->schema(), auth::permission::MODIFY, _stats);

    auto cas_shard = op->shard_for_execute(needs_read_before_write);

    if (cas_shard && !cas_shard->this_shard()) {
        _stats.api_operations.delete_item--; // uncount on this shard, will be counted in other shard
        _stats.shard_bounce_for_lwt++;
        per_table_stats->shard_bounce_for_lwt++;
        co_return co_await container().invoke_on(cas_shard->shard(), _ssg,
                [request = std::move(*op).move_request(), cs = client_state.move_to_other_shard(), gt = tracing::global_trace_state_ptr(trace_state), permit = std::move(permit), &audit_info]
                (executor& e) mutable {
            return do_with(cs.get(), [&e, request = std::move(request), trace_state = tracing::trace_state_ptr(gt), &audit_info]
                                     (service::client_state& client_state) mutable {
                //FIXME: Instead of passing  empty_service_permit() to the background operation,
                // the current permit's lifetime should be prolonged, so that it's destructed
                // only after all background operations are finished as well.
                return e.delete_item(client_state, std::move(trace_state), empty_service_permit(), std::move(request), audit_info);
            });
        });
    }
    per_table_stats->api_operations.delete_item++;
    uint64_t wcu_total = 0;
    auto res = co_await op->execute(_proxy, std::move(cas_shard), client_state, trace_state, std::move(permit), needs_read_before_write, _stats, *per_table_stats, wcu_total);
    if (op->consumed_capacity()._total_bytes > 1) {
        per_table_stats->operation_sizes.delete_item_op_size_kb.add(bytes_to_kb_ceil(op->consumed_capacity()._total_bytes));
    }
    per_table_stats->wcu_total[stats::wcu_types::DELETE_ITEM] += wcu_total;
    _stats.wcu_total[stats::wcu_types::DELETE_ITEM] += wcu_total;
    per_table_stats->api_operations.delete_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    _stats.api_operations.delete_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    co_return res;
}

using primary_key = std::pair<partition_key, clustering_key>;
struct primary_key_hash {
    schema_ptr _s;
    size_t operator()(const primary_key& key) const {
        return utils::hash_combine(partition_key::hashing(*_s)(key.first), clustering_key::hashing(*_s)(key.second));
    }
};
struct primary_key_equal {
    schema_ptr _s;
    bool operator()(const primary_key& k1, const primary_key& k2) const {
        return partition_key::equality(*_s)(k1.first, k2.first) && clustering_key::equality(*_s)(k1.second, k2.second);
    }
};

// This is a cas_request subclass for applying given put_or_delete_items to
// one partition using LWT as part as BatchWriteItem. This is a write-only
// operation, not needing the previous value of the item (the mutation to be
// done is known prior to starting the operation). Nevertheless, we want to
// do this mutation via LWT to ensure that it is serialized with other LWT
// mutations to the same partition.
// 
// The std::vector<put_or_delete_item> must remain alive until the
// storage_proxy::cas() future is resolved.
class put_or_delete_item_cas_request : public service::cas_request {
    schema_ptr schema;
    const std::vector<put_or_delete_item>& _mutation_builders;
public:
    put_or_delete_item_cas_request(schema_ptr s, const std::vector<put_or_delete_item>& b) :
        schema(std::move(s)), _mutation_builders(b) { }
    virtual ~put_or_delete_item_cas_request() = default;
    virtual std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr, const query::partition_slice& slice, api::timestamp_type ts, cdc::per_request_options& cdc_opts) override {
        std::optional<mutation> ret;
        for (const put_or_delete_item& mutation_builder : _mutation_builders) {
            // We assume all these builders have the same partition.
            if (ret) {
                ret->apply(mutation_builder.build(schema, ts));
            } else {
                ret = mutation_builder.build(schema, ts);
            }
        }
        return ret;
    }
};

future<> executor::cas_write(schema_ptr schema, service::cas_shard cas_shard, const dht::decorated_key& dk,
        const std::vector<put_or_delete_item>& mutation_builders, service::client_state& client_state,
        tracing::trace_state_ptr trace_state, service_permit permit)
{
    if (!cas_shard.this_shard()) {
        _stats.shard_bounce_for_lwt++;
        return container().invoke_on(cas_shard.shard(), _ssg,
                    [cs = client_state.move_to_other_shard(),
                    &mb = mutation_builders,
                    &dk,
                    ks = schema->ks_name(),
                    cf = schema->cf_name(),
                    gt = tracing::global_trace_state_ptr(trace_state),
                    permit = std::move(permit)]
                    (executor& self) mutable {
            return do_with(cs.get(), [&mb, &dk, ks = std::move(ks), cf = std::move(cf),
                                    trace_state = tracing::trace_state_ptr(gt), &self]
                                    (service::client_state& client_state) mutable {
                auto schema = self._proxy.data_dictionary().find_schema(ks, cf);
                service::cas_shard cas_shard(*schema, dk.token());

                //FIXME: Instead of passing empty_service_permit() to the background operation,
                // the current permit's lifetime should be prolonged, so that it's destructed
                // only after all background operations are finished as well.
                return self.cas_write(schema, std::move(cas_shard), dk, mb, client_state, std::move(trace_state), empty_service_permit());
            });
        });
    }

    auto timeout = executor::default_timeout();
    auto op = std::make_unique<put_or_delete_item_cas_request>(schema, mutation_builders);
    auto* op_ptr = op.get();
    auto cdc_opts = cdc::per_request_options{
        .alternator = true,
        .alternator_streams_increased_compatibility =
                schema->cdc_options().enabled() && _proxy.data_dictionary().get_config().alternator_streams_increased_compatibility(),
    };
    return _proxy.cas(schema, std::move(cas_shard), *op_ptr, nullptr, to_partition_ranges(dk),
            {timeout, std::move(permit), client_state, trace_state},
            db::consistency_level::LOCAL_SERIAL, db::consistency_level::LOCAL_QUORUM,
            timeout, timeout, true, std::move(cdc_opts)).finally([op = std::move(op)]{}).discard_result();
    // We discarded cas()'s future value ("is_applied") because BatchWriteItem
    // does not need to support conditional updates.
}


struct schema_decorated_key {
    schema_ptr schema;
    dht::decorated_key dk;
};
struct schema_decorated_key_hash {
    size_t operator()(const schema_decorated_key& k) const {
        return std::hash<dht::token>()(k.dk.token());
    }
};
struct schema_decorated_key_equal {
    bool operator()(const schema_decorated_key& k1, const schema_decorated_key& k2) const {
        return k1.schema == k2.schema && k1.dk.equal(*k1.schema, k2.dk);
    }
};

// FIXME: if we failed writing some of the mutations, need to return a list
// of these failed mutations rather than fail the whole write (issue #5650).
future<> executor::do_batch_write(
        std::vector<std::pair<schema_ptr, put_or_delete_item>> mutation_builders,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit) {
    if (mutation_builders.empty()) {
        return make_ready_future<>();
    }
    // NOTE: technically, do_batch_write could be reworked to use LWT only for part
    // of the batched requests and not use it for others, but it's not considered
    // likely that a batch will contain both tables which always demand LWT and ones
    // that don't - it's fragile to split a batch into multiple storage proxy requests though.
    // Hence, the decision is conservative - if any table enforces LWT,the whole batch will use it.
    const bool needs_lwt = std::ranges::any_of(mutation_builders | std::views::keys, [] (const schema_ptr& schema) {
        return rmw_operation::get_write_isolation_for_schema(schema) == rmw_operation::write_isolation::LWT_ALWAYS;
    });
    if (!needs_lwt) {
        // Do a normal write, without LWT:
        utils::chunked_vector<mutation> mutations;
        mutations.reserve(mutation_builders.size());
        api::timestamp_type now = api::new_timestamp();
        bool any_cdc_enabled = false;
        for (auto& b : mutation_builders) {
            mutations.push_back(b.second.build(b.first, now));
            any_cdc_enabled |= b.first->cdc_options().enabled();
        }
        return _proxy.mutate(std::move(mutations),
                db::consistency_level::LOCAL_QUORUM,
                executor::default_timeout(),
                trace_state,
                std::move(permit),
                db::allow_per_partition_rate_limit::yes,
                false,
                cdc::per_request_options{
                    .alternator = true,
                    .alternator_streams_increased_compatibility = any_cdc_enabled && _proxy.data_dictionary().get_config().alternator_streams_increased_compatibility(),
                });
    } else {
        // Do the write via LWT:
        // Multiple mutations may be destined for the same partition, adding
        // or deleting different items of one partition. Join them together
        // because we can do them in one cas() call.
        using map_type = std::unordered_map<schema_decorated_key, 
            std::vector<put_or_delete_item>, 
            schema_decorated_key_hash, 
            schema_decorated_key_equal>;
        auto key_builders = std::make_unique<map_type>(1, schema_decorated_key_hash{}, schema_decorated_key_equal{});
        for (auto&& b : std::move(mutation_builders)) {
            auto [it, added] = key_builders->try_emplace(schema_decorated_key {
                .schema = b.first,
                .dk = dht::decorate_key(*b.first, b.second.pk())
            });
            it->second.push_back(std::move(b.second));
        }
        auto* key_builders_ptr = key_builders.get();
        return parallel_for_each(*key_builders_ptr, [this, &client_state, trace_state, permit = std::move(permit)] (const auto& e) {
            _stats.write_using_lwt++;
            auto desired_shard = service::cas_shard(*e.first.schema, e.first.dk.token());
            auto s = e.first.schema;

            static const auto* injection_name = "alternator_executor_batch_write_wait";
            return utils::get_local_injector().inject(injection_name, [s = std::move(s)] (auto& handler) -> future<> {
                const auto ks = handler.get("keyspace");
                const auto cf = handler.get("table");
                const auto shard = std::atoll(handler.get("shard")->data());
                if (ks == s->ks_name() && cf == s->cf_name() && shard == this_shard_id()) {
                    elogger.info("{}: hit", injection_name);
                    co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
                    elogger.info("{}: continue", injection_name);
                }
            }).then([&e, desired_shard = std::move(desired_shard),
                 &client_state, trace_state = std::move(trace_state), permit = std::move(permit), this]() mutable
            {
                return cas_write(e.first.schema, std::move(desired_shard), e.first.dk,
                    std::move(e.second), client_state, std::move(trace_state), std::move(permit));
            });
        }).finally([key_builders = std::move(key_builders)]{});
    }
}

sstring print_names_for_audit(const std::set<sstring>& names) {
    sstring res;
    // Might have been useful to loop twice, with the 1st loop learning the total size of the names for the res to then reserve()
    for(const auto& name : names) {
        if (!res.empty()) {
            res += "|";
        }
        res += name;
    }
    return res;
}

future<executor::request_return_type> executor::batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.batch_write_item++;
    auto start_time = std::chrono::steady_clock::now();
    const rjson::value& request_items = get_member(request, "RequestItems", "BatchWriteItem content");
    validate_is_object(request_items, "RequestItems");
    if (request_items.ObjectEmpty()) {
        co_return api_error::validation("RequestItems can't be empty");
    }

    const auto maximum_batch_write_size = _proxy.data_dictionary().get_config().alternator_max_items_in_batch_write();

    size_t total_items = 0;
    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        if (!it->value.IsArray() || it->value.Empty()) {
            co_return api_error::validation("Member of RequestItems must be a non-empty array of WriteRequest objects");
        }
        total_items += it->value.Size();
    }
    if (total_items > maximum_batch_write_size) {
        co_return api_error::validation(fmt::format("Invalid length of BatchWriteItem command, got {} items, "
            "maximum is {} (from configuration variable alternator_max_items_in_batch_write)", total_items, maximum_batch_write_size));
    }
    bool should_add_wcu = wcu_consumed_capacity_counter::should_add_capacity(request);
    rjson::value consumed_capacity = rjson::empty_array();
    std::vector<std::pair<schema_ptr, put_or_delete_item>> mutation_builders;
    // WCU calculation is performed at the end of execution.
    // We need to keep track of changes per table, both for internal metrics
    // and to be able to return the values if should_add_wcu is true.
    // For each table, we need its stats and schema.
    std::vector<std::pair<lw_shared_ptr<stats>, schema_ptr>> per_table_wcu;

    std::set<sstring> table_names; // for auditing
    // FIXME: will_log() here doesn't pass keyspace/table, so keyspace-level audit
    // filtering is bypassed — a batch spanning multiple tables is audited as a whole.
    bool should_audit = _audit.local_is_initialized() && _audit.local().will_log(audit::statement_category::DML);
    mutation_builders.reserve(request_items.MemberCount());
    per_table_wcu.reserve(request_items.MemberCount());
    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        schema_ptr schema = get_table_from_batch_request(_proxy, it);
        lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *(schema));
        per_table_stats->api_operations.batch_write_item++;
        per_table_stats->api_operations.batch_write_item_batch_total += it->value.Size();
        per_table_stats->api_operations.batch_write_item_histogram.add(it->value.Size());
        tracing::add_alternator_table_name(trace_state, schema->cf_name());
        if (should_audit) {
            table_names.insert(schema->cf_name());
        }

        std::unordered_set<primary_key, primary_key_hash, primary_key_equal> used_keys(
                1, primary_key_hash{schema}, primary_key_equal{schema});
        for (auto& request : it->value.GetArray()) {
            auto& r = get_single_member(request, "RequestItems element");
            const auto r_name = rjson::to_string_view(r.name);
            if (r_name == "PutRequest") {
                const rjson::value& item = get_member(r.value, "Item", "PutRequest");
                validate_is_object(item, "Item in PutRequest");
                auto&& put_item = put_or_delete_item(
                        item, schema, put_or_delete_item::put_item{},
                        si_key_attributes(_proxy.data_dictionary().find_table(schema->ks_name(), schema->cf_name())));
                mutation_builders.emplace_back(schema, std::move(put_item));
                auto mut_key = std::make_pair(mutation_builders.back().second.pk(), mutation_builders.back().second.ck());
                if (used_keys.contains(mut_key)) {
                    co_return api_error::validation("Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else if (r_name == "DeleteRequest") {
                const rjson::value& key = get_member(r.value, "Key", "DeleteRequest");
                validate_is_object(key, "Key in DeleteRequest");
                mutation_builders.emplace_back(schema, put_or_delete_item(
                        key, schema, put_or_delete_item::delete_item{}));
                auto mut_key = std::make_pair(mutation_builders.back().second.pk(),
                        mutation_builders.back().second.ck());
                if (used_keys.contains(mut_key)) {
                    co_return api_error::validation("Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else {
                co_return api_error::validation(fmt::format("Unknown BatchWriteItem request type: {}", r_name));
            }
        }
        per_table_wcu.emplace_back(std::make_pair(per_table_stats, schema));
    }
    for (const auto& b : mutation_builders) {
        co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, b.first, auth::permission::MODIFY, _stats);
    }
    // If alternator_force_read_before_write is true we will first get the previous item size
    // and only then do send the mutation.
    if (_proxy.data_dictionary().get_config().alternator_force_read_before_write()) {
        std::vector<future<uint64_t>> previous_items_sizes;
        previous_items_sizes.reserve(mutation_builders.size());

        // Parallel get all previous item sizes
        for (const auto& b : mutation_builders) {
            previous_items_sizes.emplace_back(get_previous_item_size(
                _proxy,
                client_state,
                b.first,
                b.second.pk(),
                b.second.ck(),
                permit));
        }
        size_t pos = 0;
        // We are going to wait for all the requests
        for (auto&& pi : previous_items_sizes) {
            auto res = co_await std::move(pi);
            if (mutation_builders[pos].second.length_in_bytes() < res) {
                mutation_builders[pos].second.set_length_in_bytes(res);
            }
            pos++;
        }
    }


    size_t wcu_put_units = 0;
    size_t wcu_delete_units = 0;

    size_t pos = 0;
    size_t total_wcu;
    // Here we calculate the per-table WCU.
    // The size in the mutation is based either on the operation size,
    // or, if we performed a read-before-write, on the larger of the operation size
    // and the previous item's size.
    for (const auto& w : per_table_wcu) {
        total_wcu = 0;
        // The following loop goes over all items from the same table
        while(pos < mutation_builders.size() && w.second->id() == mutation_builders[pos].first->id()) {
            uint64_t item_size = mutation_builders[pos].second.length_in_bytes();
            size_t wcu = wcu_consumed_capacity_counter::get_units(item_size ? item_size : 1);
            total_wcu += wcu;
            if (mutation_builders[pos].second.is_put_item()) {
                w.first->wcu_total[stats::PUT_ITEM] += wcu;
                wcu_put_units += wcu;
            } else {
                w.first->wcu_total[stats::DELETE_ITEM] += wcu;
                wcu_delete_units += wcu;
            }
            w.first->operation_sizes.batch_write_item_op_size_kb.add(bytes_to_kb_ceil(item_size));
            pos++;
        }
        if (should_add_wcu) {
            rjson::value entry = rjson::empty_object();
            rjson::add(entry, "TableName", rjson::from_string(w.second->cf_name()));
            rjson::add(entry, "CapacityUnits", total_wcu);
            rjson::push_back(consumed_capacity, std::move(entry));
        }
    }
    _stats.wcu_total[stats::PUT_ITEM] += wcu_put_units;
    _stats.wcu_total[stats::DELETE_ITEM] += wcu_delete_units;
    _stats.api_operations.batch_write_item_batch_total += total_items;
    _stats.api_operations.batch_write_item_histogram.add(total_items);
    co_await do_batch_write(std::move(mutation_builders), client_state, trace_state, std::move(permit));
    // FIXME: Issue #5650: If we failed writing some of the updates,
    // need to return a list of these failed updates in UnprocessedItems
    // rather than fail the whole write (issue #5650).
    rjson::value ret = rjson::empty_object();
    rjson::add(ret, "UnprocessedItems", rjson::empty_object());
    if (should_add_wcu) {
        rjson::add(ret, "ConsumedCapacity", std::move(consumed_capacity));
    }
    auto duration = std::chrono::steady_clock::now() - start_time;
    _stats.api_operations.batch_write_item_latency.mark(duration);
    for (const auto& w : per_table_wcu) {
        w.first->api_operations.batch_write_item_latency.mark(duration);
    }
    maybe_audit(audit_info, audit::statement_category::DML, "",
                print_names_for_audit(table_names), "BatchWriteItem", request, db::consistency_level::LOCAL_QUORUM);
    co_return rjson::print(std::move(ret));
}

static const std::string_view get_item_type_string(const rjson::value& v) {
    const rjson::value::Member& mem = get_single_member(v, "Item");
    return rjson::to_string_view(mem.name);
}

static bool check_needs_read_before_write(const parsed::value& v) {
    return std::visit(overloaded_functor {
        [&] (const parsed::constant& c) -> bool {
            return false;
        },
        [&] (const parsed::value::function_call& f) -> bool {
            return std::ranges::any_of(f._parameters, [&] (const parsed::value& param) {
                return check_needs_read_before_write(param);
            });
        },
        [&] (const parsed::path& p) -> bool {
            return true;
        }
    }, v._value);
}

static bool check_needs_read_before_write(const attribute_path_map<parsed::update_expression::action>& update_expression) {
    return std::ranges::any_of(update_expression, [](const auto& p) {
        if (!p.second.has_value()) {
            // If the action is not on the top-level attribute, we need to
            // read the old item: we change only a part of the top-level
            // attribute, and write the full top-level attribute back.
            return true;
        }
        // Otherwise, the action p.second.get_value() is just on top-level
        // attribute. Check if it needs read-before-write:
        return std::visit(overloaded_functor {
            [&] (const parsed::update_expression::action::set& a) -> bool {
                return check_needs_read_before_write(a._rhs._v1) || (a._rhs._op != 'v' && check_needs_read_before_write(a._rhs._v2));
            },
            [&] (const parsed::update_expression::action::remove& a) -> bool {
                return false;
            },
            [&] (const parsed::update_expression::action::add& a) -> bool {
                return true;
            },
            [&] (const parsed::update_expression::action::del& a) -> bool {
                return true;
            }
        }, p.second.get_value()._action);
    });
}

/*!
 * \brief estimate_value_size provides a rough size estimation
 * for an rjson value object.
 *
 * When calculating RCU and WCU, we need to determine the length of the JSON representation
 * (specifically, the length of each key and each value).
 *
 * When possible, this is calculated as a side effect of other operations.
 * estimate_value_size is used when this calculation cannot be performed directly,
 * but we still need an estimated value.
 *
 * It achieves this without streaming any values and uses a fixed size for numbers.
 * The aim is not to provide a perfect 1-to-1 size calculation, as WCU is calculated
 * in 1KB units. A ballpark estimate is sufficient.
 */
static size_t estimate_value_size(const rjson::value& value) {
    size_t size = 0;

    if (value.IsString()) {
        size += value.GetStringLength();
    }
    else if (value.IsNumber()) {
        size += 8;
    }
    else if (value.IsBool()) {
        size += 5;
    }
    else if (value.IsArray()) {
        for (auto& v : value.GetArray()) {
            size += estimate_value_size(v);  // Recursively calculate array element sizes
        }
    }
    else if (value.IsObject()) {
        for (auto it = value.MemberBegin(); it != value.MemberEnd(); ++it) {
            size += it->name.GetStringLength();  // Size of the key
            size += estimate_value_size(it->value);   // Size of the value
        }
    }
    return size;
}

class update_item_operation : public rmw_operation {
public:
    // Some information parsed during the constructor to check for input
    // errors, and cached to be used again during apply().
    rjson::value* _attribute_updates;
    // Instead of keeping a parsed::update_expression with an unsorted list
    // list of actions, we keep them in an attribute_path_map which groups
    // them by top-level attribute, and detects forbidden overlaps/conflicts.
    attribute_path_map<parsed::update_expression::action> _update_expression;

    // Saved list of GSI keys in the table being updated, used for
    // validate_value_if_index_key()
    std::unordered_map<bytes, std::string> _key_attributes;
    // Saved map of vector index target attributes to their dimensions, used
    // for validate_value_if_vector_index_attribute()
    std::unordered_map<bytes, int> _vector_index_attributes;

    parsed::condition_expression _condition_expression;

    update_item_operation(parsed::expression_cache& parsed_expression_cache, service::storage_proxy& proxy, rjson::value&& request);
    virtual ~update_item_operation() = default;
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts, cdc::per_request_options& cdc_opts) const override;
    bool needs_read_before_write() const;

private:
    void delete_attribute(bytes&& column_name, const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts, deletable_row& row,
            attribute_collector& modified_attrs) const;
    void update_attribute(bytes&& column_name, const rjson::value& json_value, const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts,
            deletable_row& row, attribute_collector& modified_attrs, const attribute_path_map_node<parsed::update_expression::action>* h = nullptr) const;
    void apply_attribute_updates(const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts, deletable_row& row,
            attribute_collector& modified_attrs, bool& any_updates, bool& any_deletes) const;
    void apply_update_expression(const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts, deletable_row& row,
            attribute_collector& modified_attrs, bool& any_updates, bool& any_deletes) const;
};

update_item_operation::update_item_operation(parsed::expression_cache& parsed_expression_cache, service::storage_proxy& proxy, rjson::value&& update_info)
    : rmw_operation(proxy, std::move(update_info))
{
    const rjson::value* key = rjson::find(_request, "Key");
    if (!key) {
        throw api_error::validation("UpdateItem requires a Key parameter");
    }
    _pk = pk_from_json(*key, _schema);
    _ck = ck_from_json(*key, _schema);
    check_key(*key, _schema);

    const rjson::value* expression_attribute_names = rjson::find(_request, "ExpressionAttributeNames");
    const rjson::value* expression_attribute_values = rjson::find(_request, "ExpressionAttributeValues");
    std::unordered_set<std::string> used_attribute_names;
    std::unordered_set<std::string> used_attribute_values;

    const rjson::value* update_expression = rjson::find(_request, "UpdateExpression");
    if (update_expression) {
        if (!update_expression->IsString()) {
            throw api_error::validation("UpdateExpression must be a string");
        }
        try {
            parsed::update_expression expr = parsed_expression_cache.parse_update_expression(rjson::to_string_view(*update_expression));
            resolve_update_expression(expr,
                    expression_attribute_names, expression_attribute_values,
                    used_attribute_names, used_attribute_values);
            for (auto& action : expr.actions()) {
                // Unfortunately we need to copy the action's path, because
                // we std::move the action object.
                auto p = action._path;
                attribute_path_map_add("UpdateExpression", _update_expression, p, std::move(action));
            }
        } catch(expressions_syntax_error& e) {
            throw api_error::validation(e.what());
        }
    }
    _attribute_updates = rjson::find(_request, "AttributeUpdates");
    if (_attribute_updates) {
        if (!_attribute_updates->IsObject()) {
            throw api_error::validation("AttributeUpdates must be an object");
        }
        for (auto it = std::as_const(*_attribute_updates).MemberBegin(); it != std::as_const(*_attribute_updates).MemberEnd(); ++it) {
            validate_attr_name_length("AttributeUpdates", it->name.GetStringLength(), false);
        }
    }

    _condition_expression = get_parsed_condition_expression(parsed_expression_cache, _request);
    resolve_condition_expression(_condition_expression,
            expression_attribute_names, expression_attribute_values,
            used_attribute_names, used_attribute_values);

    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "UpdateItem");
    verify_all_are_used(expression_attribute_values, used_attribute_values, "ExpressionAttributeValues", "UpdateItem");

    // DynamoDB forbids having both old-style AttributeUpdates or Expected
    // and new-style UpdateExpression or ConditionExpression in the same request
    const rjson::value* expected = rjson::find(_request, "Expected");
    if (update_expression && _attribute_updates) {
        throw api_error::validation(
                format("UpdateItem does not allow both AttributeUpdates and UpdateExpression to be given together"));
    }
    if (update_expression && expected) {
        throw api_error::validation(
                format("UpdateItem does not allow both old-style Expected and new-style UpdateExpression to be given together"));
    }
    if (_attribute_updates && !_condition_expression.empty()) {
        throw api_error::validation(
                format("UpdateItem does not allow both old-style AttributeUpdates and new-style ConditionExpression to be given together"));
    }
    if (_pk.representation().size() > 2) {
        // ScyllaDB uses two extra bytes compared to DynamoDB for the key bytes length
        _consumed_capacity._total_bytes += _pk.representation().size() - 2;
    }
    if (_ck.representation().size() > 2) {
        // ScyllaDB uses two extra bytes compared to DynamoDB for the key bytes length
        _consumed_capacity._total_bytes += _ck.representation().size() - 2;
    }
    if (expression_attribute_names) {
        _consumed_capacity._total_bytes += estimate_value_size(*expression_attribute_names);
    }
    if (expression_attribute_values) {
        _consumed_capacity._total_bytes += estimate_value_size(*expression_attribute_values);
    }

    _key_attributes = si_key_attributes(proxy.data_dictionary().find_table(
        _schema->ks_name(), _schema->cf_name()));
    _vector_index_attributes = vector_index_attributes(*_schema);
}

// These are the cases where update_item_operation::apply() needs to use
// "previous_item" for certain AttributeUpdates operations (ADD or DELETE)
static bool check_needs_read_before_write_attribute_updates(rjson::value *attribute_updates) {
    if (!attribute_updates) {
        return false;
    }
    // We already confirmed in update_item_operation::update_item_operation()
    // that _attribute_updates, when it exists, is a map
    for (auto it = attribute_updates->MemberBegin(); it != attribute_updates->MemberEnd(); ++it) {
        rjson::value* action = rjson::find(it->value, "Action");
        if (action) {
            std::string_view action_s = rjson::to_string_view(*action);
            if (action_s == "ADD") {
                return true;
            }
            // For DELETE operation, it only needs a read before write if the
            // "Value" option is used. Without it, it's just a delete.
            if (action_s == "DELETE" && it->value.HasMember("Value")) {
                return true;
            }
        }
    }
    return false;
}

bool
update_item_operation::needs_read_before_write() const {
    return check_needs_read_before_write(_update_expression) ||
           check_needs_read_before_write(_condition_expression) ||
           check_needs_read_before_write_attribute_updates(_attribute_updates) ||
           _request.HasMember("Expected") ||
           (_returnvalues != returnvalues::NONE && _returnvalues != returnvalues::UPDATED_NEW);
}

// action_result() returns the result of applying an UpdateItem action -
// this result is either a JSON object or an unset optional which indicates
// the action was a deletion. The caller (update_item_operation::apply()
// below) will either write this JSON as the content of a column, or
// use it as a piece in a bigger top-level attribute.
static std::optional<rjson::value> action_result(
        const parsed::update_expression::action& action,
        const rjson::value* previous_item) {
    return std::visit(overloaded_functor {
        [&] (const parsed::update_expression::action::set& a) -> std::optional<rjson::value> {
            return calculate_value(a._rhs, previous_item);
        },
        [&] (const parsed::update_expression::action::remove& a) -> std::optional<rjson::value> {
            return std::nullopt;
        },
        [&] (const parsed::update_expression::action::add& a) -> std::optional<rjson::value> {
            parsed::value base;
            parsed::value addition;
            base.set_path(action._path);
            addition.set_constant(a._valref);
            rjson::value v1 = calculate_value(base, calculate_value_caller::UpdateExpression, previous_item);
            rjson::value v2 = calculate_value(addition, calculate_value_caller::UpdateExpression, previous_item);
            rjson::value result;
            // An ADD can be used to create a new attribute (when
            // v1.IsNull()) or to add to a pre-existing attribute:
            if (v1.IsNull()) {
                const auto v2_type = get_item_type_string(v2);
                if (v2_type == "N" || v2_type == "SS" || v2_type == "NS" || v2_type == "BS") {
                    result = v2;
                } else {
                    throw api_error::validation(format("An operand in the update expression has an incorrect data type: {}", v2));
                }
            } else {
                const auto v1_type = get_item_type_string(v1);
                if (v1_type == "N") {
                    if (get_item_type_string(v2) != "N") {
                        throw api_error::validation(fmt::format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, rjson::print(v2)));
                    }
                    result = number_add(v1, v2);
                } else if (v1_type == "SS" || v1_type == "NS" || v1_type == "BS") {
                    if (get_item_type_string(v2) != v1_type) {
                        throw api_error::validation(fmt::format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, rjson::print(v2)));
                    }
                    result = set_sum(v1, v2);
                } else {
                    throw api_error::validation(format("An operand in the update expression has an incorrect data type: {}", v1));
                }
            }
            return result;
        },
        [&] (const parsed::update_expression::action::del& a) -> std::optional<rjson::value> {
            parsed::value base;
            parsed::value subset;
            base.set_path(action._path);
            subset.set_constant(a._valref);
            rjson::value v1 = calculate_value(base, calculate_value_caller::UpdateExpression, previous_item);
            rjson::value v2 = calculate_value(subset, calculate_value_caller::UpdateExpression, previous_item);
            if (!v1.IsNull()) {
                return set_diff(v1, v2);
            }
            // When we return nullopt here, we ask to *delete* this attribute,
            // which is unnecessary because we know the attribute does not
            // exist anyway. This is a waste, but a small one. Note that also
            // for the "remove" action above we don't bother to check if the
            // previous_item add anything to remove.
            return std::nullopt;
        }
    }, action._action);
}

}

// Print an attribute_path_map_node<action> as the list of paths it contains:
template <> struct fmt::formatter<alternator::attribute_path_map_node<alternator::parsed::update_expression::action>> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    // this function recursively call into itself, so we have to forward declare it.
    auto format(const alternator::attribute_path_map_node<alternator::parsed::update_expression::action>& h, fmt::format_context& ctx) const
      -> decltype(ctx.out());
};

auto fmt::formatter<alternator::attribute_path_map_node<alternator::parsed::update_expression::action>>::format(const alternator::attribute_path_map_node<alternator::parsed::update_expression::action>& h, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto out = ctx.out();
    if (h.has_value()) {
        out = fmt::format_to(out, " {}", h.get_value()._path);
    } else if (h.has_members()) {
        for (auto& member : h.get_members()) {
            out = fmt::format_to(out, "{}", *member.second);
        }
    } else if (h.has_indexes()) {
        for (auto& index : h.get_indexes()) {
            out = fmt::format_to(out, "{}", *index.second);
        }
    }
    return out;
}

namespace alternator {

// Apply the hierarchy of actions in an attribute_path_map_node<action> to a
// JSON object which uses DynamoDB's serialization conventions. The complete,
// unmodified, previous_item is also necessary for the right-hand sides of the
// actions. Modifies obj in-place or returns false if it is to be removed.
static bool hierarchy_actions(
        rjson::value& obj,
        const attribute_path_map_node<parsed::update_expression::action>& h,
        const rjson::value* previous_item)
{
    if (!obj.IsObject() || obj.MemberCount() != 1) {
        // This shouldn't happen. We shouldn't have stored malformed objects.
        // But today Alternator does not validate the structure of nested
        // documents before storing them, so this can happen on read.
        throw api_error::validation(format("Malformed value object read: {}", obj));
    }
    const char* type = obj.MemberBegin()->name.GetString();
    rjson::value& v = obj.MemberBegin()->value;
    if (h.has_value()) {
        // Action replacing everything in this position in the hierarchy
        std::optional<rjson::value> newv = action_result(h.get_value(), previous_item);
        if (newv) {
            obj = std::move(*newv);
        } else {
            return false;
        }
    } else if (h.has_members()) {
        if (type[0] != 'M' || !v.IsObject()) {
            // A .something on a non-map doesn't work.
            throw api_error::validation(fmt::format("UpdateExpression: document paths not valid for this item:{}", h));
        }
        for (const auto& member : h.get_members()) {
            std::string attr = member.first;
            const attribute_path_map_node<parsed::update_expression::action>& subh = *member.second;
            rjson::value *subobj = rjson::find(v, attr);
            if (subobj) {
                if (!hierarchy_actions(*subobj, subh, previous_item)) {
                    rjson::remove_member(v, attr);
                }
            } else {
                // When a.b does not exist, setting a.b itself (i.e.
                // subh.has_value()) is fine, but setting a.b.c is not.
                if (subh.has_value()) {
                    std::optional<rjson::value> newv = action_result(subh.get_value(), previous_item);
                    if (newv) {
                        // This is the !subobj case, so v doesn't have an
                        // attr member so we can use add()
                        rjson::add_with_string_name(v, attr, std::move(*newv));
                    } else {
                        // Removing a.b when a is a map but a.b doesn't exist
                        // is silently ignored. It's not considered an error.
                    }
                } else {
                    throw api_error::validation(format("UpdateExpression: document paths not valid for this item:{}", h));
                }
            }
        }
    } else if (h.has_indexes()) {
        if (type[0] != 'L' || !v.IsArray()) {
            // A [i] on a non-list doesn't work.
            throw api_error::validation(format("UpdateExpression: document paths not valid for this item:{}", h));
        }
        unsigned nremoved = 0;
        for (const auto& index : h.get_indexes()) {
            unsigned i = index.first - nremoved;
            const attribute_path_map_node<parsed::update_expression::action>& subh = *index.second;
            if (i < v.Size()) {
                if (!hierarchy_actions(v[i], subh, previous_item)) {
                    v.Erase(v.Begin() + i);
                    // If we have the actions "REMOVE a[1] SET a[3] = :val",
                    // the index 3 refers to the original indexes, before any
                    // items were removed. So we offset the next indexes
                    // (which are guaranteed to be higher than i - indexes is
                    // a sorted map) by an increased "nremoved".
                    nremoved++;
                }
            } else {
                // If a[7] does not exist, setting a[7] itself (i.e.
                // subh.has_value()) is fine - and appends an item, though
                // not necessarily with index 7. But setting a[7].b will
                // not work.
                if (subh.has_value()) {
                    std::optional<rjson::value> newv = action_result(subh.get_value(), previous_item);
                    if (newv) {
                        rjson::push_back(v, std::move(*newv));
                    } else {
                        // Removing a[7] when the list has fewer elements is
                        // silently ignored. It's not considered an error.
                    }
                } else {
                    throw api_error::validation(format("UpdateExpression: document paths not valid for this item:{}", h));
                }
            }
        }
    }
    return true;
}

void update_item_operation::delete_attribute(bytes&& column_name, const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts,
        deletable_row& row, attribute_collector& modified_attrs) const {
    if (_returnvalues == returnvalues::ALL_NEW) {
        rjson::remove_member(_return_attributes, to_string_view(column_name));
    } else if (_returnvalues == returnvalues::UPDATED_OLD && previous_item) {
        std::string_view cn = to_string_view(column_name);
        const rjson::value* col = rjson::find(*previous_item, cn);
        if (col) {
            // In the UPDATED_OLD case the item starts empty and column
            // names are unique, so we can use add()
            rjson::add_with_string_name(_return_attributes, cn, rjson::copy(*col));
        }
    }
    const column_definition* cdef = find_attribute(*_schema, column_name);
    if (cdef) {
        row.cells().apply(*cdef, atomic_cell::make_dead(ts, gc_clock::now()));
    } else {
        modified_attrs.del(std::move(column_name), ts);
    }
}

void update_item_operation::update_attribute(bytes&& column_name, const rjson::value& json_value, const std::unique_ptr<rjson::value>& previous_item,
        const api::timestamp_type ts, deletable_row& row, attribute_collector& modified_attrs,
        const attribute_path_map_node<parsed::update_expression::action>* h) const {
    if (_returnvalues == returnvalues::ALL_NEW) {
        rjson::replace_with_string_name(_return_attributes, to_string_view(column_name), rjson::copy(json_value));
    } else if (_returnvalues == returnvalues::UPDATED_NEW) {
        rjson::value&& v = rjson::copy(json_value);
        if (h) {
            // If the operation was only on specific attribute paths,
            // leave only them in _return_attributes.
            if (hierarchy_filter(v, *h)) {
                // In the UPDATED_NEW case, _return_attributes starts
                // empty and the attribute names are unique, so we can
                // use add().
                rjson::add_with_string_name(_return_attributes, to_string_view(column_name), std::move(v));
            }
        } else {
            rjson::add_with_string_name(_return_attributes, to_string_view(column_name), std::move(v));
        }
    } else if (_returnvalues == returnvalues::UPDATED_OLD && previous_item) {
        std::string_view cn = to_string_view(column_name);
        const rjson::value* col = rjson::find(*previous_item, cn);
        if (col) {
            rjson::value&& v = rjson::copy(*col);
            if (h) {
                if (hierarchy_filter(v, *h)) {
                    // In the UPDATED_OLD case, _return_attributes starts
                    // empty and the attribute names are unique, so we can
                    // use add().
                    rjson::add_with_string_name(_return_attributes, cn, std::move(v));
                }
            } else {
                rjson::add_with_string_name(_return_attributes, cn, std::move(v));
            }
        }
    }
    const column_definition* cdef = find_attribute(*_schema, column_name);
    if (cdef) {
        bytes column_value = get_key_from_typed_value(json_value, *cdef);
        row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, column_value));
    } else {
        // This attribute may be a key column of one of the GSIs or LSIs,
        // in which case there are some limitations on the value.
        if (!_key_attributes.empty()) {
            validate_value_if_index_key(_key_attributes, column_name, json_value);
        }
        // This attribute may also be a vector index target column,
        // in which case it must be a list of the right number of floats.
        if (!_vector_index_attributes.empty()) {
            validate_value_if_vector_index_attribute(_vector_index_attributes, column_name, json_value);
        }
        modified_attrs.put(std::move(column_name), serialize_item(json_value), ts);
    }
}

inline void update_item_operation::apply_attribute_updates(const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts, deletable_row& row,
        attribute_collector& modified_attrs, bool& any_updates, bool& any_deletes) const {
    for (auto it = _attribute_updates->MemberBegin(); it != _attribute_updates->MemberEnd(); ++it) {
        // Note that it.key() is the name of the column, *it is the operation
        bytes column_name = to_bytes(rjson::to_string_view(it->name));
        const column_definition* cdef = _schema->get_column_definition(column_name);
        if (cdef && cdef->is_primary_key()) {
            throw api_error::validation(format("UpdateItem cannot update key column {}", rjson::to_string_view(it->name)));
        }
        std::string action = rjson::to_string((it->value)["Action"]);
        if (action == "DELETE") {
            // The DELETE operation can do two unrelated tasks. Without a
            // "Value" option, it is used to delete an attribute. With a
            // "Value" option, it is used to delete a set of elements from
            // a set attribute of the same type.
            if (it->value.HasMember("Value")) {
                // Subtracting sets needs a read of previous_item, so
                // check_needs_read_before_write_attribute_updates()
                // returns true in this case, and previous_item is
                // available to us when the item exists.
                const rjson::value* v1 = previous_item ? rjson::find(*previous_item, to_string_view(column_name)) : nullptr;
                const rjson::value& v2 = (it->value)["Value"];
                validate_value(v2, "AttributeUpdates");
                const auto v2_type = get_item_type_string(v2);
                if (v2_type != "SS" && v2_type != "NS" && v2_type != "BS") {
                    throw api_error::validation(fmt::format("AttributeUpdates DELETE operation with Value only valid for sets, got type {}", v2_type));
                }
                if (v1) {
                    std::optional<rjson::value> result = set_diff(*v1, v2);
                    if (result) {
                        any_updates = true;
                        update_attribute(std::move(column_name), *result, previous_item, ts, row, modified_attrs);
                    } else {
                        // DynamoDB does not allow empty sets - if the
                        // result is empty, delete the attribute.
                        any_deletes = true;
                        delete_attribute(std::move(column_name), previous_item, ts, row, modified_attrs);
                    }
                } else {
                    // if the attribute or item don't exist, the DELETE
                    // operation should silently do nothing - and not
                    // create an empty item. It's a waste to call
                    // do_delete() on an attribute we already know is
                    // deleted, so we can just mark any_deletes = true.
                    any_deletes = true;
                }
            } else {
                any_deletes = true;
                delete_attribute(std::move(column_name), previous_item, ts, row, modified_attrs);
            }
        } else if (action == "PUT") {
            const rjson::value& value = (it->value)["Value"];
            validate_value(value, "AttributeUpdates");
            any_updates = true;
            update_attribute(std::move(column_name), value, previous_item, ts, row, modified_attrs);
        } else if (action == "ADD") {
            // Note that check_needs_read_before_write_attribute_updates()
            // made sure we retrieved previous_item (if exists) when there
            // is an ADD action.
            const rjson::value* v1 = previous_item ? rjson::find(*previous_item, to_string_view(column_name)) : nullptr;
            const rjson::value& v2 = (it->value)["Value"];
            validate_value(v2, "AttributeUpdates");
            // An ADD can be used to create a new attribute (when
            // !v1) or to add to a pre-existing attribute:
            if (!v1) {
                const auto v2_type = get_item_type_string(v2);
                if (v2_type == "N" || v2_type == "SS" || v2_type == "NS" || v2_type == "BS" || v2_type == "L") {
                    any_updates = true;
                    update_attribute(std::move(column_name), v2, previous_item, ts, row, modified_attrs);
                } else {
                    throw api_error::validation(format("An operand in the AttributeUpdates ADD has an incorrect data type: {}", v2));
                }
            } else {
                const auto v1_type = get_item_type_string(*v1);
                const auto v2_type = get_item_type_string(v2);
                if (v2_type != v1_type) {
                    throw api_error::validation(fmt::format("Operand type mismatch in AttributeUpdates ADD. Expected {}, got {}", v1_type, v2_type));
                }
                if (v1_type == "N") {
                    any_updates = true;
                    update_attribute(std::move(column_name), number_add(*v1, v2), previous_item, ts, row, modified_attrs);
                } else if (v1_type == "SS" || v1_type == "NS" || v1_type == "BS") {
                    any_updates = true;
                    update_attribute(std::move(column_name), set_sum(*v1, v2), previous_item, ts, row, modified_attrs);
                } else if (v1_type == "L") {
                    // The DynamoDB documentation doesn't say it supports
                    // lists in ADD operations, but it turns out that it
                    // does. Interestingly, this is only true for
                    // AttributeUpdates (this code) - the similar ADD
                    // in UpdateExpression doesn't support lists.
                    any_updates = true;
                    update_attribute(std::move(column_name), list_concatenate(*v1, v2), previous_item, ts, row, modified_attrs);
                } else {
                    throw api_error::validation(format("An operand in the AttributeUpdates ADD has an incorrect data type: {}", *v1));
                }
            }
        } else {
            throw api_error::validation(fmt::format("Unknown Action value '{}' in AttributeUpdates", action));
        }
    }
}

inline void update_item_operation::apply_update_expression(const std::unique_ptr<rjson::value>& previous_item, const api::timestamp_type ts, deletable_row& row,
        attribute_collector& modified_attrs, bool& any_updates, bool& any_deletes) const {
    for (auto& actions : _update_expression) {
        // The actions of _update_expression are grouped by top-level
        // attributes. Here, all actions in actions.second share the same
        // top-level attribute actions.first.
        std::string column_name = actions.first;
        const column_definition* cdef = _schema->get_column_definition(to_bytes(column_name));
        if (cdef && cdef->is_primary_key()) {
            throw api_error::validation(fmt::format("UpdateItem cannot update key column {}", column_name));
        }
        if (actions.second.has_value()) {
            // An action on a top-level attribute column_name. The single
            // action is actions.second.get_value(). We can simply invoke
            // the action and replace the attribute with its result:
            std::optional<rjson::value> result = action_result(actions.second.get_value(), previous_item.get());
            if (result) {
                any_updates = true;
                update_attribute(to_bytes(column_name), *result, previous_item, ts, row, modified_attrs, &actions.second);
            } else {
                any_deletes = true;
                delete_attribute(to_bytes(column_name), previous_item, ts, row, modified_attrs);
            }
        } else {
            // We have actions on a path or more than one path in the same
            // top-level attribute column_name - but not on the top-level
            // attribute as a whole. We already read the full top-level
            // attribute (see check_needs_read_before_write()), and now we
            // need to modify pieces of it and write back the entire
            // top-level attribute.
            if (!previous_item) {
                throw api_error::validation(format("UpdateItem cannot update nested document path on non-existent item"));
            }
            const rjson::value* toplevel = rjson::find(*previous_item, column_name);
            if (!toplevel) {
                throw api_error::validation(fmt::format("UpdateItem cannot update document path: missing attribute {}", column_name));
            }
            rjson::value result = rjson::copy(*toplevel);
            any_updates = true;
            hierarchy_actions(result, actions.second, previous_item.get());
            update_attribute(to_bytes(column_name), std::move(result), previous_item, ts, row, modified_attrs, &actions.second);
        }
    }
}

std::optional<mutation> update_item_operation::apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts, cdc::per_request_options& cdc_opts) const {
    if (_consumed_capacity._total_bytes == 0) {
        _consumed_capacity._total_bytes = 1;
    }
    if (!verify_expected(_request, previous_item.get()) || !verify_condition_expression(_condition_expression, previous_item.get())) {
        if (previous_item && _returnvalues_on_condition_check_failure == returnvalues_on_condition_check_failure::ALL_OLD) {
            _return_attributes = std::move(*previous_item);
        }
        // If the update is to be cancelled because of an unfulfilled
        // condition, return an empty optional mutation, which is more
        // efficient than throwing an exception.
        return {};
    }

    // In the ReturnValues=ALL_NEW case, we make a copy of previous_item into
    // _return_attributes and parts of it will be overwritten by the new
    // updates (in do_update() and do_delete()). We need to make a copy and
    // cannot overwrite previous_item directly because we still need its
    // original content for update expressions. For example, the expression
    // "REMOVE a SET b=a" is valid, and needs the original value of a to
    // stick around.
    // Note that for ReturnValues=ALL_OLD, we don't need to copy here, and
    // can just move previous_item later, when we don't need it any more.
    if (_returnvalues == returnvalues::ALL_NEW) {
        if (previous_item) {
            _return_attributes = rjson::copy(*previous_item);
        } else {
            // If there is no previous item, usually a new item is created
            // and contains the given key. This may be cancelled at the end
            // of this function if the update is just deletes.
            _return_attributes = rjson::copy(rjson::get(_request, "Key"));
        }
    } else if (_returnvalues == returnvalues::UPDATED_OLD || _returnvalues == returnvalues::UPDATED_NEW) {
        _return_attributes = rjson::empty_object();
    }

    mutation m(_schema, _pk);
    bool any_updates = false;
    bool any_deletes = false;
    auto& row = m.partition().clustered_row(*_schema, _ck);
    auto modified_attrs = attribute_collector();
    if (!_update_expression.empty()) {
        apply_update_expression(previous_item, ts, row, modified_attrs, any_updates, any_deletes);
    }
    if (_attribute_updates) {
        apply_attribute_updates(previous_item, ts, row, modified_attrs, any_updates, any_deletes);
    }
    if (!modified_attrs.empty()) {
        auto serialized_map = modified_attrs.to_mut().serialize(*attrs_type());
        row.cells().apply(attrs_column(*_schema), std::move(serialized_map));
    }
    // To allow creation of an item with no attributes, we need a row marker.
    // Note that unlike Scylla, even an "update" operation needs to add a row
    // marker. An update with only DELETE operations must not add a row marker
    // (this was issue #5862) but any other update, even an empty one, should.
    if (any_updates || !any_deletes) {
        row.apply(row_marker(ts));
    } else if (_returnvalues == returnvalues::ALL_NEW && !previous_item) {
        // There was no pre-existing item, and we're not creating one, so
        // don't report the new item in the returned Attributes.
        _return_attributes = rjson::null_value();
    }
    if (_returnvalues == returnvalues::ALL_OLD && previous_item) {
        _return_attributes = std::move(*previous_item);
    }
    // ReturnValues=UPDATED_OLD/NEW never return an empty Attributes field,
    // even if a new item was created. Instead it should be missing entirely.
    if (_returnvalues == returnvalues::UPDATED_OLD || _returnvalues == returnvalues::UPDATED_NEW) {
        if (_return_attributes.MemberCount() == 0) {
            _return_attributes = rjson::null_value();
        }
    }

    return m;
}

future<executor::request_return_type> executor::update_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.update_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("update_item {}", request);

    auto op = make_shared<update_item_operation>(*_parsed_expression_cache, _proxy, std::move(request));

    if (!audit_info) {
        // On LWT shard bounce, audit_info is already set on the originating shard.
        // The reference captured in the bounce lambda points back to the original
        // coroutine frame, which remains alive for the entire cross-shard call.
        // Only reads of this pointer occur on the target shard — no writes or frees.
        maybe_audit(audit_info, audit::statement_category::DML, op->schema()->ks_name(),
                    op->schema()->cf_name(), "UpdateItem", op->request(), db::consistency_level::LOCAL_QUORUM);
    }

    tracing::add_alternator_table_name(trace_state, op->schema()->cf_name());
    const bool needs_read_before_write = _proxy.data_dictionary().get_config().alternator_force_read_before_write() || op->needs_read_before_write();

    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, op->schema(), auth::permission::MODIFY, _stats);

    auto cas_shard = op->shard_for_execute(needs_read_before_write);

    if (cas_shard && !cas_shard->this_shard()) {
        _stats.api_operations.update_item--; // uncount on this shard, will be counted in other shard
        _stats.shard_bounce_for_lwt++;
        co_return co_await container().invoke_on(cas_shard->shard(), _ssg,
                [request = std::move(*op).move_request(), cs = client_state.move_to_other_shard(), gt = tracing::global_trace_state_ptr(trace_state), permit = std::move(permit), &audit_info]
                (executor& e) mutable {
            return do_with(cs.get(), [&e, request = std::move(request), trace_state = tracing::trace_state_ptr(gt), &audit_info]
                                     (service::client_state& client_state) mutable {
                //FIXME: Instead of passing empty_service_permit() to the background operation,
                // the current permit's lifetime should be prolonged, so that it's destructed
                // only after all background operations are finished as well.
                return e.update_item(client_state, std::move(trace_state), empty_service_permit(), std::move(request), audit_info);
            });
        });
    }
    lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *(op->schema()));
    per_table_stats->api_operations.update_item++;
    uint64_t wcu_total = 0;
    auto res = co_await op->execute(_proxy, std::move(cas_shard), client_state, trace_state, std::move(permit), needs_read_before_write, _stats, *per_table_stats, wcu_total);
    per_table_stats->operation_sizes.update_item_op_size_kb.add(bytes_to_kb_ceil(op->consumed_capacity()._total_bytes));
    per_table_stats->wcu_total[stats::wcu_types::UPDATE_ITEM] += wcu_total;
    _stats.wcu_total[stats::wcu_types::UPDATE_ITEM] += wcu_total;
    per_table_stats->api_operations.update_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    _stats.api_operations.update_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    co_return res;
}

future<executor::request_return_type> executor::list_tables(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.list_tables++;
    elogger.trace("Listing tables {}", request);

    maybe_audit(audit_info, audit::statement_category::QUERY, "", "", "ListTables", request);

    co_await utils::get_local_injector().inject("alternator_list_tables", [] (auto& handler) -> future<> {
        handler.set("waiting", true);
        co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
    });

    rjson::value* exclusive_start_json = rjson::find(request, "ExclusiveStartTableName");
    rjson::value* limit_json = rjson::find(request, "Limit");
    std::string exclusive_start = exclusive_start_json ? rjson::to_string(*exclusive_start_json) : "";
    int limit = limit_json ? limit_json->GetInt() : 100;
    if (limit < 1 || limit > 100) {
        co_return api_error::validation("Limit must be greater than 0 and no greater than 100");
    }

    auto tables = _proxy.data_dictionary().get_tables(); // hold on to temporary, table_names isn't a container, it's a view
    auto table_names = tables
            | std::views::filter([this] (data_dictionary::table t) {
                        return t.schema()->ks_name().find(KEYSPACE_NAME_PREFIX) == 0 &&
                            !t.schema()->is_view() &&
                            !cdc::is_log_for_some_table(_proxy.local_db(), t.schema()->ks_name(), t.schema()->cf_name());
                    })
            | std::views::transform([] (data_dictionary::table t) {
                        return t.schema()->cf_name();
                    });

    rjson::value response = rjson::empty_object();
    rjson::add(response, "TableNames", rjson::empty_array());
    rjson::value& all_tables = response["TableNames"];

    //TODO(sarna): Dynamo doesn't declare any ordering when listing tables,
    // but our implementation is vulnerable to changes, because the tables
    // are stored in an unordered map. We may consider (partially) sorting
    // the results before returning them to the client, especially if there
    // is an implicit order of elements that Dynamo imposes.
    auto table_names_it = [&table_names, &exclusive_start] {
        if (!exclusive_start.empty()) {
            auto it = std::ranges::find_if(table_names, [&exclusive_start] (const sstring& table_name) { return table_name == exclusive_start; });
            return std::next(it, it != table_names.end());
        } else {
            return table_names.begin();
        }
    }();
    while (limit > 0 && table_names_it != table_names.end()) {
        rjson::push_back(all_tables, rjson::from_string(*table_names_it));
        --limit;
        ++table_names_it;
    }

    if (table_names_it != table_names.end()) {
        auto& last_table_name = *std::prev(all_tables.End());
        rjson::add(response, "LastEvaluatedTableName", rjson::copy(last_table_name));
    }

    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::describe_endpoints(client_state& client_state, service_permit permit, rjson::value request, std::string host_header, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_endpoints++;

    maybe_audit(audit_info, audit::statement_category::QUERY, "", "", "DescribeEndpoints", request);

    // The alternator_describe_endpoints configuration can be used to disable
    // the DescribeEndpoints operation, or set it to return a fixed string
    std::string override = _proxy.data_dictionary().get_config().alternator_describe_endpoints();
    if (!override.empty()) {
        if (override == "disabled") {
            _stats.unsupported_operations++;
            co_return api_error::unknown_operation(
                "DescribeEndpoints disabled by configuration (alternator_describe_endpoints=disabled)");
        }
        host_header = std::move(override);
    }
    rjson::value response = rjson::empty_object();
    // Without having any configuration parameter to say otherwise, we tell
    // the user to return to the same endpoint they used to reach us. The only
    // way we can know this is through the "Host:" header in the request,
    // which typically exists (and in fact is mandatory in HTTP 1.1).
    // A "Host:" header includes both host name and port, exactly what we need
    // to return.
    if (host_header.empty()) {
        co_return api_error::validation("DescribeEndpoints needs a 'Host:' header in request");
    }
    rjson::add(response, "Endpoints", rjson::empty_array());
    rjson::push_back(response["Endpoints"], rjson::empty_object());
    rjson::add(response["Endpoints"][0], "Address", rjson::from_string(host_header));
    rjson::add(response["Endpoints"][0], "CachePeriodInMinutes", rjson::value(1440));
    co_return rjson::print(std::move(response));
}

static locator::replication_strategy_config_options get_network_topology_options(service::storage_proxy& sp, gms::gossiper& gossiper, int rf) {
    locator::replication_strategy_config_options options;
    for (const auto& dc : sp.get_token_metadata_ptr()->get_datacenter_racks_token_owners() | std::views::keys) {
        options.emplace(dc, std::to_string(rf));
    }
    return options;
}

future<executor::request_return_type> executor::describe_continuous_backups(client_state& client_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.describe_continuous_backups++;
    // Unlike most operations which return ResourceNotFound when the given
    // table doesn't exists, this operation returns a TableNoteFoundException.
    // So we can't use the usual get_table() wrapper and need a bit more code:
    std::string table_name = get_table_name(request);
    sstring ks_name = sstring(executor::KEYSPACE_NAME_PREFIX) + table_name;
    maybe_audit(audit_info, audit::statement_category::QUERY, ks_name, table_name, "DescribeContinuousBackups", request);
    schema_ptr schema;
    try {
        schema = _proxy.data_dictionary().find_schema(ks_name, table_name);
    } catch(data_dictionary::no_such_column_family&) {
        // DynamoDB returns validation error even when table does not exist
        // and the table name is invalid.
        validate_table_name(table_name);

        throw api_error::table_not_found(
                fmt::format("Table {} not found", table_name));
    }
    rjson::value desc = rjson::empty_object();
    rjson::add(desc, "ContinuousBackupsStatus", "DISABLED");
    rjson::value pitr = rjson::empty_object();
    rjson::add(pitr, "PointInTimeRecoveryStatus", "DISABLED");
    rjson::add(desc, "PointInTimeRecoveryDescription", std::move(pitr));
    rjson::value response = rjson::empty_object();
    rjson::add(response, "ContinuousBackupsDescription", std::move(desc));
    co_return rjson::print(std::move(response));
}

// Create the metadata for the keyspace in which we put the alternator
// table if it doesn't already exist.
// Currently, we automatically configure the keyspace based on the number
// of nodes in the cluster: A cluster with 3 or more live nodes, gets RF=3.
// A smaller cluster (presumably, a test only), gets RF=1. The user may
// manually create the keyspace to override this predefined behavior.
static lw_shared_ptr<keyspace_metadata> create_keyspace_metadata(std::string_view keyspace_name, service::storage_proxy& sp, gms::gossiper& gossiper, api::timestamp_type ts,
            const std::map<sstring, sstring>& tags_map, const gms::feature_service& feat, const db::tablets_mode_t::mode tablets_mode) {
    // Whether to use tablets for the table (actually for the keyspace of the
    // table) is determined by tablets_mode (taken from the configuration
    // option "tablets_mode_for_new_keyspaces"), as well as the presence and
    // the value of a per-table tag system:initial_tablets
    // (INITIAL_TABLETS_TAG_KEY).
    // Setting the tag with a numeric value will enable a specific initial number
    // of tablets (setting the value to 0 means enabling tablets with
    // an automatic selection of the best number of tablets).
    // Setting this tag to any non-numeric value (e.g., an empty string or the
    // word "none") will ask to disable tablets.
    // When vnodes are asked for by the tag value, but tablets are enforced by config,
    // throw an exception to the client.
    std::optional<unsigned> initial_tablets;
    if (feat.tablets) {
        auto it = tags_map.find(INITIAL_TABLETS_TAG_KEY);
        if (it != tags_map.end()) {
            // Tag set. If it's a valid number, use it. If not - e.g., it's
            // empty or a word like "none", disable tablets by setting
            // initial_tablets to a disengaged optional.
            try {
                initial_tablets = std::stol(tags_map.at(INITIAL_TABLETS_TAG_KEY));
            } catch (...) {
                if (tablets_mode == db::tablets_mode_t::mode::enforced) {
                    throw api_error::validation(format("Tag {} containing non-numerical value requests vnodes, but vnodes are forbidden by configuration option `tablets_mode_for_new_keyspaces: enforced`", INITIAL_TABLETS_TAG_KEY));
                }
                initial_tablets = std::nullopt;
                elogger.trace("Following {} tag containing non-numerical value, Alternator will attempt to create a keyspace {} with vnodes.", INITIAL_TABLETS_TAG_KEY, keyspace_name);
            }
        } else {
            // No per-table tag present, use the value from config
            if (tablets_mode == db::tablets_mode_t::mode::enabled || tablets_mode == db::tablets_mode_t::mode::enforced) {
                initial_tablets = 0;
                elogger.trace("Following the `tablets_mode_for_new_keyspaces` flag from the settings, Alternator will attempt to create a keyspace {} with tablets.", keyspace_name);
            } else {
                initial_tablets = std::nullopt;
                elogger.trace("Following the `tablets_mode_for_new_keyspaces` flag from the settings, Alternator will attempt to create a keyspace {} with vnodes.", keyspace_name);
            }
        }
    }

    int endpoint_count = gossiper.num_endpoints();
    int rf = 3;
    if (endpoint_count < rf) {
        rf = 1;
        elogger.warn("Creating keyspace '{}' for Alternator with unsafe RF={} because cluster only has {} nodes.",
                     keyspace_name, rf, endpoint_count);
    }
    auto opts = get_network_topology_options(sp, gossiper, rf);
    cql3::statements::ks_prop_defs props;
    opts["class"] = sstring("NetworkTopologyStrategy");
    props.add_property(cql3::statements::ks_prop_defs::KW_REPLICATION, opts);
    std::map<sstring, sstring> tablet_opts;
    if (initial_tablets) {
        tablet_opts["initial"] = std::to_string(*initial_tablets);
    }
    tablet_opts["enabled"] = initial_tablets ? "true" : "false";
    props.add_property(cql3::statements::ks_prop_defs::KW_TABLETS, std::move(tablet_opts));
    props.validate();
    return props.as_ks_metadata(sstring(keyspace_name), *sp.get_token_metadata_ptr(), feat, sp.local_db().get_config());
}

future<> executor::start() {
    // Currently, nothing to do on initialization. We delay the keyspace
    // creation (create_keyspace()) until a table is actually created.
    return make_ready_future<>();
}

future<> executor::stop() {
    co_await _describe_table_info_manager->stop();
    // disconnect from the value source, but keep the value unchanged.
    s_default_timeout_in_ms = utils::updateable_value<uint32_t>{s_default_timeout_in_ms()};
    co_await _parsed_expression_cache->stop();
}

} // namespace alternator
