/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>
#include <seastar/core/sleep.hh>
#include "alternator/executor.hh"
#include "cdc/log.hh"
#include "auth/service.hh"
#include "db/config.hh"
#include "log.hh"
#include "schema/schema_builder.hh"
#include "exceptions/exceptions.hh"
#include "service/client_state.hh"
#include "timestamp.hh"
#include "types/map.hh"
#include "schema/schema.hh"
#include "query-request.hh"
#include "query-result-reader.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "bytes.hh"
#include "service/pager/query_pagers.hh"
#include <functional>
#include "error.hh"
#include "serialization.hh"
#include "expressions.hh"
#include "conditions.hh"
#include "cql3/util.hh"
#include <optional>
#include "utils/assert.hh"
#include "utils/overloaded_functor.hh"
#include <seastar/json/json_elements.hh>
#include <boost/algorithm/cxx11/any_of.hpp>
#include "collection_mutation.hh"
#include "schema/schema.hh"
#include "db/tags/extension.hh"
#include "db/tags/utils.hh"
#include "replica/database.hh"
#include "alternator/rmw_operation.hh"
#include <seastar/core/coroutine.hh>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm/find_end.hpp>
#include <unordered_set>
#include "service/storage_proxy.hh"
#include "gms/gossiper.hh"
#include "utils/error_injection.hh"
#include "db/schema_tables.hh"
#include "utils/rjson.hh"

using namespace std::chrono_literals;

logging::logger elogger("alternator-executor");

namespace alternator {

enum class table_status {
    active = 0,
    creating,
    updating,
    deleting
};

static sstring_view table_status_to_sstring(table_status tbl_status) {
    switch(tbl_status) {
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

static lw_shared_ptr<keyspace_metadata> create_keyspace_metadata(std::string_view keyspace_name, service::storage_proxy& sp, gms::gossiper& gossiper, api::timestamp_type, const std::map<sstring, sstring>& tags_map);

static map_type attrs_type() {
    static thread_local auto t = map_type_impl::get_instance(utf8_type, bytes_type, true);
    return t;
}

static const column_definition& attrs_column(const schema& schema) {
    const column_definition* cdef = schema.get_column_definition(bytes(executor::ATTRS_COLUMN_NAME));
    SCYLLA_ASSERT(cdef);
    return *cdef;
}

make_jsonable::make_jsonable(rjson::value&& value)
    : _value(std::move(value))
{}
std::string make_jsonable::to_json() const {
    return rjson::print(_value);
}

json::json_return_type make_streamed(rjson::value&& value) {
    // CMH. json::json_return_type uses std::function, not noncopyable_function. 
    // Need to make a copyable version of value. Gah.
    auto rs = make_shared<rjson::value>(std::move(value));
    std::function<future<>(output_stream<char>&&)> func = [rs](output_stream<char>&& os) mutable -> future<> {
        // move objects to coroutine frame.
        auto los = std::move(os);
        auto lrs = std::move(rs);
        std::exception_ptr ex;
        try {
            co_await rjson::print(*lrs, los);
        } catch (...) {
            // at this point, we cannot really do anything. HTTP headers and return code are
            // already written, and quite potentially a portion of the content data.
            // just log + rethrow. It is probably better the HTTP server closes connection
            // abruptly or something...
            ex = std::current_exception();
            elogger.error("Exception during streaming HTTP response: {}", ex);
        }
        co_await los.close();
        co_await rjson::destroy_gently(std::move(*lrs));
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
        co_return;
    };
    return func;
}

json_string::json_string(std::string&& value)
    : _value(std::move(value))
{}
std::string json_string::to_json() const {
    return _value;
}

void executor::supplement_table_info(rjson::value& descr, const schema& schema, service::storage_proxy& sp) {
    rjson::add(descr, "CreationDateTime", rjson::value(std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count()));
    rjson::add(descr, "TableStatus", "ACTIVE");
    rjson::add(descr, "TableId", rjson::from_string(schema.id().to_sstring()));

    executor::supplement_table_stream_info(descr, schema, sp);
}

// We would have liked to support table names up to 255 bytes, like DynamoDB.
// But Scylla creates a directory whose name is the table's name plus 33
// bytes (dash and UUID), and since directory names are limited to 255 bytes,
// we need to limit table names to 222 bytes, instead of 255.
// See https://github.com/scylladb/scylla/issues/4480
static constexpr int max_table_name_length = 222;

static bool valid_table_name_chars(std::string_view name) {
    for (auto c : name) {
        if ((c < 'a' || c > 'z') &&
            (c < 'A' || c > 'Z') &&
            (c < '0' || c > '9') &&
            c != '_' &&
            c != '-' &&
            c != '.') {
            return false;
        }
    }
    return true;
}

// The DynamoDB developer guide, https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
// specifies that table names "names must be between 3 and 255 characters long
// and can contain only the following characters: a-z, A-Z, 0-9, _ (underscore), - (dash), . (dot)
// validate_table_name throws the appropriate api_error if this validation fails.
static void validate_table_name(const std::string& name) {
    if (name.length() < 3 || name.length() > max_table_name_length) {
        throw api_error::validation(
                format("TableName must be at least 3 characters long and at most {} characters long", max_table_name_length));
    }
    if (!valid_table_name_chars(name)) {
        throw api_error::validation(
                "TableName must satisfy regular expression pattern: [a-zA-Z0-9_.-]+");
    }
}

// In DynamoDB index names are local to a table, while in Scylla, materialized
// view names are global (in a keyspace). So we need to compose a unique name
// for the view taking into account both the table's name and the index name.
// We concatenate the table and index name separated by a delim character
// (a character not allowed by DynamoDB in ordinary table names, default: ":").
// The downside of this approach is that it limits the sum of the lengths,
// instead of each component individually as DynamoDB does.
// The view_name() function assumes the table_name has already been validated
// but validates the legality of index_name and the combination of both.
static std::string view_name(const std::string& table_name, std::string_view index_name, const std::string& delim = ":") {
    if (index_name.length() < 3) {
        throw api_error::validation("IndexName must be at least 3 characters long");
    }
    if (!valid_table_name_chars(index_name)) {
        throw api_error::validation(
                format("IndexName '{}' must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", index_name));
    }
    std::string ret = table_name + delim + std::string(index_name);
    if (ret.length() > max_table_name_length) {
        throw api_error::validation(
                format("The total length of TableName ('{}') and IndexName ('{}') cannot exceed {} characters",
                        table_name, index_name, max_table_name_length - delim.size()));
    }
    return ret;
}

static std::string lsi_name(const std::string& table_name, std::string_view index_name) {
    return view_name(table_name, index_name, "!:");
}

/** Extract table name from a request.
 *  Most requests expect the table's name to be listed in a "TableName" field.
 *  This convenience function returns the name or api_error in case the
 *  table name is missing or not a string.
 */
static std::optional<std::string> find_table_name(const rjson::value& request) {
    const rjson::value* table_name_value = rjson::find(request, "TableName");
    if (!table_name_value) {
        return std::nullopt;
    }
    if (!table_name_value->IsString()) {
        throw api_error::validation("Non-string TableName field in request");
    }
    std::string table_name = table_name_value->GetString();
    return table_name;
}

static std::string get_table_name(const rjson::value& request) {
    auto name = find_table_name(request);
    if (!name) {
        throw api_error::validation("Missing TableName field in request");
    }
    return *name;
}

/** Extract table schema from a request.
 *  Many requests expect the table's name to be listed in a "TableName" field
 *  and need to look it up as an existing table. This convenience function
 *  does this, with the appropriate validation and api_error in case the table
 *  name is missing, invalid or the table doesn't exist. If everything is
 *  successful, it returns the table's schema.
 */
schema_ptr executor::find_table(service::storage_proxy& proxy, const rjson::value& request) {
    auto table_name = find_table_name(request);
    if (!table_name) {
        return nullptr;
    }
    try {
        return proxy.data_dictionary().find_schema(sstring(executor::KEYSPACE_NAME_PREFIX) + sstring(*table_name), *table_name);
    } catch(data_dictionary::no_such_column_family&) {
        // DynamoDB returns validation error even when table does not exist
        // and the table name is invalid.
        validate_table_name(table_name.value());

        throw api_error::resource_not_found(
                format("Requested resource not found: Table: {} not found", *table_name));
    }
}

schema_ptr get_table(service::storage_proxy& proxy, const rjson::value& request) {
    auto schema = executor::find_table(proxy, request);
    if (!schema) {
        // if we get here then the name was missing, since syntax or missing actual CF 
        // checks throw. Slow path, but just call get_table_name to generate exception. 
        get_table_name(request);
    }
    return schema;
}

static std::tuple<bool, std::string_view, std::string_view> try_get_internal_table(data_dictionary::database db, std::string_view table_name) {
    size_t it = table_name.find(executor::INTERNAL_TABLE_PREFIX);
    if (it != 0) {
        return {false, "", ""};
    }
    table_name.remove_prefix(executor::INTERNAL_TABLE_PREFIX.size());
    size_t delim = table_name.find_first_of('.');
    if (delim == std::string_view::npos) {
        return {false, "", ""};
    }
    std::string_view ks_name = table_name.substr(0, delim);
    table_name.remove_prefix(ks_name.size() + 1);
    // Only internal keyspaces can be accessed to avoid leakage
    auto ks = db.try_find_keyspace(ks_name);
    if (!ks || !ks->is_internal()) {
        return {false, "", ""};
    }
    return {true, ks_name, table_name};
}

// get_table_or_view() is similar to to get_table(), except it returns either
// a table or a materialized view from which to read, based on the TableName
// and optional IndexName in the request. Only requests like Query and Scan
// which allow IndexName should use this function.
enum class table_or_view_type { base, lsi, gsi };
static std::pair<schema_ptr, table_or_view_type>
get_table_or_view(service::storage_proxy& proxy, const rjson::value& request) {
    table_or_view_type type = table_or_view_type::base;
    std::string table_name = get_table_name(request);

    auto [is_internal_table, internal_ks_name, internal_table_name] = try_get_internal_table(proxy.data_dictionary(), table_name);
    if (is_internal_table) {
        try {
            return { proxy.data_dictionary().find_schema(sstring(internal_ks_name), sstring(internal_table_name)), type };
        } catch (data_dictionary::no_such_column_family&) {
            // DynamoDB returns validation error even when table does not exist
            // and the table name is invalid.
            validate_table_name(table_name);

            throw api_error::resource_not_found(
                format("Requested resource not found: Internal table: {}.{} not found", internal_ks_name, internal_table_name));
        }
    }

    std::string keyspace_name = executor::KEYSPACE_NAME_PREFIX + table_name;
    const rjson::value* index_name = rjson::find(request, "IndexName");
    std::string orig_table_name;
    if (index_name) {
        if (index_name->IsString()) {
            orig_table_name = std::move(table_name);
            table_name = view_name(orig_table_name, rjson::to_string_view(*index_name));
            type = table_or_view_type::gsi;
        } else {
            throw api_error::validation(
                    format("Non-string IndexName '{}'", rjson::to_string_view(*index_name)));
        }
        // If no tables for global indexes were found, the index may be local
        if (!proxy.data_dictionary().has_schema(keyspace_name, table_name)) {
            type = table_or_view_type::lsi;
            table_name = lsi_name(orig_table_name, rjson::to_string_view(*index_name));
        }
    }

    try {
        return { proxy.data_dictionary().find_schema(keyspace_name, table_name), type };
    } catch(data_dictionary::no_such_column_family&) {
        if (index_name) {
            // DynamoDB returns a different error depending on whether the
            // base table doesn't exist (ResourceNotFoundException) or it
            // does exist but the index does not (ValidationException).
            if (proxy.data_dictionary().has_schema(keyspace_name, orig_table_name)) {
                throw api_error::validation(
                    format("Requested resource not found: Index '{}' for table '{}'", index_name->GetString(), orig_table_name));
            } else {
                throw api_error::resource_not_found(
                    format("Requested resource not found: Table: {} not found", orig_table_name));
            }
        } else {
            throw api_error::resource_not_found(
                format("Requested resource not found: Table: {} not found", table_name));
        }
    }
}

// Convenience function for getting the value of a string attribute, or a
// default value if it is missing. If the attribute exists, but is not a
// string, a descriptive api_error is thrown.
static std::string get_string_attribute(const rjson::value& value, std::string_view attribute_name, const char* default_return) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value)
        return default_return;
    if (!attribute_value->IsString()) {
        throw api_error::validation(format("Expected string value for attribute {}, got: {}",
                attribute_name, value));
    }
    return std::string(attribute_value->GetString(), attribute_value->GetStringLength());
}

// Convenience function for getting the value of a boolean attribute, or a
// default value if it is missing. If the attribute exists, but is not a
// bool, a descriptive api_error is thrown.
static bool get_bool_attribute(const rjson::value& value, std::string_view attribute_name, bool default_return) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value) {
        return default_return;
    }
    if (!attribute_value->IsBool()) {
        throw api_error::validation(format("Expected boolean value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetBool();
}

// Convenience function for getting the value of an integer attribute, or
// an empty optional if it is missing. If the attribute exists, but is not
// an integer, a descriptive api_error is thrown.
static std::optional<int> get_int_attribute(const rjson::value& value, std::string_view attribute_name) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value)
        return {};
    if (!attribute_value->IsInt()) {
        throw api_error::validation(format("Expected integer value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetInt();
}

// Sets a KeySchema object inside the given JSON parent describing the key
// attributes of the the given schema as being either HASH or RANGE keys.
// Additionally, adds to a given map mappings between the key attribute
// names and their type (as a DynamoDB type string).
void executor::describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string,std::string>* attribute_types) {
    rjson::value key_schema = rjson::empty_array();
    for (const column_definition& cdef : schema.partition_key_columns()) {
        rjson::value key = rjson::empty_object();
        rjson::add(key, "AttributeName", rjson::from_string(cdef.name_as_text()));
        rjson::add(key, "KeyType", "HASH");
        rjson::push_back(key_schema, std::move(key));
        if (attribute_types) {
            (*attribute_types)[cdef.name_as_text()] = type_to_string(cdef.type);
        }
    }
    for (const column_definition& cdef : schema.clustering_key_columns()) {
        rjson::value key = rjson::empty_object();
        rjson::add(key, "AttributeName", rjson::from_string(cdef.name_as_text()));
        rjson::add(key, "KeyType", "RANGE");
        rjson::push_back(key_schema, std::move(key));
        if (attribute_types) {
            (*attribute_types)[cdef.name_as_text()] = type_to_string(cdef.type);
        }
        // FIXME: this "break" can avoid listing some clustering key columns
        // we added for GSIs just because they existed in the base table -
        // but not in all cases. We still have issue #5320. See also
        // reproducer in test_gsi_2_describe_table_schema.
        break;
    }
    rjson::add(parent, "KeySchema", std::move(key_schema));

}

void executor::describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string,std::string>& attribute_types) {
    describe_key_schema(parent, schema, &attribute_types);
}

static rjson::value generate_arn_for_table(const schema& schema) {
    return rjson::from_string(format("arn:scylla:alternator:{}:scylla:table/{}", schema.ks_name(), schema.cf_name()));
}

static rjson::value generate_arn_for_index(const schema& schema, std::string_view index_name) {
    return rjson::from_string(format(
        "arn:scylla:alternator:{}:scylla:table/{}/index/{}",
        schema.ks_name(), schema.cf_name(), index_name));
}

static rjson::value fill_table_description(schema_ptr schema, table_status tbl_status, service::storage_proxy const& proxy)
{
    rjson::value table_description = rjson::empty_object();
    rjson::add(table_description, "TableName", rjson::from_string(schema->cf_name()));
    // FIXME: take the tables creation time, not the current time!
    size_t creation_date_seconds = std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count();
    // FIXME: In DynamoDB the CreateTable implementation is asynchronous, and
    // the table may be in "Creating" state until creating is finished.
    // We don't currently do this in Alternator - instead CreateTable waits
    // until the table is really available. So/ DescribeTable returns either
    // ACTIVE or doesn't exist at all (and DescribeTable returns an error).
    // The states CREATING and UPDATING are not currently returned.
    rjson::add(table_description, "TableStatus", rjson::from_string(table_status_to_sstring(tbl_status)));
    rjson::add(table_description, "TableArn", generate_arn_for_table(*schema));
    rjson::add(table_description, "TableId", rjson::from_string(schema->id().to_sstring()));
    // FIXME: Instead of hardcoding, we should take into account which mode was chosen
    // when the table was created. But, Spark jobs expect something to be returned
    // and PAY_PER_REQUEST seems closer to reality than PROVISIONED.
    rjson::add(table_description, "BillingModeSummary", rjson::empty_object());
    rjson::add(table_description["BillingModeSummary"], "BillingMode", "PAY_PER_REQUEST");
    rjson::add(table_description["BillingModeSummary"], "LastUpdateToPayPerRequestDateTime", rjson::value(creation_date_seconds));
    // In PAY_PER_REQUEST billing mode, provisioned capacity should return 0
    rjson::add(table_description, "ProvisionedThroughput", rjson::empty_object());
    rjson::add(table_description["ProvisionedThroughput"], "ReadCapacityUnits", 0);
    rjson::add(table_description["ProvisionedThroughput"], "WriteCapacityUnits", 0);
    rjson::add(table_description["ProvisionedThroughput"], "NumberOfDecreasesToday", 0);

   

    data_dictionary::table t = proxy.data_dictionary().find_column_family(schema);
    
    if (tbl_status != table_status::deleting) {
        rjson::add(table_description, "CreationDateTime", rjson::value(creation_date_seconds));
        std::unordered_map<std::string,std::string> key_attribute_types;
        // Add base table's KeySchema and collect types for AttributeDefinitions:
        executor::describe_key_schema(table_description, *schema, key_attribute_types);
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
                // Add indexes's KeySchema and collect types for AttributeDefinitions:
                executor::describe_key_schema(view_entry, *vptr, key_attribute_types);
                // Add projection type
                rjson::value projection = rjson::empty_object();
                rjson::add(projection, "ProjectionType", "ALL");
                // FIXME: we have to get ProjectionType from the schema when it is added
                rjson::add(view_entry, "Projection", std::move(projection));
                // Local secondary indexes are marked by an extra '!' sign occurring before the ':' delimiter
                rjson::value& index_array = (delim_it > 1 && cf_name[delim_it-1] == '!') ? lsi_array : gsi_array;
                rjson::push_back(index_array, std::move(view_entry));
            }
            if (!lsi_array.Empty()) {
                rjson::add(table_description, "LocalSecondaryIndexes", std::move(lsi_array));
            }
            if (!gsi_array.Empty()) {
                rjson::add(table_description, "GlobalSecondaryIndexes", std::move(gsi_array));
            }
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
    executor::supplement_table_stream_info(table_description, *schema, proxy);

    // FIXME: still missing some response fields (issue #5026)
    return table_description;
}

bool is_alternator_keyspace(const sstring& ks_name) {
    return ks_name.find(executor::KEYSPACE_NAME_PREFIX) == 0;
}

sstring executor::table_name(const schema& s) {
    return s.cf_name();
}

future<executor::request_return_type> executor::describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.describe_table++;
    elogger.trace("Describing table {}", request);

    schema_ptr schema = get_table(_proxy, request);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    rjson::value table_description = fill_table_description(schema, table_status::active, _proxy);
    rjson::value response = rjson::empty_object();
    rjson::add(response, "Table", std::move(table_description));
    elogger.trace("returning {}", response);
    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(response)));
}

future<executor::request_return_type> executor::delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.delete_table++;
    elogger.trace("Deleting table {}", request);

    std::string table_name = get_table_name(request);
    // DynamoDB returns validation error even when table does not exist
    // and the table name is invalid.
    validate_table_name(table_name);

    std::string keyspace_name = executor::KEYSPACE_NAME_PREFIX + table_name;
    tracing::add_table_name(trace_state, keyspace_name, table_name);
    auto& p = _proxy.container();

    schema_ptr schema = get_table(_proxy, request);
    rjson::value table_description = fill_table_description(schema, table_status::deleting, _proxy);

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::DROP,
            auth::make_data_resource(schema->ks_name(), schema->cf_name())))) {
        co_return api_error::access_denied(format(
            "DROP permissions denied on {}.{} by RBAC", schema->ks_name(), schema->cf_name()));
    }

    co_await _mm.container().invoke_on(0, [&, cs = client_state.move_to_other_shard()] (service::migration_manager& mm) -> future<> {
        // FIXME: the following needs to be in a loop. If mm.announce() below
        // fails, we need to retry the whole thing.
        auto group0_guard = co_await mm.start_group0_operation();

        std::optional<data_dictionary::table> tbl = p.local().data_dictionary().try_find_table(keyspace_name, table_name);
        if (!tbl) {
            throw api_error::resource_not_found(format("Requested resource not found: Table: {} not found", table_name));
        }

        auto m = co_await service::prepare_column_family_drop_announcement(_proxy, keyspace_name, table_name, group0_guard.write_timestamp(), service::drop_views::yes);
        auto m2 = co_await service::prepare_keyspace_drop_announcement(_proxy.local_db(), keyspace_name, group0_guard.write_timestamp());

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
        co_await auth::revoke_all(*cs.get().get_auth_service(),
            auth::make_data_resource(schema->ks_name(), schema->cf_name()), mc);
        for (const view_ptr& v : tbl->views()) {
            co_await auth::revoke_all(*cs.get().get_auth_service(),
                auth::make_data_resource(v->ks_name(), v->cf_name()), mc);
        }
        std::tie(m, group0_guard) = co_await std::move(mc).extract();

        co_await mm.announce(std::move(m), std::move(group0_guard), format("alternator-executor: delete {} table", table_name));
    });

    rjson::value response = rjson::empty_object();
    rjson::add(response, "TableDescription", std::move(table_description));
    elogger.trace("returning {}", response);
    co_return make_jsonable(std::move(response));
}

static data_type parse_key_type(const std::string& type) {
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
            format("Invalid key type '{}', can only be S, B or N.", type));
}


static void add_column(schema_builder& builder, const std::string& name, const rjson::value& attribute_definitions, column_kind kind) {
    // FIXME: Currently, the column name ATTRS_COLUMN_NAME is not allowed
    // because we use it for our untyped attribute map, and we can't have a
    // second column with the same name. We should fix this, by renaming
    // some column names which we want to reserve.
    if (name == executor::ATTRS_COLUMN_NAME) {
        throw api_error::validation(format("Column name '{}' is currently reserved. FIXME.", name));
    }
    for (auto it = attribute_definitions.Begin(); it != attribute_definitions.End(); ++it) {
        const rjson::value& attribute_info = *it;
        if (attribute_info["AttributeName"].GetString() == name) {
            auto type = attribute_info["AttributeType"].GetString();
            builder.with_column(to_bytes(name), parse_key_type(type), kind);
            return;
        }
    }
    throw api_error::validation(
            format("KeySchema key '{}' missing in AttributeDefinitions", name));
}

// Parse the KeySchema request attribute, which specifies the column names
// for a key. A KeySchema must include up to two elements, the first must be
// the HASH key name, and the second one, if exists, must be a RANGE key name.
// The function returns the two column names - the first is the hash key
// and always present, the second is the range key and may be an empty string.
static std::pair<std::string, std::string> parse_key_schema(const rjson::value& obj) {
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
    if (!v || !v->IsString() || v->GetString() != std::string("HASH")) {
        throw api_error::validation("First key in KeySchema must be a HASH key");
    }
    v = rjson::find((*key_schema)[0], "AttributeName");
    if (!v || !v->IsString()) {
        throw api_error::validation("First key in KeySchema must have string AttributeName");
    }
    std::string hash_key = v->GetString();
    std::string range_key;
    if (key_schema->Size() == 2) {
        if (!(*key_schema)[1].IsObject()) {
            throw api_error::validation("Second element of KeySchema must be an object");
        }
        v = rjson::find((*key_schema)[1], "KeyType");
        if (!v || !v->IsString() || v->GetString() != std::string("RANGE")) {
            throw api_error::validation("Second key in KeySchema must be a RANGE key");
        }
        v = rjson::find((*key_schema)[1], "AttributeName");
        if (!v || !v->IsString()) {
            throw api_error::validation("Second key in KeySchema must have string AttributeName");
        }
        range_key = v->GetString();
    }
    return {hash_key, range_key};
}

static schema_ptr get_table_from_arn(service::storage_proxy& proxy, std::string_view arn) {
    // Expected format: arn:scylla:alternator:${KEYSPACE_NAME}:scylla:table/${TABLE_NAME};
    constexpr size_t prefix_size = sizeof("arn:scylla:alternator:") - 1;
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
        size_t keyspace_end = arn.find_first_of(':', prefix_size);
        std::string_view keyspace_name = arn.substr(prefix_size, keyspace_end - prefix_size);
        size_t table_start = arn.find_first_of('/');
        std::string_view table_name = arn.substr(table_start + 1);
        if (table_name.find('/') != std::string_view::npos) {
            // A table name cannot contain a '/' - if it does, it's not a
            // table ARN, it may be an index. DynamoDB returns a
            // ValidationException in that case - see #10786.
            throw api_error::validation(format("ResourceArn '{}' is not a valid table ARN", table_name));
        }
        // FIXME: remove sstring creation once find_schema gains a view-based interface
        return proxy.data_dictionary().find_schema(sstring(keyspace_name), sstring(table_name));
    } catch (const data_dictionary::no_such_column_family& e) {
        throw api_error::access_denied("Incorrect resource identifier");
    } catch (const std::out_of_range& e) {
        throw api_error::access_denied("Incorrect resource identifier");
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
                    format("Incorrect write isolation tag {}. Allowed values: {}", value, allowed_write_isolation_values));
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
        throw std::runtime_error(format("Invalid --alternator-write-isolation "
                "setting '{}'. Allowed values: {}.",
                value, allowed_write_isolation_values));
    }
    default_write_isolation = parse_write_isolation(value);
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
            tags_map.erase(sstring(it->GetString(), it->GetStringLength()));
        }
    }

    if (tags_map.size() > 50) {
        throw api_error::validation("Number of Tags exceed the current limit for the provided ResourceArn");
    }
    validate_tags(tags_map);
}

const std::map<sstring, sstring>& get_tags_of_table_or_throw(schema_ptr schema) {
    auto tags_ptr = db::get_tags_of_table(schema);
    if (tags_ptr) {
        return *tags_ptr;
    } else {
        throw api_error::validation(format("Table {} does not have valid tagging information", schema->ks_name()));
    }
}

future<executor::request_return_type> executor::tag_resource(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.tag_resource++;

    const rjson::value* arn = rjson::find(request, "ResourceArn");
    if (!arn || !arn->IsString()) {
        co_return api_error::access_denied("Incorrect resource identifier");
    }
    schema_ptr schema = get_table_from_arn(_proxy, rjson::to_string_view(*arn));
    const rjson::value* tags = rjson::find(request, "Tags");
    if (!tags || !tags->IsArray()) {
        co_return api_error::validation("Cannot parse tags");
    }
    if (tags->Size() < 1) {
        co_return api_error::validation("The number of tags must be at least 1") ;
    }
    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::ALTER, auth::make_data_resource(schema->ks_name(), schema->cf_name())))) {
        co_return api_error::access_denied(format(
            "ALTER permissions denied on {}.{} by RBAC", schema->ks_name(), schema->cf_name()));
    }
    co_await db::modify_tags(_mm, schema->ks_name(), schema->cf_name(), [tags](std::map<sstring, sstring>& tags_map) {
        update_tags_map(*tags, tags_map, update_tags_action::add_tags);
    });
    co_return json_string("");
}

future<executor::request_return_type> executor::untag_resource(client_state& client_state, service_permit permit, rjson::value request) {
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
    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::ALTER, auth::make_data_resource(schema->ks_name(), schema->cf_name())))) {
        co_return api_error::access_denied(format(
            "ALTER permissions denied on {}.{} by RBAC", schema->ks_name(), schema->cf_name()));
    }

    co_await db::modify_tags(_mm, schema->ks_name(), schema->cf_name(), [tags](std::map<sstring, sstring>& tags_map) {
        update_tags_map(*tags, tags_map, update_tags_action::delete_tags);
    });
    co_return json_string("");
}

future<executor::request_return_type> executor::list_tags_of_resource(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.list_tags_of_resource++;
    const rjson::value* arn = rjson::find(request, "ResourceArn");
    if (!arn || !arn->IsString()) {
        return make_ready_future<request_return_type>(api_error::access_denied("Incorrect resource identifier"));
    }
    schema_ptr schema = get_table_from_arn(_proxy, rjson::to_string_view(*arn));

    auto tags_map = get_tags_of_table_or_throw(schema);
    rjson::value ret = rjson::empty_object();
    rjson::add(ret, "Tags", rjson::empty_array());

    rjson::value& tags = ret["Tags"];
    for (auto& tag_entry : tags_map) {
        rjson::value new_entry = rjson::empty_object();
        rjson::add(new_entry, "Key", rjson::from_string(tag_entry.first));
        rjson::add(new_entry, "Value", rjson::from_string(tag_entry.second));
        rjson::push_back(tags, std::move(new_entry));
    }

    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
}

static void verify_billing_mode(const rjson::value& request) {
        // Alternator does not yet support billing or throughput limitations, but
    // let's verify that BillingMode is at least legal.
    std::string billing_mode = get_string_attribute(request, "BillingMode", "PROVISIONED");
    if (billing_mode == "PAY_PER_REQUEST") {
        if (rjson::find(request, "ProvisionedThroughput")) {
            throw api_error::validation("When BillingMode=PAY_PER_REQUEST, ProvisionedThroughput cannot be specified.");
        }
    } else if (billing_mode == "PROVISIONED") {
        if (!rjson::find(request, "ProvisionedThroughput")) {
            throw api_error::validation("When BillingMode=PROVISIONED, ProvisionedThroughput must be specified.");
        }
    } else {
        throw api_error::validation("Unknown BillingMode={}. Must be PAY_PER_REQUEST or PROVISIONED.");
    }
}

// Validate that a AttributeDefinitions parameter in CreateTable is valid, and
// throws user-facing api_error::validation if it's not.
// In particular, verify that the same AttributeName doesn't appear more than
// once (Issue #13870).
static void validate_attribute_definitions(const rjson::value& attribute_definitions){
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
        auto [it2, added] = seen_attribute_names.emplace(rjson::to_string_view(*attribute_name));
        if (!added) {
            throw api_error::validation(format("Duplicate AttributeName={} in AttributeDefinitions",
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
}

static future<executor::request_return_type> create_table_on_shard0(service::client_state&& client_state, tracing::trace_state_ptr trace_state, rjson::value request, service::storage_proxy& sp, service::migration_manager& mm, gms::gossiper& gossiper) {
    SCYLLA_ASSERT(this_shard_id() == 0);

    // We begin by parsing and validating the content of the CreateTable
    // command. We can't inspect the current database schema at this point
    // (e.g., verify that this table doesn't already exist) - we can only
    // do this further down - after taking group0_guard.
    std::string table_name = get_table_name(request);
    validate_table_name(table_name);

    if (table_name.find(executor::INTERNAL_TABLE_PREFIX) == 0) {
        co_return api_error::validation(format("Prefix {} is reserved for accessing internal tables", executor::INTERNAL_TABLE_PREFIX));
    }
    std::string keyspace_name = executor::KEYSPACE_NAME_PREFIX + table_name;
    const rjson::value& attribute_definitions = request["AttributeDefinitions"];
    validate_attribute_definitions(attribute_definitions);

    tracing::add_table_name(trace_state, keyspace_name, table_name);

    schema_builder builder(keyspace_name, table_name);
    auto [hash_key, range_key] = parse_key_schema(request);
    add_column(builder, hash_key, attribute_definitions, column_kind::partition_key);
    if (!range_key.empty()) {
        add_column(builder, range_key, attribute_definitions, column_kind::clustering_key);
    }
    builder.with_column(bytes(executor::ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);

    verify_billing_mode(request);

    schema_ptr partial_schema = builder.build();

    // Parse GlobalSecondaryIndexes parameters before creating the base
    // table, so if we have a parse errors we can fail without creating
    // any table.
    const rjson::value* gsi = rjson::find(request, "GlobalSecondaryIndexes");
    std::vector<schema_builder> view_builders;
    std::vector<sstring> where_clauses;
    std::unordered_set<std::string> index_names;
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
                co_return api_error::validation(format("Duplicate IndexName '{}', ", index_name));
            }
            std::string vname(view_name(table_name, index_name));
            elogger.trace("Adding GSI {}", index_name);
            // FIXME: read and handle "Projection" parameter. This will
            // require the MV code to copy just parts of the attrs map.
            schema_builder view_builder(keyspace_name, vname);
            auto [view_hash_key, view_range_key] = parse_key_schema(g);
            if (partial_schema->get_column_definition(to_bytes(view_hash_key)) == nullptr) {
                // A column that exists in a global secondary index is upgraded from being a map entry
                // to having a regular column definition in the base schema
                add_column(builder, view_hash_key, attribute_definitions, column_kind::regular_column);
            }
            add_column(view_builder, view_hash_key, attribute_definitions, column_kind::partition_key);
            if (!view_range_key.empty()) {
                if (partial_schema->get_column_definition(to_bytes(view_range_key)) == nullptr) {
                    // A column that exists in a global secondary index is upgraded from being a map entry
                    // to having a regular column definition in the base schema
                    if (partial_schema->get_column_definition(to_bytes(view_hash_key)) == nullptr) {
                        // FIXME: this is alternator limitation only, because Scylla's materialized views
                        // we use underneath do not allow more than 1 base regular column to be part of the MV key
                        elogger.warn("Only 1 regular column from the base table should be used in the GSI key in order to ensure correct liveness management without assumptions");
                    }
                    add_column(builder, view_range_key, attribute_definitions, column_kind::regular_column);
                }
                add_column(view_builder, view_range_key, attribute_definitions, column_kind::clustering_key);
            }
            // Base key columns which aren't part of the index's key need to
            // be added to the view nonetheless, as (additional) clustering
            // key(s).
            if  (hash_key != view_hash_key && hash_key != view_range_key) {
                add_column(view_builder, hash_key, attribute_definitions, column_kind::clustering_key);
            }
            if  (!range_key.empty() && range_key != view_hash_key && range_key != view_range_key) {
                add_column(view_builder, range_key, attribute_definitions, column_kind::clustering_key);
            }
            // GSIs have no tags:
            view_builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>());
            sstring where_clause = format("{} IS NOT NULL", cql3::util::maybe_quote(view_hash_key));
            if (!view_range_key.empty()) {
                where_clause = format("{} AND {} IS NOT NULL", where_clause,
                    cql3::util::maybe_quote(view_range_key));
            }
            where_clauses.push_back(std::move(where_clause));
            view_builders.emplace_back(std::move(view_builder));
        }
    }

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
                co_return api_error::validation(format("Duplicate IndexName '{}', ", index_name));
            }
            std::string vname(lsi_name(table_name, index_name));
            elogger.trace("Adding LSI {}", index_name);
            if (range_key.empty()) {
                co_return api_error::validation("LocalSecondaryIndex requires that the base table have a range key");
            }
            // FIXME: read and handle "Projection" parameter. This will
            // require the MV code to copy just parts of the attrs map.
            schema_builder view_builder(keyspace_name, vname);
            auto [view_hash_key, view_range_key] = parse_key_schema(l);
            if (view_hash_key != hash_key) {
                co_return api_error::validation("LocalSecondaryIndex hash key must match the base table hash key");
            }
            add_column(view_builder, view_hash_key, attribute_definitions, column_kind::partition_key);
            if (view_range_key.empty()) {
                co_return api_error::validation("LocalSecondaryIndex must specify a sort key");
            }
            if (view_range_key == hash_key) {
                co_return api_error::validation("LocalSecondaryIndex sort key cannot be the same as hash key");
              }
            if (view_range_key != range_key) {
                add_column(builder, view_range_key, attribute_definitions, column_kind::regular_column);
            }
            add_column(view_builder, view_range_key, attribute_definitions, column_kind::clustering_key);
            // Base key columns which aren't part of the index's key need to
            // be added to the view nonetheless, as (additional) clustering
            // key(s).
            if  (!range_key.empty() && view_range_key != range_key) {
                add_column(view_builder, range_key, attribute_definitions, column_kind::clustering_key);
            }
            view_builder.with_column(bytes(executor::ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);
            // Note above we don't need to add virtual columns, as all
            // base columns were copied to view. TODO: reconsider the need
            // for virtual columns when we support Projection.
            sstring where_clause = format("{} IS NOT NULL", cql3::util::maybe_quote(view_hash_key));
            if (!view_range_key.empty()) {
                where_clause = format("{} AND {} IS NOT NULL", where_clause,
                    cql3::util::maybe_quote(view_range_key));
            }
            where_clauses.push_back(std::move(where_clause));
            // LSIs have no tags, but Scylla's "synchronous_updates" feature
            // (which an LSIs need), is actually implemented as a tag so we
            // need to add it here:
            std::map<sstring, sstring> tags_map = {{db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY, "true"}};
            view_builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));
            view_builders.emplace_back(std::move(view_builder));
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
        executor::add_stream_options(*stream_specification, builder, sp);
    }

    // Parse the "Tags" parameter early, so we can avoid creating the table
    // at all if this parsing failed.
    const rjson::value* tags = rjson::find(request, "Tags");
    std::map<sstring, sstring> tags_map;
    if (tags && tags->IsArray()) {
        update_tags_map(*tags, tags_map, update_tags_action::add_tags);
    }
    builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::CREATE,
            auth::resource(auth::resource_kind::data)))) {
        co_return api_error::access_denied("CREATE permissions denied on ALL KEYSPACES by RBAC");
    }

    schema_ptr schema = builder.build();
    auto where_clause_it = where_clauses.begin();
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
        view_builder.with_view_info(*schema, include_all_columns, *where_clause_it);
        ++where_clause_it;
    }

    // FIXME: the following needs to be in a loop. If mm.announce() below
    // fails, we need to retry the whole thing.
    auto group0_guard = co_await mm.start_group0_operation();
    auto ts = group0_guard.write_timestamp();
    std::vector<mutation> schema_mutations;
    auto ksm = create_keyspace_metadata(keyspace_name, sp, gossiper, ts, tags_map);
    // Alternator Streams doesn't yet work when the table uses tablets (#16317)
    if (stream_specification && stream_specification->IsObject()) {
        auto stream_enabled = rjson::find(*stream_specification, "StreamEnabled");
        if (stream_enabled && stream_enabled->IsBool() && stream_enabled->GetBool()) {
            locator::replication_strategy_params params(ksm->strategy_options(), ksm->initial_tablets());
            auto rs = locator::abstract_replication_strategy::create_replication_strategy(ksm->strategy_name(), params);
            if (rs->uses_tablets()) {
                co_return api_error::validation("Streams not yet supported on a table using tablets (issue #16317). "
                "If you want to use streams, create a table with vnodes by setting the tag 'experimental:initial_tablets' set to 'none'.");
            }
        }
    }
    try {
        schema_mutations = service::prepare_new_keyspace_announcement(sp.local_db(), ksm, ts);
    } catch (exceptions::already_exists_exception&) {
        if (sp.data_dictionary().has_schema(keyspace_name, table_name)) {
            co_return api_error::resource_in_use(format("Table {} already exists", table_name));
        }
    }
    if (sp.data_dictionary().try_find_table(schema->id())) {
        // This should never happen, the ID is supposed to be unique
        co_return api_error::internal(format("Table with ID {} already exists", schema->id()));
    }
    co_await service::prepare_new_column_family_announcement(schema_mutations, sp, *ksm, schema, ts);
    for (schema_builder& view_builder : view_builders) {
        view_ptr view(view_builder.build());
        db::schema_tables::add_table_or_view_to_schema_mutation(
            view, ts, true, schema_mutations);
        // add_table_or_view_to_schema_mutation() is a low-level function that
        // doesn't call the callbacks that prepare_new_view_announcement()
        // calls. So we need to call this callback here :-( If we don't, among
        // other things *tablets* will not be created for the new view.
        // These callbacks need to be called in a Seastar thread.
        co_await seastar::async([&sp, &ksm, &view, &schema_mutations, ts] {
            return sp.local_db().get_notifier().before_create_column_family(*ksm, *view, schema_mutations, ts);
        });

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
    co_await auth::grant_applicable_permissions(
        *client_state.get_auth_service(), *client_state.user(),
        auth::make_data_resource(schema->ks_name(), schema->cf_name()), mc);
    for (const schema_builder& view_builder : view_builders) {
        co_await auth::grant_applicable_permissions(
            *client_state.get_auth_service(), *client_state.user(),
            auth::make_data_resource(view_builder.ks_name(), view_builder.cf_name()), mc);
    }
    std::tie(schema_mutations, group0_guard) = co_await std::move(mc).extract();

    co_await mm.announce(std::move(schema_mutations), std::move(group0_guard), format("alternator-executor: create {} table", table_name));

    co_await mm.wait_for_schema_agreement(sp.local_db(), db::timeout_clock::now() + 10s, nullptr);
    rjson::value status = rjson::empty_object();
    executor::supplement_table_info(request, *schema, sp);
    rjson::add(status, "TableDescription", std::move(request));
    co_return make_jsonable(std::move(status));
}

future<executor::request_return_type> executor::create_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.create_table++;
    elogger.trace("Creating table {}", request);

    co_return co_await _mm.container().invoke_on(0, [&, tr = tracing::global_trace_state_ptr(trace_state), request = std::move(request), &sp = _proxy.container(), &g = _gossiper.container(), client_state_other_shard = client_state.move_to_other_shard()]
                                        (service::migration_manager& mm) mutable -> future<executor::request_return_type> {
        co_return co_await create_table_on_shard0(client_state_other_shard.get(), tr, std::move(request), sp.local(), mm, g.local());
    });
}

future<executor::request_return_type> executor::update_table(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.update_table++;
    elogger.trace("Updating table {}", request);

    static const std::vector<sstring> unsupported = {
        "GlobalSecondaryIndexUpdates", 
        "ProvisionedThroughput",
        "ReplicaUpdates",
        "SSESpecification", 
    };

    for (auto& s : unsupported) {
        if (rjson::find(request, s)) {
            co_await coroutine::return_exception(api_error::validation(s + " not supported"));
        }
    }

    if (rjson::find(request, "BillingMode")) {
        verify_billing_mode(request);
    }

    co_return co_await _mm.container().invoke_on(0, [&p = _proxy.container(), request = std::move(request), gt = tracing::global_trace_state_ptr(std::move(trace_state)), client_state_other_shard = client_state.move_to_other_shard()]
                                                (service::migration_manager& mm) mutable -> future<executor::request_return_type> {
        // FIXME: the following needs to be in a loop. If mm.announce() below
        // fails, we need to retry the whole thing.
        auto group0_guard = co_await mm.start_group0_operation();

        schema_ptr tab = get_table(p.local(), request);

        tracing::add_table_name(gt, tab->ks_name(), tab->cf_name());

        // the ugly but harmless conversion to string_view here is because
        // Seastar's sstring is missing a find(std::string_view) :-()
        if (std::string_view(tab->cf_name()).find(INTERNAL_TABLE_PREFIX) == 0) {
            co_await coroutine::return_exception(api_error::validation(format("Prefix {} is reserved for accessing internal tables", INTERNAL_TABLE_PREFIX)));
        }

        schema_builder builder(tab);

        rjson::value* stream_specification = rjson::find(request, "StreamSpecification");
        if (stream_specification && stream_specification->IsObject()) {
            add_stream_options(*stream_specification, builder, p.local());
            // Alternator Streams doesn't yet work when the table uses tablets (#16317)
            auto stream_enabled = rjson::find(*stream_specification, "StreamEnabled");
            if (stream_enabled && stream_enabled->IsBool() && stream_enabled->GetBool() &&
                p.local().local_db().find_keyspace(tab->ks_name()).get_replication_strategy().uses_tablets()) {
                co_return api_error::validation("Streams not yet supported on a table using tablets (issue #16317). "
                    "If you want to enable streams, re-create this table with vnodes (with the tag 'experimental:initial_tablets' set to 'none').");
            }
        }

        auto schema = builder.build();

        if (!co_await client_state_other_shard.get().check_has_permission(auth::command_desc(
            auth::permission::ALTER, auth::make_data_resource(schema->ks_name(), schema->cf_name())))) {
            co_return api_error::access_denied(format(
                "ALTER permissions denied on {}.{} by RBAC", schema->ks_name(), schema->cf_name()));
        }

        auto m = co_await service::prepare_column_family_update_announcement(p.local(), schema,  std::vector<view_ptr>(), group0_guard.write_timestamp());

        co_await mm.announce(std::move(m), std::move(group0_guard), format("alternator-executor: update {} table", tab->cf_name()));

        co_await mm.wait_for_schema_agreement(p.local().local_db(), db::timeout_clock::now() + 10s, nullptr);

        rjson::value status = rjson::empty_object();
        supplement_table_info(request, *schema, p.local());
        rjson::add(status, "TableDescription", std::move(request));
        co_return make_jsonable(std::move(status));
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

// After calling pk_from_json() and ck_from_json() to extract the pk and ck
// components of a key, and if that succeeded, call check_key() to further
// check that the key doesn't have any spurious components.
static void check_key(const rjson::value& key, const schema_ptr& schema) {
    if (key.MemberCount() != (schema->clustering_key_size() == 0 ? 1 : 2)) {
        throw api_error::validation("Given key attribute not in schema");
    }
}

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
        throw api_error::validation(format("{}: unknown type {} for value {}", caller, type, v));
    }
}

// The put_or_delete_item class builds the mutations needed by the PutItem and
// DeleteItem operations - either as stand-alone commands or part of a list
// of commands in BatchWriteItems.
// put_or_delete_item splits each operation into two stages: Constructing the
// object parses and validates the user input (throwing exceptions if there
// are input errors). Later, build() generates the actual mutation, with a
// specified timestamp. This split is needed because of the peculiar needs of
// BatchWriteItems and LWT. BatchWriteItems needs all parsing to happen before
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
public:
    struct delete_item {};
    struct put_item {};
    put_or_delete_item(const rjson::value& key, schema_ptr schema, delete_item);
    put_or_delete_item(const rjson::value& item, schema_ptr schema, put_item);
    // put_or_delete_item doesn't keep a reference to schema (so it can be
    // moved between shards for LWT) so it needs to be given again to build():
    mutation build(schema_ptr schema, api::timestamp_type ts) const;
    const partition_key& pk() const { return _pk; }
    const clustering_key& ck() const { return _ck; }
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

put_or_delete_item::put_or_delete_item(const rjson::value& item, schema_ptr schema, put_item)
        : _pk(pk_from_json(item, schema)), _ck(ck_from_json(item, schema)) {
    _cells = std::vector<cell>();
    _cells->reserve(item.MemberCount());
    for (auto it = item.MemberBegin(); it != item.MemberEnd(); ++it) {
        bytes column_name = to_bytes(it->name.GetString());
        validate_value(it->value, "PutItem");
        const column_definition* cdef = find_attribute(*schema, column_name);
        if (!cdef) {
            bytes value = serialize_item(it->value);
            _cells->push_back({std::move(column_name), serialize_item(it->value)});
        } else if (!cdef->is_primary_key()) {
            // Fixed-type regular column can be used for GSI key
            _cells->push_back({std::move(column_name),
                    get_key_from_typed_value(it->value, *cdef)});
        }
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
    if (!attrs_collector.empty()) {
        auto serialized_map = attrs_collector.to_mut().serialize(*attrs_type());
        row.cells().apply(attrs_column(*schema), std::move(serialized_map));
    }
    // To allow creation of an item with no attributes, we need a row marker.
    row.apply(row_marker(ts));
    // PutItem is supposed to completely replace the old item, so we need to
    // also have a tombstone removing old cells. We can't use the timestamp
    // ts, because when data and tombstone tie on timestamp, the tombstone
    // wins. So we need to use ts-1. Note that we use this trick also in
    // Scylla proper, to implement the operation to replace an entire
    // collection ("UPDATE .. SET x = ..") - see
    // cql3::update_parameters::make_tombstone_just_before().
    if (use_partition_tombstone) {
        m.partition().apply(tombstone(ts-1, gc_clock::now()));
    } else {
        row.apply(tombstone(ts-1, gc_clock::now()));
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
        
static future<std::unique_ptr<rjson::value>> get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const partition_key& pk,
        const clustering_key& ck,
        service_permit permit,
        alternator::stats& stats);

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
    auto regular_columns = boost::copy_range<query::column_id_vector>(
            schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
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
        throw api_error::validation(format("Unrecognized value for ReturnValues: {}", s));
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
        throw api_error::validation(format("Unrecognized value for ReturnValuesOnConditionCheckFailure: {}", s));
    }
}

rmw_operation::rmw_operation(service::storage_proxy& proxy, rjson::value&& request)
    : _request(std::move(request))
    , _schema(get_table(proxy, _request))
    , _write_isolation(get_write_isolation_for_schema(_schema))
    , _returnvalues(parse_returnvalues(_request))
    , _returnvalues_on_condition_check_failure(parse_returnvalues_on_condition_check_failure(_request))
{
    // _pk and _ck will be assigned later, by the subclass's constructor
    // (each operation puts the key in a slightly different location in
    // the request).
}

std::optional<mutation> rmw_operation::apply(foreign_ptr<lw_shared_ptr<query::result>> qr, const query::partition_slice& slice, api::timestamp_type ts) {
    if (qr->row_count()) {
        auto selection = cql3::selection::selection::wildcard(_schema);
        auto previous_item = executor::describe_single_item(_schema, slice, *selection, *qr, {});
        if (previous_item) {
            return apply(std::make_unique<rjson::value>(std::move(*previous_item)), ts);
        }
    }
    return apply(std::unique_ptr<rjson::value>(), ts);
}

rmw_operation::write_isolation rmw_operation::get_write_isolation_for_schema(schema_ptr schema) {
    const auto& tags = get_tags_of_table_or_throw(schema);
    auto it = tags.find(WRITE_ISOLATION_TAG_KEY);
    if (it == tags.end() || it->second.empty()) {
        return default_write_isolation;
    }
    return parse_write_isolation(it->second);
}

// shard_for_execute() checks whether execute() must be called on a specific
// other shard. Running execute() on a specific shard is necessary only if it
// will use LWT (storage_proxy::cas()). This is because cas() can only be
// called on the specific shard owning (as per cas_shard()) _pk's token.
// Knowing if execute() will call cas() or not may depend on whether there is
// a read-before-write, but not just on it - depending on configuration,
// execute() may unconditionally use cas() for every write. Unfortunately,
// this requires duplicating here a bit of logic from execute().
std::optional<shard_id> rmw_operation::shard_for_execute(bool needs_read_before_write) {
    if (_write_isolation == write_isolation::FORBID_RMW ||
        (_write_isolation == write_isolation::LWT_RMW_ONLY && !needs_read_before_write) ||
        _write_isolation == write_isolation::UNSAFE_RMW) {
        return {};
    }
    // If we're still here, cas() *will* be called by execute(), so let's
    // find the appropriate shard to run it on:
    auto token = dht::get_token(*_schema, _pk);
    auto desired_shard = service::storage_proxy::cas_shard(*_schema, token);
    if (desired_shard == this_shard_id()) {
        return {};
    }
    return desired_shard;
}

// Build the return value from the different RMW operations (UpdateItem,
// PutItem, DeleteItem). All these return nothing by default, but can
// optionally return Attributes if requested via the ReturnValues option.
static future<executor::request_return_type> rmw_operation_return(rjson::value&& attributes) {
    rjson::value ret = rjson::empty_object();
    if (!attributes.IsNull()) {
        rjson::add(ret, "Attributes", std::move(attributes));
    }
    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
}

static future<std::unique_ptr<rjson::value>> get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const partition_key& pk,
        const clustering_key& ck,
        service_permit permit,
        alternator::stats& stats)
{
    stats.reads_before_write++;
    auto selection = cql3::selection::selection::wildcard(schema);
    auto command = previous_item_read_command(proxy, schema, ck, selection);
    command->allow_limit = db::allow_per_partition_rate_limit::yes;
    auto cl = db::consistency_level::LOCAL_QUORUM;

    return proxy.query(schema, command, to_partition_ranges(*schema, pk), cl, service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state)).then(
            [schema, command, selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) {
        auto previous_item = executor::describe_single_item(schema, command->slice, *selection, *qr.query_result, {});
        if (previous_item) {
            return make_ready_future<std::unique_ptr<rjson::value>>(std::make_unique<rjson::value>(std::move(*previous_item)));
        } else {
            return make_ready_future<std::unique_ptr<rjson::value>>();
        }
    });
}

future<executor::request_return_type> rmw_operation::execute(service::storage_proxy& proxy,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit,
        bool needs_read_before_write,
        stats& stats) {
    if (needs_read_before_write) {
        if (_write_isolation == write_isolation::FORBID_RMW) {
            throw api_error::validation("Read-modify-write operations are disabled by 'forbid_rmw' write isolation policy. Refer to https://github.com/scylladb/scylla/blob/master/docs/alternator/alternator.md#write-isolation-policies for more information.");
        }
        stats.reads_before_write++;
        if (_write_isolation == write_isolation::UNSAFE_RMW) {
            // This is the old, unsafe, read before write which does first
            // a read, then a write. TODO: remove this mode entirely.
            return get_previous_item(proxy, client_state, schema(), _pk, _ck, permit, stats).then(
                    [this, &proxy, trace_state, permit = std::move(permit)] (std::unique_ptr<rjson::value> previous_item) mutable {
                std::optional<mutation> m = apply(std::move(previous_item), api::new_timestamp());
                if (!m) {
                    return make_ready_future<executor::request_return_type>(api_error::conditional_check_failed("The conditional request failed", std::move(_return_attributes)));
                }
                return proxy.mutate(std::vector<mutation>{std::move(*m)}, db::consistency_level::LOCAL_QUORUM, executor::default_timeout(), trace_state, std::move(permit), db::allow_per_partition_rate_limit::yes).then([this] () mutable {
                    return rmw_operation_return(std::move(_return_attributes));
                });
            });
        }
    } else if (_write_isolation != write_isolation::LWT_ALWAYS) {
        std::optional<mutation> m = apply(nullptr, api::new_timestamp());
        SCYLLA_ASSERT(m); // !needs_read_before_write, so apply() did not check a condition
        return proxy.mutate(std::vector<mutation>{std::move(*m)}, db::consistency_level::LOCAL_QUORUM, executor::default_timeout(), trace_state, std::move(permit), db::allow_per_partition_rate_limit::yes).then([this] () mutable {
            return rmw_operation_return(std::move(_return_attributes));
        });
    }
    // If we're still here, we need to do this write using LWT:
    stats.write_using_lwt++;
    auto timeout = executor::default_timeout();
    auto selection = cql3::selection::selection::wildcard(schema());
    auto read_command = needs_read_before_write ?
            previous_item_read_command(proxy, schema(), _ck, selection) :
            nullptr;
    return proxy.cas(schema(), shared_from_this(), read_command, to_partition_ranges(*schema(), _pk),
            {timeout, std::move(permit), client_state, trace_state},
            db::consistency_level::LOCAL_SERIAL, db::consistency_level::LOCAL_QUORUM, timeout, timeout).then([this, read_command] (bool is_applied) mutable {
        if (!is_applied) {
            return make_ready_future<executor::request_return_type>(api_error::conditional_check_failed("The conditional request failed", std::move(_return_attributes)));
        }
        return rmw_operation_return(std::move(_return_attributes));
    });
}

static parsed::condition_expression get_parsed_condition_expression(rjson::value& request) {
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
        return parse_condition_expression(rjson::to_string_view(*condition_expression), "ConditionExpression");
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

// Fail the expression if it has unused attribute names or values. This is
// how DynamoDB behaves, so we do too.
static void verify_all_are_used(const rjson::value* field,
        const std::unordered_set<std::string>& used, const char* field_name, const char* operation) {
    if (!field) {
        return;
    }
    for (auto it = field->MemberBegin(); it != field->MemberEnd(); ++it) {
        if (!used.contains(it->name.GetString())) {
            throw api_error::validation(
                format("{} has spurious '{}', not used in {}",
                    field_name, it->name.GetString(), operation));
        }
    }
}

class put_item_operation : public rmw_operation {
private:
    put_or_delete_item _mutation_builder;
public:
    parsed::condition_expression _condition_expression;
    put_item_operation(service::storage_proxy& proxy, rjson::value&& request)
        : rmw_operation(proxy, std::move(request))
        , _mutation_builder(rjson::get(_request, "Item"), schema(), put_or_delete_item::put_item{}) {
        _pk = _mutation_builder.pk();
        _ck = _mutation_builder.ck();
        if (_returnvalues != returnvalues::NONE && _returnvalues != returnvalues::ALL_OLD) {
            throw api_error::validation(format("PutItem supports only NONE or ALL_OLD for ReturnValues"));
        }
        _condition_expression = get_parsed_condition_expression(_request);
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
    }
    bool needs_read_before_write() const {
        return _request.HasMember("Expected") ||
               check_needs_read_before_write(_condition_expression) ||
               _returnvalues == returnvalues::ALL_OLD;
    }
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts) const override {
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

future<executor::request_return_type> executor::put_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.put_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("put_item {}", request);

    auto op = make_shared<put_item_operation>(_proxy, std::move(request));
    tracing::add_table_name(trace_state, op->schema()->ks_name(), op->schema()->cf_name());
    const bool needs_read_before_write = op->needs_read_before_write();

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::MODIFY,
            auth::make_data_resource(op->schema()->ks_name(), op->schema()->cf_name())))) {
        co_return api_error::access_denied(format(
            "MODIFY permissions denied on {}.{} by RBAC", op->schema()->ks_name(), op->schema()->cf_name()));
    }

    if (auto shard = op->shard_for_execute(needs_read_before_write); shard) {
        _stats.api_operations.put_item--; // uncount on this shard, will be counted in other shard
        _stats.shard_bounce_for_lwt++;
        co_return co_await container().invoke_on(*shard, _ssg,
                [request = std::move(*op).move_request(), cs = client_state.move_to_other_shard(), gt = tracing::global_trace_state_ptr(trace_state), permit = std::move(permit)]
                (executor& e) mutable {
            return do_with(cs.get(), [&e, request = std::move(request), trace_state = tracing::trace_state_ptr(gt)]
                                     (service::client_state& client_state) mutable {
                //FIXME: A corresponding FIXME can be found in transport/server.cc when a message must be bounced
                // to another shard - once it is solved, this place can use a similar solution. Instead of passing
                // empty_service_permit() to the background operation, the current permit's lifetime should be prolonged,
                // so that it's destructed only after all background operations are finished as well.
                return e.put_item(client_state, std::move(trace_state), empty_service_permit(), std::move(request));
            });
        });
    }
    co_return co_await op->execute(_proxy, client_state, trace_state, std::move(permit), needs_read_before_write, _stats).finally([op, start_time, this] {
        _stats.api_operations.put_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    });
}

class delete_item_operation : public rmw_operation {
private:
    put_or_delete_item _mutation_builder;
public:
    parsed::condition_expression _condition_expression;
    delete_item_operation(service::storage_proxy& proxy, rjson::value&& request)
        : rmw_operation(proxy, std::move(request))
        , _mutation_builder(rjson::get(_request, "Key"), schema(), put_or_delete_item::delete_item{}) {
        _pk = _mutation_builder.pk();
        _ck = _mutation_builder.ck();
        if (_returnvalues != returnvalues::NONE && _returnvalues != returnvalues::ALL_OLD) {
            throw api_error::validation(format("DeleteItem supports only NONE or ALL_OLD for ReturnValues"));
        }
        _condition_expression = get_parsed_condition_expression(_request);
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
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts) const override {
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
    virtual ~delete_item_operation() = default;
};

future<executor::request_return_type> executor::delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.delete_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("delete_item {}", request);

    auto op = make_shared<delete_item_operation>(_proxy, std::move(request));
    tracing::add_table_name(trace_state, op->schema()->ks_name(), op->schema()->cf_name());
    const bool needs_read_before_write = op->needs_read_before_write();

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::MODIFY,
            auth::make_data_resource(op->schema()->ks_name(), op->schema()->cf_name())))) {
        co_return api_error::access_denied(format(
            "MODIFY permissions denied on {}.{} by RBAC", op->schema()->ks_name(), op->schema()->cf_name()));
    }

    if (auto shard = op->shard_for_execute(needs_read_before_write); shard) {
        _stats.api_operations.delete_item--; // uncount on this shard, will be counted in other shard
        _stats.shard_bounce_for_lwt++;
        co_return co_await container().invoke_on(*shard, _ssg,
                [request = std::move(*op).move_request(), cs = client_state.move_to_other_shard(), gt = tracing::global_trace_state_ptr(trace_state), permit = std::move(permit)]
                (executor& e) mutable {
            return do_with(cs.get(), [&e, request = std::move(request), trace_state = tracing::trace_state_ptr(gt)]
                                     (service::client_state& client_state) mutable {
                //FIXME: A corresponding FIXME can be found in transport/server.cc when a message must be bounced
                // to another shard - once it is solved, this place can use a similar solution. Instead of passing
                // empty_service_permit() to the background operation, the current permit's lifetime should be prolonged,
                // so that it's destructed only after all background operations are finished as well.
                return e.delete_item(client_state, std::move(trace_state), empty_service_permit(), std::move(request));
            });
        });
    }
    co_return co_await op->execute(_proxy, client_state, trace_state, std::move(permit), needs_read_before_write, _stats).finally([op, start_time, this] {
        _stats.api_operations.delete_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    });
}

static schema_ptr get_table_from_batch_request(const service::storage_proxy& proxy, const rjson::value::ConstMemberIterator& batch_request) {
    sstring table_name = batch_request->name.GetString(); // JSON keys are always strings
    validate_table_name(table_name);
    try {
        return proxy.data_dictionary().find_schema(sstring(executor::KEYSPACE_NAME_PREFIX) + table_name, table_name);
    } catch(data_dictionary::no_such_column_family&) {
        throw api_error::resource_not_found(format("Requested resource not found: Table: {} not found", table_name));
    }
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
// one partition using LWT as part as BatchWriteItems. This is a write-only
// operation, not needing the previous value of the item (the mutation to be
// done is known prior to starting the operation). Nevertheless, we want to
// do this mutation via LWT to ensure that it is serialized with other LWT
// mutations to the same partition.
class put_or_delete_item_cas_request : public service::cas_request {
    schema_ptr schema;
    std::vector<put_or_delete_item> _mutation_builders;
public:
    put_or_delete_item_cas_request(schema_ptr s, std::vector<put_or_delete_item>&& b) :
        schema(std::move(s)), _mutation_builders(std::move(b)) { }
    virtual ~put_or_delete_item_cas_request() = default;
    virtual std::optional<mutation> apply(foreign_ptr<lw_shared_ptr<query::result>> qr, const query::partition_slice& slice, api::timestamp_type ts) override {
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

static future<> cas_write(service::storage_proxy& proxy, schema_ptr schema, dht::decorated_key dk, std::vector<put_or_delete_item>&& mutation_builders,
        service::client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit) {
    auto timeout = executor::default_timeout();
    auto op = seastar::make_shared<put_or_delete_item_cas_request>(schema, std::move(mutation_builders));
    return proxy.cas(schema, op, nullptr, to_partition_ranges(dk),
            {timeout, std::move(permit), client_state, trace_state},
            db::consistency_level::LOCAL_SERIAL, db::consistency_level::LOCAL_QUORUM,
            timeout, timeout).discard_result();
    // We discarded cas()'s future value ("is_applied") because BatchWriteItems
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
static future<> do_batch_write(service::storage_proxy& proxy,
        smp_service_group ssg,
        std::vector<std::pair<schema_ptr, put_or_delete_item>> mutation_builders,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit,
        stats& stats) {
    if (mutation_builders.empty()) {
        return make_ready_future<>();
    }
    // NOTE: technically, do_batch_write could be reworked to use LWT only for part
    // of the batched requests and not use it for others, but it's not considered
    // likely that a batch will contain both tables which always demand LWT and ones
    // that don't - it's fragile to split a batch into multiple storage proxy requests though.
    // Hence, the decision is conservative - if any table enforces LWT,the whole batch will use it.
    const bool needs_lwt = boost::algorithm::any_of(mutation_builders | boost::adaptors::map_keys, [] (const schema_ptr& schema) {
        return rmw_operation::get_write_isolation_for_schema(schema) == rmw_operation::write_isolation::LWT_ALWAYS;
    });
    if (!needs_lwt) {
        // Do a normal write, without LWT:
        std::vector<mutation> mutations;
        mutations.reserve(mutation_builders.size());
        api::timestamp_type now = api::new_timestamp();
        for (auto& b : mutation_builders) {
            mutations.push_back(b.second.build(b.first, now));
        }
        return proxy.mutate(std::move(mutations),
                db::consistency_level::LOCAL_QUORUM,
                executor::default_timeout(),
                trace_state,
                std::move(permit),
                db::allow_per_partition_rate_limit::yes);
    } else {
        // Do the write via LWT:
        // Multiple mutations may be destined for the same partition, adding
        // or deleting different items of one partition. Join them together
        // because we can do them in one cas() call.
        std::unordered_map<schema_decorated_key, std::vector<put_or_delete_item>, schema_decorated_key_hash, schema_decorated_key_equal>
            key_builders(1, schema_decorated_key_hash{}, schema_decorated_key_equal{});
        for (auto& b : mutation_builders) {
            auto dk = dht::decorate_key(*b.first, b.second.pk());
            auto [it, added] = key_builders.try_emplace(schema_decorated_key{b.first, dk});
            it->second.push_back(std::move(b.second));
        }
        return parallel_for_each(std::move(key_builders), [&proxy, &client_state, &stats, trace_state, ssg, permit = std::move(permit)] (auto& e) {
            stats.write_using_lwt++;
            auto desired_shard = service::storage_proxy::cas_shard(*e.first.schema, e.first.dk.token());
            if (desired_shard == this_shard_id()) {
                return cas_write(proxy, e.first.schema, e.first.dk, std::move(e.second), client_state, trace_state, permit);
            } else {
                stats.shard_bounce_for_lwt++;
                return proxy.container().invoke_on(desired_shard, ssg,
                            [cs = client_state.move_to_other_shard(),
                             mb = e.second,
                             dk = e.first.dk,
                             ks = e.first.schema->ks_name(),
                             cf = e.first.schema->cf_name(),
                             gt =  tracing::global_trace_state_ptr(trace_state),
                             permit = std::move(permit)]
                            (service::storage_proxy& proxy) mutable {
                    return do_with(cs.get(), [&proxy, mb = std::move(mb), dk = std::move(dk), ks = std::move(ks), cf = std::move(cf),
                                              trace_state = tracing::trace_state_ptr(gt)]
                                              (service::client_state& client_state) mutable {
                        auto schema = proxy.data_dictionary().find_schema(ks, cf);
                        //FIXME: A corresponding FIXME can be found in transport/server.cc when a message must be bounced
                        // to another shard - once it is solved, this place can use a similar solution. Instead of passing
                        // empty_service_permit() to the background operation, the current permit's lifetime should be prolonged,
                        // so that it's destructed only after all background operations are finished as well.
                        return cas_write(proxy, schema, dk, std::move(mb), client_state, std::move(trace_state), empty_service_permit());
                    });
                });
            }
        });
    }
}

future<executor::request_return_type> executor::batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.batch_write_item++;
    rjson::value& request_items = request["RequestItems"];
    auto start_time = std::chrono::steady_clock::now();

    std::vector<std::pair<schema_ptr, put_or_delete_item>> mutation_builders;
    mutation_builders.reserve(request_items.MemberCount());
    uint batch_size = 0;
    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        batch_size++;
        schema_ptr schema = get_table_from_batch_request(_proxy, it);
        tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());
        std::unordered_set<primary_key, primary_key_hash, primary_key_equal> used_keys(
                1, primary_key_hash{schema}, primary_key_equal{schema});
        for (auto& request : it->value.GetArray()) {
            if (!request.IsObject() || request.MemberCount() != 1) {
                co_return api_error::validation(format("Invalid BatchWriteItem request: {}", request));
            }
            auto r = request.MemberBegin();
            const std::string r_name = r->name.GetString();
            if (r_name == "PutRequest") {
                const rjson::value& put_request = r->value;
                const rjson::value& item = put_request["Item"];
                mutation_builders.emplace_back(schema, put_or_delete_item(
                        item, schema, put_or_delete_item::put_item{}));
                auto mut_key = std::make_pair(mutation_builders.back().second.pk(), mutation_builders.back().second.ck());
                if (used_keys.contains(mut_key)) {
                    co_return api_error::validation("Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else if (r_name == "DeleteRequest") {
                const rjson::value& key = (r->value)["Key"];
                mutation_builders.emplace_back(schema, put_or_delete_item(
                        key, schema, put_or_delete_item::delete_item{}));
                auto mut_key = std::make_pair(mutation_builders.back().second.pk(),
                        mutation_builders.back().second.ck());
                if (used_keys.contains(mut_key)) {
                    co_return api_error::validation("Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else {
                co_return api_error::validation(format("Unknown BatchWriteItem request type: {}", r_name));
            }
        }
    }

    for (const auto& b : mutation_builders) {
        if (!co_await client_state.check_has_permission(auth::command_desc(
                auth::permission::MODIFY,
                auth::make_data_resource(b.first->ks_name(), b.first->cf_name())))) {
            co_return api_error::access_denied(format(
                "MODIFY permissions denied on {}.{} by RBAC", b.first->ks_name(), b.first->cf_name()));
        }
    }

    _stats.api_operations.batch_write_item_batch_total += batch_size;
    co_return co_await do_batch_write(_proxy, _ssg, std::move(mutation_builders), client_state, trace_state, std::move(permit), _stats).then([start_time, this] () {
        // FIXME: Issue #5650: If we failed writing some of the updates,
        // need to return a list of these failed updates in UnprocessedItems
        // rather than fail the whole write (issue #5650).
        rjson::value ret = rjson::empty_object();
        rjson::add(ret, "UnprocessedItems", rjson::empty_object());
        _stats.api_operations.batch_write_item_latency.mark(std::chrono::steady_clock::now() - start_time);
        return make_ready_future<executor::request_return_type>(make_jsonable(std::move(ret)));
    });
}

static std::string get_item_type_string(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error::validation(format("Item has invalid format: {}", v));
    }
    auto it = v.MemberBegin();
    return it->name.GetString();
}

// attrs_to_get saves for each top-level attribute an attrs_to_get_node,
// a hierarchy of subparts that need to be kept. The following function
// takes a given JSON value and drops its parts which weren't asked to be
// kept. It modifies the given JSON value, or returns false to signify that
// the entire object should be dropped.
// Note that The JSON value is assumed to be encoded using the DynamoDB
// conventions - i.e., it is really a map whose key has a type string,
// and the value is the real object.
template<typename T>
static bool hierarchy_filter(rjson::value& val, const attribute_path_map_node<T>& h) {
    if (!val.IsObject() || val.MemberCount() != 1) {
        // This shouldn't happen. We shouldn't have stored malformed objects.
        // But today Alternator does not validate the structure of nested
        // documents before storing them, so this can happen on read.
        throw api_error::internal(format("Malformed value object read: {}", val));
    }
    const char* type = val.MemberBegin()->name.GetString();
    rjson::value& v = val.MemberBegin()->value;
    if (h.has_members()) {
        const auto& members = h.get_members();
        if (type[0] != 'M' || !v.IsObject()) {
            // If v is not an object (dictionary, map), none of the members
            // can match.
            return false;
        }
        rjson::value newv = rjson::empty_object();
        for (auto it = v.MemberBegin(); it != v.MemberEnd(); ++it) {
            std::string attr = it->name.GetString();
            auto x = members.find(attr);
            if (x != members.end()) {
                if (x->second) {
                    // Only a part of this attribute is to be filtered, do it.
                    if (hierarchy_filter(it->value, *x->second)) {
                        // because newv started empty and attr are unique
                        // (keys of v), we can use add() here
                        rjson::add_with_string_name(newv, attr, std::move(it->value));
                    }
                } else {
                    // The entire attribute is to be kept
                    rjson::add_with_string_name(newv, attr, std::move(it->value));
                }
            }
        }
        if (newv.MemberCount() == 0) {
            return false;
        }
        v = newv;
    } else if (h.has_indexes()) {
        const auto& indexes = h.get_indexes();
        if (type[0] != 'L' || !v.IsArray()) {
            return false;
        }
        rjson::value newv = rjson::empty_array();
        const auto& a = v.GetArray();
        for (unsigned i = 0; i < v.Size(); i++) {
            auto x = indexes.find(i);
            if (x != indexes.end()) {
                if (x->second) {
                    if (hierarchy_filter(a[i], *x->second)) {
                        rjson::push_back(newv, std::move(a[i]));
                    }
                } else {
                    // The entire attribute is to be kept
                    rjson::push_back(newv, std::move(a[i]));
                }
            }
        }
        if (newv.Size() == 0) {
            return false;
        }
        v = newv;
    }
    return true;
}

// Add a path to a attribute_path_map. Throws a validation error if the path
// "overlaps" with one already in the filter (one is a sub-path of the other)
// or "conflicts" with it (both a member and index is requested).
template<typename T>
void attribute_path_map_add(const char* source, attribute_path_map<T>& map, const parsed::path& p, T value = {}) {
   using node = attribute_path_map_node<T>;
    // The first step is to look for the top-level attribute (p.root()):
    auto it = map.find(p.root());
    if (it == map.end()) {
        if (p.has_operators()) {
            it = map.emplace(p.root(), node {std::nullopt}).first;
        } else {
            (void) map.emplace(p.root(), node {std::move(value)}).first;
            // Value inserted for top-level node. We're done.
            return;
        }
    } else if(!p.has_operators()) {
        // If p is top-level and we already have it or a part of it
        // in map, it's a forbidden overlapping path.
        throw api_error::validation(format(
            "Invalid {}: two document paths overlap at {}", source, p.root()));
    } else if (it->second.has_value()) {
        // If we're here, it != map.end() && p.has_operators && it->second.has_value().
        // This means the top-level attribute already has a value, and we're
        // trying to add a non-top-level value. It's an overlap.
        throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p.root()));
    }
    node* h = &it->second;
    // The second step is to walk h from the top-level node to the inner node
    // where we're supposed to insert the value:
    for (const auto& op : p.operators()) {
        std::visit(overloaded_functor {
            [&] (const std::string& member) {
                if (h->is_empty()) {
                    *h = node {typename node::members_t()};
                } else if (h->has_indexes()) {
                    throw api_error::validation(format("Invalid {}: two document paths conflict at {}", source, p));
                } else if (h->has_value()) {
                    throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p));
                }
                typename node::members_t& members = h->get_members();
                auto it = members.find(member);
                if (it == members.end()) {
                    it = members.insert({member, std::make_unique<node>()}).first;
                }
                h = it->second.get();
            },
            [&] (unsigned index) {
                if (h->is_empty()) {
                    *h = node {typename node::indexes_t()};
                } else if (h->has_members()) {
                    throw api_error::validation(format("Invalid {}: two document paths conflict at {}", source, p));
                } else if (h->has_value()) {
                    throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p));
                }
                typename node::indexes_t& indexes = h->get_indexes();
                auto it = indexes.find(index);
                if (it == indexes.end()) {
                    it = indexes.insert({index, std::make_unique<node>()}).first;
                }
                h = it->second.get();
            }
        }, op);
    }
    // Finally, insert the value in the node h.
    if (h->is_empty()) {
        *h = node {std::move(value)};
    } else {
        throw api_error::validation(format("Invalid {}: two document paths overlap at {}", source, p));
    }
}

// A very simplified version of the above function for the special case of
// adding only top-level attribute. It's not only simpler, we also use a
// different error message, referring to a "duplicate attribute"instead of
// "overlapping paths". DynamoDB also has this distinction (errors in
// AttributesToGet refer to duplicates, not overlaps, but errors in
// ProjectionExpression refer to overlap - even if it's an exact duplicate).
template<typename T>
void attribute_path_map_add(const char* source, attribute_path_map<T>& map, const std::string& attr, T value = {}) {
   using node = attribute_path_map_node<T>;
    auto it = map.find(attr);
    if (it == map.end()) {
        map.emplace(attr, node {std::move(value)});
    } else {
        throw api_error::validation(format(
            "Invalid {}: Duplicate attribute: {}", source, attr));
    }
}

// Parse the "Select" parameter of a Scan or Query operation, throwing a
// ValidationException in various forbidden combinations of options and
// finally returning one of three options:
// 1. regular - the default scan behavior of returning all or specific
//    attributes ("ALL_ATTRIBUTES" or "SPECIFIC_ATTRIBUTES").
// 2. count - just count the items ("COUNT")
// 3. projection - return projected attributes ("ALL_PROJECTED_ATTRIBUTES")
// An ValidationException is thrown when recognizing an invalid combination
// of options - such as ALL_PROJECTED_ATTRIBUTES for a base table, or
// SPECIFIC_ATTRIBUTES without ProjectionExpression or AttributesToGet.
enum class select_type { regular, count, projection };
static select_type parse_select(const rjson::value& request, table_or_view_type table_type) {
    const rjson::value* select_value = rjson::find(request, "Select");
    if (!select_value) {
        // If "Select" is not specified, it defaults to ALL_ATTRIBUTES
        // on a base table, or ALL_PROJECTED_ATTRIBUTES on an index
        return table_type == table_or_view_type::base ?
            select_type::regular : select_type::projection;
    }
    if (!select_value->IsString()) {
        throw api_error::validation("Select parameter must be a string");
    }
    std::string_view select = rjson::to_string_view(*select_value);
    const bool has_attributes_to_get = request.HasMember("AttributesToGet");
    const bool has_projection_expression = request.HasMember("ProjectionExpression");
    if (select == "SPECIFIC_ATTRIBUTES") {
        if (has_projection_expression || has_attributes_to_get) {
            return select_type::regular;
        }
        throw api_error::validation("Select=SPECIFIC_ATTRIBUTES requires AttributesToGet or ProjectionExpression");
    }
    if (has_projection_expression || has_attributes_to_get) {
        throw api_error::validation("AttributesToGet or ProjectionExpression require Select to be either SPECIFIC_ATTRIBUTES or missing");
    }
    if (select == "COUNT") {
        return select_type::count;
    }
    if (select == "ALL_ATTRIBUTES") {
        // FIXME: when we support projections (#5036), if this is a GSI and
        // not all attributes are projected to it, we should throw.
        return select_type::regular;
    }
    if (select == "ALL_PROJECTED_ATTRIBUTES") {
        if (table_type == table_or_view_type::base) {
            throw api_error::validation("ALL_PROJECTED_ATTRIBUTES only allowed for indexes");
        }
        return select_type::projection;
    }
    throw api_error::validation(format("Unknown Select value '{}'. Allowed choices: ALL_ATTRIBUTES, SPECIFIC_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT",
        select));
}

// calculate_attrs_to_get() takes either AttributesToGet or
// ProjectionExpression parameters (having both is *not* allowed),
// and returns the list of cells we need to read, or a disengaged optional
// when *all* attributes are to be returned.
// However, in our current implementation, only top-level attributes are
// stored as separate cells - a nested document is stored serialized together
// (as JSON) in the same cell. So this function return a map - each key is the
// top-level attribute we will need need to read, and the value for each
// top-level attribute is the partial hierarchy (struct hierarchy_filter)
// that we will need to extract from that serialized JSON.
// For example, if ProjectionExpression lists a.b and a.c[2], we
// return one top-level attribute name, "a", with the value "{b, c[2]}".

static std::optional<attrs_to_get> calculate_attrs_to_get(const rjson::value& req, std::unordered_set<std::string>& used_attribute_names, select_type select = select_type::regular) {
    if (select == select_type::count) {
        // An empty map asks to retrieve no attributes. Note that this is
        // different from a disengaged optional which means retrieve all.
        return attrs_to_get();
    }
    // FIXME: also need to handle select_type::projection
    const bool has_attributes_to_get = req.HasMember("AttributesToGet");
    const bool has_projection_expression = req.HasMember("ProjectionExpression");
    if (has_attributes_to_get && has_projection_expression) {
        throw api_error::validation(
                format("GetItem does not allow both ProjectionExpression and AttributesToGet to be given together"));
    }
    if (has_attributes_to_get) {
        const rjson::value& attributes_to_get = req["AttributesToGet"];
        attrs_to_get ret;
        for (auto it = attributes_to_get.Begin(); it != attributes_to_get.End(); ++it) {
            attribute_path_map_add("AttributesToGet", ret, it->GetString());
        }
        if (ret.empty()) {
            throw api_error::validation("Empty AttributesToGet is not allowed. Consider using Select=COUNT instead.");
        }
        return ret;
    } else if (has_projection_expression) {
        const rjson::value& projection_expression = req["ProjectionExpression"];
        const rjson::value* expression_attribute_names = rjson::find(req, "ExpressionAttributeNames");
        std::vector<parsed::path> paths_to_get;
        try {
            paths_to_get = parse_projection_expression(rjson::to_string_view(projection_expression));
        } catch(expressions_syntax_error& e) {
            throw api_error::validation(e.what());
        }
        resolve_projection_expression(paths_to_get, expression_attribute_names, used_attribute_names);
        attrs_to_get ret;
        for (const parsed::path& p : paths_to_get) {
            attribute_path_map_add("ProjectionExpression", ret, p);
        }
        return ret;
    }
    // An disengaged optional asks to read everything
    return std::nullopt;
}

/**
 * Helper routine to extract data when we already have 
 * row, etc etc.
 * 
 * Note: include_all_embedded_attributes means we should
 * include all values in the `ATTRS_COLUMN_NAME` map column.
 * 
 * We could change the behaviour to simply include all values 
 * from this column if the `ATTRS_COLUMN_NAME` is explicit in 
 * `attrs_to_get`, but I am scared to do that now in case 
 * there is some corner case in existing code.
 * 
 * Explicit bool means we can be sure all previous calls are 
 * as before.
 */ 
void executor::describe_single_item(const cql3::selection::selection& selection,
    const std::vector<managed_bytes_opt>& result_row,
    const std::optional<attrs_to_get>& attrs_to_get,
    rjson::value& item,
    bool include_all_embedded_attributes) 
{
    const auto& columns = selection.get_columns();
    auto column_it = columns.begin();
    for (const managed_bytes_opt& cell : result_row) {
        std::string column_name = (*column_it)->name_as_text();
        if (cell && column_name != executor::ATTRS_COLUMN_NAME) {
            if (!attrs_to_get || attrs_to_get->contains(column_name)) {
                // item is expected to start empty, and column_name are unique
                // so add() makes sense
                rjson::add_with_string_name(item, column_name, rjson::empty_object());
                rjson::value& field = item[column_name.c_str()];
                cell->with_linearized([&] (bytes_view linearized_cell) {
                    rjson::add_with_string_name(field, type_to_string((*column_it)->type), json_key_column_value(linearized_cell, **column_it));
                });
            }
        } else if (cell) {
            auto deserialized = attrs_type()->deserialize(*cell);
            auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
            for (auto entry : keys_and_values) {
                std::string attr_name = value_cast<sstring>(entry.first);
                if (include_all_embedded_attributes || !attrs_to_get || attrs_to_get->contains(attr_name)) {
                    bytes value = value_cast<bytes>(entry.second);
                    rjson::value v = deserialize_item(value);
                    if (attrs_to_get) {
                        auto it = attrs_to_get->find(attr_name);
                        if (it != attrs_to_get->end()) {
                            // attrs_to_get may have asked for only part of
                            // this attribute. hierarchy_filter() modifies v,
                            // and returns false when nothing is to be kept.
                            if (!hierarchy_filter(v, it->second)) {
                                continue;
                            }
                        }
                    }
                    // item is expected to start empty, and attribute
                    // names are unique so add() makes sense
                    rjson::add_with_string_name(item, attr_name, std::move(v));
                }
            }
        }
        ++column_it;
    }
}

std::optional<rjson::value> executor::describe_single_item(schema_ptr schema,
        const query::partition_slice& slice,
        const cql3::selection::selection& selection,
        const query::result& query_result,
        const std::optional<attrs_to_get>& attrs_to_get) {
    rjson::value item = rjson::empty_object();

    cql3::selection::result_set_builder builder(selection, gc_clock::now());
    query::result_view::consume(query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, selection));

    auto result_set = builder.build();
    if (result_set->empty()) {
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return {};
    }
    if (result_set->size() > 1) {
        // If the result set contains multiple rows, the code should have
        // called describe_multi_item(), not this function.
        throw std::logic_error("describe_single_item() asked to describe multiple items");
    }
    describe_single_item(selection, *result_set->rows().begin(), attrs_to_get, item);
    return item;
}

future<std::vector<rjson::value>> executor::describe_multi_item(schema_ptr schema,
        const query::partition_slice&& slice,
        shared_ptr<cql3::selection::selection> selection,
        foreign_ptr<lw_shared_ptr<query::result>> query_result,
        shared_ptr<const std::optional<attrs_to_get>> attrs_to_get) {
    cql3::selection::result_set_builder builder(*selection, gc_clock::now());
    query::result_view::consume(*query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, *selection));
    auto result_set = builder.build();
    std::vector<rjson::value> ret;
    for (auto& result_row : result_set->rows()) {
        rjson::value item = rjson::empty_object();
        describe_single_item(*selection, result_row, *attrs_to_get, item);
        ret.push_back(std::move(item));
        co_await coroutine::maybe_yield();
    }
    co_return ret;
}

static bool check_needs_read_before_write(const parsed::value& v) {
    return std::visit(overloaded_functor {
        [&] (const parsed::constant& c) -> bool {
            return false;
        },
        [&] (const parsed::value::function_call& f) -> bool {
            return boost::algorithm::any_of(f._parameters, [&] (const parsed::value& param) {
                return check_needs_read_before_write(param);
            });
        },
        [&] (const parsed::path& p) -> bool {
            return true;
        }
    }, v._value);
}

static bool check_needs_read_before_write(const attribute_path_map<parsed::update_expression::action>& update_expression) {
    return boost::algorithm::any_of(update_expression, [](const auto& p) {
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

class update_item_operation  : public rmw_operation {
public:
    // Some information parsed during the constructor to check for input
    // errors, and cached to be used again during apply().
    rjson::value* _attribute_updates;
    // Instead of keeping a parsed::update_expression with an unsorted list
    // list of actions, we keep them in an attribute_path_map which groups
    // them by top-level attribute, and detects forbidden overlaps/conflicts.
    attribute_path_map<parsed::update_expression::action> _update_expression;

    parsed::condition_expression _condition_expression;

    update_item_operation(service::storage_proxy& proxy, rjson::value&& request);
    virtual ~update_item_operation() = default;
    virtual std::optional<mutation> apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts) const override;
    bool needs_read_before_write() const;
};

update_item_operation::update_item_operation(service::storage_proxy& proxy, rjson::value&& update_info)
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
            parsed::update_expression expr = parse_update_expression(rjson::to_string_view(*update_expression));
            resolve_update_expression(expr,
                    expression_attribute_names, expression_attribute_values,
                    used_attribute_names, used_attribute_values);
            if (expr.empty()) {
                throw api_error::validation("Empty expression in UpdateExpression is not allowed");
            }
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
    }

    _condition_expression = get_parsed_condition_expression(_request);
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
                std::string v2_type = get_item_type_string(v2);
                if (v2_type == "N" || v2_type == "SS" || v2_type == "NS" || v2_type == "BS") {
                    result = v2;
                } else {
                    throw api_error::validation(format("An operand in the update expression has an incorrect data type: {}", v2));
                }
            } else {
                std::string v1_type = get_item_type_string(v1);
                if (v1_type == "N") {
                    if (get_item_type_string(v2) != "N") {
                        throw api_error::validation(format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, rjson::print(v2)));
                    }
                    result = number_add(v1, v2);
                } else if (v1_type == "SS" || v1_type == "NS" || v1_type == "BS") {
                    if (get_item_type_string(v2) != v1_type) {
                        throw api_error::validation(format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, rjson::print(v2)));
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
            throw api_error::validation(format("UpdateExpression: document paths not valid for this item:{}", h));
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

std::optional<mutation>
update_item_operation::apply(std::unique_ptr<rjson::value> previous_item, api::timestamp_type ts) const {
    if (!verify_expected(_request, previous_item.get()) ||
        !verify_condition_expression(_condition_expression, previous_item.get())) {
        if (previous_item && _returnvalues_on_condition_check_failure ==
            returnvalues_on_condition_check_failure::ALL_OLD) {
            _return_attributes = std::move(*previous_item);
        }
        // If the update is to be cancelled because of an unfulfilled
        // condition, return an empty optional mutation, which is more
        // efficient than throwing an exception.
        return {};
    }

    mutation m(_schema, _pk);
    auto& row = m.partition().clustered_row(*_schema, _ck);
    attribute_collector attrs_collector;
    bool any_updates = false;
    auto do_update = [&] (bytes&& column_name, const rjson::value& json_value,
                          const attribute_path_map_node<parsed::update_expression::action>* h = nullptr) {
        any_updates = true;
        if (_returnvalues == returnvalues::ALL_NEW) {
            rjson::replace_with_string_name(_return_attributes,
                to_sstring_view(column_name), rjson::copy(json_value));
        } else if (_returnvalues == returnvalues::UPDATED_NEW) {
            rjson::value&& v = rjson::copy(json_value);
            if (h) {
                // If the operation was only on specific attribute paths,
                // leave only them in _return_attributes.
                if (hierarchy_filter(v, *h)) {
                    // In the UPDATED_NEW case, _return_attributes starts
                    // empty and the attribute names are unique, so we can
                    // use add().
                    rjson::add_with_string_name(_return_attributes,
                        to_sstring_view(column_name), std::move(v));
                }
            } else {
                rjson::add_with_string_name(_return_attributes,
                    to_sstring_view(column_name), std::move(v));
            }
        } else if (_returnvalues == returnvalues::UPDATED_OLD && previous_item) {
            std::string_view cn =  to_sstring_view(column_name);
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
            attrs_collector.put(std::move(column_name), serialize_item(json_value), ts);
        }
    };
    bool any_deletes = false;
    auto do_delete = [&] (bytes&& column_name) {
        any_deletes = true;
        if (_returnvalues == returnvalues::ALL_NEW) {
            rjson::remove_member(_return_attributes, to_sstring_view(column_name));
        } else if (_returnvalues == returnvalues::UPDATED_OLD && previous_item) {
            std::string_view cn =  to_sstring_view(column_name);
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
            attrs_collector.del(std::move(column_name), ts);
        }
    };

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
            // and contains they given key. This may be cancelled at the end
            // of this function if the update is just deletes.
           _return_attributes = rjson::copy(rjson::get(_request, "Key"));
        }
    } else if (_returnvalues == returnvalues::UPDATED_OLD ||
               _returnvalues == returnvalues::UPDATED_NEW) {
        _return_attributes = rjson::empty_object();
    }

    if (!_update_expression.empty()) {
        for (auto& actions : _update_expression) {
            // The actions of _update_expression are grouped by top-level
            // attributes. Here, all actions in actions.second share the same
            // top-level attribute actions.first.
            std::string column_name = actions.first;
            const column_definition* cdef = _schema->get_column_definition(to_bytes(column_name));
            if (cdef && cdef->is_primary_key()) {
                throw api_error::validation(format("UpdateItem cannot update key column {}", column_name));
            }
            if (actions.second.has_value()) {
                // An action on a top-level attribute column_name. The single
                // action is actions.second.get_value(). We can simply invoke
                // the action and replace the attribute with its result:
                std::optional<rjson::value> result = action_result(actions.second.get_value(), previous_item.get());
                if (result) {
                    do_update(to_bytes(column_name), *result);
                } else {
                    do_delete(to_bytes(column_name));
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
                const rjson::value *toplevel = rjson::find(*previous_item, column_name);
                if (!toplevel) {
                    throw api_error::validation(format("UpdateItem cannot update document path: missing attribute {}",
                        column_name));
                }
                rjson::value result = rjson::copy(*toplevel);
                hierarchy_actions(result, actions.second, previous_item.get());
                do_update(to_bytes(column_name), std::move(result), &actions.second);
            }
        }
    }
    if (_returnvalues == returnvalues::ALL_OLD && previous_item) {
        _return_attributes = std::move(*previous_item);
    }
    if (_attribute_updates) {
        for (auto it = _attribute_updates->MemberBegin(); it != _attribute_updates->MemberEnd(); ++it) {
            // Note that it.key() is the name of the column, *it is the operation
            bytes column_name = to_bytes(it->name.GetString());
            const column_definition* cdef = _schema->get_column_definition(column_name);
            if (cdef && cdef->is_primary_key()) {
                throw api_error::validation(
                        format("UpdateItem cannot update key column {}", it->name.GetString()));
            }
            std::string action = (it->value)["Action"].GetString();
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
                    const rjson::value* v1 = previous_item ? rjson::find(*previous_item, to_sstring_view(column_name)) : nullptr;
                    const rjson::value& v2 = (it->value)["Value"];
                    validate_value(v2, "AttributeUpdates");
                    std::string v2_type = get_item_type_string(v2);
                    if (v2_type != "SS" && v2_type != "NS" && v2_type != "BS") {
                        throw api_error::validation(format("AttributeUpdates DELETE operation with Value only valid for sets, got type {}", v2_type));
                    }
                    if (v1) {
                        std::optional<rjson::value> result = set_diff(*v1, v2);
                        if (result) {
                            do_update(std::move(column_name), *result);
                        } else {
                            // DynamoDB does not allow empty sets - if the
                            // result is empty, delete the attribute.
                            do_delete(std::move(column_name));
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
                    do_delete(std::move(column_name));
                }
            } else if (action == "PUT") {
                const rjson::value& value = (it->value)["Value"];
                validate_value(value, "AttributeUpdates");
                do_update(std::move(column_name), value);
            } else if (action == "ADD") {
                // Note that check_needs_read_before_write_attribute_updates()
                // made sure we retrieved previous_item (if exists) when there
                // is an ADD action.
                const rjson::value* v1 = previous_item ? rjson::find(*previous_item, to_sstring_view(column_name)) : nullptr;
                const rjson::value& v2 = (it->value)["Value"];
                validate_value(v2, "AttributeUpdates");
                // An ADD can be used to create a new attribute (when
                // !v1) or to add to a pre-existing attribute:
                if (!v1) {
                    std::string v2_type = get_item_type_string(v2);
                    if (v2_type == "N" || v2_type == "SS" || v2_type == "NS" || v2_type == "BS" || v2_type == "L") {
                        do_update(std::move(column_name), v2);
                    } else {
                        throw api_error::validation(format("An operand in the AttributeUpdates ADD has an incorrect data type: {}", v2));
                    }
                } else {
                    std::string v1_type = get_item_type_string(*v1);
                    std::string v2_type = get_item_type_string(v2);
                    if (v2_type != v1_type) {
                        throw api_error::validation(format("Operand type mismatch in AttributeUpdates ADD. Expected {}, got {}", v1_type, v2_type));
                    }
                    if (v1_type == "N") {
                        do_update(std::move(column_name), number_add(*v1, v2));
                    } else if (v1_type == "SS" || v1_type == "NS" || v1_type == "BS") {
                        do_update(std::move(column_name), set_sum(*v1, v2));
                    } else if (v1_type == "L") {
                        // The DynamoDB documentation doesn't say it supports
                        // lists in ADD operations, but it turns out that it
                        // does. Interestingly, this is only true for
                        // AttributeUpdates (this code) - the similar ADD
                        // in UpdateExpression doesn't support lists.
                        do_update(std::move(column_name), list_concatenate(*v1, v2));
                    } else {
                        throw api_error::validation(format("An operand in the AttributeUpdates ADD has an incorrect data type: {}", *v1));
                    }
                }
            } else {
                throw api_error::validation(
                        format("Unknown Action value '{}' in AttributeUpdates", action));
            }
        }
    }
    if (!attrs_collector.empty()) {
        auto serialized_map = attrs_collector.to_mut().serialize(*attrs_type());
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
    // ReturnValues=UPDATED_OLD/NEW never return an empty Attributes field,
    // even if a new item was created. Instead it should be missing entirely.
    if (_returnvalues == returnvalues::UPDATED_OLD || _returnvalues == returnvalues::UPDATED_NEW) {
        if (_return_attributes.MemberCount() == 0) {
            _return_attributes = rjson::null_value();
        }
    }

    return m;
}

future<executor::request_return_type> executor::update_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.update_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("update_item {}", request);

    auto op = make_shared<update_item_operation>(_proxy, std::move(request));
    tracing::add_table_name(trace_state, op->schema()->ks_name(), op->schema()->cf_name());
    const bool needs_read_before_write = op->needs_read_before_write();

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::MODIFY,
            auth::make_data_resource(op->schema()->ks_name(), op->schema()->cf_name())))) {
        co_return api_error::access_denied(format(
            "MODIFY permissions denied on {}.{} by RBAC", op->schema()->ks_name(), op->schema()->cf_name()));
    }

    if (auto shard = op->shard_for_execute(needs_read_before_write); shard) {
        _stats.api_operations.update_item--; // uncount on this shard, will be counted in other shard
        _stats.shard_bounce_for_lwt++;
        co_return co_await container().invoke_on(*shard, _ssg,
                [request = std::move(*op).move_request(), cs = client_state.move_to_other_shard(), gt = tracing::global_trace_state_ptr(trace_state), permit = std::move(permit)]
                (executor& e) mutable {
            return do_with(cs.get(), [&e, request = std::move(request), trace_state = tracing::trace_state_ptr(gt)]
                                     (service::client_state& client_state) mutable {
                //FIXME: A corresponding FIXME can be found in transport/server.cc when a message must be bounced
                // to another shard - once it is solved, this place can use a similar solution. Instead of passing
                // empty_service_permit() to the background operation, the current permit's lifetime should be prolonged,
                // so that it's destructed only after all background operations are finished as well.
                return e.update_item(client_state, std::move(trace_state), empty_service_permit(), std::move(request));
            });
        });
    }
    co_return co_await op->execute(_proxy, client_state, trace_state, std::move(permit), needs_read_before_write, _stats).finally([op, start_time, this] {
        _stats.api_operations.update_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    });
}

// Check according to the request's "ConsistentRead" field, which consistency
// level we need to use for the read. The field can be True for strongly
// consistent reads, or False for eventually consistent reads, or if this
// field is absence, we default to eventually consistent reads.
// In Scylla, eventually-consistent reads are implemented as consistency
// level LOCAL_ONE, and strongly-consistent reads as LOCAL_QUORUM.
static db::consistency_level get_read_consistency(const rjson::value& request) {
    const rjson::value* consistent_read_value = rjson::find(request, "ConsistentRead");
    bool consistent_read = false;
    if (consistent_read_value && !consistent_read_value->IsNull()) {
        if (consistent_read_value->IsBool()) {
            consistent_read = consistent_read_value->GetBool();
        } else {
            throw api_error::validation("ConsistentRead flag must be a boolean");
        }
    }
    return consistent_read ? db::consistency_level::LOCAL_QUORUM : db::consistency_level::LOCAL_ONE;
}

// describe_item() wraps the result of describe_single_item() by a map
// as needed by the GetItem request. It should not be used for other purposes,
// use describe_single_item() instead.
static rjson::value describe_item(schema_ptr schema,
        const query::partition_slice& slice,
        const cql3::selection::selection& selection,
        const query::result& query_result,
        const std::optional<attrs_to_get>& attrs_to_get) {
    std::optional<rjson::value> opt_item = executor::describe_single_item(std::move(schema), slice, selection, std::move(query_result), attrs_to_get);
    if (!opt_item) {
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return rjson::empty_object();
    }
    rjson::value item_descr = rjson::empty_object();
    rjson::add(item_descr, "Item", std::move(*opt_item));
    return item_descr;
}

future<executor::request_return_type> executor::get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.get_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("Getting item {}", request);

    schema_ptr schema = get_table(_proxy, request);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    rjson::value& query_key = request["Key"];
    db::consistency_level cl = get_read_consistency(request);

    partition_key pk = pk_from_json(query_key, schema);
    dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*schema, pk))};

    std::vector<query::clustering_range> bounds;
    if (schema->clustering_key_size() == 0) {
        bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    } else {
        clustering_key ck = ck_from_json(query_key, schema);
        bounds.push_back(query::clustering_range::make_singular(std::move(ck)));
    }
    check_key(query_key, schema);

    //TODO(sarna): It would be better to fetch only some attributes of the map, not all
    auto regular_columns = boost::copy_range<query::column_id_vector>(
            schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));

    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, _proxy.get_max_result_size(partition_slice),
            query::tombstone_limit(_proxy.get_tombstone_limit()));

    std::unordered_set<std::string> used_attribute_names;
    auto attrs_to_get = calculate_attrs_to_get(request, used_attribute_names);
    const rjson::value* expression_attribute_names = rjson::find(request, "ExpressionAttributeNames");
    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "GetItem");

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::SELECT,
            auth::make_data_resource(schema->ks_name(), schema->cf_name())))) {
        co_return api_error::access_denied(format(
            "SELECT permissions denied on {}.{} by RBAC", schema->ks_name(), schema->cf_name()));
    }

    co_return co_await _proxy.query(schema, std::move(command), std::move(partition_ranges), cl,
            service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state, trace_state)).then(
            [this, schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = std::move(attrs_to_get), start_time = std::move(start_time)] (service::storage_proxy::coordinator_query_result qr) mutable {
        _stats.api_operations.get_item_latency.mark(std::chrono::steady_clock::now() - start_time);
        return make_ready_future<executor::request_return_type>(make_jsonable(describe_item(schema, partition_slice, *selection, *qr.query_result, std::move(attrs_to_get))));
    });
}

static void check_big_object(const rjson::value& val, int& size_left);
static void check_big_array(const rjson::value& val, int& size_left);

bool is_big(const rjson::value& val, int big_size) {
    if (val.IsString()) {
        return ssize_t(val.GetStringLength()) > big_size;
    } else if (val.IsObject()) {
        check_big_object(val, big_size);
        return big_size < 0;
    } else if (val.IsArray()) {
        check_big_array(val, big_size);
        return big_size < 0;
    }
    return false;
}

static void check_big_array(const rjson::value& val, int& size_left) {
    // Assume a fixed size of 10 bytes for each number, boolean, etc., or
    // beginning of a sub-object. This doesn't have to be accurate.
    size_left -= 10 * val.Size();
    for (const auto& v : val.GetArray()) {
        if (size_left < 0) {
            return;
        }
        // Note that we avoid recursive calls for the leaves (anything except
        // array or object) because usually those greatly outnumber the trunk.
        if (v.IsString()) {
            size_left -= v.GetStringLength();
        } else if (v.IsObject()) {
            check_big_object(v, size_left);
        } else if (v.IsArray()) {
            check_big_array(v, size_left);
        }
    }
}

static void check_big_object(const rjson::value& val, int& size_left) {
    size_left -= 10 * val.MemberCount();
    for (const auto& m : val.GetObject()) {
        if (size_left < 0) {
            return;
        }
        size_left -= m.name.GetStringLength();
        if (m.value.IsString()) {
            size_left -= m.value.GetStringLength();
        } else if (m.value.IsObject()) {
            check_big_object(m.value, size_left);
        } else if (m.value.IsArray()) {
            check_big_array(m.value, size_left);
        }
    }
}

future<executor::request_return_type> executor::batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    // FIXME: In this implementation, an unbounded batch size can cause
    // unbounded response JSON object to be buffered in memory, unbounded
    // parallelism of the requests, and unbounded amount of non-preemptable
    // work in the following loops. So we should limit the batch size, and/or
    // the response size, as DynamoDB does.
    _stats.api_operations.batch_get_item++;
    rjson::value& request_items = request["RequestItems"];
    auto start_time = std::chrono::steady_clock::now();
    // We need to validate all the parameters before starting any asynchronous
    // query, and fail the entire request on any parse error. So we parse all
    // the input into our own vector "requests", each element a table_requests
    // listing all the request aimed at a single table. For efficiency, inside
    // each table_requests we further group together all reads going to the
    // same partition, so we can later send them together.
    struct table_requests {
        schema_ptr schema;
        db::consistency_level cl;
        ::shared_ptr<const std::optional<alternator::attrs_to_get>> attrs_to_get;
        // clustering_keys keeps a sorted set of clustering keys. It must
        // be sorted for the read below (see #10827). Additionally each
        // clustering key is mapped to the original rjson::value "Key".
        using clustering_keys = std::map<clustering_key, rjson::value*, clustering_key::less_compare>;
        std::unordered_map<partition_key, clustering_keys, partition_key::hashing, partition_key::equality> requests;
        table_requests(schema_ptr s)
            : schema(std::move(s))
            , requests(8, partition_key::hashing(*schema), partition_key::equality(*schema))
        {}
        void add(rjson::value& key) {
            auto pk = pk_from_json(key, schema);
            auto it = requests.find(pk);
            if (it == requests.end()) {
                it = requests.emplace(pk, clustering_key::less_compare(*schema)).first;
            }
            auto ck = ck_from_json(key, schema);
            if (auto [_, inserted] = it->second.emplace(ck, &key); !inserted) {
                throw api_error::validation("Provided list of item keys contains duplicates");
            }
        }
    };
    std::vector<table_requests> requests;

    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        table_requests rs(get_table_from_batch_request(_proxy, it));
        tracing::add_table_name(trace_state, sstring(executor::KEYSPACE_NAME_PREFIX) + rs.schema->cf_name(), rs.schema->cf_name());
        rs.cl = get_read_consistency(it->value);
        std::unordered_set<std::string> used_attribute_names;
        rs.attrs_to_get = ::make_shared<const std::optional<attrs_to_get>>(calculate_attrs_to_get(it->value, used_attribute_names));
        const rjson::value* expression_attribute_names = rjson::find(it->value, "ExpressionAttributeNames");
        verify_all_are_used(expression_attribute_names, used_attribute_names,"ExpressionAttributeNames", "GetItem");
        auto& keys = (it->value)["Keys"];
        for (rjson::value& key : keys.GetArray()) {
            rs.add(key);
            check_key(key, rs.schema);
        }
        requests.emplace_back(std::move(rs));
    }

    for (const table_requests& tr : requests) {
        if (!co_await client_state.check_has_permission(auth::command_desc(
                auth::permission::SELECT,
                auth::make_data_resource(tr.schema->ks_name(), tr.schema->cf_name())))) {
            co_return api_error::access_denied(format(
                "SELECT permissions denied on {}.{} by RBAC", tr.schema->ks_name(), tr.schema->cf_name()));
        }
    }

    _stats.api_operations.batch_get_item_batch_total += requests.size();
    // If we got here, all "requests" are valid, so let's start the
    // requests for the different partitions all in parallel.
    std::vector<future<std::vector<rjson::value>>> response_futures;
    for (const auto& rs : requests) {
        for (const auto &r : rs.requests) {
            auto& pk = r.first;
            auto& cks = r.second;
            dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*rs.schema, pk))};
            std::vector<query::clustering_range> bounds;
            if (rs.schema->clustering_key_size() == 0) {
                bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            } else {
                for (auto& ck : cks) {
                    bounds.push_back(query::clustering_range::make_singular(ck.first));
                }
            }
            auto regular_columns = boost::copy_range<query::column_id_vector>(
                    rs.schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
            auto selection = cql3::selection::selection::wildcard(rs.schema);
            auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
            auto command = ::make_lw_shared<query::read_command>(rs.schema->id(), rs.schema->version(), partition_slice, _proxy.get_max_result_size(partition_slice),
                    query::tombstone_limit(_proxy.get_tombstone_limit()));
            command->allow_limit = db::allow_per_partition_rate_limit::yes;
            future<std::vector<rjson::value>> f = _proxy.query(rs.schema, std::move(command), std::move(partition_ranges), rs.cl,
                    service::storage_proxy::coordinator_query_options(executor::default_timeout(), permit, client_state, trace_state)).then(
                    [schema = rs.schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = rs.attrs_to_get] (service::storage_proxy::coordinator_query_result qr) mutable {
                utils::get_local_injector().inject("alternator_batch_get_item", [] { throw std::runtime_error("batch_get_item injection"); });
                return describe_multi_item(std::move(schema), std::move(partition_slice), std::move(selection), std::move(qr.query_result), std::move(attrs_to_get));
            });
            response_futures.push_back(std::move(f));
        }
    }

    // Wait for all requests to complete, and then return the response.
    // In case of full failure (no reads succeeded), an arbitrary error
    // from one of the operations will be returned.
    bool some_succeeded = false;
    std::exception_ptr eptr;

    rjson::value response = rjson::empty_object();
    rjson::add(response, "Responses", rjson::empty_object());
    rjson::add(response, "UnprocessedKeys", rjson::empty_object());

    auto fut_it = response_futures.begin();
    for (const auto& rs : requests) {
        auto table = table_name(*rs.schema);
        for (const auto &r : rs.requests) {
            auto& pk = r.first;
            auto& cks = r.second;
            auto& fut = *fut_it;
            ++fut_it;
            try {
                std::vector<rjson::value> results = co_await std::move(fut);
                some_succeeded = true;
                if (!response["Responses"].HasMember(table)) {
                    rjson::add_with_string_name(response["Responses"], table, rjson::empty_array());
                }
                for (rjson::value& json : results) {
                    rjson::push_back(response["Responses"][table], std::move(json));
                }
            } catch(...) {
                eptr = std::current_exception();
                // This read of potentially several rows in one partition,
                // failed. We need to add the row key(s) to UnprocessedKeys.
                if (!response["UnprocessedKeys"].HasMember(table)) {
                    // Add the table's entry in UnprocessedKeys. Need to copy
                    // all the table's parameters from the request except the
                    // Keys field, which we start empty and then build below.
                    rjson::add_with_string_name(response["UnprocessedKeys"], table, rjson::empty_object());
                    rjson::value& unprocessed_item = response["UnprocessedKeys"][table];
                    rjson::value& request_item = request_items[table];
                    for (auto it = request_item.MemberBegin(); it != request_item.MemberEnd(); ++it) {
                        if (it->name != "Keys") {
                            rjson::add_with_string_name(unprocessed_item,
                                rjson::to_string_view(it->name), rjson::copy(it->value));
                        }
                    }
                    rjson::add_with_string_name(unprocessed_item, "Keys", rjson::empty_array());
                }
                for (auto& ck : cks) {
                    rjson::push_back(response["UnprocessedKeys"][table]["Keys"], std::move(*ck.second));
                }
            }
        }
    }
    elogger.trace("Unprocessed keys: {}", response["UnprocessedKeys"]);
    if (!some_succeeded && eptr) {
        co_await coroutine::return_exception_ptr(std::move(eptr));
    }
    _stats.api_operations.batch_get_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    if (is_big(response)) {
        co_return make_streamed(std::move(response));
    } else {
        co_return make_jsonable(std::move(response));
    }
}

// "filter" represents a condition that can be applied to individual items
// read by a Query or Scan operation, to decide whether to keep the item.
// A filter is constructed from a Query or Scan request. This uses the
// relevant fields in the query (FilterExpression or QueryFilter/ScanFilter +
// ConditionalOperator). These fields are pre-checked and pre-parsed as much
// as possible, to ensure that later checking of many items is efficient.
class filter {
private:
    // Holding QueryFilter/ScanFilter + ConditionalOperator:
    struct conditions_filter {
        bool require_all;
        rjson::value conditions;
    };
    // Holding a parsed FilterExpression:
    struct expression_filter {
        parsed::condition_expression expression;
    };
    std::optional<std::variant<conditions_filter, expression_filter>> _imp;
public:
    // Filtering for Scan and Query are very similar, but there are some
    // small differences, especially the names of the request attributes.
    enum class request_type { SCAN, QUERY };
    // Note that a filter does not store pointers to the query used to
    // construct it.
    filter(const rjson::value& request, request_type rt,
            std::unordered_set<std::string>& used_attribute_names,
            std::unordered_set<std::string>& used_attribute_values);
    bool check(const rjson::value& item) const;
    bool filters_on(std::string_view attribute) const;
    // for_filters_on() runs the given function on the attributes that the
    // filter works on. It may run for the same attribute more than once if
    // used more than once in the filter.
    void for_filters_on(const noncopyable_function<void(std::string_view)>& func) const;
    operator bool() const { return bool(_imp); }
};

filter::filter(const rjson::value& request, request_type rt,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values) {
    const rjson::value* expression = rjson::find(request, "FilterExpression");
    const char* conditions_attribute = (rt == request_type::SCAN) ? "ScanFilter" : "QueryFilter";
    const rjson::value* conditions = rjson::find(request, conditions_attribute);
    auto conditional_operator = get_conditional_operator(request);
    if (conditional_operator != conditional_operator_type::MISSING &&
        (!conditions || (conditions->IsObject() && conditions->GetObject().ObjectEmpty()))) {
            throw api_error::validation(
                    format("'ConditionalOperator' parameter cannot be specified for missing or empty {}",
                            conditions_attribute));
    }
    if (expression && conditions) {
        throw api_error::validation(
                format("FilterExpression and {} are not allowed together", conditions_attribute));
    }
    if (expression) {
        if (!expression->IsString()) {
            throw api_error::validation("FilterExpression must be a string");
        }
        if (expression->GetStringLength() == 0) {
            throw api_error::validation("FilterExpression must not be empty");
        }
        if (rjson::find(request, "AttributesToGet")) {
            throw api_error::validation("Cannot use both old-style and new-style parameters in same request: FilterExpression and AttributesToGet");
        }
        try {
            auto parsed = parse_condition_expression(rjson::to_string_view(*expression), "FilterExpression");
            const rjson::value* expression_attribute_names = rjson::find(request, "ExpressionAttributeNames");
            const rjson::value* expression_attribute_values = rjson::find(request, "ExpressionAttributeValues");
            resolve_condition_expression(parsed,
                    expression_attribute_names, expression_attribute_values,
                    used_attribute_names, used_attribute_values);
            _imp = expression_filter { std::move(parsed) };
        } catch(expressions_syntax_error& e) {
            throw api_error::validation(e.what());
        }
    }
    if (conditions) {
        if (rjson::find(request, "ProjectionExpression")) {
            throw api_error::validation(format("Cannot use both old-style and new-style parameters in same request: {} and ProjectionExpression", conditions_attribute));
        }
        bool require_all = conditional_operator != conditional_operator_type::OR;
        _imp = conditions_filter { require_all, rjson::copy(*conditions) };
    }
}

bool filter::check(const rjson::value& item) const {
    if (!_imp) {
        return true;
    }
    return std::visit(overloaded_functor {
        [&] (const conditions_filter& f) -> bool {
            return verify_condition(f.conditions, f.require_all, &item);
        },
        [&] (const expression_filter& f) -> bool {
            return verify_condition_expression(f.expression, &item);
        }
    }, *_imp);
}

bool filter::filters_on(std::string_view attribute) const {
    if (!_imp) {
        return false;
    }
    return std::visit(overloaded_functor {
        [&] (const conditions_filter& f) -> bool {
            for (auto it = f.conditions.MemberBegin(); it != f.conditions.MemberEnd(); ++it) {
                if (rjson::to_string_view(it->name) == attribute) {
                    return true;
                }
            }
            return false;
        },
        [&] (const expression_filter& f) -> bool {
            return condition_expression_on(f.expression, attribute);
        }
    }, *_imp);
}

void filter::for_filters_on(const noncopyable_function<void(std::string_view)>& func) const {
    if (_imp) {
        std::visit(overloaded_functor {
            [&] (const conditions_filter& f) -> void {
                for (auto it = f.conditions.MemberBegin(); it != f.conditions.MemberEnd(); ++it) {
                    func(rjson::to_string_view(it->name));
                }
            },
            [&] (const expression_filter& f) -> void {
                return for_condition_expression_on(f.expression, func);
            }
        }, *_imp);
    }
}

class describe_items_visitor {
    typedef std::vector<const column_definition*> columns_t;
    const columns_t& _columns;
    const std::optional<attrs_to_get>& _attrs_to_get;
    std::unordered_set<std::string> _extra_filter_attrs;
    const filter& _filter;
    typename columns_t::const_iterator _column_it;
    rjson::value _item;
    rjson::value _items;
    size_t _scanned_count;

public:
    describe_items_visitor(const columns_t& columns, const std::optional<attrs_to_get>& attrs_to_get, filter& filter)
            : _columns(columns)
            , _attrs_to_get(attrs_to_get)
            , _filter(filter)
            , _column_it(columns.begin())
            , _item(rjson::empty_object())
            , _items(rjson::empty_array())
            , _scanned_count(0)
    {
        // _filter.check() may need additional attributes not listed in
        // _attrs_to_get (i.e., not requested as part of the output).
        // We list those in _extra_filter_attrs. We will include them in
        // the JSON but take them out before finally returning the JSON.
        if (_attrs_to_get) {
            _filter.for_filters_on([&] (std::string_view attr) {
                std::string a(attr); // no heterogeneous maps searches :-(
                if (!_attrs_to_get->contains(a)) {
                    _extra_filter_attrs.emplace(std::move(a));
                }
            });
        }
    }

    void start_row() {
        _column_it = _columns.begin();
    }

    void accept_value(managed_bytes_view_opt result_bytes_view) {
        if (!result_bytes_view) {
            ++_column_it;
            return;
        }
        result_bytes_view->with_linearized([this] (bytes_view bv) {
            std::string column_name = (*_column_it)->name_as_text();
            if (column_name != executor::ATTRS_COLUMN_NAME) {
                if (!_attrs_to_get || _attrs_to_get->contains(column_name) || _extra_filter_attrs.contains(column_name)) {
                    if (!_item.HasMember(column_name.c_str())) {
                        rjson::add_with_string_name(_item, column_name, rjson::empty_object());
                    }
                    rjson::value& field = _item[column_name.c_str()];
                    rjson::add_with_string_name(field, type_to_string((*_column_it)->type), json_key_column_value(bv, **_column_it));
                }
            } else {
                auto deserialized = attrs_type()->deserialize(bv);
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (!_attrs_to_get || _attrs_to_get->contains(attr_name) || _extra_filter_attrs.contains(attr_name)) {
                        bytes value = value_cast<bytes>(entry.second);
                        // Even if _attrs_to_get asked to keep only a part of a
                        // top-level attribute, we keep the entire attribute
                        // at this stage, because the item filter might still
                        // need the other parts (it was easier for us to keep
                        // extra_filter_attrs at top-level granularity). We'll
                        // filter the unneeded parts after item filtering.
                        rjson::add_with_string_name(_item, attr_name, deserialize_item(value));
                    }
                }
            }
        });
        ++_column_it;
    }

    void end_row() {
        if (_filter.check(_item)) {
            // As noted above, we kept entire top-level attributes listed in
            // _attrs_to_get. We may need to only keep parts of them.
            if (_attrs_to_get) {
                for (const auto& attr: *_attrs_to_get) {
                    // If !attr.has_value() it means we were asked not to keep
                    // attr entirely, but just parts of it.
                    if (!attr.second.has_value()) {
                        rjson::value* toplevel= rjson::find(_item, attr.first);
                        if (toplevel && !hierarchy_filter(*toplevel, attr.second)) {
                            rjson::remove_member(_item, attr.first);
                        }
                    }
                }
            }
            // Remove the extra attributes _extra_filter_attrs which we had
            // to add just for the filter, and not requested to be returned:
            for (const auto& attr : _extra_filter_attrs) {
                rjson::remove_member(_item, attr);
            }

            rjson::push_back(_items, std::move(_item));
        }
        _item = rjson::empty_object();
        ++_scanned_count;
    }

    rjson::value get_items() && {
        return std::move(_items);
    }

    size_t get_scanned_count() {
        return _scanned_count;
    }
};

static future<std::tuple<rjson::value, size_t>> describe_items(const cql3::selection::selection& selection, std::unique_ptr<cql3::result_set> result_set, std::optional<attrs_to_get>&& attrs_to_get, filter&& filter) {
    describe_items_visitor visitor(selection.get_columns(), attrs_to_get, filter);
    co_await result_set->visit_gently(visitor);
    auto scanned_count = visitor.get_scanned_count();
    rjson::value items = std::move(visitor).get_items();
    rjson::value items_descr = rjson::empty_object();
    auto size = items.Size();
    rjson::add(items_descr, "Count", rjson::value(size));
    rjson::add(items_descr, "ScannedCount", rjson::value(scanned_count));
    // If attrs_to_get && attrs_to_get->empty(), this means the user asked not
    // to get any attributes (i.e., a Scan or Query with Select=COUNT) and we
    // shouldn't return "Items" at all.
    // TODO: consider optimizing the case of Select=COUNT without a filter.
    // In that case, we currently build a list of empty items and here drop
    // it. We could just count the items and not bother with the empty items.
    // (However, remember that when we do have a filter, we need the items).
    if (!attrs_to_get || !attrs_to_get->empty()) {
        rjson::add(items_descr, "Items", std::move(items));
    }
    co_return std::tuple<rjson::value, size_t>{std::move(items_descr), size};
}

static rjson::value encode_paging_state(const schema& schema, const service::pager::paging_state& paging_state) {
    rjson::value last_evaluated_key = rjson::empty_object();
    std::vector<bytes> exploded_pk = paging_state.get_partition_key().explode();
    auto exploded_pk_it = exploded_pk.begin();
    for (const column_definition& cdef : schema.partition_key_columns()) {
        rjson::add_with_string_name(last_evaluated_key, std::string_view(cdef.name_as_text()), rjson::empty_object());
        rjson::value& key_entry = last_evaluated_key[cdef.name_as_text()];
        rjson::add_with_string_name(key_entry, type_to_string(cdef.type), json_key_column_value(*exploded_pk_it, cdef));
        ++exploded_pk_it;
    }
    auto pos = paging_state.get_position_in_partition();
    if (pos.has_key()) {
        auto exploded_ck = pos.key().explode();
        auto exploded_ck_it = exploded_ck.begin();
        for (const column_definition& cdef : schema.clustering_key_columns()) {
            rjson::add_with_string_name(last_evaluated_key, std::string_view(cdef.name_as_text()), rjson::empty_object());
            rjson::value& key_entry = last_evaluated_key[cdef.name_as_text()];
            rjson::add_with_string_name(key_entry, type_to_string(cdef.type), json_key_column_value(*exploded_ck_it, cdef));
            ++exploded_ck_it;
        }
    }
    // To avoid possible conflicts (and thus having to reserve these names) we
    // avoid adding the weight and region fields of the position to the paging
    // state. Alternator will never need these as it doesn't have range
    // tombstones (the only thing that can generate a position other than at(row)).
    // We conditionally include these fields when reading CQL tables through alternator.
    if (!is_alternator_keyspace(schema.ks_name()) && (!pos.has_key() || pos.get_bound_weight() != bound_weight::equal)) {
        rjson::add_with_string_name(last_evaluated_key, scylla_paging_region, rjson::empty_object());
        rjson::add(last_evaluated_key[scylla_paging_region.data()], "S", rjson::from_string(fmt::to_string(pos.region())));
        rjson::add_with_string_name(last_evaluated_key, scylla_paging_weight, rjson::empty_object());
        rjson::add(last_evaluated_key[scylla_paging_weight.data()], "N", static_cast<int>(pos.get_bound_weight()));
    }
    return last_evaluated_key;
}

static future<executor::request_return_type> do_query(service::storage_proxy& proxy,
        schema_ptr table_schema,
        const rjson::value* exclusive_start_key,
        dht::partition_range_vector partition_ranges,
        std::vector<query::clustering_range> ck_bounds,
        std::optional<attrs_to_get> attrs_to_get,
        uint32_t limit,
        db::consistency_level cl,
        filter filter,
        query::partition_slice::option_set custom_opts,
        service::client_state& client_state,
        cql3::cql_stats& cql_stats,
        tracing::trace_state_ptr trace_state,
        service_permit permit) {
    lw_shared_ptr<service::pager::paging_state> old_paging_state = nullptr;

    tracing::trace(trace_state, "Performing a database query");

    // Reverse the schema and the clustering bounds as the underlying code expects
    // reversed queries in the native reversed format.
    auto query_schema = table_schema;
    const bool reversed = custom_opts.contains<query::partition_slice::option::reversed>();
    if (reversed) {
        query_schema = table_schema->get_reversed();

        std::reverse(ck_bounds.begin(), ck_bounds.end());
        for (auto& bound : ck_bounds) {
            bound = query::reverse(bound);
        }
    }

    if (exclusive_start_key) {
        partition_key pk = pk_from_json(*exclusive_start_key, table_schema);
        auto pos = position_in_partition::for_partition_start();
        if (table_schema->clustering_key_size() > 0) {
            pos = pos_from_json(*exclusive_start_key, table_schema);
        }
        old_paging_state = make_lw_shared<service::pager::paging_state>(pk, pos, query::max_partitions, query_id::create_null_id(), service::pager::paging_state::replicas_per_token_range{}, std::nullopt, 0);
    }

    if (!co_await client_state.check_has_permission(auth::command_desc(
            auth::permission::SELECT,
            auth::make_data_resource(table_schema->ks_name(), table_schema->cf_name())))) {
        co_return api_error::access_denied(format(
            "SELECT permissions denied on {}.{} by RBAC", table_schema->ks_name(), table_schema->cf_name()));
    }

    auto regular_columns = boost::copy_range<query::column_id_vector>(
            table_schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
    auto static_columns = boost::copy_range<query::column_id_vector>(
            table_schema->static_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
    auto selection = cql3::selection::selection::wildcard(table_schema);
    query::partition_slice::option_set opts = selection->get_query_options();
    opts.add(custom_opts);
    auto partition_slice = query::partition_slice(std::move(ck_bounds), std::move(static_columns), std::move(regular_columns), opts);
    auto command = ::make_lw_shared<query::read_command>(query_schema->id(), query_schema->version(), partition_slice, proxy.get_max_result_size(partition_slice),
        query::tombstone_limit(proxy.get_tombstone_limit()));

    elogger.trace("Executing read query (reversed {}): table schema {}, query schema {}", partition_slice.is_reversed(), table_schema->version(), query_schema->version());

    auto query_state_ptr = std::make_unique<service::query_state>(client_state, trace_state, std::move(permit));

    // FIXME: should be moved above, set on opts, so get_max_result_size knows it?
    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto query_options = std::make_unique<cql3::query_options>(cl, std::vector<cql3::raw_value>{});
    query_options = std::make_unique<cql3::query_options>(std::move(query_options), std::move(old_paging_state));
    auto p = service::pager::query_pagers::pager(proxy, query_schema, selection, *query_state_ptr, *query_options, command, std::move(partition_ranges), nullptr);

    std::unique_ptr<cql3::result_set> rs = co_await p->fetch_page(limit, gc_clock::now(), executor::default_timeout());
    if (!p->is_exhausted()) {
        rs->get_metadata().set_paging_state(p->state());
    }
    auto paging_state = rs->get_metadata().paging_state();
    bool has_filter = filter;
    auto [items, size] = co_await describe_items(*selection, std::move(rs), std::move(attrs_to_get), std::move(filter));
    if (paging_state) {
        rjson::add(items, "LastEvaluatedKey", encode_paging_state(*table_schema, *paging_state));
    }
    if (has_filter){
        cql_stats.filtered_rows_read_total += p->stats().rows_read_total;
        // update our "filtered_row_matched_total" for all the rows matched, despited the filter
        cql_stats.filtered_rows_matched_total += size;
    }
    if (is_big(items)) {
        co_return executor::request_return_type(make_streamed(std::move(items)));
    }
    co_return executor::request_return_type(make_jsonable(std::move(items)));
}

static dht::token token_for_segment(int segment, int total_segments) {
    SCYLLA_ASSERT(total_segments > 1 && segment >= 0 && segment < total_segments);
    uint64_t delta = std::numeric_limits<uint64_t>::max() / total_segments;
    return dht::token::from_int64(std::numeric_limits<int64_t>::min() + delta * segment);
}

static dht::partition_range get_range_for_segment(int segment, int total_segments) {
    if (total_segments == 1) {
        return dht::partition_range::make_open_ended_both_sides();
    }
    if (segment == 0) {
        dht::token ending_token = token_for_segment(1, total_segments);
        return dht::partition_range::make_ending_with(
                dht::partition_range::bound(dht::ring_position::ending_at(ending_token), false));
    } else if (segment == total_segments - 1) {
        dht::token starting_token = token_for_segment(segment, total_segments);
        return dht::partition_range::make_starting_with(
                dht::partition_range::bound(dht::ring_position::starting_at(starting_token)));
    } else {
        dht::token starting_token = token_for_segment(segment, total_segments);
        dht::token ending_token = token_for_segment(segment + 1, total_segments);
        return dht::partition_range::make(
            dht::partition_range::bound(dht::ring_position::starting_at(starting_token)),
            dht::partition_range::bound(dht::ring_position::ending_at(ending_token), false)
        );
    }
}

future<executor::request_return_type> executor::scan(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.scan++;
    elogger.trace("Scanning {}", request);

    auto [schema, table_type] = get_table_or_view(_proxy, request);

    auto segment = get_int_attribute(request, "Segment");
    auto total_segments = get_int_attribute(request, "TotalSegments");
    if (segment || total_segments) {
        if (!segment || !total_segments) {
            return make_ready_future<request_return_type>(api_error::validation(
                    "Both Segment and TotalSegments attributes need to be present for a parallel scan"));
        }
        if (*segment < 0 || *segment >= *total_segments) {
            return make_ready_future<request_return_type>(api_error::validation(
                    "Segment must be non-negative and less than TotalSegments"));
        }
        if (*total_segments < 0 || *total_segments > 1000000) {
            return make_ready_future<request_return_type>(api_error::validation(
                    "TotalSegments must be non-negative and less or equal to 1000000"));
        }
    }

    rjson::value* exclusive_start_key = rjson::find(request, "ExclusiveStartKey");

    db::consistency_level cl = get_read_consistency(request);
    if (table_type == table_or_view_type::gsi && cl != db::consistency_level::LOCAL_ONE) {
        return make_ready_future<request_return_type>(api_error::validation(
                "Consistent reads are not allowed on global indexes (GSI)"));
    }
    rjson::value* limit_json = rjson::find(request, "Limit");
    uint32_t limit = limit_json ? limit_json->GetUint64() : std::numeric_limits<uint32_t>::max();
    if (limit <= 0) {
        return make_ready_future<request_return_type>(api_error::validation("Limit must be greater than 0"));
    }

    select_type select = parse_select(request, table_type);

    std::unordered_set<std::string> used_attribute_names;
    std::unordered_set<std::string> used_attribute_values;
    auto attrs_to_get = calculate_attrs_to_get(request, used_attribute_names, select);

    dht::partition_range_vector partition_ranges;
    if (segment) {
        auto range = get_range_for_segment(*segment, *total_segments);
        if (exclusive_start_key) {
            auto ring_pos = dht::ring_position{dht::decorate_key(*schema, pk_from_json(*exclusive_start_key, schema))};
            if (!range.contains(ring_pos, dht::ring_position_comparator(*schema))) {
                return make_ready_future<request_return_type>(api_error::validation(
                    format("The provided starting key is invalid: Invalid ExclusiveStartKey. Please use ExclusiveStartKey "
                           "with correct Segment. TotalSegments: {} Segment: {}", *total_segments, *segment)));
            }
        }
        partition_ranges.push_back(range);
    } else {
        partition_ranges.push_back(dht::partition_range::make_open_ended_both_sides());
    }
    std::vector<query::clustering_range> ck_bounds{query::clustering_range::make_open_ended_both_sides()};

    filter filter(request, filter::request_type::SCAN, used_attribute_names, used_attribute_values);
    // Note: Unlike Query, Scan does allow a filter on the key attributes.
    // For some *specific* cases of key filtering, such an equality test on
    // partition key or comparison operator for the sort key, we could have
    // optimized the filtering by modifying partition_ranges and/or
    // ck_bounds. We haven't done this optimization yet.

    const rjson::value* expression_attribute_names = rjson::find(request, "ExpressionAttributeNames");
    const rjson::value* expression_attribute_values = rjson::find(request, "ExpressionAttributeValues");
    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "Scan");
    verify_all_are_used(expression_attribute_values, used_attribute_values, "ExpressionAttributeValues", "Scan");

    return do_query(_proxy, schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl,
            std::move(filter), query::partition_slice::option_set(), client_state, _stats.cql_stats, trace_state, std::move(permit));
}

static dht::partition_range calculate_pk_bound(schema_ptr schema, const column_definition& pk_cdef, const rjson::value& comp_definition, const rjson::value& attrs) {
    auto op = get_comparison_operator(comp_definition);
    if (op != comparison_operator_type::EQ) {
        throw api_error::validation(format("Hash key can only be restricted with equality operator (EQ). {} not supported.", comp_definition));
    }
    if (attrs.Size() != 1) {
        throw api_error::validation(format("A single attribute is required for a hash key EQ restriction: {}", attrs));
    }
    bytes raw_value = get_key_from_typed_value(attrs[0], pk_cdef);
    partition_key pk = partition_key::from_singular_bytes(*schema, std::move(raw_value));
    auto decorated_key = dht::decorate_key(*schema, pk);
    return dht::partition_range(decorated_key);
}

static query::clustering_range get_clustering_range_for_begins_with(bytes&& target, const clustering_key& ck, schema_ptr schema, data_type t) {
    auto it = boost::range::find_end(target, bytes("\xFF"), std::not_equal_to<bytes::value_type>());
    if (it != target.end()) {
        ++*it;
        target.resize(std::distance(target.begin(), it) + 1);
        clustering_key upper_limit = clustering_key::from_single_value(*schema, target);
        return query::clustering_range::make(query::clustering_range::bound(ck), query::clustering_range::bound(upper_limit, false));
    }
    return query::clustering_range::make_starting_with(query::clustering_range::bound(ck));
}

static query::clustering_range calculate_ck_bound(schema_ptr schema, const column_definition& ck_cdef, const rjson::value& comp_definition, const rjson::value& attrs) {
    auto op = get_comparison_operator(comp_definition);
    const size_t expected_attrs_size = (op == comparison_operator_type::BETWEEN) ? 2 : 1;
    if (attrs.Size() != expected_attrs_size) {
        throw api_error::validation(format("{} arguments expected for a sort key restriction: {}", expected_attrs_size, attrs));
    }
    bytes raw_value = get_key_from_typed_value(attrs[0], ck_cdef);
    clustering_key ck = clustering_key::from_single_value(*schema, raw_value);
    switch (op) {
    case comparison_operator_type::EQ:
        return query::clustering_range(ck);
    case comparison_operator_type::LE:
        return query::clustering_range::make_ending_with(query::clustering_range::bound(ck));
    case comparison_operator_type::LT:
        return query::clustering_range::make_ending_with(query::clustering_range::bound(ck, false));
    case comparison_operator_type::GE:
        return query::clustering_range::make_starting_with(query::clustering_range::bound(ck));
    case comparison_operator_type::GT:
        return query::clustering_range::make_starting_with(query::clustering_range::bound(ck, false));
    case comparison_operator_type::BETWEEN: {
        bytes raw_upper_limit = get_key_from_typed_value(attrs[1], ck_cdef);
        clustering_key upper_limit = clustering_key::from_single_value(*schema, raw_upper_limit);
        return query::clustering_range::make(query::clustering_range::bound(ck), query::clustering_range::bound(upper_limit));
    }
    case comparison_operator_type::BEGINS_WITH: {
        if (raw_value.empty()) {
            return query::clustering_range::make_open_ended_both_sides();
        }
        // NOTICE(sarna): A range starting with given prefix and ending (non-inclusively) with a string "incremented" by a single
        // character at the end. Throws for NUMBER instances.
        if (!ck_cdef.type->is_compatible_with(*utf8_type)) {
            throw api_error::validation(format("BEGINS_WITH operator cannot be applied to type {}", type_to_string(ck_cdef.type)));
        }
        return get_clustering_range_for_begins_with(std::move(raw_value), ck, schema, ck_cdef.type);
    }
    default:
        throw api_error::validation(format("Operator {} not supported for sort key", comp_definition));
    }
}

// Calculates primary key bounds from KeyConditions
static std::pair<dht::partition_range_vector, std::vector<query::clustering_range>>
calculate_bounds_conditions(schema_ptr schema, const rjson::value& conditions) {
    dht::partition_range_vector partition_ranges;
    std::vector<query::clustering_range> ck_bounds;

    for (auto it = conditions.MemberBegin(); it != conditions.MemberEnd(); ++it) {
        std::string key = it->name.GetString();
        const rjson::value& condition = it->value;

        const rjson::value& comp_definition = rjson::get(condition, "ComparisonOperator");
        const rjson::value& attr_list = rjson::get(condition, "AttributeValueList");

        const column_definition& pk_cdef = schema->partition_key_columns().front();
        const column_definition* ck_cdef = schema->clustering_key_size() > 0 ? &schema->clustering_key_columns().front() : nullptr;
        if (sstring(key) == pk_cdef.name_as_text()) {
            if (!partition_ranges.empty()) {
                throw api_error::validation("Currently only a single restriction per key is allowed");
            }
            partition_ranges.push_back(calculate_pk_bound(schema, pk_cdef, comp_definition, attr_list));
        }
        if (ck_cdef && sstring(key) == ck_cdef->name_as_text()) {
            if (!ck_bounds.empty()) {
                throw api_error::validation("Currently only a single restriction per key is allowed");
            }
            ck_bounds.push_back(calculate_ck_bound(schema, *ck_cdef, comp_definition, attr_list));
        }
    }

    // Validate that a query's conditions must be on the hash key, and
    // optionally also on the sort key if it exists.
    if (partition_ranges.empty()) {
        throw api_error::validation(format("Query missing condition on hash key '{}'", schema->partition_key_columns().front().name_as_text()));
    }
    if (schema->clustering_key_size() == 0) {
        if (conditions.MemberCount() != 1) {
            throw api_error::validation("Only one condition allowed in table with only hash key");
        }
    } else {
        if (conditions.MemberCount() == 2 && ck_bounds.empty()) {
            throw api_error::validation(format("Query missing condition on sort key '{}'", schema->clustering_key_columns().front().name_as_text()));
        } else if (conditions.MemberCount() > 2) {
            throw api_error::validation("Only one or two conditions allowed in table with hash key and sort key");
        }
    }

    if (ck_bounds.empty()) {
        ck_bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    }

    return {std::move(partition_ranges), std::move(ck_bounds)};
}

// Extract the top-level column name specified in a KeyConditionExpression.
// If a nested attribute path is given, a ValidationException is generated.
// If the column name is a #reference to ExpressionAttributeNames, the
// reference is resolved.
// Note this function returns a string_view, which may refer to data in the
// given parsed::value or expression_attribute_names.
static std::string_view get_toplevel(const parsed::value& v,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names)
{
    const parsed::path& path = std::get<parsed::path>(v._value);
    if (path.has_operators()) {
        throw api_error::validation("KeyConditionExpression does not support nested attributes");
    }
    std::string_view column_name = path.root();
    if (column_name.size() > 0 && column_name[0] == '#') {
        used_attribute_names.emplace(column_name);
        if (!expression_attribute_names) {
            throw api_error::validation(
                    format("ExpressionAttributeNames missing, entry '{}' required by KeyConditionExpression",
                            column_name));
        }
        const rjson::value* value = rjson::find(*expression_attribute_names, column_name);
        if (!value || !value->IsString()) {
            throw api_error::validation(
                    format("ExpressionAttributeNames missing entry '{}' required by KeyConditionExpression",
                            column_name));
        }
        column_name = rjson::to_string_view(*value);
    }
    return column_name;
}

// Extract a constant value specified in a KeyConditionExpression.
// This constant was originally parsed as a reference (:name) to a member of
// ExpressionAttributeValues, but at this point, after resolve_value(), it
// was already converted into a JSON value.
// This function decodes the value (using its given expected type) into bytes
// which Scylla uses as the actual key value. If the value has the wrong type,
// or the input had other problems, a ValidationException is thrown.
static bytes get_constant_value(const parsed::value& v,
        const column_definition& column)
{
    const parsed::constant& constant = std::get<parsed::constant>(v._value);
    const parsed::constant::literal& lit = std::get<parsed::constant::literal>(constant._value);
    return get_key_from_typed_value(*lit, column);
}

// condition_expression_and_list extracts a list of ANDed primitive conditions
// from a condition_expression. This is useful for KeyConditionExpression,
// which may not use OR or NOT. If the given condition_expression does use
// OR or NOT, this function throws a ValidationException.
static void condition_expression_and_list(
        const parsed::condition_expression& condition_expression,
        std::vector<const parsed::primitive_condition*>& conditions)
{
    if (condition_expression._negated) {
        throw api_error::validation("KeyConditionExpression cannot use NOT");
    }
    std::visit(overloaded_functor {
        [&] (const parsed::primitive_condition& cond) {
            conditions.push_back(&cond);
        },
        [&] (const parsed::condition_expression::condition_list& list) {
            if (list.op == '|' && list.conditions.size() > 1) {
                throw api_error::validation("KeyConditionExpression cannot use OR");
            }
            for (const parsed::condition_expression& cond : list.conditions) {
                condition_expression_and_list(cond, conditions);
            }
        }
    }, condition_expression._expression);
}

// Calculates primary key bounds from KeyConditionExpression
static std::pair<dht::partition_range_vector, std::vector<query::clustering_range>>
calculate_bounds_condition_expression(schema_ptr schema,
        const rjson::value& expression,
        const rjson::value* expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_values,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names)
{
    if (!expression.IsString()) {
        throw api_error::validation("KeyConditionExpression must be a string");
    }
    if (expression.GetStringLength() == 0) {
        throw api_error::validation("KeyConditionExpression must not be empty");
    }
    // We parse the KeyConditionExpression with the same parser we use for
    // ConditionExpression. But KeyConditionExpression only supports a subset
    // of the ConditionExpression features, so we have many additional
    // verifications below that the key condition is legal. Briefly, a valid
    // key condition must contain a single partition key and a single
    // sort-key range.
    parsed::condition_expression p;
    try {
        p = parse_condition_expression(rjson::to_string_view(expression), "KeyConditionExpression");
    } catch(expressions_syntax_error& e) {
        throw api_error::validation(e.what());
    }
    resolve_condition_expression(p,
            expression_attribute_names, expression_attribute_values,
            used_attribute_names, used_attribute_values);
    std::vector<const parsed::primitive_condition*> conditions;
    condition_expression_and_list(p, conditions);

    if (conditions.size() < 1 || conditions.size() > 2) {
        throw api_error::validation(
                "KeyConditionExpression syntax error: must have 1 or 2 conditions");
    }
    // Scylla allows us to have an (equality) constraint on the partition key
    // pk_cdef, and a range constraint on the *first* clustering key ck_cdef.
    // Note that this is also good enough for our GSI implementation - the
    // GSI's user-specified sort key will be the first clustering key.
    // FIXME: In the case described in issue #5320 (base and GSI both have
    // just hash key - but different ones), this may allow the user to Query
    // using the base key which isn't officially part of the GSI.
    const column_definition& pk_cdef = schema->partition_key_columns().front();
    const column_definition* ck_cdef = schema->clustering_key_size() > 0 ?
            &schema->clustering_key_columns().front() : nullptr;

    dht::partition_range_vector partition_ranges;
    std::vector<query::clustering_range> ck_bounds;
    for (const parsed::primitive_condition* condp : conditions) {
        const parsed::primitive_condition& cond = *condp;
        // In all comparison operators, one operand must be a column name,
        // the other is a constant (value reference). We remember which is
        // which in toplevel_ind, and also the column name in key (not just
        // for comparison operators).
        std::string_view key;
        int toplevel_ind;
        switch (cond._values.size()) {
        case 1: {
            // The only legal single-value condition is a begin_with() function,
            // and it must have two parameters - a top-level attribute and a
            // value reference..
            const parsed::value::function_call *f = std::get_if<parsed::value::function_call>(&cond._values[0]._value);
            if (!f) {
                throw api_error::validation("KeyConditionExpression cannot be just a value");
            }
            if (f->_function_name != "begins_with") {
                throw api_error::validation(
                        format("KeyConditionExpression function '{}' not supported",f->_function_name));
            }
            if (f->_parameters.size() != 2 || !f->_parameters[0].is_path() ||
                    !f->_parameters[1].is_constant()) {
                throw api_error::validation(
                        "KeyConditionExpression begins_with() takes attribute and value");
            }
            key = get_toplevel(f->_parameters[0], expression_attribute_names, used_attribute_names);
            toplevel_ind = -1;
            break;
        }
        case 2:
            if (cond._values[0].is_path() && cond._values[1].is_constant()) {
                toplevel_ind = 0;
            } else if (cond._values[1].is_path() && cond._values[0].is_constant()) {
                toplevel_ind = 1;
            } else {
                throw api_error::validation("KeyConditionExpression must compare attribute with constant");
            }
            key = get_toplevel(cond._values[toplevel_ind],  expression_attribute_names, used_attribute_names);
            break;
        case 3:
            // Only BETWEEN has three operands. First must be a column name,
            // two other must be value references (constants):
            if (cond._op != parsed::primitive_condition::type::BETWEEN) {
                // Shouldn't happen unless we have a bug in the parser
                throw std::logic_error(format("Wrong number of values {} in primitive_condition", cond._values.size()));
            }
            if (cond._values[0].is_path() && cond._values[1].is_constant() && cond._values[2].is_constant()) {
                toplevel_ind = 0;
                key = get_toplevel(cond._values[0], expression_attribute_names, used_attribute_names);
            } else {
                throw api_error::validation("KeyConditionExpression must compare attribute with constants");
            }
            break;
        default:
            // Shouldn't happen unless we have a bug in the parser
            throw std::logic_error(format("Wrong number of values {} in primitive_condition", cond._values.size()));
        }
        if (cond._op == parsed::primitive_condition::type::IN) {
            throw api_error::validation("KeyConditionExpression does not support IN operator");
        } else if (cond._op == parsed::primitive_condition::type::NE) {
            throw api_error::validation("KeyConditionExpression does not support NE operator");
        } else if (cond._op == parsed::primitive_condition::type::EQ) {
            // the EQ operator (=) is the only one which can be used for both
            // the partition key and sort key:
            if (sstring(key) == pk_cdef.name_as_text()) {
                if (!partition_ranges.empty()) {
                    throw api_error::validation(
                            "KeyConditionExpression allows only one condition for each key");
                }
                bytes raw_value = get_constant_value(cond._values[!toplevel_ind], pk_cdef);
                partition_key pk = partition_key::from_singular_bytes(*schema, std::move(raw_value));
                auto decorated_key = dht::decorate_key(*schema, pk);
                partition_ranges.push_back(dht::partition_range(decorated_key));
            } else if (ck_cdef && sstring(key) == ck_cdef->name_as_text()) {
                if (!ck_bounds.empty()) {
                    throw api_error::validation(
                            "KeyConditionExpression allows only one condition for each key");
                }
                bytes raw_value = get_constant_value(cond._values[!toplevel_ind], *ck_cdef);
                clustering_key ck = clustering_key::from_single_value(*schema, raw_value);
                ck_bounds.push_back(query::clustering_range(ck));
            } else {
                throw api_error::validation(
                        format("KeyConditionExpression condition on non-key attribute {}", key));
            }
            continue;
        }
        // If we're still here, it's any other operator besides EQ, and these
        // are allowed *only* on the clustering key:
        if (sstring(key) == pk_cdef.name_as_text()) {
            throw api_error::validation(
                    format("KeyConditionExpression only '=' condition is supported on partition key {}", key));
        } else if (!ck_cdef || sstring(key) != ck_cdef->name_as_text()) {
            throw api_error::validation(
                    format("KeyConditionExpression condition on non-key attribute {}", key));
        }
        if (!ck_bounds.empty()) {
            throw api_error::validation(
                    "KeyConditionExpression allows only one condition for each key");
        }
        if (cond._op == parsed::primitive_condition::type::BETWEEN) {
            clustering_key ck1 = clustering_key::from_single_value(*schema,
                    get_constant_value(cond._values[1], *ck_cdef));
            clustering_key ck2 = clustering_key::from_single_value(*schema,
                    get_constant_value(cond._values[2], *ck_cdef));
            ck_bounds.push_back(query::clustering_range::make(
                    query::clustering_range::bound(ck1), query::clustering_range::bound(ck2)));
            continue;
        } else if (cond._values.size() == 1) {
            // We already verified above, that this case this can only be a
            // function call to begins_with(), with the first parameter the
            // key, the second the value reference.
            bytes raw_value = get_constant_value(
                    std::get<parsed::value::function_call>(cond._values[0]._value)._parameters[1], *ck_cdef);
            if (!ck_cdef->type->is_compatible_with(*utf8_type)) {
                // begins_with() supported on bytes and strings (both stored
                // in the database as strings) but not on numbers.
                throw api_error::validation(
                        format("KeyConditionExpression begins_with() not supported on type {}",
                                type_to_string(ck_cdef->type)));
            } else if (raw_value.empty()) {
                ck_bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            } else {
                clustering_key ck = clustering_key::from_single_value(*schema, raw_value);
                ck_bounds.push_back(get_clustering_range_for_begins_with(std::move(raw_value), ck, schema, ck_cdef->type));
            }
            continue;
        }

        // All remaining operator have one value reference parameter in index
        // !toplevel_ind. Note how toplevel_ind==1 reverses the direction of
        // an inequality.
        bytes raw_value = get_constant_value(cond._values[!toplevel_ind], *ck_cdef);
        clustering_key ck = clustering_key::from_single_value(*schema, raw_value);
        if ((cond._op == parsed::primitive_condition::type::LT && toplevel_ind == 0) ||
            (cond._op == parsed::primitive_condition::type::GT && toplevel_ind == 1)) {
            ck_bounds.push_back(query::clustering_range::make_ending_with(query::clustering_range::bound(ck, false)));
        } else if ((cond._op == parsed::primitive_condition::type::GT && toplevel_ind == 0) ||
                   (cond._op == parsed::primitive_condition::type::LT && toplevel_ind == 1)) {
            ck_bounds.push_back(query::clustering_range::make_starting_with(query::clustering_range::bound(ck, false)));
        } else if ((cond._op == parsed::primitive_condition::type::LE && toplevel_ind == 0) ||
                   (cond._op == parsed::primitive_condition::type::GE && toplevel_ind == 1)) {
            ck_bounds.push_back(query::clustering_range::make_ending_with(query::clustering_range::bound(ck)));
        } else if ((cond._op == parsed::primitive_condition::type::GE && toplevel_ind == 0) ||
                   (cond._op == parsed::primitive_condition::type::LE && toplevel_ind == 1)) {
            ck_bounds.push_back(query::clustering_range::make_starting_with(query::clustering_range::bound(ck)));
        }
    }

    if (partition_ranges.empty()) {
        throw api_error::validation(
                format("KeyConditionExpression requires a condition on partition key {}", pk_cdef.name_as_text()));
    }
    if (ck_bounds.empty()) {
        ck_bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    }
    return {std::move(partition_ranges), std::move(ck_bounds)};
}

future<executor::request_return_type> executor::query(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request) {
    _stats.api_operations.query++;
    elogger.trace("Querying {}", request);

    auto [schema, table_type] = get_table_or_view(_proxy, request);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    rjson::value* exclusive_start_key = rjson::find(request, "ExclusiveStartKey");
    db::consistency_level cl = get_read_consistency(request);
    if (table_type == table_or_view_type::gsi && cl != db::consistency_level::LOCAL_ONE) {
        return make_ready_future<request_return_type>(api_error::validation(
                "Consistent reads are not allowed on global indexes (GSI)"));
    }
    rjson::value* limit_json = rjson::find(request, "Limit");
    uint32_t limit = limit_json ? limit_json->GetUint64() : std::numeric_limits<uint32_t>::max();
    if (limit <= 0) {
        return make_ready_future<request_return_type>(api_error::validation("Limit must be greater than 0"));
    }

    const bool forward = get_bool_attribute(request, "ScanIndexForward", true);

    rjson::value* key_conditions = rjson::find(request, "KeyConditions");
    rjson::value* key_condition_expression = rjson::find(request, "KeyConditionExpression");
    std::unordered_set<std::string> used_attribute_values;
    std::unordered_set<std::string> used_attribute_names;
    if (key_conditions && key_condition_expression) {
        throw api_error::validation("Query does not allow both "
                "KeyConditions and KeyConditionExpression to be given together");
    } else if (!key_conditions && !key_condition_expression) {
        throw api_error::validation("Query must have one of "
                "KeyConditions or KeyConditionExpression");
    }

    const rjson::value* expression_attribute_names = rjson::find(request, "ExpressionAttributeNames");
    const rjson::value* expression_attribute_values = rjson::find(request, "ExpressionAttributeValues");

    // exactly one of key_conditions or key_condition_expression
    auto [partition_ranges, ck_bounds] = key_conditions
                ? calculate_bounds_conditions(schema, *key_conditions)
                : calculate_bounds_condition_expression(schema, *key_condition_expression,
                        expression_attribute_values,
                        used_attribute_values,
                        expression_attribute_names,
                        used_attribute_names);

    filter filter(request, filter::request_type::QUERY,
            used_attribute_names, used_attribute_values);

    // A query is not allowed to filter on the partition key or the sort key.
    for (const column_definition& cdef : schema->partition_key_columns()) { // just one
        if (filter.filters_on(cdef.name_as_text())) {
            return make_ready_future<request_return_type>(api_error::validation(
                    format("QueryFilter can only contain non-primary key attributes: Partition key attribute: {}", cdef.name_as_text())));
        }
    }
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        if (filter.filters_on(cdef.name_as_text())) {
            return make_ready_future<request_return_type>(api_error::validation(
                    format("QueryFilter can only contain non-primary key attributes: Sort key attribute: {}", cdef.name_as_text())));
        }
        // FIXME: this "break" can avoid listing some clustering key columns
        // we added for GSIs just because they existed in the base table -
        // but not in all cases. We still have issue #5320.
        break;
    }

    select_type select = parse_select(request, table_type);

    auto attrs_to_get = calculate_attrs_to_get(request, used_attribute_names, select);
    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "Query");
    verify_all_are_used(expression_attribute_values, used_attribute_values, "ExpressionAttributeValues", "Query");
    query::partition_slice::option_set opts;
    opts.set_if<query::partition_slice::option::reversed>(!forward);
    return do_query(_proxy, schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl,
            std::move(filter), opts, client_state, _stats.cql_stats, std::move(trace_state), std::move(permit));
}

future<executor::request_return_type> executor::list_tables(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.list_tables++;
    elogger.trace("Listing tables {}", request);

    rjson::value* exclusive_start_json = rjson::find(request, "ExclusiveStartTableName");
    rjson::value* limit_json = rjson::find(request, "Limit");
    std::string exclusive_start = exclusive_start_json ? exclusive_start_json->GetString() : "";
    int limit = limit_json ? limit_json->GetInt() : 100;
    if (limit < 1 || limit > 100) {
        return make_ready_future<request_return_type>(api_error::validation("Limit must be greater than 0 and no greater than 100"));
    }

    auto tables = _proxy.data_dictionary().get_tables(); // hold on to temporary, table_names isn't a container, it's a view
    auto table_names = tables
            | boost::adaptors::filtered([this] (data_dictionary::table t) {
                        return t.schema()->ks_name().find(KEYSPACE_NAME_PREFIX) == 0 &&
                            !t.schema()->is_view() &&
                            !cdc::is_log_for_some_table(_proxy.local_db(), t.schema()->ks_name(), t.schema()->cf_name());
                    })
            | boost::adaptors::transformed([] (data_dictionary::table t) {
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
            auto it = boost::find_if(table_names, [&exclusive_start] (const sstring& table_name) { return table_name == exclusive_start; });
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

    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(response)));
}

future<executor::request_return_type> executor::describe_endpoints(client_state& client_state, service_permit permit, rjson::value request, std::string host_header) {
    _stats.api_operations.describe_endpoints++;
    // The alternator_describe_endpoints configuration can be used to disable
    // the DescribeEndpoints operation, or set it to return a fixed string
    std::string override = _proxy.data_dictionary().get_config().alternator_describe_endpoints();
    if (!override.empty()) {
        if (override == "disabled") {
            _stats.unsupported_operations++;
            return make_ready_future<request_return_type>(api_error::unknown_operation(
                "DescribeEndpoints disabled by configuration (alternator_describe_endpoints=disabled)"));
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
        return make_ready_future<request_return_type>(api_error::validation("DescribeEndpoints needs a 'Host:' header in request"));
    }
    rjson::add(response, "Endpoints", rjson::empty_array());
    rjson::push_back(response["Endpoints"], rjson::empty_object());
    rjson::add(response["Endpoints"][0], "Address", rjson::from_string(host_header));
    rjson::add(response["Endpoints"][0], "CachePeriodInMinutes", rjson::value(1440));
    return make_ready_future<executor::request_return_type>(make_jsonable(std::move(response)));
}

static std::map<sstring, sstring> get_network_topology_options(service::storage_proxy& sp, gms::gossiper& gossiper, int rf) {
    std::map<sstring, sstring> options;
    sstring rf_str = std::to_string(rf);
    auto& topology = sp.get_token_metadata_ptr()->get_topology();
    for (const gms::inet_address& addr : gossiper.get_live_members()) {
        options.emplace(topology.get_datacenter(addr), rf_str);
    };
    return options;
}

future<executor::request_return_type> executor::describe_continuous_backups(client_state& client_state, service_permit permit, rjson::value request) {
    _stats.api_operations.describe_continuous_backups++;
    // Unlike most operations which return ResourceNotFound when the given
    // table doesn't exists, this operation returns a TableNoteFoundException.
    // So we can't use the usual get_table() wrapper and need a bit more code:
    std::string table_name = get_table_name(request);
    schema_ptr schema;
    try {
        schema = _proxy.data_dictionary().find_schema(sstring(executor::KEYSPACE_NAME_PREFIX) + table_name, table_name);
    } catch(data_dictionary::no_such_column_family&) {
        // DynamoDB returns validation error even when table does not exist
        // and the table name is invalid.
        validate_table_name(table_name);

        throw api_error::table_not_found(
                format("Table {} not found", table_name));
    }
    rjson::value desc = rjson::empty_object();
    rjson::add(desc, "ContinuousBackupsStatus", "DISABLED");
    rjson::value pitr = rjson::empty_object();
    rjson::add(pitr, "PointInTimeRecoveryStatus", "DISABLED");
    rjson::add(desc, "PointInTimeRecoveryDescription", std::move(pitr));
    rjson::value response = rjson::empty_object();
    rjson::add(response, "ContinuousBackupsDescription", std::move(desc));
    co_return make_jsonable(std::move(response));
}

// Create the metadata for the keyspace in which we put the alternator
// table if it doesn't already exist.
// Currently, we automatically configure the keyspace based on the number
// of nodes in the cluster: A cluster with 3 or more live nodes, gets RF=3.
// A smaller cluster (presumably, a test only), gets RF=1. The user may
// manually create the keyspace to override this predefined behavior.
static lw_shared_ptr<keyspace_metadata> create_keyspace_metadata(std::string_view keyspace_name, service::storage_proxy& sp, gms::gossiper& gossiper, api::timestamp_type ts, const std::map<sstring, sstring>& tags_map) {
    int endpoint_count = gossiper.num_endpoints();
    int rf = 3;
    if (endpoint_count < rf) {
        rf = 1;
        elogger.warn("Creating keyspace '{}' for Alternator with unsafe RF={} because cluster only has {} nodes.",
                keyspace_name, rf, endpoint_count);
    }
    auto opts = get_network_topology_options(sp, gossiper, rf);

    // Even if the "tablets" experimental feature is available, we currently
    // do not enable tablets by default on Alternator tables because LWT is
    // not yet fully supported with tablets.
    // The user can override the choice of whether or not to use tablets at
    // table-creation time by supplying the following tag with a numeric value
    // (setting the value to 0 means enabling tablets with automatic selection
    // of the best number of tablets).
    // Setting this tag to any non-numeric value (e.g., an empty string or the
    // word "none") will ask to disable tablets.
    // If we make this tag a permanent feature, it will get a "system:" prefix -
    // until then we give it the "experimental:" prefix to not commit to it.
    static constexpr auto INITIAL_TABLETS_TAG_KEY = "experimental:initial_tablets";
    // initial_tablets currently defaults to unset, so tablets will not be
    // used by default on new Alternator tables. Change this initialization
    // to 0 enable tablets by default, with automatic number of tablets.
    std::optional<unsigned> initial_tablets;
    if (sp.get_db().local().get_config().enable_tablets()) {
        auto it = tags_map.find(INITIAL_TABLETS_TAG_KEY);
        if (it != tags_map.end()) {
            // Tag set. If it's a valid number, use it. If not - e.g., it's
            // empty or a word like "none", disable tablets by setting
            // initial_tablets to a disengaged optional.
            try {
                initial_tablets = std::stol(tags_map.at(INITIAL_TABLETS_TAG_KEY));
            } catch(...) {
                initial_tablets = std::nullopt;
            }
        }
    }
    return keyspace_metadata::new_keyspace(keyspace_name, "org.apache.cassandra.locator.NetworkTopologyStrategy", std::move(opts), initial_tablets);
}

future<> executor::start() {
    // Currently, nothing to do on initialization. We delay the keyspace
    // creation (create_keyspace()) until a table is actually created.
    return make_ready_future<>();
}

}
