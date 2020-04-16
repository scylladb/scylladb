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

#include <regex>

#include "base64.hh"

#include "alternator/executor.hh"
#include "log.hh"
#include "schema_builder.hh"
#include "exceptions/exceptions.hh"
#include "timestamp.hh"
#include "database.hh"
#include "types/map.hh"
#include "schema.hh"
#include "query-request.hh"
#include "query-result-reader.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "cql3/type_json.hh"
#include "bytes.hh"
#include "cql3/update_parameters.hh"
#include "server.hh"
#include "service/pager/query_pagers.hh"
#include <functional>
#include "error.hh"
#include "serialization.hh"
#include "expressions.hh"
#include "conditions.hh"
#include "cql3/constants.hh"
#include <optional>
#include "utils/big_decimal.hh"
#include "seastar/json/json_elements.hh"
#include <boost/algorithm/cxx11/any_of.hpp>
#include "collection_mutation.hh"

#include <boost/range/adaptors.hpp>

logging::logger elogger("alternator-executor");

namespace alternator {

static map_type attrs_type() {
    static thread_local auto t = map_type_impl::get_instance(utf8_type, bytes_type, true);
    return t;
}

static const column_definition& attrs_column(const schema& schema) {
    const column_definition* cdef = schema.get_column_definition(bytes(executor::ATTRS_COLUMN_NAME));
    assert(cdef);
    return *cdef;
}

struct make_jsonable : public json::jsonable {
    rjson::value _value;
public:
    explicit make_jsonable(rjson::value&& value) : _value(std::move(value)) {}
    virtual std::string to_json() const override {
        return rjson::print(_value);
    }
};
struct json_string : public json::jsonable {
    std::string _value;
public:
    explicit json_string(std::string&& value) : _value(std::move(value)) {}
    virtual std::string to_json() const override {
        return _value;
    }
};

static void supplement_table_info(rjson::value& descr, const schema& schema) {
    rjson::set(descr, "CreationDateTime", rjson::value(std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count()));
    rjson::set(descr, "TableStatus", "ACTIVE");
    auto schema_id_str = schema.id().to_sstring();
    rjson::set(descr, "TableId", rjson::from_string(schema_id_str));
}

// We would have liked to support table names up to 255 bytes, like DynamoDB.
// But Scylla creates a directory whose name is the table's name plus 33
// bytes (dash and UUID), and since directory names are limited to 255 bytes,
// we need to limit table names to 222 bytes, instead of 255.
// See https://github.com/scylladb/scylla/issues/4480
static constexpr int max_table_name_length = 222;

// The DynamoDB developer guide, https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
// specifies that table names "names must be between 3 and 255 characters long
// and can contain only the following characters: a-z, A-Z, 0-9, _ (underscore), - (dash), . (dot)
// validate_table_name throws the appropriate api_error if this validation fails.
static void validate_table_name(const std::string& name) {
    if (name.length() < 3 || name.length() > max_table_name_length) {
        throw api_error("ValidationException",
                format("TableName must be at least 3 characters long and at most {} characters long", max_table_name_length));
    }
    static const std::regex valid_table_name_chars ("[a-zA-Z0-9_.-]*");
    if (!std::regex_match(name.c_str(), valid_table_name_chars)) {
        throw api_error("ValidationException",
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
static std::string view_name(const std::string& table_name, const std::string& index_name, const std::string& delim = ":") {
    static const std::regex valid_index_name_chars ("[a-zA-Z0-9_.-]*");
    if (index_name.length() < 3) {
        throw api_error("ValidationException", "IndexName must be at least 3 characters long");
    }
    if (!std::regex_match(index_name.c_str(), valid_index_name_chars)) {
        throw api_error("ValidationException",
                format("IndexName '{}' must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", index_name));
    }
    std::string ret = table_name + delim + index_name;
    if (ret.length() > max_table_name_length) {
        throw api_error("ValidationException",
                format("The total length of TableName ('{}') and IndexName ('{}') cannot exceed {} characters",
                        table_name, index_name, max_table_name_length - delim.size()));
    }
    return ret;
}

static std::string lsi_name(const std::string& table_name, const std::string& index_name) {
    return view_name(table_name, index_name, "!:");
}

/** Extract table name from a request.
 *  Most requests expect the table's name to be listed in a "TableName" field.
 *  This convenience function returns the name, with appropriate validation
 *  and api_error in case the table name is missing or not a string, or
 *  doesn't pass validate_table_name().
 */
static std::string get_table_name(const rjson::value& request) {
    const rjson::value& table_name_value = rjson::get(request, "TableName");
    if (!table_name_value.IsString()) {
        throw api_error("ValidationException",
                "Missing or non-string TableName field in request");
    }
    std::string table_name = table_name_value.GetString();
    validate_table_name(table_name);
    return table_name;
}


/** Extract table schema from a request.
 *  Many requests expect the table's name to be listed in a "TableName" field
 *  and need to look it up as an existing table. This convenience function
 *  does this, with the appropriate validation and api_error in case the table
 *  name is missing, invalid or the table doesn't exist. If everything is
 *  successful, it returns the table's schema.
 */
static schema_ptr get_table(service::storage_proxy& proxy, const rjson::value& request) {
    std::string table_name = get_table_name(request);
    try {
        return proxy.get_db().local().find_schema(executor::KEYSPACE_NAME, table_name);
    } catch(no_such_column_family&) {
        throw api_error("ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
    }
}

// get_table_or_view() is similar to to get_table(), except it returns either
// a table or a materialized view from which to read, based on the TableName
// and optional IndexName in the request. Only requests like Query and Scan
// which allow IndexName should use this function.
static schema_ptr get_table_or_view(service::storage_proxy& proxy, const rjson::value& request) {
    std::string table_name = get_table_name(request);
    const rjson::value* index_name = rjson::find(request, "IndexName");
    std::string orig_table_name;
    if (index_name) {
        if (index_name->IsString()) {
            orig_table_name = std::move(table_name);
            table_name = view_name(orig_table_name, index_name->GetString());
        } else {
            throw api_error("ValidationException",
                    format("Non-string IndexName '{}'", index_name->GetString()));
        }
    }

    // If no tables for global indexes were found, the index may be local
    if (!proxy.get_db().local().has_schema(executor::KEYSPACE_NAME, table_name)) {
        table_name = lsi_name(orig_table_name, index_name->GetString());
    }

    try {
        return proxy.get_db().local().find_schema(executor::KEYSPACE_NAME, table_name);
    } catch(no_such_column_family&) {
        if (index_name) {
            // DynamoDB returns a different error depending on whether the
            // base table doesn't exist (ResourceNotFoundException) or it
            // does exist but the index does not (ValidationException).
            if (proxy.get_db().local().has_schema(executor::KEYSPACE_NAME, orig_table_name)) {
                throw api_error("ValidationException",
                    format("Requested resource not found: Index '{}' for table '{}'", index_name->GetString(), orig_table_name));
            } else {
                throw api_error("ResourceNotFoundException",
                    format("Requested resource not found: Table: {} not found", orig_table_name));
            }
        } else {
            throw api_error("ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
        }
    }
}

// Convenience function for getting the value of a string attribute, or a
// default value if it is missing. If the attribute exists, but is not a
// string, a descriptive api_error is thrown.
static std::string get_string_attribute(const rjson::value& value, rjson::string_ref_type attribute_name, const char* default_return) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value)
        return default_return;
    if (!attribute_value->IsString()) {
        throw api_error("ValidationException", format("Expected string value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetString();
}

// Convenience function for getting the value of a boolean attribute, or a
// default value if it is missing. If the attribute exists, but is not a
// bool, a descriptive api_error is thrown.
static bool get_bool_attribute(const rjson::value& value, rjson::string_ref_type attribute_name, bool default_return) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value) {
        return default_return;
    }
    if (!attribute_value->IsBool()) {
        throw api_error("ValidationException", format("Expected boolean value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetBool();
}

// Convenience function for getting the value of an integer attribute, or
// an empty optional if it is missing. If the attribute exists, but is not
// an integer, a descriptive api_error is thrown.
static std::optional<int> get_int_attribute(const rjson::value& value, rjson::string_ref_type attribute_name) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value)
        return {};
    if (!attribute_value->IsInt()) {
        throw api_error("ValidationException", format("Expected integer value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetInt();
}

// Sets a KeySchema object inside the given JSON parent describing the key
// attributes of the the given schema as being either HASH or RANGE keys.
// Additionally, adds to a given map mappings between the key attribute
// names and their type (as a DynamoDB type string).
static void describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string,std::string>& attribute_types) {
    rjson::value key_schema = rjson::empty_array();
    for (const column_definition& cdef : schema.partition_key_columns()) {
        rjson::value key = rjson::empty_object();
        rjson::set(key, "AttributeName", rjson::from_string(cdef.name_as_text()));
        rjson::set(key, "KeyType", "HASH");
        rjson::push_back(key_schema, std::move(key));
        attribute_types[cdef.name_as_text()] = type_to_string(cdef.type);

    }
    for (const column_definition& cdef : schema.clustering_key_columns()) {
        rjson::value key = rjson::empty_object();
        rjson::set(key, "AttributeName", rjson::from_string(cdef.name_as_text()));
        rjson::set(key, "KeyType", "RANGE");
        rjson::push_back(key_schema, std::move(key));
        attribute_types[cdef.name_as_text()] = type_to_string(cdef.type);
        // FIXME: this "break" can avoid listing some clustering key columns
        // we added for GSIs just because they existed in the base table -
        // but not in all cases. We still have issue #5320. See also
        // reproducer in test_gsi_2_describe_table_schema.
        break;
    }
    rjson::set(parent, "KeySchema", std::move(key_schema));

}

future<json::json_return_type> executor::describe_table(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.describe_table++;
    rjson::value request = rjson::parse(content);
    elogger.trace("Describing table {}", request);

    schema_ptr schema = get_table(_proxy, request);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    rjson::value table_description = rjson::empty_object();
    rjson::set(table_description, "TableName", rjson::from_string(schema->cf_name()));
    // FIXME: take the tables creation time, not the current time!
    size_t creation_date_seconds = std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count();
    rjson::set(table_description, "CreationDateTime", rjson::value(creation_date_seconds));
    // FIXME: In DynamoDB the CreateTable implementation is asynchronous, and
    // the table may be in "Creating" state until creating is finished.
    // We don't currently do this in Alternator - instead CreateTable waits
    // until the table is really available. So/ DescribeTable returns either
    // ACTIVE or doesn't exist at all (and DescribeTable returns an error).
    // The other states (CREATING, UPDATING, DELETING) are not currently
    // returned.
    rjson::set(table_description, "TableStatus", "ACTIVE");
    // FIXME: Instead of hardcoding, we should take into account which mode was chosen
    // when the table was created. But, Spark jobs expect something to be returned
    // and PAY_PER_REQUEST seems closer to reality than PROVISIONED.
    rjson::set(table_description, "BillingModeSummary", rjson::empty_object());
    rjson::set(table_description["BillingModeSummary"], "BillingMode", "PAY_PER_REQUEST");
    rjson::set(table_description["BillingModeSummary"], "LastUpdateToPayPerRequestDateTime", rjson::value(creation_date_seconds));

    std::unordered_map<std::string,std::string> key_attribute_types;
    // Add base table's KeySchema and collect types for AttributeDefinitions:
    describe_key_schema(table_description, *schema, key_attribute_types);

    table& t = _proxy.get_db().local().find_column_family(schema);
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
            rjson::set(view_entry, "IndexName", rjson::from_string(index_name));
            // Add indexes's KeySchema and collect types for AttributeDefinitions:
            describe_key_schema(view_entry, *vptr, key_attribute_types);
            // Local secondary indexes are marked by an extra '!' sign occurring before the ':' delimiter
            rjson::value& index_array = (delim_it > 1 && cf_name[delim_it-1] == '!') ? lsi_array : gsi_array;
            rjson::push_back(index_array, std::move(view_entry));
        }
        if (!lsi_array.Empty()) {
            rjson::set(table_description, "LocalSecondaryIndexes", std::move(lsi_array));
        }
        if (!gsi_array.Empty()) {
            rjson::set(table_description, "GlobalSecondaryIndexes", std::move(gsi_array));
        }
    }
    // Use map built by describe_key_schema() for base and indexes to produce
    // AttributeDefinitions for all key columns:
    rjson::value attribute_definitions = rjson::empty_array();
    for (auto& type : key_attribute_types) {
        rjson::value key = rjson::empty_object();
        rjson::set(key, "AttributeName", rjson::from_string(type.first));
        rjson::set(key, "AttributeType", rjson::from_string(type.second));
        rjson::push_back(attribute_definitions, std::move(key));
    }
    rjson::set(table_description, "AttributeDefinitions", std::move(attribute_definitions));

    // FIXME: still missing some response fields (issue #5026)

    rjson::value response = rjson::empty_object();
    rjson::set(response, "Table", std::move(table_description));
    elogger.trace("returning {}", response);
    return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
}

future<json::json_return_type> executor::delete_table(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.delete_table++;
    rjson::value request = rjson::parse(content);
    elogger.trace("Deleting table {}", request);

    std::string table_name = get_table_name(request);
    tracing::add_table_name(trace_state, KEYSPACE_NAME, table_name);

    if (!_proxy.get_db().local().has_schema(KEYSPACE_NAME, table_name)) {
        throw api_error("ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
    }
    return _mm.announce_column_family_drop(KEYSPACE_NAME, table_name, false, service::migration_manager::drop_views::yes).then([table_name = std::move(table_name)] {
        // FIXME: need more attributes?
        rjson::value table_description = rjson::empty_object();
        rjson::set(table_description, "TableName", rjson::from_string(table_name));
        rjson::set(table_description, "TableStatus", "DELETING");
        rjson::value response = rjson::empty_object();
        rjson::set(response, "TableDescription", std::move(table_description));
        elogger.trace("returning {}", response);
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
    });
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
    throw api_error("ValidationException",
            format("Invalid key type '{}', can only be S, B or N.", type));
}


static void add_column(schema_builder& builder, const std::string& name, const rjson::value& attribute_definitions, column_kind kind) {
    // FIXME: Currently, the column name ATTRS_COLUMN_NAME is not allowed
    // because we use it for our untyped attribute map, and we can't have a
    // second column with the same name. We should fix this, by renaming
    // some column names which we want to reserve.
    if (name == executor::ATTRS_COLUMN_NAME) {
        throw api_error("ValidationException", format("Column name '{}' is currently reserved. FIXME.", name));
    }
    for (auto it = attribute_definitions.Begin(); it != attribute_definitions.End(); ++it) {
        const rjson::value& attribute_info = *it;
        if (attribute_info["AttributeName"].GetString() == name) {
            auto type = attribute_info["AttributeType"].GetString();
            builder.with_column(to_bytes(name), parse_key_type(type), kind);
            return;
        }
    }
    throw api_error("ValidationException",
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
        throw api_error("ValidationException", "Missing KeySchema member");
    }
    if (!key_schema->IsArray() || key_schema->Size() < 1 || key_schema->Size() > 2) {
        throw api_error("ValidationException", "KeySchema must list exactly one or two key columns");
    }
    if (!(*key_schema)[0].IsObject()) {
        throw api_error("ValidationException", "First element of KeySchema must be an object");
    }
    const rjson::value *v = rjson::find((*key_schema)[0], "KeyType");
    if (!v || !v->IsString() || v->GetString() != std::string("HASH")) {
        throw api_error("ValidationException", "First key in KeySchema must be a HASH key");
    }
    v = rjson::find((*key_schema)[0], "AttributeName");
    if (!v || !v->IsString()) {
        throw api_error("ValidationException", "First key in KeySchema must have string AttributeName");
    }
    std::string hash_key = v->GetString();
    std::string range_key;
    if (key_schema->Size() == 2) {
        if (!(*key_schema)[1].IsObject()) {
            throw api_error("ValidationException", "Second element of KeySchema must be an object");
        }
        v = rjson::find((*key_schema)[1], "KeyType");
        if (!v || !v->IsString() || v->GetString() != std::string("RANGE")) {
            throw api_error("ValidationException", "Second key in KeySchema must be a RANGE key");
        }
        v = rjson::find((*key_schema)[1], "AttributeName");
        if (!v || !v->IsString()) {
            throw api_error("ValidationException", "Second key in KeySchema must have string AttributeName");
        }
        range_key = v->GetString();
    }
    return {hash_key, range_key};
}


future<json::json_return_type> executor::create_table(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.create_table++;
    rjson::value table_info = rjson::parse(content);
    elogger.trace("Creating table {}", table_info);
    std::string table_name = get_table_name(table_info);
    const rjson::value& attribute_definitions = table_info["AttributeDefinitions"];

    tracing::add_table_name(trace_state, KEYSPACE_NAME, table_name);

    schema_builder builder(KEYSPACE_NAME, table_name);
    auto [hash_key, range_key] = parse_key_schema(table_info);
    add_column(builder, hash_key, attribute_definitions, column_kind::partition_key);
    if (!range_key.empty()) {
        add_column(builder, range_key, attribute_definitions, column_kind::clustering_key);
    }
    builder.with_column(bytes(ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);

    // Alternator does not yet support billing or throughput limitations, but
    // let's verify that BillingMode is at least legal.
    std::string billing_mode = get_string_attribute(table_info, "BillingMode", "PROVISIONED");
    if (billing_mode == "PAY_PER_REQUEST") {
        if (rjson::find(table_info, "ProvisionedThroughput")) {
            throw api_error("ValidationException", "When BillingMode=PAY_PER_REQUEST, ProvisionedThroughput cannot be specified.");
        }
    } else if (billing_mode == "PROVISIONED") {
        if (!rjson::find(table_info, "ProvisionedThroughput")) {
            throw api_error("ValidationException", "When BillingMode=PROVISIONED, ProvisionedThroughput must be specified.");
        }
    } else {
        throw api_error("ValidationException", "Unknown BillingMode={}. Must be PAY_PER_REQUEST or PROVISIONED.");
    }

    schema_ptr partial_schema = builder.build();

    // Parse GlobalSecondaryIndexes parameters before creating the base
    // table, so if we have a parse errors we can fail without creating
    // any table.
    const rjson::value* gsi = rjson::find(table_info, "GlobalSecondaryIndexes");
    std::vector<schema_builder> view_builders;
    std::vector<sstring> where_clauses;
    if (gsi) {
        if (!gsi->IsArray()) {
            throw api_error("ValidationException", "GlobalSecondaryIndexes must be an array.");
        }
        for (const rjson::value& g : gsi->GetArray()) {
            const rjson::value* index_name = rjson::find(g, "IndexName");
            if (!index_name || !index_name->IsString()) {
                throw api_error("ValidationException", "GlobalSecondaryIndexes IndexName must be a string.");
            }
            std::string vname(view_name(table_name, index_name->GetString()));
            elogger.trace("Adding GSI {}", index_name->GetString());
            // FIXME: read and handle "Projection" parameter. This will
            // require the MV code to copy just parts of the attrs map.
            schema_builder view_builder(KEYSPACE_NAME, vname);
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
            // be added to the view nontheless, as (additional) clustering
            // key(s).
            if  (hash_key != view_hash_key && hash_key != view_range_key) {
                add_column(view_builder, hash_key, attribute_definitions, column_kind::clustering_key);
            }
            if  (!range_key.empty() && range_key != view_hash_key && range_key != view_range_key) {
                add_column(view_builder, range_key, attribute_definitions, column_kind::clustering_key);
            }
            sstring where_clause = "\"" + view_hash_key + "\" IS NOT NULL";
            if (!view_range_key.empty()) {
                where_clause = where_clause + " AND \"" + view_hash_key + "\" IS NOT NULL";
            }
            where_clauses.push_back(std::move(where_clause));
            view_builders.emplace_back(std::move(view_builder));
        }
    }

    const rjson::value* lsi = rjson::find(table_info, "LocalSecondaryIndexes");
    if (lsi) {
        if (!lsi->IsArray()) {
            throw api_error("ValidationException", "LocalSecondaryIndexes must be an array.");
        }
        for (const rjson::value& l : lsi->GetArray()) {
            const rjson::value* index_name = rjson::find(l, "IndexName");
            if (!index_name || !index_name->IsString()) {
                throw api_error("ValidationException", "LocalSecondaryIndexes IndexName must be a string.");
            }
            std::string vname(lsi_name(table_name, index_name->GetString()));
            elogger.trace("Adding LSI {}", index_name->GetString());
            // FIXME: read and handle "Projection" parameter. This will
            // require the MV code to copy just parts of the attrs map.
            schema_builder view_builder(KEYSPACE_NAME, vname);
            auto [view_hash_key, view_range_key] = parse_key_schema(l);
            if (view_hash_key != hash_key) {
                throw api_error("ValidationException", "LocalSecondaryIndex hash key must match the base table hash key");
            }
            add_column(view_builder, view_hash_key, attribute_definitions, column_kind::partition_key);
            if (view_range_key.empty()) {
                throw api_error("ValidationException", "LocalSecondaryIndex must specify a sort key");
            }
            if (view_range_key == hash_key) {
                throw api_error("ValidationException", "LocalSecondaryIndex sort key cannot be the same as hash key");
              }
            if (view_range_key != range_key) {
                add_column(builder, view_range_key, attribute_definitions, column_kind::regular_column);
            }
            add_column(view_builder, view_range_key, attribute_definitions, column_kind::clustering_key);
            // Base key columns which aren't part of the index's key need to
            // be added to the view nontheless, as (additional) clustering
            // key(s).
            if  (!range_key.empty() && view_range_key != range_key) {
                add_column(view_builder, range_key, attribute_definitions, column_kind::clustering_key);
            }
            view_builder.with_column(bytes(ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);
            // Note above we don't need to add virtual columns, as all
            // base columns were copied to view. TODO: reconsider the need
            // for virtual columns when we support Projection.
            sstring where_clause = "\"" + view_hash_key + "\" IS NOT NULL";
            if (!view_range_key.empty()) {
                where_clause = where_clause + " AND \"" + view_range_key + "\" IS NOT NULL";
            }
            where_clauses.push_back(std::move(where_clause));
            view_builders.emplace_back(std::move(view_builder));
        }
    }
    if (rjson::find(table_info, "SSESpecification")) {
        throw api_error("ValidationException", "SSESpecification: configuring encryption-at-rest is not yet supported.");
    }
    if (rjson::find(table_info, "StreamSpecification")) {
        throw api_error("ValidationException", "StreamSpecification: streams (CDC) is not yet supported.");
    }
    // FIXME: we should read the Tags property, and save them somewhere.

    schema_ptr schema = builder.build();
    auto where_clause_it = where_clauses.begin();
    for (auto& view_builder : view_builders) {
        // Note below we don't need to add virtual columns, as all
        // base columns were copied to view. TODO: reconsider the need
        // for virtual columns when we support Projection.
        for (const column_definition& regular_cdef : schema->regular_columns()) {
            try {
                //TODO: add a non-throwing API for finding a column in a schema builder
                view_builder.find_column(*cql3::to_identifier(regular_cdef));
            } catch (std::invalid_argument&) {
                view_builder.with_column(regular_cdef.name(), regular_cdef.type, column_kind::regular_column);
            }
        }
        const bool include_all_columns = true;
        view_builder.with_view_info(*schema, include_all_columns, *where_clause_it);
        ++where_clause_it;
    }

    return futurize_apply([&] { return _mm.announce_new_column_family(schema, false); }).then([table_info = std::move(table_info), schema, view_builders = std::move(view_builders)] () mutable {
        return parallel_for_each(std::move(view_builders), [schema] (schema_builder builder) {
            return service::get_local_migration_manager().announce_new_view(view_ptr(builder.build()));
        }).then([table_info = std::move(table_info), schema] () mutable {
            rjson::value status = rjson::empty_object();
            supplement_table_info(table_info, *schema);
            rjson::set(status, "TableDescription", std::move(table_info));
            return make_ready_future<json::json_return_type>(make_jsonable(std::move(status)));
        });
    }).handle_exception_type([table_name = std::move(table_name)] (exceptions::already_exists_exception&) {
        return make_exception_future<json::json_return_type>(
                api_error("ResourceInUseException",
                        format("Table {} already exists", table_name)));
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
public:
    attribute_collector() : collected(attrs_type()->get_keys_type()->as_less_comparator()) { }
    void put(bytes&& name, bytes&& val, api::timestamp_type ts) {
        add(std::move(name), atomic_cell::make_live(*bytes_type, ts, std::move(val), atomic_cell::collection_member::yes));

    }
    void del(bytes&& name, api::timestamp_type ts) {
        add(std::move(name), atomic_cell::make_dead(ts, gc_clock::now()));
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

static mutation make_item_mutation(const rjson::value& item, schema_ptr schema) {
    partition_key pk = pk_from_json(item, schema);
    clustering_key ck = ck_from_json(item, schema);

    mutation m(schema, pk);
    attribute_collector attrs_collector;

    auto ts = api::new_timestamp();

    auto& row = m.partition().clustered_row(*schema, ck);

    for (auto it = item.MemberBegin(); it != item.MemberEnd(); ++it) {
        bytes column_name = to_bytes(it->name.GetString());
        const column_definition* cdef = schema->get_column_definition(column_name);
        if (!cdef) {
            bytes value = serialize_item(it->value);
            attrs_collector.put(std::move(column_name), std::move(value), ts);
        } else if (!cdef->is_primary_key()) {
            // Explicitly defined regular columns can appear as a result of creating a global secondary index
            bytes column_value = get_key_from_typed_value(it->value, *cdef, type_to_string(cdef->type));
            row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, column_value));
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
    const bool use_partition_tombstone = schema->clustering_key_size() == 0;
    if (use_partition_tombstone) {
        m.partition().apply(tombstone(ts-1, gc_clock::now()));
    } else {
        row.apply(tombstone(ts-1, gc_clock::now()));
    }
    return m;
}

// The DynamoDB API doesn't let the client control the server's timeout.
// Let's pick something reasonable:
static db::timeout_clock::time_point default_timeout() {
    return db::timeout_clock::now() + 10s;
}

static future<std::unique_ptr<rjson::value>> maybe_get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const rjson::value& item,
        bool need_read_before_write,
        alternator::stats& stats);

future<json::json_return_type> executor::put_item(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.put_item++;
    auto start_time = std::chrono::steady_clock::now();
    rjson::value update_info = rjson::parse(content);
    elogger.trace("Updating value {}", update_info);

    schema_ptr schema = get_table(_proxy, update_info);
    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    if (rjson::find(update_info, "ConditionExpression")) {
        throw api_error("ValidationException", "ConditionExpression is not yet implemented in alternator");
    }
    auto return_values = get_string_attribute(update_info, "ReturnValues", "NONE");
    if (return_values != "NONE") {
        // FIXME: Need to support also the ALL_OLD option. See issue #5053.
        throw api_error("ValidationException", format("Unsupported ReturnValues={} for PutItem operation", return_values));
    }

    const bool has_expected = update_info.HasMember("Expected");

    const rjson::value& item = update_info["Item"];

    mutation m = make_item_mutation(item, schema);

    return maybe_get_previous_item(_proxy, client_state, schema, item, has_expected, _stats).then(
            [this, schema, has_expected,  update_info = rjson::copy(update_info), m = std::move(m),
             &client_state, start_time, trace_state] (std::unique_ptr<rjson::value> previous_item) mutable {
        if (has_expected) {
            verify_expected(update_info, previous_item);
        }
        return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::LOCAL_QUORUM, default_timeout(), trace_state, empty_service_permit()).then([this, start_time] () {
            _stats.api_operations.put_item_latency.add(std::chrono::steady_clock::now() - start_time, _stats.api_operations.put_item_latency._count + 1);
            // Without special options on what to return, PutItem returns nothing.
            return make_ready_future<json::json_return_type>(json_string(""));
        });
    });

}

// After calling pk_from_json() and ck_from_json() to extract the pk and ck
// components of a key, and if that succeeded, call check_key() to further
// check that the key doesn't have any spurious components.
static void check_key(const rjson::value& key, const schema_ptr& schema) {
    if (key.MemberCount() != (schema->clustering_key_size() == 0 ? 1 : 2)) {
        throw api_error("ValidationException", "Given key attribute not in schema");
    }
}

static mutation make_delete_item_mutation(const rjson::value& key, schema_ptr schema) {
    partition_key pk = pk_from_json(key, schema);
    clustering_key ck = ck_from_json(key, schema);
    check_key(key, schema);
    mutation m(schema, pk);
    const bool use_partition_tombstone = schema->clustering_key_size() == 0;
    if (use_partition_tombstone) {
        m.partition().apply(tombstone(api::new_timestamp(), gc_clock::now()));
    } else {
        auto& row = m.partition().clustered_row(*schema, ck);
        row.apply(tombstone(api::new_timestamp(), gc_clock::now()));
    }
    return m;
}

future<json::json_return_type> executor::delete_item(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.delete_item++;
    auto start_time = std::chrono::steady_clock::now();
    rjson::value update_info = rjson::parse(content);

    schema_ptr schema = get_table(_proxy, update_info);
    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    if (rjson::find(update_info, "ConditionExpression")) {
        throw api_error("ValidationException", "ConditionExpression is not yet implemented in alternator");
    }
    auto return_values = get_string_attribute(update_info, "ReturnValues", "NONE");
    if (return_values != "NONE") {
        // FIXME: Need to support also the ALL_OLD option. See issue #5053.
        throw api_error("ValidationException", format("Unsupported ReturnValues={} for DeleteItem operation", return_values));
    }
    const bool has_expected = update_info.HasMember("Expected");

    const rjson::value& key = update_info["Key"];

    mutation m = make_delete_item_mutation(key, schema);
    check_key(key, schema);

    return maybe_get_previous_item(_proxy, client_state, schema, key, has_expected, _stats).then(
            [this, schema, has_expected,  update_info = rjson::copy(update_info), m = std::move(m),
             &client_state, start_time, trace_state] (std::unique_ptr<rjson::value> previous_item) mutable {
        if (has_expected) {
            verify_expected(update_info, previous_item);
        }
        return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::LOCAL_QUORUM, default_timeout(), trace_state, empty_service_permit()).then([this, start_time] () {
            _stats.api_operations.delete_item_latency.add(std::chrono::steady_clock::now() - start_time, _stats.api_operations.delete_item_latency._count + 1);
            // Without special options on what to return, DeleteItem returns nothing.
            return make_ready_future<json::json_return_type>(json_string(""));
        });
    });

}

static schema_ptr get_table_from_batch_request(const service::storage_proxy& proxy, const rjson::value::ConstMemberIterator& batch_request) {
    std::string table_name = batch_request->name.GetString(); // JSON keys are always strings
    validate_table_name(table_name);
    try {
        return proxy.get_db().local().find_schema(executor::KEYSPACE_NAME, table_name);
    } catch(no_such_column_family&) {
        throw api_error("ResourceNotFoundException", format("Requested resource not found: Table: {} not found", table_name));
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

future<json::json_return_type> executor::batch_write_item(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.batch_write_item++;
    rjson::value batch_info = rjson::parse(content);
    rjson::value& request_items = batch_info["RequestItems"];

    std::vector<mutation> mutations;
    mutations.reserve(request_items.MemberCount());

    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        schema_ptr schema = get_table_from_batch_request(_proxy, it);
        tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());
        std::unordered_set<primary_key, primary_key_hash, primary_key_equal> used_keys(1, primary_key_hash{schema}, primary_key_equal{schema});
        for (auto& request : it->value.GetArray()) {
            if (!request.IsObject() || request.MemberCount() != 1) {
                throw api_error("ValidationException", format("Invalid BatchWriteItem request: {}", request));
            }
            auto r = request.MemberBegin();
            const std::string r_name = r->name.GetString();
            if (r_name == "PutRequest") {
                const rjson::value& put_request = r->value;
                const rjson::value& item = put_request["Item"];
                mutations.push_back(make_item_mutation(item, schema));
                // make_item_mutation returns a mutation with a single clustering row
                auto mut_key = std::make_pair(mutations.back().key(), mutations.back().partition().clustered_rows().begin()->key());
                if (used_keys.count(mut_key) > 0) {
                    throw api_error("ValidationException", "Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else if (r_name == "DeleteRequest") {
                const rjson::value& key = (r->value)["Key"];
                mutations.push_back(make_delete_item_mutation(key, schema));
                // make_delete_item_mutation returns a mutation with a single clustering row
                auto mut_key = std::make_pair(mutations.back().key(), mutations.back().partition().clustered_rows().begin()->key());
                if (used_keys.count(mut_key) > 0) {
                    throw api_error("ValidationException", "Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else {
                throw api_error("ValidationException", format("Unknown BatchWriteItem request type: {}", r_name));
            }
        }
    }

    return _proxy.mutate(std::move(mutations), db::consistency_level::LOCAL_QUORUM, default_timeout(), trace_state, empty_service_permit()).then([] () {
        // Without special options on what to return, BatchWriteItem returns nothing,
        // unless there are UnprocessedItems - it's possible to just stop processing a batch
        // due to throttling. TODO(sarna): Consider UnprocessedItems when returning.
        rjson::value ret = rjson::empty_object();
        rjson::set(ret, "UnprocessedItems", rjson::empty_object());
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(ret)));
    });
}

// resolve_update_path() takes a path given in an update expression, replaces
// references like #name with the real name from ExpressionAttributeNames,
// and returns the fixed path. We also verify that the top-level attribute
// being modified is NOT one of the key attributes - those cannot be updated.
// If one of the above checks fails, a validation exception is thrown.
// FIXME: currently, we only support top-level attribute updates, and this
// function returns the column name;
struct allow_key_columns_tag;
using allow_key_columns = bool_class<allow_key_columns_tag>;
static std::string resolve_update_path(const parsed::path& p,
        const rjson::value& update_info,
        const schema_ptr& schema,
        std::unordered_set<std::string>& used_attribute_names,
        allow_key_columns allow_key_columns) {
    if (p.has_operators()) {
        throw api_error("ValidationException", "UpdateItem does not yet support nested updates (FIXME)");
    }
    auto column_name = p.root();
    if (column_name.size() > 0 && column_name[0] == '#') {
        const rjson::value& expression_attribute_names = rjson::get(update_info, "ExpressionAttributeNames");
        const rjson::value& value = rjson::get(expression_attribute_names, rjson::string_ref_type(column_name.c_str()));
        if (!value.IsString()) {
            throw api_error("ValidationException",
                    format("ExpressionAttributeNames missing entry '{}' required by UpdateExpression",
                            column_name));
        }
        used_attribute_names.emplace(std::move(column_name));
        column_name = value.GetString();
    }
    const column_definition* cdef = schema->get_column_definition(to_bytes(column_name));
    if (!allow_key_columns && cdef && cdef->is_primary_key()) {
        throw api_error("ValidationException",
                format("UpdateItem cannot update key column {}", column_name));
    }
    return column_name;
}

// Fail the expression if it has unused attribute names or values. This is
// how DynamoDB behaves, so we do too.
static void verify_all_are_used(const rjson::value& req, const char* field,
        const std::unordered_set<std::string>& used, const char* operation) {
    const rjson::value* attribute_names = rjson::find(req, rjson::string_ref_type(field));
    if (!attribute_names) {
        return;
    }
    for (auto it = attribute_names->MemberBegin(); it != attribute_names->MemberEnd(); ++it) {
        if (!used.count(it->name.GetString())) {
            throw api_error("ValidationException",
                format("{} has spurious '{}', not used in {}",
                       field, it->name.GetString(), operation));
        }
    }
}

// Check if a given JSON object encodes a list (i.e., it is a {"L": [...]}
// and returns a pointer to that list.
static const rjson::value* unwrap_list(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        return nullptr;
    }
    auto it = v.MemberBegin();
    if (it->name != std::string("L")) {
        return nullptr;
    }
    return &(it->value);
}

static std::string get_item_type_string(const rjson::value& v) {
    if (!v.IsObject() || v.MemberCount() != 1) {
        throw api_error("ValidationException", format("Item has invalid format: {}", v));
    }
    auto it = v.MemberBegin();
    return it->name.GetString();
}

// Take two JSON-encoded list values (remember that a list value is
// {"L": [...the actual list]}) and return the concatenation, again as
// a list value.
static rjson::value list_concatenate(const rjson::value& v1, const rjson::value& v2) {
    const rjson::value* list1 = unwrap_list(v1);
    const rjson::value* list2 = unwrap_list(v2);
    if (!list1 || !list2) {
        throw api_error("ValidationException", "UpdateExpression: list_append() given a non-list");
    }
    rjson::value cat = rjson::copy(*list1);
    for (const auto& a : list2->GetArray()) {
        rjson::push_back(cat, rjson::copy(a));
    }
    rjson::value ret = rjson::empty_object();
    rjson::set(ret, "L", std::move(cat));
    return ret;
}

// Take two JSON-encoded set values (e.g. {"SS": [...the actual set]}) and return the sum of both sets,
// again as a set value.
static rjson::value set_sum(const rjson::value& v1, const rjson::value& v2) {
    auto [set1_type, set1] = unwrap_set(v1);
    auto [set2_type, set2] = unwrap_set(v2);
    if (set1_type != set2_type) {
        throw api_error("ValidationException", format("Mismatched set types: {} and {}", set1_type, set2_type));
    }
    if (!set1 || !set2) {
        throw api_error("ValidationException", "UpdateExpression: ADD operation for sets must be given sets as arguments");
    }
    rjson::value sum = rjson::copy(*set1);
    std::set<rjson::value, rjson::single_value_comp> set1_raw;
    for (auto it = sum.Begin(); it != sum.End(); ++it) {
        set1_raw.insert(rjson::copy(*it));
    }
    for (const auto& a : set2->GetArray()) {
        if (set1_raw.count(a) == 0) {
            rjson::push_back(sum, rjson::copy(a));
        }
    }
    rjson::value ret = rjson::empty_object();
    rjson::set_with_string_name(ret, set1_type, std::move(sum));
    return ret;
}

// Take two JSON-encoded set values (e.g. {"SS": [...the actual list]}) and return the difference of s1 - s2,
// again as a set value.
static rjson::value set_diff(const rjson::value& v1, const rjson::value& v2) {
    auto [set1_type, set1] = unwrap_set(v1);
    auto [set2_type, set2] = unwrap_set(v2);
    if (set1_type != set2_type) {
        throw api_error("ValidationException", format("Mismatched set types: {} and {}", set1_type, set2_type));
    }
    if (!set1 || !set2) {
        throw api_error("ValidationException", "UpdateExpression: DELETE operation can only be performed on a set");
    }
    std::set<rjson::value, rjson::single_value_comp> set1_raw;
    for (auto it = set1->Begin(); it != set1->End(); ++it) {
        set1_raw.insert(rjson::copy(*it));
    }
    for (const auto& a : set2->GetArray()) {
        set1_raw.erase(a);
    }
    rjson::value ret = rjson::empty_object();
    rjson::set_with_string_name(ret, set1_type, rjson::empty_array());
    rjson::value& result_set = ret[set1_type];
    for (const auto& a : set1_raw) {
        rjson::push_back(result_set, rjson::copy(a));
    }
    return ret;
}

// Take two JSON-encoded numeric values ({"N": "thenumber"}) and return the
// sum, again as a JSON-encoded number.
static rjson::value number_add(const rjson::value& v1, const rjson::value& v2) {
    auto n1 = unwrap_number(v1, "UpdateExpression");
    auto n2 = unwrap_number(v2, "UpdateExpression");
    rjson::value ret = rjson::empty_object();
    std::string str_ret = std::string((n1 + n2).to_string());
    rjson::set(ret, "N", rjson::from_string(str_ret));
    return ret;
}

static rjson::value number_subtract(const rjson::value& v1, const rjson::value& v2) {
    auto n1 = unwrap_number(v1, "UpdateExpression");
    auto n2 = unwrap_number(v2, "UpdateExpression");
    rjson::value ret = rjson::empty_object();
    std::string str_ret = std::string((n1 - n2).to_string());
    rjson::set(ret, "N", rjson::from_string(str_ret));
    return ret;
}

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

// Given a parsed::value, which can refer either to a constant value from
// ExpressionAttributeValues, to the value of some attribute, or to a function
// of other values, this function calculates the resulting value.
static rjson::value calculate_value(const parsed::value& v,
        const rjson::value& expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values,
        const rjson::value& update_info,
        schema_ptr schema,
        const std::unique_ptr<rjson::value>& previous_item) {
    return std::visit(overloaded {
        [&] (const std::string& valref) -> rjson::value {
            const rjson::value& value = rjson::get(expression_attribute_values, rjson::string_ref_type(valref.c_str()));
            if (value.IsNull()) {
                throw api_error("ValidationException",
                        format("ExpressionAttributeValues missing entry '{}' required by UpdateExpression", valref));
            }
            used_attribute_values.emplace(std::move(valref));
            return rjson::copy(value);
        },
        [&] (const parsed::value::function_call& f) -> rjson::value {
            if (f._function_name == "list_append") {
                if (f._parameters.size() != 2) {
                    throw api_error("ValidationException",
                            format("UpdateExpression: list_append() accepts 2 parameters, got {}", f._parameters.size()));
                }
                rjson::value v1 = calculate_value(f._parameters[0], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                rjson::value v2 = calculate_value(f._parameters[1], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                return list_concatenate(v1, v2);
            } else if (f._function_name == "if_not_exists") {
                if (f._parameters.size() != 2) {
                    throw api_error("ValidationException",
                            format("UpdateExpression: if_not_exists() accepts 2 parameters, got {}", f._parameters.size()));
                }
                if (!std::holds_alternative<parsed::path>(f._parameters[0]._value)) {
                    throw api_error("ValidationException", "UpdateExpression: if_not_exists() must include path as its first argument");
                }
                rjson::value v1 = calculate_value(f._parameters[0], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                rjson::value v2 = calculate_value(f._parameters[1], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                return v1.IsNull() ? std::move(v2) : std::move(v1);
            } else {
                throw api_error("ValidationException",
                        format("UpdateExpression: unknown function '{}' called.", f._function_name));
            }
        },
        [&] (const parsed::path& p) -> rjson::value {
            if (!previous_item || previous_item->IsNull() || previous_item->ObjectEmpty()) {
                return rjson::null_value();
            }
            std::string update_path = resolve_update_path(p, update_info, schema, used_attribute_names, allow_key_columns::yes);
            rjson::value* previous_value = rjson::find((*previous_item)["Item"], rjson::string_ref_type(update_path.c_str()));
            return previous_value ? rjson::copy(*previous_value) : rjson::null_value();
        }
    }, v._value);
}

// Same as calculate_value() above, except takes a set_rhs, which may be
// either a single value, or v1+v2 or v1-v2.
static rjson::value calculate_value(const parsed::set_rhs& rhs,
        const rjson::value& expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values,
        const rjson::value& update_info,
        schema_ptr schema,
        const std::unique_ptr<rjson::value>& previous_item) {
    switch(rhs._op) {
    case 'v':
        return calculate_value(rhs._v1, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
    case '+': {
        rjson::value v1 = calculate_value(rhs._v1, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        rjson::value v2 = calculate_value(rhs._v2, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        return number_add(v1, v2);
    }
    case '-': {
        rjson::value v1 = calculate_value(rhs._v1, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        rjson::value v2 = calculate_value(rhs._v2, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        return number_subtract(v1, v2);
    }
    }
    // Can't happen
    return rjson::null_value();
}

static std::string resolve_projection_path(const parsed::path& p,
        const rjson::value* expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& seen_column_names) {
    if (p.has_operators()) {
        // FIXME:
        throw api_error("ValidationException", "Non-toplevel attributes in ProjectionExpression not yet implemented (FIXME)");
    }
    auto column_name = p.root();
    if (column_name.size() > 0 && column_name[0] == '#') {
        if (!expression_attribute_names) {
            throw api_error("ValidationException", "ExpressionAttributeNames parameter not found");
        }
        const rjson::value& value = rjson::get(*expression_attribute_names, rjson::string_ref_type(column_name.c_str()));
        if (!value.IsString()) {
            throw api_error("ValidationException",
                format("ExpressionAttributeNames missing entry '{}' required by ProjectionExpression", column_name));
        }
        used_attribute_names.emplace(std::move(column_name));
        column_name = value.GetString();
    }
    // FIXME: this check will need to change when we support non-toplevel attributes
    if (!seen_column_names.insert(column_name).second) {
        throw api_error("ValidationException",
                format("Invalid ProjectionExpression: two document paths overlap with each other: {} and {}.",
                        column_name, column_name));
    }
    return column_name;
}

// calculate_attrs_to_get() takes either AttributesToGet or
// ProjectionExpression parameters (having both is *not* allowed),
// and returns the list of cells we need to read.
// In our current implementation, only top-level attributes are stored
// as cells, and nested documents are stored serialized as JSON.
// So this function currently returns only the the top-level attributes
// but we also need to add, after the query, filtering to keep only
// the parts of the JSON attributes that were chosen in the paths'
// operators. Because we don't have such filtering yet (FIXME), we fail here
// if the requested paths are anything but top-level attributes.
std::unordered_set<std::string> calculate_attrs_to_get(const rjson::value& req) {
    const bool has_attributes_to_get = req.HasMember("AttributesToGet");
    const bool has_projection_expression = req.HasMember("ProjectionExpression");
    if (has_attributes_to_get && has_projection_expression) {
        throw api_error("ValidationException",
                format("GetItem does not allow both ProjectionExpression and AttributesToGet to be given together"));
    }
    if (has_attributes_to_get) {
        const rjson::value& attributes_to_get = req["AttributesToGet"];
        std::unordered_set<std::string> ret;
        for (auto it = attributes_to_get.Begin(); it != attributes_to_get.End(); ++it) {
            ret.insert(it->GetString());
        }
        return ret;
    } else if (has_projection_expression) {
        const rjson::value& projection_expression = req["ProjectionExpression"];
        const rjson::value* expression_attribute_names = rjson::find(req, "ExpressionAttributeNames");
        std::vector<parsed::path> paths_to_get;
        try {
            paths_to_get = parse_projection_expression(projection_expression.GetString());
        } catch(expressions_syntax_error& e) {
            throw api_error("ValidationException", e.what());
        }
        std::unordered_set<std::string> used_attribute_names;
        std::unordered_set<std::string> seen_column_names;
        auto ret = boost::copy_range<std::unordered_set<std::string>>(paths_to_get |
            boost::adaptors::transformed([&] (const parsed::path& p) {
                return resolve_projection_path(p, expression_attribute_names, used_attribute_names, seen_column_names);
            }));
        verify_all_are_used(req, "ExpressionAttributeNames", used_attribute_names, "ProjectionExpression");
        return ret;
    }
    // An empty set asks to read everything
    return {};
}

static std::optional<rjson::value> describe_single_item(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, foreign_ptr<lw_shared_ptr<query::result>> query_result, std::unordered_set<std::string>&& attrs_to_get) {
    rjson::value item = rjson::empty_object();

    cql3::selection::result_set_builder builder(selection, gc_clock::now(), cql_serialization_format::latest());
    query::result_view::consume(*query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, selection));

    auto result_set = builder.build();
    if (result_set->empty()) {
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return {};
    }
    // FIXME: I think this can't really be a loop, there should be exactly
    // one result after above we handled the 0 result case
    for (auto& result_row : result_set->rows()) {
        const auto& columns = selection.get_columns();
        auto column_it = columns.begin();
        for (const bytes_opt& cell : result_row) {
            std::string column_name = (*column_it)->name_as_text();
            if (cell && column_name != executor::ATTRS_COLUMN_NAME) {
                if (attrs_to_get.empty() || attrs_to_get.count(column_name) > 0) {
                    rjson::set_with_string_name(item, column_name.c_str(), rjson::empty_object());
                    rjson::value& field = item[column_name.c_str()];
                    rjson::set_with_string_name(field, type_to_string((*column_it)->type), json_key_column_value(*cell, **column_it));
                }
            } else if (cell) {
                auto deserialized = attrs_type()->deserialize(*cell, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (attrs_to_get.empty() || attrs_to_get.count(attr_name) > 0) {
                        bytes value = value_cast<bytes>(entry.second);
                        rjson::set_with_string_name(item, attr_name, deserialize_item(value));
                    }
                }
            }
            ++column_it;
        }
    }
    return item;
}

static rjson::value describe_item(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, foreign_ptr<lw_shared_ptr<query::result>> query_result, std::unordered_set<std::string>&& attrs_to_get) {
    std::optional<rjson::value> opt_item = describe_single_item(std::move(schema), slice, selection, std::move(query_result), std::move(attrs_to_get));
    if (!opt_item) {
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return rjson::empty_object();
    }
    rjson::value item_descr = rjson::empty_object();
    rjson::set(item_descr, "Item", std::move(*opt_item));
    return item_descr;
}

static bool check_needs_read_before_write(const parsed::value& v) {
    return std::visit(overloaded {
        [&] (const std::string& valref) -> bool {
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

static bool check_needs_read_before_write(const std::vector<parsed::update_expression::action>& actions) {
    return boost::algorithm::any_of(actions, [](const parsed::update_expression::action& action) {
        return std::visit(overloaded {
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
        }, action._action);
    });
}

// FIXME: Getting the previous item does not offer any synchronization guarantees nor linearizability.
// It should be overridden once we can leverage a consensus protocol.
static future<std::unique_ptr<rjson::value>> do_get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const partition_key& pk,
        const clustering_key& ck,
        alternator::stats& stats)
{
    stats.reads_before_write++;

    dht::partition_range_vector partition_ranges{dht::partition_range(dht::global_partitioner().decorate_key(*schema, pk))};
    std::vector<query::clustering_range> bounds;
    if (schema->clustering_key_size() == 0) {
        bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    } else {
        bounds.push_back(query::clustering_range::make_singular(ck));
    }

    auto regular_columns = boost::copy_range<query::column_id_vector>(
            schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto cl = db::consistency_level::LOCAL_QUORUM;

    return proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(default_timeout(), empty_service_permit(), client_state)).then(
            [schema, partition_slice = std::move(partition_slice), selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) {
        auto previous_item = describe_item(schema, partition_slice, *selection, std::move(qr.query_result), {});
        return make_ready_future<std::unique_ptr<rjson::value>>(std::make_unique<rjson::value>(std::move(previous_item)));
    });
}

static future<std::unique_ptr<rjson::value>> maybe_get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const partition_key& pk,
        const clustering_key& ck,
        bool has_update_expression,
        const parsed::update_expression& expression,
        bool has_expected,
        alternator::stats& stats)
{
    const bool needs_read_before_write = (has_update_expression && check_needs_read_before_write(expression.actions())) ||
                                         has_expected;
    if (!needs_read_before_write) {
        return make_ready_future<std::unique_ptr<rjson::value>>();
    }
    return do_get_previous_item(proxy, client_state, std::move(schema), pk, ck, stats);
}

static future<std::unique_ptr<rjson::value>> maybe_get_previous_item(
        service::storage_proxy& proxy,
        service::client_state& client_state,
        schema_ptr schema,
        const rjson::value& item,
        bool needs_read_before_write,
        alternator::stats& stats)
{
    if (!needs_read_before_write) {
        return make_ready_future<std::unique_ptr<rjson::value>>();
    }
    partition_key pk = pk_from_json(item, schema);
    clustering_key ck = ck_from_json(item, schema);
    return do_get_previous_item(proxy, client_state, std::move(schema), pk, ck, stats);
}


future<json::json_return_type> executor::update_item(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.update_item++;
    auto start_time = std::chrono::steady_clock::now();
    rjson::value update_info = rjson::parse(content);
    elogger.trace("update_item {}", update_info);
    schema_ptr schema = get_table(_proxy, update_info);
    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    if (rjson::find(update_info, "ConditionExpression")) {
        throw api_error("ValidationException", "ConditionExpression is not yet implemented in alternator");
    }
    auto return_values = get_string_attribute(update_info, "ReturnValues", "NONE");
    if (return_values != "NONE") {
        // FIXME: Need to support also ALL_OLD, UPDATED_OLD, ALL_NEW and UPDATED_NEW options. See issue #5053.
        throw api_error("ValidationException", format("Unsupported ReturnValues={} for UpdateItem operation", return_values));
    }

    if (!update_info.HasMember("Key")) {
        throw api_error("ValidationException", "UpdateItem requires a Key parameter");
    }
    const rjson::value& key = update_info["Key"];
    partition_key pk = pk_from_json(key, schema);
    clustering_key ck = ck_from_json(key, schema);
    check_key(key, schema);

    mutation m(schema, pk);
    attribute_collector attrs_collector;
    auto ts = api::new_timestamp();

    // DynamoDB forbids having both old-style AttributeUpdates or Expected
    // and new-style UpdateExpression in the same request
    const bool has_update_expression = update_info.HasMember("UpdateExpression");
    const bool has_attribute_updates = update_info.HasMember("AttributeUpdates");
    const bool has_expected = update_info.HasMember("Expected");
    if (has_update_expression && has_attribute_updates) {
        throw api_error("ValidationException",
                format("UpdateItem does not allow both AttributeUpdates and UpdateExpression to be given together"));
    }
    if (has_update_expression && has_expected) {
        throw api_error("ValidationException",
                format("UpdateItem does not allow both old-style Expected and new-style UpdateExpression to be given together"));
    }

    rjson::value attribute_updates = rjson::empty_object();
    parsed::update_expression expression;
    if (update_info.HasMember("UpdateExpression")) {
        const rjson::value& update_expression = update_info["UpdateExpression"];
        try {
            expression = parse_update_expression(update_expression.GetString());
        } catch(expressions_syntax_error& e) {
            throw api_error("ValidationException", e.what());
        }
        if (expression.empty()) {
            throw api_error("ValidationException", "Empty expression in UpdateExpression is not allowed");
        }
    } else if (has_attribute_updates) {
        attribute_updates = update_info["AttributeUpdates"];
    }

    return maybe_get_previous_item(_proxy, client_state, schema, pk, ck, has_update_expression, expression, has_expected, _stats).then(
            [this, schema, expression = std::move(expression), has_update_expression, ck = std::move(ck), has_expected,
             update_info = rjson::copy(update_info), m = std::move(m), attrs_collector = std::move(attrs_collector),
             attribute_updates = rjson::copy(attribute_updates), ts, &client_state, start_time, trace_state] (std::unique_ptr<rjson::value> previous_item) mutable {
        if (has_expected) {
            verify_expected(update_info, previous_item);
        }
        auto& row = m.partition().clustered_row(*schema, ck);
        auto do_update = [&] (bytes&& column_name, const rjson::value& json_value) {
            const column_definition* cdef = schema->get_column_definition(column_name);
            if (cdef) {
                bytes column_value = get_key_from_typed_value(json_value, *cdef, type_to_string(cdef->type));
                row.cells().apply(*cdef, atomic_cell::make_live(*cdef->type, ts, column_value));
            } else {
                attrs_collector.put(std::move(column_name), serialize_item(json_value), ts);
            }
        };
        auto do_delete = [&] (bytes&& column_name) {
            const column_definition* cdef = schema->get_column_definition(column_name);
            if (cdef) {
                row.cells().apply(*cdef, atomic_cell::make_dead(ts, gc_clock::now()));
            } else {
                attrs_collector.del(std::move(column_name), ts);
            }
        };

        if (has_update_expression) {
            std::unordered_set<std::string> seen_column_names;
            std::unordered_set<std::string> used_attribute_values;
            std::unordered_set<std::string> used_attribute_names;
            rjson::value empty_attribute_values = rjson::empty_object();
            const rjson::value* attr_values_raw = rjson::find(update_info, "ExpressionAttributeValues");
            const rjson::value& attr_values = attr_values_raw ? *attr_values_raw : empty_attribute_values;
            for (auto& action : expression.actions()) {
                std::string column_name = resolve_update_path(action._path, update_info, schema, used_attribute_names, allow_key_columns::no);
                // DynamoDB forbids multiple updates in the same expression to
                // modify overlapping document paths. Updates of one expression
                // have the same timestamp, so it's unclear which would "win".
                // FIXME: currently, without full support for document paths,
                // we only check if the paths' roots are the same.
                if (!seen_column_names.insert(column_name).second) {
                    throw api_error("ValidationException",
                            format("Invalid UpdateExpression: two document paths overlap with each other: {} and {}.",
                                    column_name, column_name));
                }
                std::visit(overloaded {
                    [&] (const parsed::update_expression::action::set& a) {
                        auto value = calculate_value(a._rhs, attr_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        do_update(to_bytes(column_name), value);
                    },
                    [&] (const parsed::update_expression::action::remove& a) {
                        do_delete(to_bytes(column_name));
                    },
                    [&] (const parsed::update_expression::action::add& a) {
                        parsed::value base;
                        parsed::value addition;
                        base.set_path(action._path);
                        addition.set_valref(a._valref);
                        rjson::value v1 = calculate_value(base, attr_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        rjson::value v2 = calculate_value(addition, attr_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        rjson::value result;
                        std::string v1_type = get_item_type_string(v1);
                        if (v1_type == "N") {
                            if (get_item_type_string(v2) != "N") {
                                throw api_error("ValidationException", format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, rjson::print(v2)));
                            }
                            result = number_add(v1, v2);
                        } else if (v1_type == "SS" || v1_type == "NS" || v1_type == "BS") {
                            if (get_item_type_string(v2) != v1_type) {
                                throw api_error("ValidationException", format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, rjson::print(v2)));
                            }
                            result = set_sum(v1, v2);
                        } else {
                            throw api_error("ValidationException", format("An operand in the update expression has an incorrect data type: {}", v1));
                        }
                        do_update(to_bytes(column_name), result);
                    },
                    [&] (const parsed::update_expression::action::del& a) {
                        parsed::value base;
                        parsed::value subset;
                        base.set_path(action._path);
                        subset.set_valref(a._valref);
                        rjson::value v1 = calculate_value(base, attr_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        rjson::value v2 = calculate_value(subset, attr_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        rjson::value result  = set_diff(v1, v2);
                        do_update(to_bytes(column_name), result);
                    }
                }, action._action);
            }
            verify_all_are_used(update_info, "ExpressionAttributeNames", used_attribute_names, "UpdateExpression");
            verify_all_are_used(update_info, "ExpressionAttributeValues", used_attribute_values, "UpdateExpression");
        }
        for (auto it = attribute_updates.MemberBegin(); it != attribute_updates.MemberEnd(); ++it) {
            // Note that it.key() is the name of the column, *it is the operation
            bytes column_name = to_bytes(it->name.GetString());
            const column_definition* cdef = schema->get_column_definition(column_name);
            if (cdef && cdef->is_primary_key()) {
                throw api_error("ValidationException",
                        format("UpdateItem cannot update key column {}", it->name.GetString()));
            }
            std::string action = (it->value)["Action"].GetString();
            if (action == "DELETE") {
                // FIXME: Currently we support only the simple case where the
                // "Value" field is missing. If it were not missing, we would
                // we need to verify the old type and/or value is same as
                // specified before deleting... We don't do this yet.
                if (it->value.HasMember("Value")) {
                    throw api_error("ValidationException",
                            format("UpdateItem DELETE with checking old value not yet supported"));
                }
                do_delete(std::move(column_name));
            } else if (action == "PUT") {
                const rjson::value& value = (it->value)["Value"];
                if (value.MemberCount() != 1) {
                    throw api_error("ValidationException",
                            format("Value field in AttributeUpdates must have just one item", it->name.GetString()));
                }
                do_update(std::move(column_name), value);
            } else {
                // FIXME: need to support "ADD" as well.
                throw api_error("ValidationException",
                    format("Unknown Action value '{}' in AttributeUpdates", action));
            }
        }
        if (!attrs_collector.empty()) {
            auto serialized_map = attrs_collector.to_mut().serialize(*attrs_type());
            row.cells().apply(attrs_column(*schema), std::move(serialized_map));
        }
        // To allow creation of an item with no attributes, we need a row marker.
        // Note that unlike Scylla, even an "update" operation needs to add a row
        // marker. TODO: a row marker isn't really needed for a DELETE operation.
        row.apply(row_marker(ts));

        elogger.trace("Applying mutation {}", m);
        return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::LOCAL_QUORUM, default_timeout(), trace_state, empty_service_permit()).then([this, start_time] () {
            // Without special options on what to return, UpdateItem returns nothing.
            _stats.api_operations.update_item_latency.add(std::chrono::steady_clock::now() - start_time, _stats.api_operations.update_item_latency._count + 1);
            return make_ready_future<json::json_return_type>(json_string(""));
        });
    });
}

// Check according to the request's "ConsistentRead" field, which consistency
// level we need to use for the read. The field can be True for strongly
// consistent reads, or False for eventually consistent reads, or if this
// field is absense, we default to eventually consistent reads.
// In Scylla, eventually-consistent reads are implemented as consistency
// level LOCAL_ONE, and strongly-consistent reads as LOCAL_QUORUM.
static db::consistency_level get_read_consistency(const rjson::value& request) {
    const rjson::value* consistent_read_value = rjson::find(request, "ConsistentRead");
    bool consistent_read = false;
    if (consistent_read_value && !consistent_read_value->IsNull()) {
        if (consistent_read_value->IsBool()) {
            consistent_read = consistent_read_value->GetBool();
        } else {
            throw api_error("ValidationException", "ConsistentRead flag must be a boolean");
        }
    }
    return consistent_read ? db::consistency_level::LOCAL_QUORUM : db::consistency_level::LOCAL_ONE;
}

future<json::json_return_type> executor::get_item(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.get_item++;
    auto start_time = std::chrono::steady_clock::now();
    rjson::value table_info = rjson::parse(content);
    elogger.trace("Getting item {}", table_info);

    schema_ptr schema = get_table(_proxy, table_info);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    rjson::value& query_key = table_info["Key"];
    db::consistency_level cl = get_read_consistency(table_info);

    partition_key pk = pk_from_json(query_key, schema);
    dht::partition_range_vector partition_ranges{dht::partition_range(dht::global_partitioner().decorate_key(*schema, pk))};

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
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto attrs_to_get = calculate_attrs_to_get(table_info);

    return _proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(default_timeout(), empty_service_permit(), client_state)).then(
            [this, schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = std::move(attrs_to_get), start_time = std::move(start_time)] (service::storage_proxy::coordinator_query_result qr) mutable {
        _stats.api_operations.get_item_latency.add(std::chrono::steady_clock::now() - start_time, _stats.api_operations.get_item_latency._count + 1);
        return make_ready_future<json::json_return_type>(make_jsonable(describe_item(schema, partition_slice, *selection, std::move(qr.query_result), std::move(attrs_to_get))));
    });
}

future<json::json_return_type> executor::batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    // FIXME: In this implementation, an unbounded batch size can cause
    // unbounded response JSON object to be buffered in memory, unbounded
    // parallelism of the requests, and unbounded amount of non-preemptable
    // work in the following loops. So we should limit the batch size, and/or
    // the response size, as DynamoDB does.
    _stats.api_operations.batch_get_item++;
    rjson::value req = rjson::parse(content);
    rjson::value& request_items = req["RequestItems"];

    // We need to validate all the parameters before starting any asynchronous
    // query, and fail the entire request on any parse error. So we parse all
    // the input into our own vector "requests".
    struct table_requests {
        schema_ptr schema;
        db::consistency_level cl;
        std::unordered_set<std::string> attrs_to_get;
        struct single_request {
            partition_key pk;
            clustering_key ck;
        };
        std::vector<single_request> requests;
    };
    std::vector<table_requests> requests;

    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        table_requests rs;
        rs.schema = get_table_from_batch_request(_proxy, it);
        tracing::add_table_name(trace_state, KEYSPACE_NAME, rs.schema->cf_name());
        rs.cl = get_read_consistency(it->value);
        rs.attrs_to_get = calculate_attrs_to_get(it->value);
        auto& keys = (it->value)["Keys"];
        for (const rjson::value& key : keys.GetArray()) {
            rs.requests.push_back({pk_from_json(key, rs.schema), ck_from_json(key, rs.schema)});
            check_key(key, rs.schema);
        }
        requests.emplace_back(std::move(rs));
    }

    // If got here, all "requests" are valid, so let's start them all
    // in parallel. The requests object are then immediately destroyed.
    std::vector<future<std::tuple<std::string, std::optional<rjson::value>>>> response_futures;
    for (const auto& rs : requests) {
        for (const auto &r : rs.requests) {
            dht::partition_range_vector partition_ranges{dht::partition_range(dht::global_partitioner().decorate_key(*rs.schema, std::move(r.pk)))};
            std::vector<query::clustering_range> bounds;
            if (rs.schema->clustering_key_size() == 0) {
                bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            } else {
                bounds.push_back(query::clustering_range::make_singular(std::move(r.ck)));
            }
            auto regular_columns = boost::copy_range<query::column_id_vector>(
                    rs.schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
            auto selection = cql3::selection::selection::wildcard(rs.schema);
            auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
            auto command = ::make_lw_shared<query::read_command>(rs.schema->id(), rs.schema->version(), partition_slice, query::max_partitions);
            future<std::tuple<std::string, std::optional<rjson::value>>> f = _proxy.query(rs.schema, std::move(command), std::move(partition_ranges), rs.cl, service::storage_proxy::coordinator_query_options(default_timeout(), empty_service_permit(), client_state)).then(
                    [schema = rs.schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = rs.attrs_to_get] (service::storage_proxy::coordinator_query_result qr) mutable {
                std::optional<rjson::value> json = describe_single_item(schema, partition_slice, *selection, std::move(qr.query_result), std::move(attrs_to_get));
                return make_ready_future<std::tuple<std::string, std::optional<rjson::value>>>(
                        std::make_tuple(schema->cf_name(), std::move(json)));
            });
            response_futures.push_back(std::move(f));
        }
    }

    // Wait for all requests to complete, and then return the response.
    // FIXME: If one of the requests failed this will fail the entire request.
    // What we should do instead is to return the failed key in the array
    // UnprocessedKeys (which the BatchGetItem API supports) and let the user
    // try again. Note that simply a missing key is *not* an error (we already
    // handled it above), but this case does include things like timeouts,
    // unavailable CL, etc.
    return when_all_succeed(response_futures.begin(), response_futures.end()).then(
            [] (std::vector<std::tuple<std::string, std::optional<rjson::value>>> responses) {
        rjson::value response = rjson::empty_object();
        rjson::set(response, "Responses", rjson::empty_object());
        for (auto& t : responses) {
            if (!response["Responses"].HasMember(std::get<0>(t).c_str())) {
                rjson::set_with_string_name(response["Responses"], std::get<0>(t), rjson::empty_array());
            }
            if (std::get<1>(t)) {
                rjson::push_back(response["Responses"][std::get<0>(t)], std::move(*std::get<1>(t)));
            }
        }
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
    });
}

class describe_items_visitor {
    typedef std::vector<const column_definition*> columns_t;
    const columns_t& _columns;
    const std::unordered_set<std::string>& _attrs_to_get;
    typename columns_t::const_iterator _column_it;
    rjson::value _item;
    rjson::value _items;

public:
    describe_items_visitor(const columns_t& columns, const std::unordered_set<std::string>& attrs_to_get)
            : _columns(columns)
            , _attrs_to_get(attrs_to_get)
            , _column_it(columns.begin())
            , _item(rjson::empty_object())
            , _items(rjson::empty_array())
    { }

    void start_row() {
        _column_it = _columns.begin();
    }

    void accept_value(const std::optional<query::result_bytes_view>& result_bytes_view) {
        if (!result_bytes_view) {
            ++_column_it;
            return;
        }
        result_bytes_view->with_linearized([this] (bytes_view bv) {
            std::string column_name = (*_column_it)->name_as_text();
            if (column_name != executor::ATTRS_COLUMN_NAME) {
                if (_attrs_to_get.empty() || _attrs_to_get.count(column_name) > 0) {
                    if (!_item.HasMember(column_name.c_str())) {
                        rjson::set_with_string_name(_item, column_name, rjson::empty_object());
                    }
                    rjson::value& field = _item[column_name.c_str()];
                    rjson::set_with_string_name(field, type_to_string((*_column_it)->type), json_key_column_value(bv, **_column_it));
                }
            } else {
                auto deserialized = attrs_type()->deserialize(bv, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (_attrs_to_get.empty() || _attrs_to_get.count(attr_name) > 0) {
                        bytes value = value_cast<bytes>(entry.second);
                        rjson::set_with_string_name(_item, attr_name, deserialize_item(value));
                    }
                }
            }
        });
        ++_column_it;
    }

    void end_row() {
        rjson::push_back(_items, std::move(_item));
        _item = rjson::empty_object();
    }

    rjson::value get_items() && {
        return std::move(_items);
    }
};

static rjson::value describe_items(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, std::unique_ptr<cql3::result_set> result_set, std::unordered_set<std::string>&& attrs_to_get) {
    describe_items_visitor visitor(selection.get_columns(), attrs_to_get);
    result_set->visit(visitor);
    rjson::value items = std::move(visitor).get_items();
    rjson::value items_descr = rjson::empty_object();
    rjson::set(items_descr, "Count", rjson::value(items.Size()));
    rjson::set(items_descr, "ScannedCount", rjson::value(items.Size())); // TODO(sarna): Update once filtering is implemented
    rjson::set(items_descr, "Items", std::move(items));
    return items_descr;
}

static rjson::value encode_paging_state(const schema& schema, const service::pager::paging_state& paging_state) {
    rjson::value last_evaluated_key = rjson::empty_object();
    std::vector<bytes> exploded_pk = paging_state.get_partition_key().explode();
    auto exploded_pk_it = exploded_pk.begin();
    for (const column_definition& cdef : schema.partition_key_columns()) {
        rjson::set_with_string_name(last_evaluated_key, cdef.name_as_text(), rjson::empty_object());
        rjson::value& key_entry = last_evaluated_key[cdef.name_as_text()];
        rjson::set_with_string_name(key_entry, type_to_string(cdef.type), rjson::parse(to_json_string(*cdef.type, *exploded_pk_it)));
        ++exploded_pk_it;
    }
    auto ck = paging_state.get_clustering_key();
    if (ck) {
        auto exploded_ck = ck->explode();
        auto exploded_ck_it = exploded_ck.begin();
        for (const column_definition& cdef : schema.clustering_key_columns()) {
            rjson::set_with_string_name(last_evaluated_key, cdef.name_as_text(), rjson::empty_object());
            rjson::value& key_entry = last_evaluated_key[cdef.name_as_text()];
            rjson::set_with_string_name(key_entry, type_to_string(cdef.type), rjson::parse(to_json_string(*cdef.type, *exploded_ck_it)));
            ++exploded_ck_it;
        }
    }
    return last_evaluated_key;
}

static future<json::json_return_type> do_query(schema_ptr schema,
        const rjson::value* exclusive_start_key,
        dht::partition_range_vector&& partition_ranges,
        std::vector<query::clustering_range>&& ck_bounds,
        std::unordered_set<std::string>&& attrs_to_get,
        uint32_t limit,
        db::consistency_level cl,
        ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions,
        service::client_state& client_state,
        cql3::cql_stats& cql_stats,
        tracing::trace_state_ptr trace_state) {
    ::shared_ptr<service::pager::paging_state> paging_state = nullptr;

    tracing::trace(trace_state, "Performing a database query");

    if (exclusive_start_key) {
        partition_key pk = pk_from_json(*exclusive_start_key, schema);
        std::optional<clustering_key> ck;
        if (schema->clustering_key_size() > 0) {
            ck = ck_from_json(*exclusive_start_key, schema);
        }
        paging_state = ::make_shared<service::pager::paging_state>(pk, ck, query::max_partitions, utils::UUID(), service::pager::paging_state::replicas_per_token_range{}, std::nullopt, 0);
    }

    auto regular_columns = boost::copy_range<query::column_id_vector>(
            schema->regular_columns() | boost::adaptors::transformed([] (const column_definition& cdef) { return cdef.id; }));
    auto selection = cql3::selection::selection::wildcard(schema);
    auto partition_slice = query::partition_slice(std::move(ck_bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto query_state_ptr = std::make_unique<service::query_state>(client_state, trace_state, empty_service_permit());

    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto query_options = std::make_unique<cql3::query_options>(cl, infinite_timeout_config, std::vector<cql3::raw_value>{});
    query_options = std::make_unique<cql3::query_options>(std::move(query_options), std::move(paging_state));
    auto p = service::pager::query_pagers::pager(schema, selection, *query_state_ptr, *query_options, command, std::move(partition_ranges), cql_stats, filtering_restrictions);

    return p->fetch_page(limit, gc_clock::now(), default_timeout()).then(
            [p, schema, cql_stats, partition_slice = std::move(partition_slice),
             selection = std::move(selection), query_state_ptr = std::move(query_state_ptr),
             attrs_to_get = std::move(attrs_to_get),
             query_options = std::move(query_options),
             filtering_restrictions = std::move(filtering_restrictions)] (std::unique_ptr<cql3::result_set> rs) mutable {
        if (!p->is_exhausted()) {
            rs->get_metadata().set_paging_state(p->state());
        }

        cql_stats.filtered_rows_matched_total += (filtering_restrictions ? rs->size() : 0);
        auto paging_state = rs->get_metadata().paging_state();
        auto items = describe_items(schema, partition_slice, *selection, std::move(rs), std::move(attrs_to_get));
        if (paging_state) {
            rjson::set(items, "LastEvaluatedKey", encode_paging_state(*schema, *paging_state));
        }
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(items)));
    });
}

// TODO(sarna):
// 1. Paging must have 1MB boundary according to the docs. IIRC we do have a replica-side reply size limit though - verify.
// 2. Filtering - by passing appropriately created restrictions to pager as a last parameter
// 3. Proper timeouts instead of gc_clock::now() and db::no_timeout
// 4. Implement parallel scanning via Segments
future<json::json_return_type> executor::scan(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.scan++;
    rjson::value request_info = rjson::parse(content);
    elogger.trace("Scanning {}", request_info);

    schema_ptr schema = get_table_or_view(_proxy, request_info);

    if (rjson::find(request_info, "FilterExpression")) {
        throw api_error("ValidationException", "FilterExpression is not yet implemented in alternator");
    }
    if (get_int_attribute(request_info, "Segment") || get_int_attribute(request_info, "TotalSegments")) {
        // FIXME: need to support parallel scan. See issue #5059.
        throw api_error("ValidationException", "Scan Segment/TotalSegments is not yet implemented in alternator");
    }

    rjson::value* exclusive_start_key = rjson::find(request_info, "ExclusiveStartKey");
    //FIXME(sarna): ScanFilter is deprecated in favor of FilterExpression
    rjson::value* scan_filter = rjson::find(request_info, "ScanFilter");
    db::consistency_level cl = get_read_consistency(request_info);
    rjson::value* limit_json = rjson::find(request_info, "Limit");
    uint32_t limit = limit_json ? limit_json->GetUint64() : query::max_partitions;
    if (limit <= 0) {
        throw api_error("ValidationException", "Limit must be greater than 0");
    }

    auto attrs_to_get = calculate_attrs_to_get(request_info);

    dht::partition_range_vector partition_ranges{dht::partition_range::make_open_ended_both_sides()};
    std::vector<query::clustering_range> ck_bounds{query::clustering_range::make_open_ended_both_sides()};

    ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions;
    if (scan_filter) {
        const cql3::query_options query_options = cql3::query_options(cl, infinite_timeout_config, std::vector<cql3::raw_value>{});
        filtering_restrictions = get_filtering_restrictions(schema, attrs_column(*schema), *scan_filter);
        partition_ranges = filtering_restrictions->get_partition_key_ranges(query_options);
        ck_bounds = filtering_restrictions->get_clustering_bounds(query_options);
    }
    return do_query(schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl, std::move(filtering_restrictions), client_state, _stats.cql_stats, trace_state);
}

static dht::partition_range calculate_pk_bound(schema_ptr schema, const column_definition& pk_cdef, comparison_operator_type op, const rjson::value& attrs) {
    if (attrs.Size() != 1) {
        throw api_error("ValidationException", format("Only a single attribute is allowed for a hash key restriction: {}", attrs));
    }
    bytes raw_value = pk_cdef.type->from_string(attrs[0][type_to_string(pk_cdef.type)].GetString());
    partition_key pk = partition_key::from_singular(*schema, pk_cdef.type->deserialize(raw_value));
    auto decorated_key = dht::global_partitioner().decorate_key(*schema, pk);
    if (op != comparison_operator_type::EQ) {
        throw api_error("ValidationException", format("Hash key {} can only be restricted with equality operator (EQ)"));
    }
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

static query::clustering_range calculate_ck_bound(schema_ptr schema, const column_definition& ck_cdef, comparison_operator_type op, const rjson::value& attrs) {
    const size_t expected_attrs_size = (op == comparison_operator_type::BETWEEN) ? 2 : 1;
    if (attrs.Size() != expected_attrs_size) {
        throw api_error("ValidationException", format("{} arguments expected for a sort key restriction: {}", expected_attrs_size, attrs));
    }
    bytes raw_value = ck_cdef.type->from_string(attrs[0][type_to_string(ck_cdef.type)].GetString());
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
        bytes raw_upper_limit = ck_cdef.type->from_string(attrs[1][type_to_string(ck_cdef.type)].GetString());
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
            throw api_error("ValidationException", format("BEGINS_WITH operator cannot be applied to type {}", type_to_string(ck_cdef.type)));
        }
        std::string raw_upper_limit_str = attrs[0][type_to_string(ck_cdef.type)].GetString();
        bytes raw_upper_limit = ck_cdef.type->from_string(raw_upper_limit_str);
        return get_clustering_range_for_begins_with(std::move(raw_upper_limit), ck, schema, ck_cdef.type);
    }
    default:
        throw api_error("ValidationException", format("Unknown primary key bound passed: {}", int(op)));
    }
}

// Calculates primary key bounds from the list of conditions
static std::pair<dht::partition_range_vector, std::vector<query::clustering_range>>
calculate_bounds(schema_ptr schema, const rjson::value& conditions) {
    dht::partition_range_vector partition_ranges;
    std::vector<query::clustering_range> ck_bounds;

    for (auto it = conditions.MemberBegin(); it != conditions.MemberEnd(); ++it) {
        std::string key = it->name.GetString();
        const rjson::value& condition = it->value;

        const rjson::value& comp_definition = rjson::get(condition, "ComparisonOperator");
        const rjson::value& attr_list = rjson::get(condition, "AttributeValueList");

        auto op = get_comparison_operator(comp_definition);

        const column_definition& pk_cdef = schema->partition_key_columns().front();
        const column_definition* ck_cdef = schema->clustering_key_size() > 0 ? &schema->clustering_key_columns().front() : nullptr;
        if (sstring(key) == pk_cdef.name_as_text()) {
            if (!partition_ranges.empty()) {
                throw api_error("ValidationException", "Currently only a single restriction per key is allowed");
            }
            partition_ranges.push_back(calculate_pk_bound(schema, pk_cdef, op, attr_list));
        }
        if (ck_cdef && sstring(key) == ck_cdef->name_as_text()) {
            if (!ck_bounds.empty()) {
                throw api_error("ValidationException", "Currently only a single restriction per key is allowed");
            }
            ck_bounds.push_back(calculate_ck_bound(schema, *ck_cdef, op, attr_list));
        }
    }

    // Validate that a query's conditions must be on the hash key, and
    // optionally also on the sort key if it exists.
    if (partition_ranges.empty()) {
        throw api_error("ValidationException", format("Query missing condition on hash key '{}'", schema->partition_key_columns().front().name_as_text()));
    }
    if (schema->clustering_key_size() == 0) {
        if (conditions.MemberCount() != 1) {
            throw api_error("ValidationException", "Only one condition allowed in table with only hash key");
        }
    } else {
        if (conditions.MemberCount() == 2 && ck_bounds.empty()) {
            throw api_error("ValidationException", format("Query missing condition on sort key '{}'", schema->clustering_key_columns().front().name_as_text()));
        } else if (conditions.MemberCount() > 2) {
            throw api_error("ValidationException", "Only one or two conditions allowed in table with hash key and sort key");
        }
    }

    if (ck_bounds.empty()) {
        ck_bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    }

    return {std::move(partition_ranges), std::move(ck_bounds)};
}

future<json::json_return_type> executor::query(client_state& client_state, tracing::trace_state_ptr trace_state, std::string content) {
    _stats.api_operations.query++;
    rjson::value request_info = rjson::parse(content);
    elogger.trace("Querying {}", request_info);

    schema_ptr schema = get_table_or_view(_proxy, request_info);

    tracing::add_table_name(trace_state, schema->ks_name(), schema->cf_name());

    rjson::value* exclusive_start_key = rjson::find(request_info, "ExclusiveStartKey");
    db::consistency_level cl = get_read_consistency(request_info);
    rjson::value* limit_json = rjson::find(request_info, "Limit");
    uint32_t limit = limit_json ? limit_json->GetUint64() : query::max_partitions;
    if (limit <= 0) {
        throw api_error("ValidationException", "Limit must be greater than 0");
    }

    if (rjson::find(request_info, "KeyConditionExpression")) {
        throw api_error("ValidationException", "KeyConditionExpression is not yet implemented in alternator");
    }
    if (rjson::find(request_info, "FilterExpression")) {
        throw api_error("ValidationException", "FilterExpression is not yet implemented in alternator");
    }
    bool forward = get_bool_attribute(request_info, "ScanIndexForward", true);
    if (!forward) {
        // FIXME: need to support the !forward (i.e., reverse sort order) case. See issue #5153.
        throw api_error("ValidationException", "ScanIndexForward=false is not yet implemented in alternator");
    }

    //FIXME(sarna): KeyConditions are deprecated in favor of KeyConditionExpression
    rjson::value& conditions = rjson::get(request_info, "KeyConditions");
    //FIXME(sarna): QueryFilter is deprecated in favor of FilterExpression
    rjson::value* query_filter = rjson::find(request_info, "QueryFilter");

    auto [partition_ranges, ck_bounds] = calculate_bounds(schema, conditions);

    auto attrs_to_get = calculate_attrs_to_get(request_info);

    ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions;
    if (query_filter) {
        filtering_restrictions = get_filtering_restrictions(schema, attrs_column(*schema), *query_filter);
        auto pk_defs = filtering_restrictions->get_partition_key_restrictions()->get_column_defs();
        auto ck_defs = filtering_restrictions->get_clustering_columns_restrictions()->get_column_defs();
        if (!pk_defs.empty()) {
            throw api_error("ValidationException", format("QueryFilter can only contain non-primary key attributes: Primary key attribute: {}", pk_defs.front()->name_as_text()));
        }
        if (!ck_defs.empty()) {
            throw api_error("ValidationException", format("QueryFilter can only contain non-primary key attributes: Primary key attribute: {}", ck_defs.front()->name_as_text()));
        }
    }
    return do_query(schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl, std::move(filtering_restrictions), client_state, _stats.cql_stats, std::move(trace_state));
}

static void validate_limit(int limit) {
    if (limit < 1 || limit > 100) {
        throw api_error("ValidationException", "Limit must be greater than 0 and no greater than 100");
    }
}

future<json::json_return_type> executor::list_tables(client_state& client_state, std::string content) {
    _stats.api_operations.list_tables++;
    rjson::value table_info = rjson::parse(content);
    elogger.trace("Listing tables {}", table_info);

    rjson::value* exclusive_start_json = rjson::find(table_info, "ExclusiveStartTableName");
    rjson::value* limit_json = rjson::find(table_info, "Limit");
    std::string exclusive_start = exclusive_start_json ? exclusive_start_json->GetString() : "";
    int limit = limit_json ? limit_json->GetInt() : 100;
    validate_limit(limit);

    auto table_names = _proxy.get_db().local().get_column_families()
            | boost::adaptors::map_values
            | boost::adaptors::filtered([] (const lw_shared_ptr<table>& t) {
                        return t->schema()->ks_name() == KEYSPACE_NAME && !t->schema()->is_view();
                    })
            | boost::adaptors::transformed([] (const lw_shared_ptr<table>& t) {
                        return t->schema()->cf_name();
                    });

    rjson::value response = rjson::empty_object();
    rjson::set(response, "TableNames", rjson::empty_array());
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
        rjson::set(response, "LastEvaluatedTableName", rjson::copy(last_table_name));
    }

    return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
}

future<json::json_return_type> executor::describe_endpoints(client_state& client_state, std::string content, std::string host_header) {
    _stats.api_operations.describe_endpoints++;
    rjson::value response = rjson::empty_object();
    // Without having any configuration parameter to say otherwise, we tell
    // the user to return to the same endpoint they used to reach us. The only
    // way we can know this is through the "Host:" header in the request,
    // which typically exists (and in fact is mandatory in HTTP 1.1).
    // A "Host:" header includes both host name and port, exactly what we need
    // to return.
    if (host_header.empty()) {
        throw api_error("ValidationException", "DescribeEndpoints needs a 'Host:' header in request");
    }
    rjson::set(response, "Endpoints", rjson::empty_array());
    rjson::push_back(response["Endpoints"], rjson::empty_object());
    rjson::set(response["Endpoints"][0], "Address", rjson::from_string(host_header));
    rjson::set(response["Endpoints"][0], "CachePeriodInMinutes", rjson::value(1440));
    return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
}

// Create the keyspace in which we put all Alternator tables, if it doesn't
// already exist.
// Currently, we automatically configure the keyspace based on the number
// of nodes in the cluster: A cluster with 3 or more live nodes, gets RF=3.
// A smaller cluster (presumably, a test only), gets RF=1. The user may
// manually create the keyspace to override this predefined behavior.
future<> executor::maybe_create_keyspace() {
    if (_proxy.get_db().local().has_keyspace(KEYSPACE_NAME)) {
        return make_ready_future<>();
    }
    return gms::get_up_endpoint_count().then([this] (int up_endpoint_count) {
        int rf = 3;
        if (up_endpoint_count < rf) {
            rf = 1;
            elogger.warn("Creating keyspace '{}' for Alternator with unsafe RF={} because cluster only has {} live nodes.",
                    KEYSPACE_NAME, rf, up_endpoint_count);
        } else {
            elogger.info("Creating keyspace '{}' for Alternator with RF={}.", KEYSPACE_NAME, rf);
        }
        auto ksm = keyspace_metadata::new_keyspace(KEYSPACE_NAME, "org.apache.cassandra.locator.SimpleStrategy", {{"replication_factor", std::to_string(rf)}}, true);
        try {
            return _mm.announce_new_keyspace(ksm, api::min_timestamp, false);
        } catch (exceptions::already_exists_exception& ignored) {
            return make_ready_future<>();
        } catch (...) {
            return make_exception_future(std::current_exception());
        }
    });
}

static tracing::trace_state_ptr create_tracing_session() {
    tracing::trace_state_props_set props;
    props.set<tracing::trace_state_props::full_tracing>();
    return tracing::tracing::get_local_tracing_instance().create_session(tracing::trace_type::QUERY, props);
}

tracing::trace_state_ptr executor::maybe_trace_query(client_state& client_state, sstring_view op, sstring_view query) {
    tracing::trace_state_ptr trace_state;
    if (tracing::tracing::get_local_tracing_instance().trace_next_query()) {
        trace_state = create_tracing_session();
        tracing::add_query(trace_state, query);
        tracing::begin(trace_state, format("Alternator {}", op), client_state.get_client_address());
    }
    return trace_state;
}

future<> executor::start() {
    // Currently, nothing to do on initialization. We delay the keyspace
    // creation (maybe_create_keyspace()) until a table is actually created.
    return make_ready_future<>();
}

}
