/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <regex>

#include "base64.hh"

#include "alternator/executor.hh"
#include "log.hh"
#include "json.hh"
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
#include <boost/algorithm/cxx11/any_of.hpp>

#include <boost/range/adaptors.hpp>

static logging::logger elogger("alternator-executor");

namespace alternator {

static map_type attrs_type() {
    static auto t = map_type_impl::get_instance(utf8_type, bytes_type, true);
    return t;
}

static const column_definition& attrs_column(const schema& schema) {
    const column_definition* cdef = schema.get_column_definition(bytes(executor::ATTRS_COLUMN_NAME));
    assert(cdef);
    return *cdef;
}

struct make_jsonable : public json::jsonable {
    Json::Value _value;
public:
    explicit make_jsonable(Json::Value&& value) : _value(std::move(value)) {}
    virtual std::string to_json() const override {
        return _value.toStyledString();
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

static void supplement_table_info(Json::Value& descr, const schema& schema) {
    descr["CreationDateTime"] = std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count();
    descr["TableStatus"] = "ACTIVE";
    descr["TableId"] = schema.id().to_sstring().c_str();
}

// The DynamoDB developer guide, https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
// specifies that table names "names must be between 3 and 255 characters long
// and can contain only the following characters: a-z, A-Z, 0-9, _ (underscore), - (dash), . (dot)
// validate_table_name throws the appropriate api_error if this validation fails.
static void validate_table_name(const std::string& name) {
    // FIXME: Although we would like to support table names up to 255
    // bytes, like DynamoDB, Scylla creates a directory whose name is the
    // table's name plus 33 bytes (dash and UUID), and since directory names
    // are limited to 255 bytes, we need to limit table names to 222 bytes,
    // instead of 255. See https://github.com/scylladb/scylla/issues/4480
    if (name.length() < 3 || name.length() > 222) {
        throw api_error("ValidationException",
                "TableName must be at least 3 characters long and at most 222 characters long");
    }
    static const std::regex valid_table_name_chars ("[a-zA-Z0-9_.-]*");
    if (!std::regex_match(name.c_str(), valid_table_name_chars)) {
        throw api_error("ValidationException",
                "TableName must satisfy regular expression pattern: [a-zA-Z0-9_.-]+");
    }
}

/** Extract table name from a request.
 *  Most requests expect the table's name to be listed in a "TableName" field.
 *  This convenience function returns the name, with appropriate validation
 *  and api_error in case the table name is missing or not a string, or
 *  doesn't pass validate_table_name().
 */
static std::string get_table_name(const Json::Value& request) {
    Json::Value table_name_value = request.get("TableName", Json::nullValue);
    if (!table_name_value.isString()) {
        throw api_error("ValidationException",
                "Missing or non-string TableName field in request");
    }
    std::string table_name = table_name_value.asString();
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
static schema_ptr get_table(service::storage_proxy& proxy, const Json::Value& request) {
    std::string table_name = get_table_name(request);
    try {
        return proxy.get_db().local().find_schema(executor::KEYSPACE_NAME, table_name);
    } catch(no_such_column_family&) {
        throw api_error("ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
    }
}

future<json::json_return_type> executor::describe_table(std::string content) {
    _stats.api_operations.describe_table++;
    Json::Value request = json::to_json_value(content);
    elogger.trace("Describing table {}", request.toStyledString());

    schema_ptr schema = get_table(_proxy, request);

    Json::Value table_description(Json::objectValue);
    table_description["TableName"] = schema->cf_name().c_str();
    // FIXME: take the tables creation time, not the current time!
    table_description["CreationDateTime"] = std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count();
    // FIXME: In DynamoDB the CreateTable implementation is asynchronous, and
    // the table may be in "Creating" state until creating is finished.
    // We don't currently do this in Alternator - instead CreateTable waits
    // until the table is really available. So/ DescribeTable returns either
    // ACTIVE or doesn't exist at all (and DescribeTable returns an error).
    // The other states (CREATING, UPDATING, DELETING) are not currently
    // returned.
    table_description["TableStatus"] = "ACTIVE";
    // FIXME: more attributes! Check https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TableDescription.html#DDB-Type-TableDescription-TableStatus but also run a test to see what DyanmoDB really fills
    // maybe for TableId or TableArn use  schema.id().to_sstring().c_str();
    // Of course, the whole schema is missing!
    Json::Value response(Json::objectValue);
    response["Table"] = std::move(table_description);
    elogger.trace("returning {}", response.toStyledString());
    return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
}

future<json::json_return_type> executor::delete_table(std::string content) {
    _stats.api_operations.delete_table++;
    Json::Value request = json::to_json_value(content);
    elogger.trace("Deleting table {}", request.toStyledString());

    std::string table_name = get_table_name(request);
    if (!_proxy.get_db().local().has_schema(KEYSPACE_NAME, table_name)) {
        throw api_error("ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
    }

    return _mm.announce_column_family_drop(KEYSPACE_NAME, table_name).then([table_name = std::move(table_name)] {
        // FIXME: need more attributes?
        Json::Value table_description(Json::objectValue);
        table_description["TableName"] = table_name.c_str();
        table_description["TableStatus"] = "DELETING";
        Json::Value response(Json::objectValue);
        response["TableDescription"] = std::move(table_description);
        elogger.trace("returning {}", response.toStyledString());
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


static void add_column(schema_builder& builder, const std::string& name, const Json::Value& attribute_definitions, column_kind kind) {
    for (const Json::Value& attribute_info : attribute_definitions) {
        if (attribute_info["AttributeName"].asString() == name) {
            auto type = attribute_info["AttributeType"].asString();
            builder.with_column(to_bytes(name), parse_key_type(type), kind);
            return;
        }
    }
    throw api_error("ValidationException",
            format("KeySchema key '{}' missing in AttributeDefinitions", name));
}

future<json::json_return_type> executor::create_table(std::string content) {
    _stats.api_operations.create_table++;
    Json::Value table_info = json::to_json_value(content);
    elogger.trace("Creating table {}", table_info.toStyledString());

    std::string table_name = get_table_name(table_info);
    const Json::Value& key_schema = table_info["KeySchema"];
    const Json::Value& attribute_definitions = table_info["AttributeDefinitions"];

    schema_builder builder(KEYSPACE_NAME, table_name);

    // DynamoDB requires that KeySchema includes up to two elements, the
    // first must be a HASH, the optional second one can be a RANGE.
    // These key names must also be present in the attributes_definitions.
    if (!key_schema.isArray() || key_schema.size() < 1 || key_schema.size() > 2) {
        throw api_error("ValidationException",
                "KeySchema must list exactly one or two key columns");
    }
    if (key_schema[0]["KeyType"] != "HASH") {
        throw api_error("ValidationException",
                "First key in KeySchema must be a HASH key");
    }
    add_column(builder, key_schema[0]["AttributeName"].asString(), attribute_definitions, column_kind::partition_key);
    if (key_schema.size() == 2) {
        if (key_schema[1]["KeyType"] != "RANGE") {
            throw api_error("ValidationException",
                    "Second key in KeySchema must be a RANGE key");
        }
        add_column(builder, key_schema[1]["AttributeName"].asString(), attribute_definitions, column_kind::clustering_key);
    }
    builder.with_column(bytes(ATTRS_COLUMN_NAME), attrs_type(), column_kind::regular_column);

    const Json::Value& gsi = table_info["GlobalSecondaryIndexes"];
    if (!gsi.isNull()) {
        throw api_error("ValidationException", "FIXME: GSI not yet supported.");
    }

    schema_ptr schema = builder.build();

    return _mm.announce_new_column_family(schema, false).then([table_info = std::move(table_info), schema] () mutable {
        Json::Value status(Json::objectValue);
        supplement_table_info(table_info, *schema);
        status["TableDescription"] = std::move(table_info);
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(status)));
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
    collection_type_impl::mutation to_mut() {
        collection_type_impl::mutation ret;
        for (auto&& e : collected) {
            ret.cells.emplace_back(e.first, std::move(e.second));
        }
        return ret;
    }
};

static mutation make_item_mutation(const Json::Value& item, schema_ptr schema) {
    partition_key pk = pk_from_json(item, schema);
    clustering_key ck = ck_from_json(item, schema);

    mutation m(schema, pk);
    attribute_collector attrs_collector;

    auto ts = api::new_timestamp();

    for (auto it = item.begin(); it != item.end(); ++it) {
        bytes column_name = to_bytes(it.key().asString());
        const column_definition* cdef = schema->get_column_definition(column_name);
        if (!cdef || !cdef->is_primary_key()) {
            bytes value = serialize_item(*it);
            attrs_collector.put(std::move(column_name), std::move(value), ts);
        }
    }

    auto serialized_map = attrs_type()->serialize_mutation_form(attrs_collector.to_mut());
    auto& row = m.partition().clustered_row(*schema, ck);
    row.cells().apply(attrs_column(*schema), std::move(serialized_map));
    // To allow creation of an item with no attributes, we need a row marker.
    row.apply(row_marker(ts));
    // PutItem is supposed to completely replace the old item, so we need to
    // also have a tombstone removing old cells. We can't use the timestamp
    // ts, because when data and tombstone tie on timestamp, the tombstone
    // wins. So we need to use ts-1. Note that we use this trick also in
    // Scylla proper, to implement the operation to replace an entire
    // collection ("UPDATE .. SET x = ..") - see
    // cql3::update_parameters::make_tombstone_just_before().
    row.apply(tombstone(ts-1, gc_clock::now()));
    return m;
}

future<json::json_return_type> executor::put_item(std::string content) {
    _stats.api_operations.put_item++;
    Json::Value update_info = json::to_json_value(content);
    elogger.trace("Updating value {}", update_info.toStyledString());

    schema_ptr schema = get_table(_proxy, update_info);
    const Json::Value& item = update_info["Item"];

    mutation m = make_item_mutation(item, schema);

    return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::LOCAL_QUORUM, db::no_timeout, tracing::trace_state_ptr(), empty_service_permit()).then([] () {
        // Without special options on what to return, PutItem returns nothing.
        return make_ready_future<json::json_return_type>(json_string(""));
    });

}

// After calling pk_from_json() and ck_from_json() to extract the pk and ck
// components of a key, and if that succeeded, call check_key() to further
// check that the key doesn't have any spurious components.
static void check_key(const Json::Value& key, const schema_ptr& schema) {
    if (key.size() != (schema->clustering_key_size() == 0 ? 1 : 2)) {
        throw api_error("ValidationException", "Given key attribute not in schema");
    }
}

static mutation make_delete_item_mutation(const Json::Value& key, schema_ptr schema) {
    partition_key pk = pk_from_json(key, schema);
    clustering_key ck = ck_from_json(key, schema);
    check_key(key, schema);
    mutation m(schema, pk);
    auto& row = m.partition().clustered_row(*schema, ck);
    row.apply(tombstone(api::new_timestamp(), gc_clock::now()));
    return m;
}

future<json::json_return_type> executor::delete_item(std::string content) {
    _stats.api_operations.delete_item++;
    Json::Value update_info = json::to_json_value(content);

    schema_ptr schema = get_table(_proxy, update_info);
    const Json::Value& key = update_info["Key"];

    mutation m = make_delete_item_mutation(key, schema);
    check_key(key, schema);

    return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::LOCAL_QUORUM, db::no_timeout, tracing::trace_state_ptr(), empty_service_permit()).then([] () {
        // Without special options on what to return, DeleteItem returns nothing.
        return make_ready_future<json::json_return_type>(json_string(""));
    });

}

static schema_ptr get_table_from_batch_request(const service::storage_proxy& proxy, const Json::Value::const_iterator& batch_request) {
    std::string table_name = batch_request.key().asString(); // JSON keys are always strings
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

future<json::json_return_type> executor::batch_write_item(std::string content) {
    _stats.api_operations.batch_write_item++;
    Json::Value batch_info = json::to_json_value(content);
    Json::Value& request_items = batch_info["RequestItems"];

    std::vector<mutation> mutations;
    mutations.reserve(request_items.size());

    for (auto it = request_items.begin(); it != request_items.end(); ++it) {
        schema_ptr schema = get_table_from_batch_request(_proxy, it);
        std::unordered_set<primary_key, primary_key_hash, primary_key_equal> used_keys(1, primary_key_hash{schema}, primary_key_equal{schema});
        for (auto&& request : *it) {
            if (!request.isObject() || request.size() != 1) {
                throw api_error("ValidationException", format("Invalid BatchWriteItem request: {}", request.toStyledString()));
            }
            auto r = request.begin();
            if (r.key() == "PutRequest") {
                const Json::Value& put_request = *r;
                const Json::Value& item = put_request["Item"];
                mutations.push_back(make_item_mutation(item, schema));
                // make_item_mutation returns a mutation with a single clustering row
                auto mut_key = std::make_pair(mutations.back().key(), mutations.back().partition().clustered_rows().begin()->key());
                if (used_keys.count(mut_key) > 0) {
                    throw api_error("ValidationException", "Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else if (r.key() == "DeleteRequest") {
                const Json::Value& key = (*r)["Key"];
                mutations.push_back(make_delete_item_mutation(key, schema));
                // make_delete_item_mutation returns a mutation with a single clustering row
                auto mut_key = std::make_pair(mutations.back().key(), mutations.back().partition().clustered_rows().begin()->key());
                if (used_keys.count(mut_key) > 0) {
                    throw api_error("ValidationException", "Provided list of item keys contains duplicates");
                }
                used_keys.insert(std::move(mut_key));
            } else {
                throw api_error("ValidationException", format("Unknown BatchWriteItem request type: {}", r.key()));
            }
        }
    }

    return _proxy.mutate(std::move(mutations), db::consistency_level::LOCAL_QUORUM, db::no_timeout, tracing::trace_state_ptr(), empty_service_permit()).then([] () {
        // Without special options on what to return, BatchWriteItem returns nothing,
        // unless there are UnprocessedItems - it's possible to just stop processing a batch
        // due to throttling. TODO(sarna): Consider UnprocessedItems when returning.
        Json::Value ret;
        ret["UnprocessedItems"] = Json::Value(Json::objectValue);
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
        const Json::Value& update_info,
        const schema_ptr& schema,
        std::unordered_set<std::string>& used_attribute_names,
        allow_key_columns allow_key_columns) {
    if (p.has_operators()) {
        throw api_error("ValidationException", "UpdateItem does not yet support nested updates (FIXME)");
    }
    auto column_name = p.root();
    if (column_name.size() > 0 && column_name[0] == '#') {
        const Json::Value& value = update_info["ExpressionAttributeNames"].get(column_name, Json::nullValue);
        if (!value.isString()) {
            throw api_error("ValidationException",
                    format("ExpressionAttributeNames missing entry '{}' required by UpdateExpression",
                            column_name));
        }
        used_attribute_names.emplace(std::move(column_name));
        column_name = value.asString();
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
static void verify_all_are_used(const Json::Value& req, const char* field,
        const std::unordered_set<std::string>& used, const char* operation) {
    auto& attribute_names = req[field];
    for (auto it = attribute_names.begin(); it != attribute_names.end(); ++it) {
        if (!used.count(it.key().asString())) {
            throw api_error("ValidationException",
                format("{} has spurious '{}', not used in {}",
                       field, it.key().asString(), operation));
        }
    }
}

// Check if a given JSON object encodes a list (i.e., it is a {"L": [...]}
// and returns a pointer to that list.
static const Json::Value* unwrap_list(const Json::Value& v) {
    if (!v.isObject() || v.size() != 1) {
        return nullptr;
    }
    auto it = v.begin();
    if (it.key() != "L") {
        return nullptr;
    }
    return &(*it);
}

static std::string get_item_type_string(const Json::Value& v) {
    if (!v.isObject() || v.size() != 1) {
        throw api_error("ValidationException", format("Item has invalid format: {}", v.toStyledString()));
    }
    auto it = v.begin();
    return it.key().asString();
}

// Check if a given JSON object encodes a set (i.e., it is a {"SS": [...]}, or "NS", "BS"
// and returns set's type and a pointer to that set. If the object does not encode a set,
// returned value is {"", nullptr}
static const std::pair<std::string, const Json::Value*> unwrap_set(const Json::Value& v) {
    if (!v.isObject() || v.size() != 1) {
        return {"", nullptr};
    }
    auto it = v.begin();
    const auto& it_key = it.key().asString();
    if (it_key != "SS" && it_key != "BS" && it_key != "NS") {
        return {"", nullptr};
    }
    return std::make_pair(it_key, &(*it));
}

// Take two JSON-encoded list values (remember that a list value is
// {"L": [...the actual list]}) and return the concatenation, again as
// a list value.
static Json::Value list_concatenate(const Json::Value& v1, const Json::Value& v2) {
    const Json::Value* list1 = unwrap_list(v1);
    const Json::Value* list2 = unwrap_list(v2);
    if (!list1 || !list2) {
        throw api_error("ValidationException", "UpdateExpression: list_append() given a non-list");
    }
    Json::Value cat = *list1;
    for (const auto& a : *list2) {
        cat.append(a);
    }
    Json::Value ret(Json::objectValue);
    ret["L"] = std::move(cat);
    return ret;
}

// Take two JSON-encoded set values (e.g. {"SS": [...the actual set]}) and return the sum of both sets,
// again as a set value.
static Json::Value set_sum(const Json::Value& v1, const Json::Value& v2) {
    auto [set1_type, set1] = unwrap_set(v1);
    auto [set2_type, set2] = unwrap_set(v2);
    if (set1_type != set2_type) {
        throw api_error("ValidationException", format("Mismatched set types: {} and {}", set1_type, set2_type));
    }
    if (!set1 || !set2) {
        throw api_error("ValidationException", "UpdateExpression: set_sum() given a non-set");
    }
    Json::Value sum = *set1;
    std::set<Json::Value> set1_raw(sum.begin(), sum.end());
    for (const auto& a : *set2) {
        if (set1_raw.count(a) == 0) {
            sum.append(a);
        }
    }
    Json::Value ret(Json::objectValue);
    ret[set1_type] = std::move(sum);
    return ret;
}

// Take two JSON-encoded set values (e.g. {"SS": [...the actual list]}) and return the difference of s1 - s2,
// again as a set value.
static Json::Value set_diff(const Json::Value& v1, const Json::Value& v2) {
    auto [set1_type, set1] = unwrap_set(v1);
    auto [set2_type, set2] = unwrap_set(v2);
    if (set1_type != set2_type) {
        throw api_error("ValidationException", format("Mismatched set types: {} and {}", set1_type, set2_type));
    }
    if (!set1 || !set2) {
        throw api_error("ValidationException", "UpdateExpression: DELETE operation can only be performed on a set");
    }
    std::set<Json::Value> set1_raw(set1->begin(), set1->end());
    for (const auto& a : *set2) {
        set1_raw.erase(a);
    }
    Json::Value ret(Json::objectValue);
    Json::Value& result_set = ret[set1_type];
    for (const auto& a : set1_raw) {
        result_set.append(a);
    }
    return ret;
}

// Check if a given JSON object encodes a number (i.e., it is a {"N": [...]}
// and returns an object representing it.
static big_decimal unwrap_number(const Json::Value& v) {
    if (!v.isObject() || v.size() != 1) {
        throw api_error("ValidationException", "UpdateExpression: invalid number object");
    }
    auto it = v.begin();
    if (it.key() != "N") {
        throw api_error("ValidationException",
                format("UpdateExpression: expected number, found type '{}'", it.key()));
    }
    if (!it->isString()) {
        throw api_error("ValidationException", "UpdateExpression: improperly formatted number constant");
    }
    // FIXME: to not lose precision, we really need to do something like:
    // return decimal_type->from_string(it->asString());
    return big_decimal(it->asString());
}

// Take two JSON-encoded numeric values ({"N": "thenumber"}) and return the
// sum, again as a JSON-encoded number.
static Json::Value number_add(const Json::Value& v1, const Json::Value& v2) {
    auto n1 = unwrap_number(v1);
    auto n2 = unwrap_number(v2);
    Json::Value ret(Json::objectValue);
    ret["N"] = std::string((n1 + n2).to_string());
    return ret;
}

static Json::Value number_subtract(const Json::Value& v1, const Json::Value& v2) {
    auto n1 = unwrap_number(v1);
    auto n2 = unwrap_number(v2);
    Json::Value ret(Json::objectValue);
    ret["N"] = std::string((n1 - n2).to_string());
    return ret;
}

template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

// Given a parsed::value, which can refer either to a constant value from
// ExpressionAttributeValues, to the value of some attribute, or to a function
// of other values, this function calculates the resulting value.
static Json::Value calculate_value(const parsed::value& v,
        const Json::Value& expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values,
        const Json::Value& update_info,
        schema_ptr schema,
        const std::unique_ptr<Json::Value>& previous_item) {
    return std::visit(overloaded {
        [&] (const std::string& valref) -> Json::Value {
            const Json::Value& value = expression_attribute_values.get(valref, Json::nullValue);
            if (value.isNull()) {
                throw api_error("ValidationException",
                        format("ExpressionAttributeValues missing entry '{}' required by UpdateExpression", valref));
            }
            used_attribute_values.emplace(std::move(valref));
            return value;
        },
        [&] (const parsed::value::function_call& f) -> Json::Value {
            if (f._function_name == "list_append") {
                if (f._parameters.size() != 2) {
                    throw api_error("ValidationException",
                            format("UpdateExpression: list_append() accepts 2 parameters, got {}", f._parameters.size()));
                }
                Json::Value v1 = calculate_value(f._parameters[0], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                Json::Value v2 = calculate_value(f._parameters[1], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                return list_concatenate(v1, v2);
            } else if (f._function_name == "if_not_exists") {
                if (f._parameters.size() != 2) {
                    throw api_error("ValidationException",
                            format("UpdateExpression: if_not_exists() accepts 2 parameters, got {}", f._parameters.size()));
                }
                if (!std::holds_alternative<parsed::path>(f._parameters[0]._value)) {
                    throw api_error("ValidationException", "UpdateExpression: if_not_exists() must include path as its first argument");
                }
                Json::Value v1 = calculate_value(f._parameters[0], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                Json::Value v2 = calculate_value(f._parameters[1], expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                return v1.isNull() ? v2 : v1;
            } else {
                throw api_error("ValidationException",
                        format("UpdateExpression: unknown function '{}' called.", f._function_name));
            }
        },
        [&] (const parsed::path& p) -> Json::Value {
            if (!previous_item) {
                return Json::nullValue;
            }
            std::string update_path = resolve_update_path(p, update_info, schema, used_attribute_names, allow_key_columns::yes);
            return (*previous_item)["Item"].get(update_path, Json::nullValue);
        }
    }, v._value);
}

// Same as calculate_value() above, except takes a set_rhs, which may be
// either a single value, or v1+v2 or v1-v2.
static Json::Value calculate_value(const parsed::set_rhs& rhs,
        const Json::Value& expression_attribute_values,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& used_attribute_values,
        const Json::Value& update_info,
        schema_ptr schema,
        const std::unique_ptr<Json::Value>& previous_item) {
    switch(rhs._op) {
    case 'v':
        return calculate_value(rhs._v1, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
    case '+': {
        Json::Value v1 = calculate_value(rhs._v1, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        Json::Value v2 = calculate_value(rhs._v2, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        return number_add(v1, v2);
    }
    case '-': {
        Json::Value v1 = calculate_value(rhs._v1, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        Json::Value v2 = calculate_value(rhs._v2, expression_attribute_values, used_attribute_names, used_attribute_values, update_info, schema, previous_item);
        return number_subtract(v1, v2);
    }
    }
    // Can't happen
    return Json::Value::null;
}

static std::string resolve_projection_path(const parsed::path& p,
        const Json::Value& expression_attribute_names,
        std::unordered_set<std::string>& used_attribute_names,
        std::unordered_set<std::string>& seen_column_names) {
    if (p.has_operators()) {
        // FIXME:
        throw api_error("ValidationException", "Non-toplevel attributes in ProjectionExpression not yet implemented (FIXME)");
    }
    auto column_name = p.root();
    if (column_name.size() > 0 && column_name[0] == '#') {
        const Json::Value& value = expression_attribute_names.get(column_name, Json::nullValue);
        if (!value.isString()) {
            throw api_error("ValidationException",
                format("ExpressionAttributeNames missing entry '{}' required by ProjectionExpression", column_name));
        }
        used_attribute_names.emplace(std::move(column_name));
        column_name = value.asString();
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
std::unordered_set<std::string> calculate_attrs_to_get(const Json::Value& req) {
    const Json::Value& attributes_to_get = req["AttributesToGet"];
    const Json::Value& projection_expression = req["ProjectionExpression"];
    if (attributes_to_get && projection_expression) {
        throw api_error("ValidationException",
                format("GetItem does not allow both ProjectionExpression and AttributesToGet to be given together"));
    }
    if (attributes_to_get) {
        return boost::copy_range<std::unordered_set<std::string>>(attributes_to_get |
                boost::adaptors::transformed(std::bind(&Json::Value::asString, std::placeholders::_1)));
    } else if (projection_expression){
        const Json::Value& expression_attribute_names = req["ExpressionAttributeNames"];
        std::vector<parsed::path> paths_to_get;
        try {
            paths_to_get = parse_projection_expression(projection_expression.asString());
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

static std::optional<Json::Value> describe_single_item(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, foreign_ptr<lw_shared_ptr<query::result>> query_result, std::unordered_set<std::string>&& attrs_to_get) {
    Json::Value item(Json::objectValue);

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
                    Json::Value& field = item[column_name.c_str()];
                    field[type_to_string((*column_it)->type)] = json_key_column_value(*cell, **column_it);
                }
            } else if (cell) {
                auto deserialized = attrs_type()->deserialize(*cell, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (attrs_to_get.empty() || attrs_to_get.count(attr_name) > 0) {
                        bytes value = value_cast<bytes>(entry.second);
                        item[attr_name] = deserialize_item(value);
                    }
                }
            }
            ++column_it;
        }
    }
    return item;
}

static Json::Value describe_item(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, foreign_ptr<lw_shared_ptr<query::result>> query_result, std::unordered_set<std::string>&& attrs_to_get) {
    std::optional<Json::Value> opt_item = describe_single_item(std::move(schema), slice, selection, std::move(query_result), std::move(attrs_to_get));
    if (!opt_item) {
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return Json::objectValue;
    }
    Json::Value item_descr(Json::objectValue);
    item_descr["Item"] = *opt_item;
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
static future<std::unique_ptr<Json::Value>> maybe_get_previous_item(service::storage_proxy& proxy, schema_ptr schema, const partition_key& pk, const clustering_key& ck,
        const Json::Value& update_expression, const parsed::update_expression& expression) {
    const bool needs_read_before_write = update_expression && check_needs_read_before_write(expression.actions());
    if (!needs_read_before_write) {
        return make_ready_future<std::unique_ptr<Json::Value>>();
    }

    dht::partition_range_vector partition_ranges{dht::partition_range(dht::global_partitioner().decorate_key(*schema, pk))};
    std::vector<query::clustering_range> bounds;
    if (schema->clustering_key_size() == 0) {
        bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    } else {
        bounds.push_back(query::clustering_range::make_singular(ck));
    }

    query::column_id_vector regular_columns{attrs_column(*schema).id};
    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto cl = db::consistency_level::LOCAL_QUORUM;

    //TODO(sarna): RBW stats
    return proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(db::no_timeout, empty_service_permit())).then(
            [schema, partition_slice = std::move(partition_slice), selection = std::move(selection)] (service::storage_proxy::coordinator_query_result qr) {
        auto previous_item = describe_item(schema, partition_slice, *selection, std::move(qr.query_result), {});
        return make_ready_future<std::unique_ptr<Json::Value>>(std::make_unique<Json::Value>(std::move(previous_item)));
    });
}

future<json::json_return_type> executor::update_item(std::string content) {
    _stats.api_operations.update_item++;
    Json::Value update_info = json::to_json_value(content);
    elogger.trace("update_item {}", update_info.toStyledString());
    schema_ptr schema = get_table(_proxy, update_info);
    // FIXME: handle missing Key.
    const Json::Value& key = update_info["Key"];
    partition_key pk = pk_from_json(key, schema);
    clustering_key ck = ck_from_json(key, schema);
    check_key(key, schema);

    mutation m(schema, pk);
    attribute_collector attrs_collector;
    auto ts = api::new_timestamp();

    const Json::Value& attribute_updates = update_info["AttributeUpdates"];
    const Json::Value& update_expression = update_info["UpdateExpression"];

    // DynamoDB forbids having both old-style AttributeUpdates and new-style
    // UpdateExpression in the same request
    if (attribute_updates && update_expression) {
        throw api_error("ValidationException",
                format("UpdateItem does not allow both AttributeUpdates and UpdateExpression to be given together"));
    }

    parsed::update_expression expression;
    if (update_expression) {
        try {
            expression = parse_update_expression(update_expression.asString());
        } catch(expressions_syntax_error& e) {
            throw api_error("ValidationException", e.what());
        }
        if (expression.empty()) {
            throw api_error("ValidationException", "Empty expression in UpdateExpression is not allowed");
        }
    }

    return maybe_get_previous_item(_proxy, schema, pk, ck, update_expression, expression).then(
            [this, schema, expression = std::move(expression), update_expression = std::move(update_expression), ck = std::move(ck),
             update_info = std::move(update_info), m = std::move(m), attrs_collector = std::move(attrs_collector), attribute_updates = std::move(attribute_updates), ts] (std::unique_ptr<Json::Value> previous_item) mutable {
        if (update_expression) {
            std::unordered_set<std::string> seen_column_names;
            std::unordered_set<std::string> used_attribute_values;
            std::unordered_set<std::string> used_attribute_names;
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
                        auto value = calculate_value(a._rhs, update_info["ExpressionAttributeValues"], used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        attrs_collector.put(to_bytes(column_name), serialize_item(value), ts);
                    },
                    [&] (const parsed::update_expression::action::remove& a) {
                        attrs_collector.del(to_bytes(column_name), ts);
                    },
                    [&] (const parsed::update_expression::action::add& a) {
                        parsed::value base;
                        parsed::value addition;
                        base.set_path(action._path);
                        addition.set_valref(a._valref);
                        Json::Value v1 = calculate_value(base, update_info["ExpressionAttributeValues"], used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        Json::Value v2 = calculate_value(addition, update_info["ExpressionAttributeValues"], used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        Json::Value result;
                        std::string v1_type = get_item_type_string(v1);
                        if (v1_type == "N") {
                            if (get_item_type_string(v2) != "N") {
                                throw api_error("ValidationException", format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, v2));
                            }
                            result = number_add(v1, v2);
                        } else if (v1_type == "SS" || v1_type == "NS" || v1_type == "BS") {
                            if (get_item_type_string(v2) != v1_type) {
                                throw api_error("ValidationException", format("Incorrect operand type for operator or function. Expected {}: {}", v1_type, v2));
                            }
                            result = set_sum(v1, v2);
                        } else {
                            throw api_error("ValidationException", format("An operand in the update expression has an incorrect data type: {}", v1));
                        }
                        attrs_collector.put(to_bytes(column_name), serialize_item(result), ts);
                    },
                    [&] (const parsed::update_expression::action::del& a) {
                        parsed::value base;
                        parsed::value subset;
                        base.set_path(action._path);
                        subset.set_valref(a._valref);
                        Json::Value v1 = calculate_value(base, update_info["ExpressionAttributeValues"], used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        Json::Value v2 = calculate_value(subset, update_info["ExpressionAttributeValues"], used_attribute_names, used_attribute_values, update_info, schema, previous_item);
                        Json::Value result  = set_diff(v1, v2);
                        attrs_collector.put(to_bytes(column_name), serialize_item(result), ts);
                    }
                }, action._action);
            }
            verify_all_are_used(update_info, "ExpressionAttributeNames", used_attribute_names, "UpdateExpression");
            verify_all_are_used(update_info, "ExpressionAttributeValues", used_attribute_values, "UpdateExpression");
        }

        for (auto it = attribute_updates.begin(); it != attribute_updates.end(); ++it) {
            // Note that it.key() is the name of the column, *it is the operation
            bytes column_name = to_bytes(it.key().asString());
            const column_definition* cdef = schema->get_column_definition(column_name);
            if (cdef && cdef->is_primary_key()) {
                throw api_error("ValidationException",
                        format("UpdateItem cannot update key column {}", it.key().asString()));
            }
            std::string action = (*it)["Action"].asString();
            if (action == "DELETE") {
                // FIXME: Currently we support only the simple case where the
                // "Value" field is missing. If it were not missing, we would
                // we need to verify the old type and/or value is same as
                // specified before deleting... We don't do this yet.
                if (!it->get("Value", "").asString().empty()) {
                    throw api_error("ValidationException",
                            format("UpdateItem DELETE with checking old value not yet supported"));
                }
                attrs_collector.del(std::move(column_name), ts);
            } else if (action == "PUT") {
                const Json::Value& value = (*it)["Value"];
                if (value.size() != 1) {
                    throw api_error("ValidationException",
                            format("Value field in AttributeUpdates must have just one item", it.key().asString()));
                }
                attrs_collector.put(std::move(column_name), serialize_item(value), ts);
            } else {
                // FIXME: need to support "ADD" as well.
                throw api_error("ValidationException",
                    format("Unknown Action value '{}' in AttributeUpdates", action));
            }
        }
        auto serialized_map = attrs_type()->serialize_mutation_form(attrs_collector.to_mut());
        auto& row = m.partition().clustered_row(*schema, ck);
        row.cells().apply(attrs_column(*schema), std::move(serialized_map));
        // To allow creation of an item with no attributes, we need a row marker.
        // Note that unlike Scylla, even an "update" operation needs to add a row
        // marker. TODO: a row marker isn't really needed for a DELETE operation.
        row.apply(row_marker(ts));

        elogger.trace("Applying mutation {}", m);
        return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::LOCAL_QUORUM, db::no_timeout, tracing::trace_state_ptr(), empty_service_permit()).then([] () {
            // Without special options on what to return, UpdateItem returns nothing.
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
static db::consistency_level get_read_consistency(const Json::Value& request) {
    Json::Value consistent_read_value = request.get("ConsistentRead", Json::nullValue);
    bool consistent_read = false;
    if (!consistent_read_value.isNull()) {
        if (consistent_read_value.isBool()) {
            consistent_read = consistent_read_value.asBool();
        } else {
            throw api_error("ValidationException", "ConsistentRead flag must be a boolean");
        }
    }
    return consistent_read ? db::consistency_level::LOCAL_QUORUM : db::consistency_level::LOCAL_ONE;
}

future<json::json_return_type> executor::get_item(std::string content) {
    _stats.api_operations.get_item++;
    Json::Value table_info = json::to_json_value(content);
    elogger.trace("Getting item {}", table_info.toStyledString());

    schema_ptr schema = get_table(_proxy, table_info);

    Json::Value query_key = table_info["Key"];
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
    query::column_id_vector regular_columns{attrs_column(*schema).id};

    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto attrs_to_get = calculate_attrs_to_get(table_info);

    return _proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(db::no_timeout, empty_service_permit())).then(
            [schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = std::move(attrs_to_get)] (service::storage_proxy::coordinator_query_result qr) mutable {
        return make_ready_future<json::json_return_type>(make_jsonable(describe_item(schema, partition_slice, *selection, std::move(qr.query_result), std::move(attrs_to_get))));
    });
}

future<json::json_return_type> executor::batch_get_item(std::string content) {
    // FIXME: In this implementation, an unbounded batch size can cause
    // unbounded response JSON object to be buffered in memory, unbounded
    // parallelism of the requests, and unbounded amount of non-preemptable
    // work in the following loops. So we should limit the batch size, and/or
    // the response size, as DynamoDB does.
    _stats.api_operations.batch_get_item++;
    Json::Value req = json::to_json_value(content);
    Json::Value& request_items = req["RequestItems"];

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

    for (auto it = request_items.begin(); it != request_items.end(); ++it) {
        table_requests rs;
        rs.schema = get_table_from_batch_request(_proxy, it);
        rs.cl = get_read_consistency(*it);
        rs.attrs_to_get = calculate_attrs_to_get(*it);
        for (const Json::Value& key : (*it)["Keys"]) {
            rs.requests.push_back({pk_from_json(key, rs.schema), ck_from_json(key, rs.schema)});
            check_key(key, rs.schema);
        }
        requests.emplace_back(std::move(rs));
    }

    // If got here, all "requests" are valid, so let's start them all
    // in parallel. The requests object are then immediately destroyed.
    std::vector<future<std::tuple<std::string, std::unique_ptr<Json::Value>>>> response_futures;
    for (const auto& rs : requests) {
        for (const auto &r : rs.requests) {
            dht::partition_range_vector partition_ranges{dht::partition_range(dht::global_partitioner().decorate_key(*rs.schema, std::move(r.pk)))};
            std::vector<query::clustering_range> bounds;
            if (rs.schema->clustering_key_size() == 0) {
                bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            } else {
                bounds.push_back(query::clustering_range::make_singular(std::move(r.ck)));
            }
            query::column_id_vector regular_columns{attrs_column(*rs.schema).id};
            auto selection = cql3::selection::selection::wildcard(rs.schema);
            auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
            auto command = ::make_lw_shared<query::read_command>(rs.schema->id(), rs.schema->version(), partition_slice, query::max_partitions);
            future<std::tuple<std::string, std::unique_ptr<Json::Value>>> f = _proxy.query(rs.schema, std::move(command), std::move(partition_ranges), rs.cl, service::storage_proxy::coordinator_query_options(db::no_timeout, empty_service_permit())).then(
                    [schema = rs.schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = rs.attrs_to_get] (service::storage_proxy::coordinator_query_result qr) mutable {
                std::optional<Json::Value> json = describe_single_item(schema, partition_slice, *selection, std::move(qr.query_result), std::move(attrs_to_get));
                // Unfortunately, future<std::optional<Json::Value>> doesn't
                // work because Json::Value doesn't have a non-throwing move
                // constructor. So we need to convert it to a std::unique_ptr.
                std::unique_ptr<Json::Value> v;
                if (json) {
                    v = std::make_unique<Json::Value>(std::move(*json));
                }
                return make_ready_future<std::tuple<std::string, std::unique_ptr<Json::Value>>>(
                        std::make_tuple(schema->cf_name(), std::move(v)));
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
            [] (std::vector<std::tuple<std::string, std::unique_ptr<Json::Value>>> responses) {
        Json::Value response = Json::objectValue;
        response["Responses"] = Json::objectValue;
        for (const auto& t : responses) {
            if (std::get<1>(t)) {
                response["Responses"][std::get<0>(t)].append(*std::get<1>(t));
            } else {
                // Even if all items requested for a particular table are
                // missing, we still need to return an empty array.
                Json::Value& x = response["Responses"][std::get<0>(t)];
                if (x.isNull()) {
                    x = Json::arrayValue;
                }
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
    Json::Value _item;
    Json::Value _items;

public:
    describe_items_visitor(const columns_t& columns, const std::unordered_set<std::string>& attrs_to_get)
            : _columns(columns)
            , _attrs_to_get(attrs_to_get)
            , _column_it(columns.begin())
            , _item(Json::objectValue)
            , _items(Json::arrayValue)
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
                    Json::Value& field = _item[column_name.c_str()];
                    field[type_to_string((*_column_it)->type)] = json_key_column_value(bv, **_column_it);
                }
            } else {
                auto deserialized = attrs_type()->deserialize(bv, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (_attrs_to_get.empty() || _attrs_to_get.count(attr_name) > 0) {
                        bytes value = value_cast<bytes>(entry.second);
                        _item[attr_name] = deserialize_item(value);
                    }
                }
            }
        });
        ++_column_it;
    }

    void end_row() {
        _items.append(std::move(_item));
        _item = Json::objectValue;
    }

    Json::Value get_items() && {
        return std::move(_items);
    }
};

static Json::Value describe_items(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, std::unique_ptr<cql3::result_set> result_set, std::unordered_set<std::string>&& attrs_to_get) {
    describe_items_visitor visitor(selection.get_columns(), attrs_to_get);
    result_set->visit(visitor);
    Json::Value items = std::move(visitor).get_items();
    Json::Value items_descr(Json::objectValue);
    items_descr["Count"] = items.size();
    items_descr["ScannedCount"] = items.size(); // TODO(sarna): Update once filtering is implemented
    items_descr["Items"] = std::move(items);
    return items_descr;
}

static Json::Value encode_paging_state(const schema& schema, const service::pager::paging_state& paging_state) {
    Json::Value last_evaluated_key(Json::objectValue);
    std::vector<bytes> exploded_pk = paging_state.get_partition_key().explode();
    auto exploded_pk_it = exploded_pk.begin();
    for (const column_definition& cdef : schema.partition_key_columns()) {
        Json::Value& key_entry = last_evaluated_key[cdef.name_as_text()];
        key_entry[type_to_string(cdef.type)] = json::to_json_value(cdef.type->to_json_string(*exploded_pk_it));
        ++exploded_pk_it;
    }
    auto ck = paging_state.get_clustering_key();
    if (ck) {
        auto exploded_ck = ck->explode();
        auto exploded_ck_it = exploded_ck.begin();
        for (const column_definition& cdef : schema.clustering_key_columns()) {
            Json::Value& key_entry = last_evaluated_key[cdef.name_as_text()];
            key_entry[type_to_string(cdef.type)] = json::to_json_value(cdef.type->to_json_string(*exploded_ck_it));
            ++exploded_ck_it;
        }
    }
    return last_evaluated_key;
}

static future<json::json_return_type> do_query(schema_ptr schema,
        const Json::Value& exclusive_start_key,
        dht::partition_range_vector&& partition_ranges,
        std::vector<query::clustering_range>&& ck_bounds,
        std::unordered_set<std::string>&& attrs_to_get,
        uint32_t limit,
        db::consistency_level cl,
        ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions) {
    ::shared_ptr<service::pager::paging_state> paging_state = nullptr;

    if (!exclusive_start_key.empty()) {
        partition_key pk = pk_from_json(exclusive_start_key, schema);
        std::optional<clustering_key> ck;
        if (schema->clustering_key_size() > 0) {
            ck = ck_from_json(exclusive_start_key, schema);
        }
        paging_state = ::make_shared<service::pager::paging_state>(pk, ck, query::max_partitions, utils::UUID(), service::pager::paging_state::replicas_per_token_range{}, std::nullopt, 0);
    }

    query::column_id_vector regular_columns{attrs_column(*schema).id};
    auto selection = cql3::selection::selection::wildcard(schema);
    auto partition_slice = query::partition_slice(std::move(ck_bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    //FIXME(sarna): This context will need to be provided once we start gathering statistics, authenticating, etc. Right now these are just stubs.
    static thread_local cql3::cql_stats dummy_stats;
    static thread_local service::client_state dummy_client_state{service::client_state::internal_tag()};
    static thread_local service::query_state dummy_query_state(dummy_client_state, empty_service_permit());

    command->slice.options.set<query::partition_slice::option::allow_short_read>();
    auto query_options = std::make_unique<cql3::query_options>(cl, infinite_timeout_config, std::vector<cql3::raw_value>{});
    query_options = std::make_unique<cql3::query_options>(std::move(query_options), std::move(paging_state));
    auto p = service::pager::query_pagers::pager(schema, selection, dummy_query_state, *query_options, command, std::move(partition_ranges), dummy_stats, filtering_restrictions);

    return p->fetch_page(limit, gc_clock::now(), db::no_timeout).then(
            [p, schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = std::move(attrs_to_get), query_options = std::move(query_options)](std::unique_ptr<cql3::result_set> rs) mutable {
        if (!p->is_exhausted()) {
            rs->get_metadata().set_paging_state(p->state());
        }

        auto paging_state = rs->get_metadata().paging_state();
        auto items = describe_items(schema, partition_slice, *selection, std::move(rs), std::move(attrs_to_get));
        if (paging_state) {
            items["LastEvaluatedKey"] = encode_paging_state(*schema, *paging_state);
        }
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(items)));
    });
}

// TODO(sarna):
// 1. Paging must have 1MB boundary according to the docs. IIRC we do have a replica-side reply size limit though - verify.
// 2. Filtering - by passing appropriately created restrictions to pager as a last parameter
// 3. Proper timeouts instead of gc_clock::now() and db::no_timeout
// 4. Implement parallel scanning via Segments
future<json::json_return_type> executor::scan(std::string content) {
    _stats.api_operations.scan++;
    Json::Value request_info = json::to_json_value(content);
    elogger.trace("Scanning {}", request_info.toStyledString());

    schema_ptr schema = get_table(_proxy, request_info);

    Json::Value exclusive_start_key = request_info["ExclusiveStartKey"];
    //FIXME(sarna): ScanFilter is deprecated in favor of FilterExpression
    const Json::Value& scan_filter = request_info["ScanFilter"];
    db::consistency_level cl = get_read_consistency(request_info);
    uint32_t limit = request_info.get("Limit", query::max_partitions).asUInt();
    if (limit <= 0) {
        throw api_error("ValidationException", "Limit must be greater than 0");
    }


    auto attrs_to_get = calculate_attrs_to_get(request_info);

    dht::partition_range_vector partition_ranges{dht::partition_range::make_open_ended_both_sides()};
    std::vector<query::clustering_range> ck_bounds{query::clustering_range::make_open_ended_both_sides()};

    auto filtering_restrictions = get_filtering_restrictions(schema, attrs_column(*schema), scan_filter);
    return do_query(schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl, std::move(filtering_restrictions));
}

static dht::partition_range calculate_pk_bound(schema_ptr schema, const column_definition& pk_cdef, comparison_operator_type op, const Json::Value& attrs) {
    if (attrs.size() != 1) {
        throw api_error("ValidationException", format("Only a single attribute is allowed for a hash key restriction: {}", attrs.toStyledString()));
    }
    bytes raw_value = pk_cdef.type->from_string(attrs[0][type_to_string(pk_cdef.type)].asString());
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
        clustering_key upper_limit = clustering_key::from_singular(*schema, t->deserialize(target));
        return query::clustering_range::make(query::clustering_range::bound(ck), query::clustering_range::bound(upper_limit, false));
    }
    return query::clustering_range::make_starting_with(query::clustering_range::bound(ck));
}

static query::clustering_range calculate_ck_bound(schema_ptr schema, const column_definition& ck_cdef, comparison_operator_type op, const Json::Value& attrs) {
    const size_t expected_attrs_size = (op == comparison_operator_type::BETWEEN) ? 2 : 1;
    if (attrs.size() != expected_attrs_size) {
        throw api_error("ValidationException", format("{} arguments expected for a sort key restriction: {}", expected_attrs_size, attrs.toStyledString()));
    }
    bytes raw_value = ck_cdef.type->from_string(attrs[0][type_to_string(ck_cdef.type)].asString());
    clustering_key ck = clustering_key::from_singular(*schema, ck_cdef.type->deserialize(raw_value));
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
        bytes raw_upper_limit = ck_cdef.type->from_string(attrs[1][type_to_string(ck_cdef.type)].asString());
        clustering_key upper_limit = clustering_key::from_singular(*schema, ck_cdef.type->deserialize(raw_upper_limit));
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
        std::string raw_upper_limit_str = attrs[0][type_to_string(ck_cdef.type)].asString();
        bytes raw_upper_limit = ck_cdef.type->from_string(raw_upper_limit_str);
        return get_clustering_range_for_begins_with(std::move(raw_upper_limit), ck, schema, ck_cdef.type);
    }
    default:
        throw api_error("ValidationException", format("Unknown primary key bound passed: {}", int(op)));
    }
}

// Calculates primary key bounds from the list of conditions
static std::pair<dht::partition_range_vector, std::vector<query::clustering_range>>
calculate_bounds(schema_ptr schema, const Json::Value& conditions) {
    dht::partition_range_vector partition_ranges;
    std::vector<query::clustering_range> ck_bounds;

    for (auto it = conditions.begin(); it != conditions.end(); ++it) {
        std::string key = it.key().asString();
        const Json::Value& condition = *it;

        Json::Value comp_definition = condition.get("ComparisonOperator", Json::Value());
        Json::Value attr_list = condition.get("AttributeValueList", Json::Value(Json::arrayValue));

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
        if (conditions.size() != 1) {
            throw api_error("ValidationException", "Only one condition allowed in table with only hash key");
        }
    } else {
        if (conditions.size() == 2 && ck_bounds.empty()) {
            throw api_error("ValidationException", format("Query missing condition on sort key '{}'", schema->clustering_key_columns().front().name_as_text()));
        } else if (conditions.size() > 2) {
            throw api_error("ValidationException", "Only one or two conditions allowed in table with hash key and sort key");
        }
    }

    if (ck_bounds.empty()) {
        ck_bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    }

    return {std::move(partition_ranges), std::move(ck_bounds)};
}

future<json::json_return_type> executor::query(std::string content) {
    _stats.api_operations.query++;
    Json::Value request_info = json::to_json_value(content);
    elogger.trace("Querying {}", request_info.toStyledString());

    schema_ptr schema = get_table(_proxy, request_info);

    Json::Value exclusive_start_key = request_info["ExclusiveStartKey"];
    db::consistency_level cl = get_read_consistency(request_info);
    uint32_t limit = request_info.get("Limit", query::max_partitions).asUInt();
    if (limit <= 0) {
        throw api_error("ValidationException", "Limit must be greater than 0");
    }

    //FIXME(sarna): KeyConditions are deprecated in favor of KeyConditionExpression
    const Json::Value& conditions = request_info["KeyConditions"];
    //FIXME(sarna): QueryFilter is deprecated in favor of FilterExpression
    const Json::Value& query_filter = request_info["QueryFilter"];

    auto [partition_ranges, ck_bounds] = calculate_bounds(schema, conditions);

    auto attrs_to_get = calculate_attrs_to_get(request_info);

    auto filtering_restrictions = get_filtering_restrictions(schema, attrs_column(*schema), query_filter);
    return do_query(schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl, std::move(filtering_restrictions));
}

static void validate_limit(int limit) {
    if (limit < 1 || limit > 100) {
        throw api_error("ValidationException", "Limit must be greater than 0 and no greater than 100");
    }
}

future<json::json_return_type> executor::list_tables(std::string content) {
    _stats.api_operations.list_tables++;
    Json::Value table_info = json::to_json_value(content);
    elogger.trace("Listing tables {}", table_info.toStyledString());

    std::string exclusive_start = table_info.get("ExclusiveStartTableName", "").asString();
    int limit = table_info.get("Limit", 100).asInt();
    validate_limit(limit);

    auto table_names = _proxy.get_db().local().get_column_families()
            | boost::adaptors::map_values
            | boost::adaptors::filtered([] (const lw_shared_ptr<table>& t) {
                        return t->schema()->ks_name() == KEYSPACE_NAME;
                    })
            | boost::adaptors::transformed([] (const lw_shared_ptr<table>& t) {
                        return t->schema()->cf_name();
                    });

    Json::Value response;
    Json::Value& all_tables = response["TableNames"];
    all_tables = Json::Value(Json::arrayValue);

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
        all_tables.append(Json::Value(table_names_it->c_str()));
        --limit;
        ++table_names_it;
    }

    if (table_names_it != table_names.end()) {
        response["LastEvaluatedTableName"] = *std::prev(all_tables.end());
    }

    return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
}

future<json::json_return_type> executor::describe_endpoints(std::string content, std::string host_header) {
    _stats.api_operations.describe_endpoints++;
    Json::Value response;
    // Without having any configuration parameter to say otherwise, we tell
    // the user to return to the same endpoint they used to reach us. The only
    // way we can know this is through the "Host:" header in the request,
    // which typically exists (and in fact is mandatory in HTTP 1.1).
    // A "Host:" header includes both host name and port, exactly what we need
    // to return.
    if (host_header.empty()) {
        throw api_error("ValidationException", "DescribeEndpoints needs a 'Host:' header in request");
    }
    response["Endpoints"][0]["Address"] = host_header;
    response["Endpoints"][0]["CachePeriodInMinutes"] = 1440;
    return make_ready_future<json::json_return_type>(make_jsonable(std::move(response)));
}

future<> executor::start() {
    if (engine().cpu_id() != 0) {
        return make_ready_future<>();
    }

    // FIXME: the RF of this keyspace should be configurable: RF=1 makes
    // sense on test setups, but not on real clusters.
    auto ksm = keyspace_metadata::new_keyspace(KEYSPACE_NAME, "org.apache.cassandra.locator.SimpleStrategy", {{"replication_factor", "1"}}, true);
    try {
        return _mm.announce_new_keyspace(ksm, api::min_timestamp, false);
    } catch(exceptions::already_exists_exception& ignored) {
        return make_ready_future<>();
    } catch(...) {
        return make_exception_future(std::current_exception());
    }
}

}
