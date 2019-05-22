/*
 * Copyright 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * See the LICENSE.PROPRIETARY file in the top-level directory for licensing information.
 */

#include <regex>

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

#include <boost/range/adaptors.hpp>

static logging::logger elogger("alternator-executor");

namespace alternator {

static map_type attrs_type() {
    static auto t = map_type_impl::get_instance(utf8_type, utf8_type, true);
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


#if 0 /* not used yet */
/*
 * Full representation should cover:
         "B": blob,
         "BOOL": boolean,
         "BS": [ blob ],
         "L": [
            "AttributeValue"
         ],
         "M": {
            "string" : "AttributeValue"
         },
         "N": "string",
         "NS": [ "string" ],
         "NULL": boolean,
         "S": "string",
         "SS": [ "string" ]

   TODO(sarna): boost::bimap
 */
static data_type parse_type(std::string type) {
    static thread_local std::unordered_map<std::string, data_type> types = {
        {"S", utf8_type},
        {"B", bytes_type},
        {"BOOL", boolean_type},
        {"N", long_type}, //FIXME(sarna): It's actually a special generic number type, not long
    };
    auto it = types.find(type);
    if (it == types.end()) {
        throw std::runtime_error(format("Unknown type {}", type));
    }
    return it->second;
}
#endif

static std::string type_to_string(data_type type) {
    static thread_local std::unordered_map<data_type, std::string> types = {
        {utf8_type, "S"},
        {bytes_type, "B"},
        {boolean_type, "BOOL"},
        {long_type, "N"},
    };
    auto it = types.find(type);
    if (it == types.end()) {
        throw std::runtime_error(format("Unknown type {}", type->name()));
    }
    return it->second;
}

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
        case 'N': return long_type; // FIXME: this actually needs to be a new number type, not long
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

static bytes get_key_column_value(const Json::Value& item, const column_definition& column) {
    std::string column_name = column.name_as_text();
    std::string expected_type = type_to_string(column.type);

    Json::Value key_typed_value = item.get(column_name, Json::nullValue);
    if (!key_typed_value.isObject() || key_typed_value.size() != 1) {
        throw api_error("ValidationException",
                format("Missing or invalid value object for key column {}: {}",
                        column_name, item.toStyledString()));
    }
    auto it = key_typed_value.begin();
    if (it.key().asString() != expected_type) {
        throw api_error("ValidationException",
                format("Expected type {} for key column {}, got type {}",
                        expected_type, column_name, it.key().asString()));
    }
    // FIXME: if expected_type is B, we need to do base64 decoding!
    return column.type->from_string(it->asString());

}

static partition_key pk_from_json(const Json::Value& item, schema_ptr schema) {
    std::vector<bytes> raw_pk;
    // FIXME: this is a loop, but we really allow only one partition key column.
    for (const column_definition& cdef : schema->partition_key_columns()) {
        bytes raw_value = get_key_column_value(item, cdef);
        raw_pk.push_back(std::move(raw_value));
    }
   return partition_key::from_exploded(raw_pk);
}

static clustering_key ck_from_json(const Json::Value& item, schema_ptr schema) {
    if (schema->clustering_key_size() == 0) {
        return clustering_key::make_empty();
    }
    std::vector<bytes> raw_ck;
    // FIXME: this is a loop, but we really allow only one clustering key column.
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        bytes raw_value = get_key_column_value(item,  cdef);
        raw_ck.push_back(std::move(raw_value));
    }

    return clustering_key::from_exploded(raw_ck);
}

static mutation make_item_mutation(const Json::Value& item, schema_ptr schema) {
    partition_key pk = pk_from_json(item, schema);
    clustering_key ck = ck_from_json(item, schema);

    mutation m(schema, pk);
    collection_type_impl::mutation attrs_mut;

    auto ts = api::new_timestamp();

    for (auto it = item.begin(); it != item.end(); ++it) {
        bytes column_name = to_bytes(it.key().asString());
        const column_definition* cdef = schema->get_column_definition(column_name);
        if (!cdef || !cdef->is_primary_key()) {
            bytes value = utf8_type->decompose(sstring(it->toStyledString()));
            attrs_mut.cells.emplace_back(column_name, atomic_cell::make_live(*utf8_type, ts, value, atomic_cell::collection_member::yes));
        }
    }

    auto serialized_map = attrs_type()->serialize_mutation_form(std::move(attrs_mut));
    auto& row = m.partition().clustered_row(*schema, ck);
    row.cells().apply(attrs_column(*schema), std::move(serialized_map));
    // To allow creation of an item with no attributes, we need a row marker.
    row.apply(row_marker(ts));
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

static schema_ptr get_table_from_batch_request(const service::storage_proxy& proxy, const Json::Value::const_iterator& batch_request) {
    std::string table_name = batch_request.key().asString(); // JSON keys are always strings
    validate_table_name(table_name);
    try {
        return proxy.get_db().local().find_schema(executor::KEYSPACE_NAME, table_name);
    } catch(no_such_column_family&) {
        throw api_error("ResourceNotFoundException", format("Requested resource not found: Table: {} not found", table_name));
    }
}

future<json::json_return_type> executor::batch_write_item(std::string content) {
    _stats.api_operations.batch_write_item++;
    Json::Value batch_info = json::to_json_value(content);
    Json::Value& request_items = batch_info["RequestItems"];

    std::vector<mutation> mutations;
    mutations.reserve(request_items.size());
    for (auto it = request_items.begin(); it != request_items.end(); ++it) {
        schema_ptr schema = get_table_from_batch_request(_proxy, it);
        for (auto&& request : *it) {
            //FIXME(sarna): Add support for DeleteRequest too
            const Json::Value& put_request = request.get("PutRequest", Json::Value());
            const Json::Value& item = put_request["Item"];
            mutations.push_back(make_item_mutation(item, schema));
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

future<json::json_return_type> executor::update_item(std::string content) {
    _stats.api_operations.update_item++;
    Json::Value update_info = json::to_json_value(content);
    elogger.trace("update_item {}", update_info.toStyledString());
    schema_ptr schema = get_table(_proxy, update_info);
    // FIXME: handle missing Key.
    const Json::Value& key = update_info["Key"];
    // FIXME: handle missing components in Key, extra stuff, etc.
    partition_key pk = pk_from_json(key, schema);
    clustering_key ck = ck_from_json(key, schema);

    mutation m(schema, pk);
    collection_type_impl::mutation attrs_mut;
    auto ts = api::new_timestamp();

    // FIXME: handle case of missing AttributeUpdates (we don't support the newer UpdateExpression yet).
    const Json::Value& attribute_updates = update_info["AttributeUpdates"];
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
            attrs_mut.cells.emplace_back(column_name, atomic_cell::make_dead(ts, gc_clock::now()));
        } else if (action == "PUT") {
            const Json::Value& value = (*it)["Value"];
            if (value.size() != 1) {
                throw api_error("ValidationException",
                        format("Value field in AttributeUpdates must have just one item", it.key().asString()));
            }
            // At this point, value is a dict with a single entry.
            // value.begin().key() is its key (a type) and
            // value.begin()->asString() is its value. But we currently
            // serialize both together, with value.toStyledString().
            bytes val = utf8_type->decompose(sstring(value.toStyledString()));
            attrs_mut.cells.emplace_back(column_name, atomic_cell::make_live(*utf8_type, ts, val, atomic_cell::collection_member::yes));
        } else {
            // FIXME: need to support "ADD" as well.
            throw api_error("ValidationException",
                format("Unknown Action value '{}' in AttributeUpdates", action));
        }
    }
    auto serialized_map = attrs_type()->serialize_mutation_form(std::move(attrs_mut));
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
}

static Json::Value describe_item(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, foreign_ptr<lw_shared_ptr<query::result>> query_result, std::unordered_set<std::string>&& attrs_to_get) {
    Json::Value item(Json::objectValue);

    cql3::selection::result_set_builder builder(selection, gc_clock::now(), cql_serialization_format::latest());
    query::result_view::consume(*query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, selection));

    auto result_set = builder.build();
    if (result_set->empty()) {
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return Json::objectValue;
    }
    // FIXME: I think this can't really be a loop, there should be exactly
    // one result after above we handled the 0 result case
    for (auto& result_row : result_set->rows()) {
        const auto& columns = selection.get_columns();
        auto column_it = columns.begin();
        for (const bytes_opt& cell : result_row) {
            std::string column_name = (*column_it)->name_as_text();
            if (column_name != executor::ATTRS_COLUMN_NAME) {
                if (attrs_to_get.empty() || attrs_to_get.count(column_name) > 0) {
                    Json::Value& field = item[column_name.c_str()];
                    field[type_to_string((*column_it)->type)] = json::to_json_value((*column_it)->type->to_json_string(cell));
                }
            } else if (cell) {
                auto deserialized = attrs_type()->deserialize(*cell, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (attrs_to_get.empty() || attrs_to_get.count(attr_name) > 0) {
                        item[attr_name] = json::to_json_value(value_cast<sstring>(entry.second));
                    }
                }
            }
            ++column_it;
        }
    }
    Json::Value item_descr(Json::objectValue);
    item_descr["Item"] = item;
    return item_descr;
}

future<json::json_return_type> executor::get_item(std::string content) {
    _stats.api_operations.get_item++;
    Json::Value table_info = json::to_json_value(content);
    elogger.trace("Getting item {}", table_info.toStyledString());

    schema_ptr schema = get_table(_proxy, table_info);
    //FIXME(sarna): AttributesToGet is deprecated with more generic ProjectionExpression in the newest API
    Json::Value attributes_to_get = table_info["AttributesToGet"];
    Json::Value query_key = table_info["Key"];
    db::consistency_level cl = table_info["ConsistentRead"].asBool() ? db::consistency_level::QUORUM : db::consistency_level::ONE;

    partition_key pk = pk_from_json(query_key, schema);
    dht::partition_range_vector partition_ranges{dht::partition_range(dht::global_partitioner().decorate_key(*schema, pk))};

    std::vector<query::clustering_range> bounds;
    if (schema->clustering_key_size() == 0) {
        bounds.push_back(query::clustering_range::make_open_ended_both_sides());
    } else {
        clustering_key ck = ck_from_json(query_key, schema);
        bounds.push_back(query::clustering_range::make_singular(std::move(ck)));
    }

    //TODO(sarna): It would be better to fetch only some attributes of the map, not all
    query::column_id_vector regular_columns{attrs_column(*schema).id};

    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto attrs_to_get = boost::copy_range<std::unordered_set<std::string>>(attributes_to_get | boost::adaptors::transformed(std::bind(&Json::Value::asString, std::placeholders::_1)));

    return _proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(db::no_timeout, empty_service_permit())).then(
            [schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = std::move(attrs_to_get)] (service::storage_proxy::coordinator_query_result qr) mutable {
        return make_ready_future<json::json_return_type>(make_jsonable(describe_item(schema, partition_slice, *selection, std::move(qr.query_result), std::move(attrs_to_get))));
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
                    field[type_to_string((*_column_it)->type)] = json::to_json_value((*_column_it)->type->to_json_string(bytes(bv)));
                }
            } else {
                auto deserialized = attrs_type()->deserialize(bv, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    std::string attr_name = value_cast<sstring>(entry.first);
                    if (_attrs_to_get.empty() || _attrs_to_get.count(attr_name) > 0) {
                        _item[attr_name] = json::to_json_value(value_cast<sstring>(entry.second));
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
        db::consistency_level cl) {
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
    ::shared_ptr<cql3::restrictions::statement_restrictions> filtering_restrictions = nullptr;
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
    //FIXME(sarna): AttributesToGet is deprecated with more generic ProjectionExpression in the newest API
    Json::Value attributes_to_get = request_info["AttributesToGet"];
    Json::Value exclusive_start_key = request_info["ExclusiveStartKey"];
    db::consistency_level cl = request_info["ConsistentRead"].asBool() ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    uint32_t limit = request_info.get("Limit", query::max_partitions).asUInt();
    if (limit <= 0) {
        throw api_error("ValidationException", "Limit must be greater than 0");
    }

    auto attrs_to_get = boost::copy_range<std::unordered_set<std::string>>(attributes_to_get | boost::adaptors::transformed(std::bind(&Json::Value::asString, std::placeholders::_1)));

    dht::partition_range_vector partition_ranges{dht::partition_range::make_open_ended_both_sides()};
    std::vector<query::clustering_range> ck_bounds{query::clustering_range::make_open_ended_both_sides()};

    return do_query(schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl);
}

enum class comparison_operator_type {
    EQ, NE, LE, LT, GE, GT, IN, BETWEEN, CONTAINS, IS_NULL, NOT_NULL, BEGINS_WITH
};

static comparison_operator_type get_comparison_operator(const Json::Value& comparison_operator) {
    static std::unordered_map<std::string, comparison_operator_type> ops = {
            {"EQ", comparison_operator_type::EQ},
            {"LE", comparison_operator_type::LE},
            {"LT", comparison_operator_type::LT},
            {"GE", comparison_operator_type::GE},
            {"GT", comparison_operator_type::GT},
            {"BETWEEN", comparison_operator_type::BETWEEN},
            {"BEGINS_WITH", comparison_operator_type::BEGINS_WITH},
    }; //TODO(sarna): NE, IN, CONTAINS, NULL, NOT_NULL
    if (!comparison_operator.isString()) {
        throw api_error("ValidationException", format("Invalid comparison operator definition {}", comparison_operator.toStyledString()));
    }
    std::string op = comparison_operator.asString();
    auto it = ops.find(op);
    if (it == ops.end()) {
        throw api_error("ValidationException", format("Unsupported comparison operator {}", op));
    }
    return it->second;
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
        if (raw_upper_limit.back() != std::numeric_limits<bytes::value_type>::max()) {
            raw_upper_limit.back()++;
        } else {
            raw_upper_limit.resize(raw_upper_limit.size() + 1);
        }
        clustering_key upper_limit = clustering_key::from_singular(*schema, ck_cdef.type->deserialize(raw_upper_limit));
        return query::clustering_range::make(query::clustering_range::bound(ck), query::clustering_range::bound(upper_limit, false));
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
    //FIXME(sarna): AttributesToGet is deprecated with more generic ProjectionExpression in the newest API
    Json::Value attributes_to_get = request_info["AttributesToGet"];
    Json::Value exclusive_start_key = request_info["ExclusiveStartKey"];
    db::consistency_level cl = request_info["ConsistentRead"].asBool() ? db::consistency_level::QUORUM : db::consistency_level::ONE;
    uint32_t limit = request_info.get("Limit", query::max_partitions).asUInt();
    if (limit <= 0) {
        throw api_error("ValidationException", "Limit must be greater than 0");
    }

    //FIXME(sarna): KeyConditions are deprecated in favor of KeyConditionExpression
    const Json::Value& conditions = request_info["KeyConditions"];

    auto [partition_ranges, ck_bounds] = calculate_bounds(schema, conditions);
    auto attrs_to_get = boost::copy_range<std::unordered_set<std::string>>(attributes_to_get | boost::adaptors::transformed(std::bind(&Json::Value::asString, std::placeholders::_1)));

    return do_query(schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl);
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
    return _mm.announce_new_keyspace(ksm, api::min_timestamp, false).handle_exception_type([] (exceptions::already_exists_exception& ignored) {});
}

}
