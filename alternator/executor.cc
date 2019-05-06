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

static logging::logger elogger("alternator-executor");

namespace alternator {

static constexpr auto ATTRIBUTE_DEFINITIONS = "AttributeDefinitions";
static constexpr auto KEY_SCHEMA = "KeySchema";
static constexpr auto TABLE_NAME = "TableName";
static constexpr auto TABLE_DESCRIPTION = "TableDescription";
static constexpr auto ATTRIBUTE_NAME = "AttributeName";
static constexpr auto ATTRIBUTE_TYPE = "AttributeType";
static constexpr auto KEY_TYPE = "KeyType";
static constexpr auto HASH = "HASH";
static constexpr auto RANGE = "RANGE";
static constexpr auto CREATION_DATE_TIME = "CreationDateTime";
static constexpr auto TABLE_ID = "TableId";
static constexpr auto TABLE_STATUS = "TableStatus";
static constexpr auto ACTIVE = "ACTIVE";
static constexpr auto ATTRIBUTES_TO_GET = "AttributesToGet";
static constexpr auto KEY = "Key";
static constexpr auto CONSISTENT_READ = "ConsistentRead";
static constexpr auto ATTRS = "attrs";
static constexpr auto ITEM = "Item";

static map_type attrs_type() {
    static auto t = map_type_impl::get_instance(utf8_type, utf8_type, true);
    return t;
}

struct make_jsonable : public json::jsonable {
    Json::Value _value;
public:
    explicit make_jsonable(Json::Value&& value) : _value(std::move(value)) {}
    virtual std::string to_json() const override {
        return _value.toStyledString();
    }
};

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
static data_type parse_type(sstring type) {
    static thread_local std::unordered_map<sstring, data_type> types = {
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

static sstring type_to_sstring(data_type type) {
    static thread_local std::unordered_map<data_type, sstring> types = {
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
    descr[CREATION_DATE_TIME] = std::chrono::duration_cast<std::chrono::seconds>(gc_clock::now().time_since_epoch()).count();
    descr[TABLE_STATUS] = ACTIVE;
    descr[TABLE_ID] = schema.id().to_sstring().c_str();
}

// The DynamoDB developer guide, https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
// specifies that table names "names must be between 3 and 255 characters long
// and can contain only the following characters: a-z, A-Z, 0-9, _ (underscore), - (dash), . (dot)
// validate_table_name throws the appropriate api_error if this validation fails.
static void validate_table_name(const sstring& name) {
    if (name.length() < 3 || name.length() > 255) {
        throw api_error(reply::status_type::bad_request, "ValidationException",
                "TableName must be at least 3 characters long and at most 255 characters long");
    }
    static std::regex valid_table_name_chars ("[a-zA-Z0-9_.-]*");
    if (!std::regex_match(name.c_str(), valid_table_name_chars)) {
        throw api_error(reply::status_type::bad_request, "ValidationException",
                "TableName must satisfy regular expression pattern: [a-zA-Z0-9_.-]+");
    }
}

future<json::json_return_type> executor::describe_table(sstring content) {
    Json::Value request = json::to_json_value(content);
    elogger.trace("Describing table {}", request.toStyledString());

    // FIXME: work on error handling. E.g., what if the TableName parameter is missing? What if it's not a string?
    sstring table_name = request["TableName"].asString();
    validate_table_name(table_name);
    if (!_proxy.get_db().local().has_schema(KEYSPACE, table_name)) {
        throw api_error(reply::status_type::bad_request, "ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
    }

    Json::Value table_description(Json::objectValue);
    table_description["TableName"] = table_name.c_str();
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

future<json::json_return_type> executor::delete_table(sstring content) {
    Json::Value request = json::to_json_value(content);
    elogger.trace("Deleting table {}", request.toStyledString());

    // FIXME: work on error handling. E.g., what if the TableName parameter is missing? What if it's not a string?
    sstring table_name = request["TableName"].asString();
    validate_table_name(table_name);
    if (!_proxy.get_db().local().has_schema(KEYSPACE, table_name)) {
        throw api_error(reply::status_type::bad_request, "ResourceNotFoundException",
                format("Requested resource not found: Table: {} not found", table_name));
    }

    return _mm.announce_column_family_drop(KEYSPACE, table_name).then([table_name = std::move(table_name)] {
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

static void add_column(schema_builder& builder, const std::string& name, const Json::Value& attribute_definitions, column_kind kind) {
    for (const Json::Value& attribute_info : attribute_definitions) {
        if (attribute_info["AttributeName"].asString() == name) {
            sstring type = attribute_info["AttributeType"].asString();
            builder.with_column(to_bytes(name), parse_type(type), kind);
            return;
        }
    }
    throw api_error(reply::status_type::bad_request, "ValidationException",
            format("KeySchema key '{}' missing in AttributeDefinitions", name));
}

future<json::json_return_type> executor::create_table(sstring content) {
    Json::Value table_info = json::to_json_value(content);
    elogger.warn("Creating table {}", table_info.toStyledString());

    sstring table_name = table_info[TABLE_NAME].asString();
    validate_table_name(table_name);
    // FIXME: the table name's being valid in Dynamo doesn't make it valid
    // in Scylla. May need to quote or shorten table names.

    const Json::Value& key_schema = table_info[KEY_SCHEMA];
    const Json::Value& attribute_definitions = table_info[ATTRIBUTE_DEFINITIONS];

    schema_builder builder(KEYSPACE, table_name);

    // DynamoDB requires that KeySchema includes up to two elements, the
    // first must be a HASH, the optional second one can be a RANGE.
    // These key names must also be present in the attributes_definitions.
    if (!key_schema.isArray() || key_schema.size() < 1 || key_schema.size() > 2) {
        throw api_error(reply::status_type::bad_request, "ValidationException",
                "KeySchema must list exactly one or two key columns");
    }
    if (key_schema[0]["KeyType"] != "HASH") {
        throw api_error(reply::status_type::bad_request, "ValidationException",
                "First key in KeySchema must be a HASH key");
    }
    add_column(builder, key_schema[0]["AttributeName"].asString(), attribute_definitions, column_kind::partition_key);
    if (key_schema.size() == 2) {
        if (key_schema[1]["KeyType"] != "RANGE") {
            throw api_error(reply::status_type::bad_request, "ValidationException",
                    "Second key in KeySchema must be a RANGE key");
        }
        add_column(builder, key_schema[1]["AttributeName"].asString(), attribute_definitions, column_kind::partition_key);
    }
    builder.with_column(bytes(ATTRS), attrs_type(), column_kind::regular_column);

    schema_ptr schema = builder.build();

    return _mm.announce_new_column_family(schema, false).then([table_info = std::move(table_info), schema] () mutable {
        Json::Value status(Json::objectValue);
        supplement_table_info(table_info, *schema);
        status[TABLE_DESCRIPTION] = std::move(table_info);
        return make_ready_future<json::json_return_type>(make_jsonable(std::move(status)));
    });
}

static partition_key pk_from_json(const Json::Value& item, schema_ptr schema) {
    std::vector<bytes> raw_pk;
    for (const column_definition& cdef : schema->partition_key_columns()) {
        sstring value_str = item[cdef.name_as_text()][type_to_sstring(cdef.type)].asString();
        bytes raw_value = cdef.type->from_string(value_str);
        raw_pk.push_back(std::move(raw_value));
    }
   return partition_key::from_exploded(raw_pk);
}

static clustering_key ck_from_json(const Json::Value& item, schema_ptr schema) {
    assert(schema->clustering_key_size() > 0);
    std::vector<bytes> raw_ck;
    for (const column_definition& cdef : schema->clustering_key_columns()) {
        sstring value_str = item[cdef.name_as_text()][type_to_sstring(cdef.type)].asString();
        bytes raw_value = cdef.type->from_string(value_str);
        raw_ck.push_back(std::move(raw_value));
    }

    return clustering_key::from_exploded(raw_ck);
}

future<json::json_return_type> executor::put_item(sstring content) {
    Json::Value update_info = json::to_json_value(content);
    elogger.debug("Updating value {}", update_info.toStyledString());

    sstring table_name = update_info[TABLE_NAME].asString();
    const Json::Value& item = update_info[ITEM];
    schema_ptr schema;
    try {
        schema = _proxy.get_db().local().find_schema(KEYSPACE, table_name);
    } catch(no_such_column_family&) {
        throw api_error(reply::status_type::bad_request, "ResourceNotFoundException",
                 format("Requested resource not found: Table: {} not found", table_name));
    }
    partition_key pk = pk_from_json(item, schema);
    clustering_key ck = (schema->clustering_key_size() > 0) ? ck_from_json(item, schema) : clustering_key::make_empty();

    mutation m(schema, pk);
    collection_type_impl::mutation attrs_mut;

    for (auto it = item.begin(); it != item.end(); ++it) {
        bytes column_name = to_bytes(it.key().asString());
        const column_definition* cdef = schema->get_column_definition(column_name);
        if (!cdef || !cdef->is_primary_key()) {
            bytes value = utf8_type->decompose(sstring(it->toStyledString()));
            attrs_mut.cells.emplace_back(column_name, atomic_cell::make_live(*utf8_type, api::new_timestamp(), value, atomic_cell::collection_member::yes));
        }
        elogger.warn("{}: {}", it.key().asString(), it->toStyledString());
    }
    const column_definition* attrs_cdef = schema->get_column_definition(bytes(ATTRS));

    auto serialized_map = attrs_type()->serialize_mutation_form(std::move(attrs_mut));
    m.set_cell(ck, *attrs_cdef, std::move(serialized_map));
    elogger.warn("Applying mutation {}", m);

    return _proxy.mutate(std::vector<mutation>{std::move(m)}, db::consistency_level::QUORUM, db::no_timeout, tracing::trace_state_ptr()).then([] () {
        // The Boto3 gets confused if we return just an empty structure {},
        // thinking it isn't JSON at all. I don't know why, but putting an
        // empty Attributes field in the result solves the problem.
        return make_ready_future<json::json_return_type>("{ \"Attributes\": {} }");
    });

}

static Json::Value describe_item(schema_ptr schema, const query::partition_slice& slice, const cql3::selection::selection& selection, foreign_ptr<lw_shared_ptr<query::result>> query_result, std::unordered_set<sstring>&& attrs_to_get) {
    Json::Value item(Json::objectValue);

    cql3::selection::result_set_builder builder(selection, gc_clock::now(), cql_serialization_format::latest());
    query::result_view::consume(*query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, selection));

    auto result_set = builder.build();
    for (auto& result_row : result_set->rows()) {
        const auto& columns = selection.get_columns();
        auto column_it = columns.begin();
        for (const bytes_opt& cell : result_row) {
            sstring column_name = (*column_it)->name_as_text();
            if (column_name != ATTRS) {
                if (attrs_to_get.empty() || attrs_to_get.count(column_name) > 0) {
                    Json::Value& field = item[column_name.c_str()];
                    field[type_to_sstring((*column_it)->type)] = (*column_it)->type->to_json(cell);
                }
            } else if (cell) {
                auto deserialized = attrs_type()->deserialize(*cell, cql_serialization_format::latest());
                auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
                for (auto entry : keys_and_values) {
                    sstring attr_name = value_cast<sstring>(entry.first);
                    if (attrs_to_get.empty() || attrs_to_get.count(attr_name) > 0) {
                        item[attr_name] = json::to_json_value(value_cast<sstring>(entry.second));
                    }
                }
            }
            ++column_it;
        }
    }
    Json::Value item_descr(Json::objectValue);
    item_descr[ITEM] = item;
    return item_descr;
}

future<json::json_return_type> executor::get_item(sstring content) {
    Json::Value table_info = json::to_json_value(content);
    elogger.warn("Getting item {}", table_info.toStyledString());

    sstring table_name = table_info[TABLE_NAME].asString();
    //FIXME(sarna): AttributesToGet is deprecated with more generic ProjectionExpression in the newest API
    Json::Value attributes_to_get = table_info[ATTRIBUTES_TO_GET];
    Json::Value query_key = table_info[KEY];
    db::consistency_level cl = table_info[CONSISTENT_READ].asBool() ? db::consistency_level::QUORUM : db::consistency_level::ONE;

    schema_ptr schema = _proxy.get_db().local().find_schema(KEYSPACE, table_name);

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
    query::column_id_vector regular_columns{schema->get_column_definition(bytes(ATTRS))->id};

    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, query::max_partitions);

    auto attrs_to_get = boost::copy_range<std::unordered_set<sstring>>(attributes_to_get | boost::adaptors::transformed(std::bind(&Json::Value::asString, std::placeholders::_1)));

    return _proxy.query(schema, std::move(command), std::move(partition_ranges), cl, service::storage_proxy::coordinator_query_options(db::no_timeout)).then(
            [schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = std::move(attrs_to_get)] (service::storage_proxy::coordinator_query_result qr) mutable {
        return make_ready_future<json::json_return_type>(make_jsonable(describe_item(schema, partition_slice, *selection, std::move(qr.query_result), std::move(attrs_to_get))));
    });

    return make_ready_future<json::json_return_type>("");
}

future<> executor::start() {
    if (engine().cpu_id() != 0) {
        return make_ready_future<>();
    }

    auto ksm = keyspace_metadata::new_keyspace(KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", {{"replication_factor", "1"}}, true);
    return _mm.announce_new_keyspace(ksm, api::min_timestamp, false).handle_exception_type([] (exceptions::already_exists_exception& ignored) {});
}

}
