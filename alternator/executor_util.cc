/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "alternator/executor_util.hh"
#include "alternator/executor.hh"
#include "alternator/error.hh"
#include "auth/resource.hh"
#include "auth/service.hh"
#include "cdc/log.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/tags/utils.hh"
#include "replica/database.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "serialization.hh"
#include "service/storage_proxy.hh"
#include "types/map.hh"
#include <fmt/format.h>

namespace alternator {

extern logging::logger elogger; // from executor.cc

std::optional<int> get_int_attribute(const rjson::value& value, std::string_view attribute_name) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value)
        return {};
    if (!attribute_value->IsInt()) {
        throw api_error::validation(fmt::format("Expected integer value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetInt();
}

std::string get_string_attribute(const rjson::value& value, std::string_view attribute_name, const char* default_return) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value)
        return default_return;
    if (!attribute_value->IsString()) {
        throw api_error::validation(fmt::format("Expected string value for attribute {}, got: {}",
                attribute_name, value));
    }
    return rjson::to_string(*attribute_value);
}

bool get_bool_attribute(const rjson::value& value, std::string_view attribute_name, bool default_return) {
    const rjson::value* attribute_value = rjson::find(value, attribute_name);
    if (!attribute_value) {
        return default_return;
    }
    if (!attribute_value->IsBool()) {
        throw api_error::validation(fmt::format("Expected boolean value for attribute {}, got: {}",
                attribute_name, value));
    }
    return attribute_value->GetBool();
}

std::optional<std::string> find_table_name(const rjson::value& request) {
    const rjson::value* table_name_value = rjson::find(request, "TableName");
    if (!table_name_value) {
        return std::nullopt;
    }
    if (!table_name_value->IsString()) {
        throw api_error::validation("Non-string TableName field in request");
    }
    std::string table_name = rjson::to_string(*table_name_value);
    return table_name;
}

std::string get_table_name(const rjson::value& request) {
    auto name = find_table_name(request);
    if (!name) {
        throw api_error::validation("Missing TableName field in request");
    }
    return *name;
}

schema_ptr find_table(service::storage_proxy& proxy, const rjson::value& request) {
    auto table_name = find_table_name(request);
    if (!table_name) {
        return nullptr;
    }
    return find_table(proxy, *table_name);
}

schema_ptr find_table(service::storage_proxy& proxy, std::string_view table_name) {
    try {
        return proxy.data_dictionary().find_schema(sstring(executor::KEYSPACE_NAME_PREFIX) + sstring(table_name), table_name);
    } catch(data_dictionary::no_such_column_family&) {
        // DynamoDB returns validation error even when table does not exist
        // and the table name is invalid.
        validate_table_name(table_name);

        throw api_error::resource_not_found(
                fmt::format("Requested resource not found: Table: {} not found", table_name));
    }
}

schema_ptr get_table(service::storage_proxy& proxy, const rjson::value& request) {
    auto schema = find_table(proxy, request);
    if (!schema) {
        // if we get here then the name was missing, since syntax or missing actual CF
        // checks throw. Slow path, but just call get_table_name to generate exception.
        get_table_name(request);
    }
    return schema;
}

map_type attrs_type() {
    static thread_local auto t = map_type_impl::get_instance(utf8_type, bytes_type, true);
    return t;
}

const std::map<sstring, sstring>& get_tags_of_table_or_throw(schema_ptr schema) {
    auto tags_ptr = db::get_tags_of_table(schema);
    if (tags_ptr) {
        return *tags_ptr;
    } else {
        throw api_error::validation(format("Table {} does not have valid tagging information", schema->ks_name()));
    }
}

bool is_alternator_keyspace(std::string_view ks_name) {
    return ks_name.starts_with(executor::KEYSPACE_NAME_PREFIX);
}

// This tag is set on a GSI when the user did not specify a range key, causing
// Alternator to add the base table's range key as a spurious range key. It is
// used by describe_key_schema() to suppress reporting that key.
extern const sstring SPURIOUS_RANGE_KEY_ADDED_TO_GSI_AND_USER_DIDNT_SPECIFY_RANGE_KEY_TAG_KEY;

void describe_key_schema(rjson::value& parent, const schema& schema, std::unordered_map<std::string, std::string>* attribute_types, const std::map<sstring, sstring>* tags) {
    rjson::value key_schema = rjson::empty_array();
    const bool ignore_range_keys_as_spurious = tags != nullptr && tags->contains(SPURIOUS_RANGE_KEY_ADDED_TO_GSI_AND_USER_DIDNT_SPECIFY_RANGE_KEY_TAG_KEY);

    for (const column_definition& cdef : schema.partition_key_columns()) {
        rjson::value key = rjson::empty_object();
        rjson::add(key, "AttributeName", rjson::from_string(cdef.name_as_text()));
        rjson::add(key, "KeyType", "HASH");
        rjson::push_back(key_schema, std::move(key));
        if (attribute_types) {
            (*attribute_types)[cdef.name_as_text()] = type_to_string(cdef.type);
        }
    }
    if (!ignore_range_keys_as_spurious) {
        // NOTE: user requested key (there can be at most one) will always come first.
        // There might be more keys following it, which were added, but those were
        // not requested by the user, so we ignore them.
        for (const column_definition& cdef : schema.clustering_key_columns()) {
            rjson::value key = rjson::empty_object();
            rjson::add(key, "AttributeName", rjson::from_string(cdef.name_as_text()));
            rjson::add(key, "KeyType", "RANGE");
            rjson::push_back(key_schema, std::move(key));
            if (attribute_types) {
                (*attribute_types)[cdef.name_as_text()] = type_to_string(cdef.type);
            }
            break;
        }
    }
    rjson::add(parent, "KeySchema", std::move(key_schema));
}

// Check if the given string has valid characters for a table name, i.e. only
// a-z, A-Z, 0-9, _ (underscore), - (dash), . (dot). Note that this function
// does not check the length of the name - instead, use validate_table_name()
// to validate both the characters and the length.
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

std::string view_name(std::string_view table_name, std::string_view index_name, const std::string& delim, bool validate_len) {
    if (index_name.length() < 3) {
        throw api_error::validation("IndexName must be at least 3 characters long");
    }
    if (!valid_table_name_chars(index_name)) {
        throw api_error::validation(
                fmt::format("IndexName '{}' must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", index_name));
    }
    std::string ret = std::string(table_name) + delim + std::string(index_name);
    if (ret.length() > max_auxiliary_table_name_length && validate_len) {
        throw api_error::validation(
                fmt::format("The total length of TableName ('{}') and IndexName ('{}') cannot exceed {} characters",
                        table_name, index_name, max_auxiliary_table_name_length - delim.size()));
    }
    return ret;
}

std::string gsi_name(std::string_view table_name, std::string_view index_name, bool validate_len) {
    return view_name(table_name, index_name, ":", validate_len);
}

std::string lsi_name(std::string_view table_name, std::string_view index_name, bool validate_len) {
    return view_name(table_name, index_name, "!:", validate_len);
}

void check_key(const rjson::value& key, const schema_ptr& schema, bool allow_extra_attribute) {
    const unsigned expected = (schema->clustering_key_size() == 0 ? 1 : 2) + (allow_extra_attribute ? 1 : 0);
    if (key.MemberCount() != expected) {
        throw api_error::validation("Given key attribute not in schema");
    }
}

void verify_all_are_used(const rjson::value* field,
        const std::unordered_set<std::string>& used, const char* field_name, const char* operation) {
    if (!field) {
        return;
    }
    for (auto it = field->MemberBegin(); it != field->MemberEnd(); ++it) {
        if (!used.contains(rjson::to_string(it->name))) {
            throw api_error::validation(
                format("{} has spurious '{}', not used in {}",
                    field_name, rjson::to_string_view(it->name), operation));
        }
    }
}

// This function increments the authorization_failures counter, and may also
// log a warn-level message and/or throw an access_denied exception, depending
// on what enforce_authorization and warn_authorization are set to.
// Note that if enforce_authorization is false, this function will return
// without throwing. So a caller that doesn't want to continue after an
// authorization_error must explicitly return after calling this function.
static void authorization_error(stats& stats, bool enforce_authorization, bool warn_authorization, std::string msg) {
    stats.authorization_failures++;
    if (enforce_authorization) {
        if (warn_authorization) {
            elogger.warn("alternator_warn_authorization=true: {}", msg);
        }
        throw api_error::access_denied(std::move(msg));
    } else {
        if (warn_authorization) {
            elogger.warn("If you set alternator_enforce_authorization=true the following will be enforced: {}", msg);
        }
    }
}

future<> verify_permission(
    bool enforce_authorization,
    bool warn_authorization,
    const service::client_state& client_state,
    const schema_ptr& schema,
    auth::permission permission_to_check,
    stats& stats) {
    if (!enforce_authorization && !warn_authorization) {
        co_return;
    }
    // Unfortunately, the fix for issue #23218 did not modify the function
    // that we use here - check_has_permissions(). So if we want to allow
    // writes to internal tables (from try_get_internal_table()) only to a
    // superuser, we need to explicitly check it here.
    if (permission_to_check == auth::permission::MODIFY && is_internal_keyspace(schema->ks_name())) {
        if (!client_state.user() ||
            !client_state.user()->name ||
            !co_await client_state.get_auth_service()->underlying_role_manager().is_superuser(*client_state.user()->name)) {
                sstring username = "<anonymous>";
                if (client_state.user() && client_state.user()->name) {
                    username = client_state.user()->name.value();
                }
                authorization_error(stats, enforce_authorization, warn_authorization, fmt::format(
                    "Write access denied on internal table {}.{} to role {} because it is not a superuser",
                    schema->ks_name(), schema->cf_name(), username));
                co_return;
        }
    }
    auto resource = auth::make_data_resource(schema->ks_name(), schema->cf_name());
    if (!client_state.user() || !client_state.user()->name ||
        !co_await client_state.check_has_permission(auth::command_desc(permission_to_check, resource))) {
        sstring username = "<anonymous>";
        if (client_state.user() && client_state.user()->name) {
            username = client_state.user()->name.value();
        }
        // Using exceptions for errors makes this function faster in the
        // success path (when the operation is allowed).
        authorization_error(stats, enforce_authorization, warn_authorization, fmt::format(
            "{} access on table {}.{} is denied to role {}, client address {}",
            auth::permissions::to_string(permission_to_check),
            schema->ks_name(), schema->cf_name(), username, client_state.get_client_address()));
    }
}

// Similar to verify_permission() above, but just for CREATE operations.
// Those do not operate on any specific table, so require permissions on
// ALL KEYSPACES instead of any specific table.
future<> verify_create_permission(bool enforce_authorization, bool warn_authorization, const service::client_state& client_state, stats& stats) {
    if (!enforce_authorization && !warn_authorization) {
        co_return;
    }
    auto resource = auth::resource(auth::resource_kind::data);
    if (!co_await client_state.check_has_permission(auth::command_desc(auth::permission::CREATE, resource))) {
        sstring username = "<anonymous>";
        if (client_state.user() && client_state.user()->name) {
            username = client_state.user()->name.value();
        }
        authorization_error(stats, enforce_authorization, warn_authorization, fmt::format(
            "CREATE access on ALL KEYSPACES is denied to role {}", username));
    }
}

schema_ptr try_get_internal_table(const data_dictionary::database& db, std::string_view table_name) {
    size_t it = table_name.find(executor::INTERNAL_TABLE_PREFIX);
    if (it != 0) {
        return schema_ptr{};
    }
    table_name.remove_prefix(executor::INTERNAL_TABLE_PREFIX.size());
    size_t delim = table_name.find_first_of('.');
    if (delim == std::string_view::npos) {
        return schema_ptr{};
    }
    std::string_view ks_name = table_name.substr(0, delim);
    table_name.remove_prefix(ks_name.size() + 1);
    // Only internal keyspaces can be accessed to avoid leakage
    auto ks = db.try_find_keyspace(ks_name);
    if (!ks || !ks->is_internal()) {
        return schema_ptr{};
    }
    try {
        return db.find_schema(ks_name, table_name);
    } catch (data_dictionary::no_such_column_family&) {
        // DynamoDB returns validation error even when table does not exist
        // and the table name is invalid.
        validate_table_name(table_name);
        throw api_error::resource_not_found(
            fmt::format("Requested resource not found: Internal table: {}.{} not found", ks_name, table_name));
    }
}

schema_ptr get_table_from_batch_request(const service::storage_proxy& proxy, const rjson::value::ConstMemberIterator& batch_request) {
    sstring table_name = rjson::to_sstring(batch_request->name); // JSON keys are always strings
    try {
        return proxy.data_dictionary().find_schema(sstring(executor::KEYSPACE_NAME_PREFIX) + table_name, table_name);
    } catch(data_dictionary::no_such_column_family&) {
        // DynamoDB returns validation error even when table does not exist
        // and the table name is invalid.
        validate_table_name(table_name);
        throw api_error::resource_not_found(format("Requested resource not found: Table: {} not found", table_name));
    }
}

lw_shared_ptr<stats> get_stats_from_schema(service::storage_proxy& sp, const schema& schema) {
    try {
        replica::table& table = sp.local_db().find_column_family(schema.id());
        if (!table.get_stats().alternator_stats) {
            table.get_stats().alternator_stats = seastar::make_shared<table_stats>(schema.ks_name(), schema.cf_name());
        }
        return table.get_stats().alternator_stats->_stats;
    } catch (std::runtime_error&) {
        // If we're here it means that a table we are currently working on was deleted before the
        // operation completed, returning a temporary object is fine, if the table get deleted so will its metrics
        return make_lw_shared<stats>();
    }
}

void describe_single_item(const cql3::selection::selection& selection,
    const std::vector<managed_bytes_opt>& result_row,
    const std::optional<attrs_to_get>& attrs_to_get,
    rjson::value& item,
    uint64_t* item_length_in_bytes,
    bool include_all_embedded_attributes)
{
    const auto& columns = selection.get_columns();
    auto column_it = columns.begin();
    for (const managed_bytes_opt& cell : result_row) {
        if (!cell) {
            ++column_it;
            continue;
        }
        std::string column_name = (*column_it)->name_as_text();
        if (column_name != executor::ATTRS_COLUMN_NAME) {
            if (item_length_in_bytes) {
                (*item_length_in_bytes) += column_name.length() + cell->size();
            }
            if (!attrs_to_get || attrs_to_get->contains(column_name)) {
                // item is expected to start empty, and column_name are unique
                // so add() makes sense
                rjson::add_with_string_name(item, column_name, rjson::empty_object());
                rjson::value& field = item[column_name.c_str()];
                cell->with_linearized([&] (bytes_view linearized_cell) {
                    rjson::add_with_string_name(field, type_to_string((*column_it)->type), json_key_column_value(linearized_cell, **column_it));
                });
            }
        } else {
            auto deserialized = attrs_type()->deserialize(*cell);
            auto keys_and_values = value_cast<map_type_impl::native_type>(deserialized);
            for (auto entry : keys_and_values) {
                std::string attr_name = value_cast<sstring>(entry.first);
                if (item_length_in_bytes) {
                    (*item_length_in_bytes) += attr_name.length();
                }
                if (include_all_embedded_attributes || !attrs_to_get || attrs_to_get->contains(attr_name)) {
                    bytes value = value_cast<bytes>(entry.second);
                    if (item_length_in_bytes && value.length()) {
                        // ScyllaDB uses one extra byte compared to DynamoDB for the bytes length
                        (*item_length_in_bytes) += value.length() - 1;
                    }
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
                } else if (item_length_in_bytes) {
                    (*item_length_in_bytes) += value_cast<bytes>(entry.second).length() - 1;
                }
            }
        }
        ++column_it;
    }
}

std::optional<rjson::value> describe_single_item(schema_ptr schema,
        const query::partition_slice& slice,
        const cql3::selection::selection& selection,
        const query::result& query_result,
        const std::optional<attrs_to_get>& attrs_to_get,
        uint64_t* item_length_in_bytes) {
    rjson::value item = rjson::empty_object();

    cql3::selection::result_set_builder builder(selection, gc_clock::now());
    query::result_view::consume(query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, selection));

    auto result_set = builder.build();
    if (result_set->empty()) {
        if (item_length_in_bytes) {
            // empty results is counted as having a minimal length (e.g. 1 byte).
            (*item_length_in_bytes) += 1;
        }
        // If there is no matching item, we're supposed to return an empty
        // object without an Item member - not one with an empty Item member
        return {};
    }
    if (result_set->size() > 1) {
        // If the result set contains multiple rows, the code should have
        // called describe_multi_item(), not this function.
        throw std::logic_error("describe_single_item() asked to describe multiple items");
    }
    describe_single_item(selection, *result_set->rows().begin(), attrs_to_get, item, item_length_in_bytes);
    return item;
}

static void check_big_array(const rjson::value& val, int& size_left);
static void check_big_object(const rjson::value& val, int& size_left);

// For simplicity, we use a recursive implementation. This is fine because
// Alternator limits the depth of JSONs it reads from inputs, and doesn't
// add more than a couple of levels in its own output construction.
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

void validate_table_name(std::string_view name, const char* source) {
    if (name.length() < 3 || name.length() > max_table_name_length) {
        throw api_error::validation(
                format("{} must be at least 3 characters long and at most {} characters long", source, max_table_name_length));
    }
    if (!valid_table_name_chars(name)) {
        throw api_error::validation(
                format("{} must satisfy regular expression pattern: [a-zA-Z0-9_.-]+", source));
    }
}

void validate_cdc_log_name_length(std::string_view table_name) {
    if (cdc::log_name(table_name).length() > max_auxiliary_table_name_length) {
        // CDC will add cdc_log_suffix ("_scylla_cdc_log") to the table name
        // to create its log table, and this will exceed the maximum allowed
        // length. To provide a more helpful error message, we assume that
        // cdc::log_name() always adds a suffix of the same length.
        int suffix_len = cdc::log_name(table_name).length() - table_name.length();
        throw api_error::validation(fmt::format("Streams or vector search cannot be enabled on a table whose name is longer than {} characters: {}",
            max_auxiliary_table_name_length - suffix_len, table_name));
    }
}

body_writer make_streamed(rjson::value&& value) {
    return [value = std::move(value)](output_stream<char>&& _out) mutable -> future<> {
        auto out = std::move(_out);
        std::exception_ptr ex;
        try {
            co_await rjson::print(value, out);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        co_await rjson::destroy_gently(std::move(value));
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    };
}

void filter_batch_request_items_by_tbl_name(rjson::value& request, const audit::audit_table_set& tbl_name_filter) {
    rjson::value& items = request["RequestItems"];
    for (auto it = items.MemberBegin(); it != items.MemberEnd(); ) {
        auto table_name = rjson::to_string_view(it->name);
        auto found = std::ranges::any_of(tbl_name_filter, [table_name] (const auto& table) {
            return table.second == table_name;
        });
        if (!found) {
            it = items.EraseMember(it);
        } else {
            ++it;
        }
    }
}

} // namespace alternator
