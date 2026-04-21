/*
 * Copyright 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// This file implements the Alternator read operations: GetItem, BatchGetItem,
// Query (including vector search) and Scan.
// Public entry points:
//   * executor::get_item()
//   * executor::batch_get_item()
//   * executor::scan()
//   * executor::query()
// Major internal functions:
//   * do_query(): the common code for Query and Scan, except vector search.
//   * query_vector(): the vector-search code path for Query with VectorSearch.
// and a number of helper functions for parsing common parameters of read
// requests such as TableName, IndexName, Select, FilterExpression,
// ConsistentRead, ProjectionExpression, and more.

#include "alternator/executor.hh"
#include "alternator/executor_util.hh"
#include "alternator/conditions.hh"
#include "alternator/expressions.hh"
#include "alternator/consumed_capacity.hh"
#include "alternator/serialization.hh"
#include "alternator/attribute_path.hh"
#include "auth/permission.hh"
#include "cql3/selection/selection.hh"
#include "cql3/result_set.hh"
#include "query/query-request.hh"
#include "schema/schema.hh"
#include "service/client_state.hh"
#include "service/pager/query_pagers.hh"
#include "service/storage_proxy.hh"
#include "index/secondary_index.hh"
#include "utils/assert.hh"
#include "utils/overloaded_functor.hh"
#include "utils/error_injection.hh"
#include "vector_search/vector_store_client.hh"
#include <seastar/core/abort_on_expiry.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <boost/range/algorithm/find_end.hpp>
#include <charconv>
#include <stdexcept>

using namespace std::chrono_literals;

namespace alternator {

extern logging::logger elogger; // from executor.cc

// make_streamed_with_extra_array() is variant of make_streamed() above, which
// builds a streaming response (a function writing to an output stream) from a
// JSON object (rjson::value) but adds to it at the end an additional array.
// The extra array is given a separate chunked_vector to avoid putting it
// inside the rjson::value - because RapidJSON does contiguous allocations for
// arrays which we want to avoid for potentially long arrays in Query/Scan
// responses (see #23535).
// If we ever fix RapidJSON to avoid contiguous allocations for arrays, or
// replace it entirely (#24458), we can remove this function and the function
// rjson::print_with_extra_array() which it calls.
static body_writer make_streamed_with_extra_array(rjson::value&& value,
        std::string array_name, utils::chunked_vector<rjson::value>&& array) {
    return [value = std::move(value), array_name = std::move(array_name), array = std::move(array)](output_stream<char>&& _out) mutable -> future<> {
        auto out = std::move(_out);
        std::exception_ptr ex;
        try {
            co_await rjson::print_with_extra_array(value, array_name, array, out);
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        co_await rjson::destroy_gently(std::move(value));
        // TODO: can/should we also destroy the array gently?
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    };
}

// select_type represents how the Select parameter of Query/Scan selects what
// to return. It is also used by calculate_attrs_to_get() to know whether to
// return no attributes (count), or specific attributes.
enum class select_type { regular, count, projection };

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

// attrs_to_get saves for each top-level attribute an attrs_to_get_node,
// a hierarchy of subparts that need to be kept. The following function
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
static std::optional<attrs_to_get> calculate_attrs_to_get(const rjson::value& req, parsed::expression_cache& parsed_expression_cache, std::unordered_set<std::string>& used_attribute_names, select_type select = select_type::regular) {
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
            attribute_path_map_add("AttributesToGet", ret, rjson::to_string(*it));
            validate_attr_name_length("AttributesToGet", it->GetStringLength(), false);
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
            paths_to_get = parsed_expression_cache.parse_projection_expression(rjson::to_string_view(projection_expression));
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
    // A disengaged optional asks to read everything
    return std::nullopt;
}

// get_table_or_view() is similar to to get_table(), except it returns either
// a table or a materialized view from which to read, based on the TableName
// and optional IndexName in the request. Only requests like Query and Scan
// which allow IndexName should use this function.
enum class table_or_view_type { base, lsi, gsi, vector_index };
static std::pair<schema_ptr, table_or_view_type>
get_table_or_view(service::storage_proxy& proxy, const rjson::value& request) {
    table_or_view_type type = table_or_view_type::base;
    std::string table_name = get_table_name(request);

    if (schema_ptr s = try_get_internal_table(proxy.data_dictionary(), table_name)) {
        return {s, type};
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
                    fmt::format("Non-string IndexName '{}'", rjson::to_string_view(*index_name)));
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
            auto base_table = proxy.data_dictionary().try_find_table(keyspace_name, orig_table_name);
            if (base_table) {
                // If the given IndexName is a vector index, not a GSI or LSI,
                // give a more helpful message than just an index not found.
                if (base_table->schema()->has_index(rjson::to_sstring(*index_name))) {
                    throw api_error::validation(
                        fmt::format("IndexName '{}' is a vector index for table '{}, so VectorSearch is mandatory in Query.", rjson::to_string_view(*index_name), orig_table_name));
                }
                throw api_error::validation(
                    fmt::format("Requested resource not found: Index '{}' for table '{}'", rjson::to_string_view(*index_name), orig_table_name));
            } else {
                throw api_error::resource_not_found(
                    fmt::format("Requested resource not found: Table: {} not found", orig_table_name));
            }
        } else {
            throw api_error::resource_not_found(
                fmt::format("Requested resource not found: Table: {} not found", table_name));
        }
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
static select_type parse_select(const rjson::value& request, table_or_view_type table_type) {
    const rjson::value* select_value = rjson::find(request, "Select");
    const bool has_attributes_to_get = request.HasMember("AttributesToGet");
    const bool has_projection_expression = request.HasMember("ProjectionExpression");
    if (!select_value) {
        // "Select" is not specified:
        // If ProjectionExpression or AttributesToGet are present,
        // then Select defaults to SPECIFIC_ATTRIBUTES:
        if (has_projection_expression || has_attributes_to_get) {
            return select_type::regular;
        }
        // Otherwise, "Select" defaults to ALL_ATTRIBUTES on a base table,
        // or ALL_PROJECTED_ATTRIBUTES on an index. This is explicitly
        // documented in the DynamoDB API reference.
        return table_type == table_or_view_type::base ?
            select_type::regular : select_type::projection;
    }
    if (!select_value->IsString()) {
        throw api_error::validation("Select parameter must be a string");
    }
    std::string_view select = rjson::to_string_view(*select_value);
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
    throw api_error::validation(fmt::format("Unknown Select value '{}'. Allowed choices: ALL_ATTRIBUTES, SPECIFIC_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES, COUNT",
        select));
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
    filter(parsed::expression_cache& parsed_expression_cache, const rjson::value& request, request_type rt,
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

filter::filter(parsed::expression_cache& parsed_expression_cache, const rjson::value& request, request_type rt,
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
            auto parsed = parsed_expression_cache.parse_condition_expression(rjson::to_string_view(*expression), "FilterExpression");
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
    // _items is a chunked_vector<rjson::value> instead of a RapidJson array
    // (rjson::value) because unfortunately RapidJson arrays are stored
    // contiguously in memory, and cause large allocations when a Query/Scan
    // returns a long list of short items (issue #23535).
    utils::chunked_vector<rjson::value> _items;
    size_t _scanned_count;

public:
    describe_items_visitor(const columns_t& columns, const std::optional<attrs_to_get>& attrs_to_get, filter& filter)
            : _columns(columns)
            , _attrs_to_get(attrs_to_get)
            , _filter(filter)
            , _column_it(columns.begin())
            , _item(rjson::empty_object())
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

            _items.push_back(std::move(_item));
        }
        _item = rjson::empty_object();
        ++_scanned_count;
    }

    utils::chunked_vector<rjson::value> get_items() && {
        return std::move(_items);
    }

    size_t get_scanned_count() {
        return _scanned_count;
    }
};

// describe_items() returns a JSON object that includes members "Count"
// and "ScannedCount", but *not* "Items" - that is returned separately
// as a chunked_vector to avoid large contiguous allocations which
// RapidJSON does of its array. The caller should add "Items" to the
// returned JSON object if needed, or print it separately.
// The returned chunked_vector (the items) is std::optional<>, because
// the user may have requested only to count items, and not return any
// items - which is different from returning an empty list of items.
static future<std::tuple<rjson::value, std::optional<utils::chunked_vector<rjson::value>>, size_t>> describe_items(
        const cql3::selection::selection& selection,
        std::unique_ptr<cql3::result_set> result_set,
        std::optional<attrs_to_get>&& attrs_to_get,
        filter&& filter) {
    describe_items_visitor visitor(selection.get_columns(), attrs_to_get, filter);
    co_await result_set->visit_gently(visitor);
    auto scanned_count = visitor.get_scanned_count();
    utils::chunked_vector<rjson::value> items = std::move(visitor).get_items();
    rjson::value items_descr = rjson::empty_object();
    auto size = items.size();
    rjson::add(items_descr, "Count", rjson::value(size));
    rjson::add(items_descr, "ScannedCount", rjson::value(scanned_count));
    // If attrs_to_get && attrs_to_get->empty(), this means the user asked not
    // to get any attributes (i.e., a Scan or Query with Select=COUNT) and we
    // shouldn't return "Items" at all.
    // TODO: consider optimizing the case of Select=COUNT without a filter.
    // In that case, we currently build a list of empty items and here drop
    // it. We could just count the items and not bother with the empty items.
    // (However, remember that when we do have a filter, we need the items).
    std::optional<utils::chunked_vector<rjson::value>> opt_items;
    if (!attrs_to_get || !attrs_to_get->empty()) {
        opt_items = std::move(items);
    }
    co_return std::tuple(std::move(items_descr), std::move(opt_items), size);
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
        // Alternator itself allows at most one column in clustering key, but 
        // user can use Alternator api to access system tables which might have
        // multiple clustering key columns. So we need to handle that case here.
        auto cdef_it = schema.clustering_key_columns().begin();        
        for(const auto &exploded_ck : pos.key().explode()) {
            rjson::add_with_string_name(last_evaluated_key, std::string_view(cdef_it->name_as_text()), rjson::empty_object());
            rjson::value& key_entry = last_evaluated_key[cdef_it->name_as_text()];
            rjson::add_with_string_name(key_entry, type_to_string(cdef_it->type), json_key_column_value(exploded_ck, *cdef_it));
            ++cdef_it;
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

// RapidJSON allocates arrays contiguously in memory, so we want to avoid
// returning a large number of items as a single rapidjson array, and use
// a chunked_vector instead. The following constant is an arbitrary cutoff
// point for when to switch from a rapidjson array to a chunked_vector.
static constexpr int max_items_for_rapidjson_array = 256;

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
        alternator::stats& stats,
        tracing::trace_state_ptr trace_state,
        service_permit permit,
        bool enforce_authorization,
        bool warn_authorization) {
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

    co_await verify_permission(enforce_authorization, warn_authorization, client_state, table_schema, auth::permission::SELECT, stats);

    auto regular_columns =
            table_schema->regular_columns() | std::views::transform(&column_definition::id)
            | std::ranges::to<query::column_id_vector>();
    auto static_columns =
            table_schema->static_columns() | std::views::transform(&column_definition::id)
            | std::ranges::to<query::column_id_vector>();
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
    auto [items_descr, opt_items, size] = co_await describe_items(*selection, std::move(rs), std::move(attrs_to_get), std::move(filter));
    if (paging_state) {
        rjson::add(items_descr, "LastEvaluatedKey", encode_paging_state(*table_schema, *paging_state));
    }
    if (has_filter) {
        stats.cql_stats.filtered_rows_read_total += p->stats().rows_read_total;
        // update our "filtered_row_matched_total" for all the rows matched, despited the filter
        stats.cql_stats.filtered_rows_matched_total += size;
    }
    if (opt_items) {
        if (opt_items->size() >= max_items_for_rapidjson_array) {
            // There are many items, better print the JSON and the array of
            // items (opt_items) separately to avoid RapidJSON's contiguous
            // allocation of arrays.
            co_return make_streamed_with_extra_array(std::move(items_descr), "Items", std::move(*opt_items));
        }
        // There aren't many items in the chunked vector opt_items,
        // let's just insert them into the JSON object and print the
        // full JSON normally.
        rjson::value items_json = rjson::empty_array();
        for (auto& item : *opt_items) {
            rjson::push_back(items_json, std::move(item));
        }
        rjson::add(items_descr, "Items", std::move(items_json));
    }
    if (is_big(items_descr)) {
        co_return make_streamed(std::move(items_descr));
    }
    co_return rjson::print(std::move(items_descr));
}

static dht::token token_for_segment(int segment, int total_segments) {
    throwing_assert(total_segments > 1 && segment >= 0 && segment < total_segments);
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

future<executor::request_return_type> executor::scan(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.scan++;
    elogger.trace("Scanning {}", request);

    auto [schema, table_type] = get_table_or_view(_proxy, request);
    db::consistency_level cl = get_read_consistency(request);
    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(), schema->cf_name(), "Scan", request, cl);
    tracing::add_alternator_table_name(trace_state, schema->cf_name());
    get_stats_from_schema(_proxy, *schema)->api_operations.scan++;
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
    auto attrs_to_get = calculate_attrs_to_get(request, *_parsed_expression_cache, used_attribute_names, select);

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

    filter filter(*_parsed_expression_cache, request, filter::request_type::SCAN, used_attribute_names, used_attribute_values);
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
            std::move(filter), query::partition_slice::option_set(), client_state, _stats, trace_state, std::move(permit), _enforce_authorization, _warn_authorization);
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
            throw api_error::validation(fmt::format("BEGINS_WITH operator cannot be applied to type {}", type_to_string(ck_cdef.type)));
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
        sstring key = rjson::to_sstring(it->name);
        const rjson::value& condition = it->value;

        const rjson::value& comp_definition = rjson::get(condition, "ComparisonOperator");
        const rjson::value& attr_list = rjson::get(condition, "AttributeValueList");

        const column_definition& pk_cdef = schema->partition_key_columns().front();
        const column_definition* ck_cdef = schema->clustering_key_size() > 0 ? &schema->clustering_key_columns().front() : nullptr;
        if (key == pk_cdef.name_as_text()) {
            if (!partition_ranges.empty()) {
                throw api_error::validation("Currently only a single restriction per key is allowed");
            }
            partition_ranges.push_back(calculate_pk_bound(schema, pk_cdef, comp_definition, attr_list));
        }
        if (ck_cdef && key == ck_cdef->name_as_text()) {
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
                    fmt::format("ExpressionAttributeNames missing, entry '{}' required by KeyConditionExpression",
                            column_name));
        }
        const rjson::value* value = rjson::find(*expression_attribute_names, column_name);
        if (!value || !value->IsString()) {
            throw api_error::validation(
                    fmt::format("ExpressionAttributeNames missing entry '{}' required by KeyConditionExpression",
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
        std::unordered_set<std::string>& used_attribute_names,
        parsed::expression_cache& parsed_expression_cache)
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
        p = parsed_expression_cache.parse_condition_expression(rjson::to_string_view(expression), "KeyConditionExpression");
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
                        fmt::format("KeyConditionExpression function '{}' not supported",f->_function_name));
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
                        fmt::format("KeyConditionExpression condition on non-key attribute {}", key));
            }
            continue;
        }
        // If we're still here, it's any other operator besides EQ, and these
        // are allowed *only* on the clustering key:
        if (sstring(key) == pk_cdef.name_as_text()) {
            throw api_error::validation(
                    fmt::format("KeyConditionExpression only '=' condition is supported on partition key {}", key));
        } else if (!ck_cdef || sstring(key) != ck_cdef->name_as_text()) {
            throw api_error::validation(
                    fmt::format("KeyConditionExpression condition on non-key attribute {}", key));
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
                        fmt::format("KeyConditionExpression begins_with() not supported on type {}",
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

static future<executor::request_return_type> query_vector(
        service::storage_proxy& proxy,
        vector_search::vector_store_client& vsc,
        rjson::value request,
        service::client_state& client_state,
        tracing::trace_state_ptr trace_state,
        service_permit permit,
        bool enforce_authorization,
        bool warn_authorization,
        alternator::stats& stats,
        parsed::expression_cache& parsed_expr_cache) {
    schema_ptr base_schema = get_table(proxy, request);
    get_stats_from_schema(proxy, *base_schema)->api_operations.query++;
    tracing::add_alternator_table_name(trace_state, base_schema->cf_name());

    // If vector search is requested, IndexName must be given and must
    // refer to a vector index - not to a GSI or LSI.
    const rjson::value* index_name_v = rjson::find(request, "IndexName");
    if (!index_name_v || !index_name_v->IsString()) {
        co_return api_error::validation(
            "VectorSearch requires IndexName referring to a vector index");
    }
    std::string_view index_name = rjson::to_string_view(*index_name_v);
    int dimensions = 0;
    bool is_vector = false;
    for (const index_metadata& im : base_schema->indices()) {
        if (im.name() == index_name) {
            const auto& opts = im.options();
            // The only secondary index we expect to see is a vector index.
            // We also expect it to have a valid "dimensions".
            auto it = opts.find(db::index::secondary_index::custom_class_option_name);
            if (it == opts.end() || it->second != "vector_index") {
                on_internal_error(elogger, fmt::format("IndexName '{}' is a secondary index but not a vector index.", index_name));
            }
            it = opts.find("dimensions");
            if (it != opts.end()) {
                try {
                    dimensions = std::stoi(it->second);
                } catch (std::logic_error&) {}
            }
            throwing_assert(dimensions > 0);
            is_vector = true;
            break;
        }
    }
    if (!is_vector) {
        co_return api_error::validation(
            format("VectorSearch IndexName '{}' is not a vector index.", index_name));
    }
    // QueryVector is required inside VectorSearch.
    const rjson::value* vector_search = rjson::find(request, "VectorSearch");
    if (!vector_search || !vector_search->IsObject()) {
        co_return api_error::validation(
            "VectorSearch requires a VectorSearch parameter");
    }
    const rjson::value* query_vector = rjson::find(*vector_search, "QueryVector");
    if (!query_vector || !query_vector->IsObject()) {
        co_return api_error::validation(
            "VectorSearch requires a QueryVector parameter");
    }
    // QueryVector should be is a DynamoDB value, which must be of type "L"
    // (a list), containing only elements of type "N" (numbers). The number
    // of these elements must be exactly the "dimensions" defined for this
    // vector index. We'll now validate all these assumptions and parse
    // all the numbers in the vector into an std::vector<float> query_vec -
    // the type that ann() wants.
    const rjson::value* qv_list = rjson::find(*query_vector, "L");
    if (!qv_list || !qv_list->IsArray()) {
        co_return api_error::validation(
            "VectorSearch QueryVector must be a list of numbers");
    }
    const auto& arr = qv_list->GetArray();
    if ((int)arr.Size() != dimensions) {
        co_return api_error::validation(
            format("VectorSearch QueryVector length {} does not match index Dimensions {}",
                arr.Size(), dimensions));
    }
    std::vector<float> query_vec;
    query_vec.reserve(arr.Size());
    for (const rjson::value& elem : arr) {
        if (!elem.IsObject()) {
            co_return api_error::validation(
                "VectorSearch QueryVector must contain only numbers");
        }
        const rjson::value* n_val = rjson::find(elem, "N");
        if (!n_val || !n_val->IsString()) {
            co_return api_error::validation(
                "VectorSearch QueryVector must contain only numbers");
        }
        std::string_view num_str = rjson::to_string_view(*n_val);
        float f;
        auto [ptr, ec] = std::from_chars(num_str.data(), num_str.data() + num_str.size(), f);
        if (ec != std::errc{} || ptr != num_str.data() + num_str.size() || !std::isfinite(f)) {
            co_return api_error::validation(
                format("VectorSearch QueryVector element '{}' is not a valid number", num_str));
        }
        query_vec.push_back(f);
    }

    // Limit is mandatory for vector search: it defines k, the number of
    // nearest neighbors to return.
    const rjson::value* limit_json = rjson::find(request, "Limit");
    if (!limit_json || !limit_json->IsUint()) {
        co_return api_error::validation("VectorSearch requires a positive integer Limit parameter");
    }
    uint32_t limit = limit_json->GetUint();
    if (limit == 0) {
        co_return api_error::validation("Limit must be greater than 0");
    }

    // Consistent reads are not supported for vector search, just like GSI.
    if (get_read_consistency(request) != db::consistency_level::LOCAL_ONE) {
        co_return api_error::validation(
            "Consistent reads are not allowed on vector indexes");
    }

    // Pagination (ExclusiveStartKey) is not supported for vector search.
    if (rjson::find(request, "ExclusiveStartKey")) {
        co_return api_error::validation(
            "VectorSearch does not support pagination (ExclusiveStartKey)");
    }

    // ScanIndexForward is not supported for vector search: the ordering of
    // results is determined by vector distance, not by the sort key.
    if (rjson::find(request, "ScanIndexForward")) {
        co_return api_error::validation(
            "VectorSearch does not support ScanIndexForward");
    }

    std::unordered_set<std::string> used_attribute_names;
    std::unordered_set<std::string> used_attribute_values;
    // Parse the Select parameter and determine which attributes to return.
    // For a vector index, the default Select is ALL_ATTRIBUTES (full items).
    // ALL_PROJECTED_ATTRIBUTES is significantly more efficient because it
    // returns what the vector store returned without looking up additional
    // base-table data. Currently only the primary key attributes are projected
    // but in the future we'll implement projecting additional attributes into
    // the vector index - these additional attributes will also be usable for
    // filtering). COUNT returns only the count without items.
    select_type select = parse_select(request, table_or_view_type::vector_index);
    std::optional<alternator::attrs_to_get> attrs_to_get_opt;
    if (select == select_type::projection) {
        // ALL_PROJECTED_ATTRIBUTES for a vector index: return only key attributes.
        alternator::attrs_to_get key_attrs;
        for (const column_definition& cdef : base_schema->partition_key_columns()) {
            attribute_path_map_add("Select", key_attrs, cdef.name_as_text());
        }
        for (const column_definition& cdef : base_schema->clustering_key_columns()) {
            attribute_path_map_add("Select", key_attrs, cdef.name_as_text());
        }
        attrs_to_get_opt = std::move(key_attrs);
    } else {
        attrs_to_get_opt = calculate_attrs_to_get(request, parsed_expr_cache, used_attribute_names, select);
    }
    // QueryFilter (the old-style API) is not supported for vector search Queries.
    if (rjson::find(request, "QueryFilter")) {
        co_return api_error::validation(
            "VectorSearch does not support QueryFilter; use FilterExpression instead");
    }
    // FilterExpression: post-filter the vector search results by any attribute.
    filter flt(parsed_expr_cache, request, filter::request_type::QUERY,
               used_attribute_names, used_attribute_values);
    const rjson::value* expression_attribute_names = rjson::find(request, "ExpressionAttributeNames");
    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "Query");
    const rjson::value* expression_attribute_values = rjson::find(request, "ExpressionAttributeValues");
    verify_all_are_used(expression_attribute_values, used_attribute_values, "ExpressionAttributeValues", "Query");

    // Verify the user has SELECT permission on the base table, as we
    // do for every type of read operation after validating the input
    // parameters.
    co_await verify_permission(enforce_authorization, warn_authorization,
            client_state, base_schema, auth::permission::SELECT, stats);

    // Query the vector store for the approximate nearest neighbors.
    auto timeout = executor::default_timeout();
    abort_on_expiry aoe(timeout);
    rjson::value pre_filter = rjson::empty_object(); // TODO, implement
    auto pkeys_result = co_await vsc.ann(
            base_schema->ks_name(), std::string(index_name), base_schema,
            std::move(query_vec), limit, pre_filter, aoe.abort_source());
    if (!pkeys_result.has_value()) {
        const sstring error_msg = std::visit(vector_search::error_visitor{}, pkeys_result.error());
        co_return api_error::validation(error_msg);
    }
    const std::vector<vector_search::primary_key>& pkeys = pkeys_result.value();

    // For SELECT=COUNT with no filter: skip fetching from the base table and
    // just return the count of candidates returned by the vector store.
    // If a filter is present, fall through to the base-table fetch to apply it.
    if (select == select_type::count && !flt) {
        rjson::value response = rjson::empty_object();
        rjson::add(response, "Count", rjson::value(static_cast<int>(pkeys.size())));
        rjson::add(response, "ScannedCount", rjson::value(static_cast<int>(pkeys.size())));
        co_return rjson::print(std::move(response));
    }

    // For SELECT=ALL_PROJECTED_ATTRIBUTES with no filter: skip fetching from
    // the base table and build items directly from the key columns returned by
    // the vector store. If a filter is present, fall through to the base-table
    // fetch to apply it.
    if (select == select_type::projection && !flt) {
        rjson::value items_json = rjson::empty_array();
        for (const auto& pkey : pkeys) {
            rjson::value item = rjson::empty_object();
            std::vector<bytes> exploded_pk = pkey.partition.key().explode();
            auto exploded_pk_it = exploded_pk.begin();
            for (const column_definition& cdef : base_schema->partition_key_columns()) {
                rjson::value key_val = rjson::empty_object();
                rjson::add_with_string_name(key_val, type_to_string(cdef.type), json_key_column_value(*exploded_pk_it, cdef));
                rjson::add_with_string_name(item, std::string_view(cdef.name_as_text()), std::move(key_val));
                ++exploded_pk_it;
            }
            if (base_schema->clustering_key_size() > 0) {
                std::vector<bytes> exploded_ck = pkey.clustering.explode();
                auto exploded_ck_it = exploded_ck.begin();
                for (const column_definition& cdef : base_schema->clustering_key_columns()) {
                    rjson::value key_val = rjson::empty_object();
                    rjson::add_with_string_name(key_val, type_to_string(cdef.type), json_key_column_value(*exploded_ck_it, cdef));
                    rjson::add_with_string_name(item, std::string_view(cdef.name_as_text()), std::move(key_val));
                    ++exploded_ck_it;
                }
            }
            rjson::push_back(items_json, std::move(item));
        }
        rjson::value response = rjson::empty_object();
        rjson::add(response, "Count", rjson::value(static_cast<int>(items_json.Size())));
        rjson::add(response, "ScannedCount", rjson::value(static_cast<int>(pkeys.size())));
        rjson::add(response, "Items", std::move(items_json));
        co_return rjson::print(std::move(response));
    }

    // TODO: For SELECT=SPECIFIC_ATTRIBUTES, if they are part of the projected
    // attributes, we should use the above optimized code path - not fall through
    // to the read from the base table as below as we need to do if the specific
    // attributes contain non-projected columns.

    // Fetch the matching items from the base table and build the response.
    // When a filter is present, we always fetch the full item so that all
    // attributes are available for filter evaluation, regardless of the
    // projection required for the final response.
    auto selection = cql3::selection::selection::wildcard(base_schema);
    auto regular_columns = base_schema->regular_columns()
            | std::views::transform(&column_definition::id)
            | std::ranges::to<query::column_id_vector>();
    auto attrs_to_get = ::make_shared<const std::optional<alternator::attrs_to_get>>(
        flt ? std::nullopt : std::move(attrs_to_get_opt));

    rjson::value items_json = rjson::empty_array();
    int matched_count = 0;

    // Query each primary key individually, in the order returned by the
    // vector store, to preserve vector-distance ordering in the response.
    // FIXME: do this more efficiently with a batched read that preserves ordering.
    for (const auto& pkey : pkeys) {
        std::vector<query::clustering_range> bounds{
                base_schema->clustering_key_size() > 0
                    ? query::clustering_range::make_singular(pkey.clustering)
                    : query::clustering_range::make_open_ended_both_sides()};
        auto partition_slice = query::partition_slice(std::move(bounds), {},
                regular_columns, selection->get_query_options());
        auto command = ::make_lw_shared<query::read_command>(
                base_schema->id(), base_schema->version(), partition_slice,
                proxy.get_max_result_size(partition_slice),
                query::tombstone_limit(proxy.get_tombstone_limit()));
        service::storage_proxy::coordinator_query_result qr =
                co_await proxy.query(base_schema, command,
                        {dht::partition_range(pkey.partition)},
                        db::consistency_level::LOCAL_ONE,
                        service::storage_proxy::coordinator_query_options(
                                timeout, permit, client_state, trace_state));
        auto opt_item = describe_single_item(base_schema, partition_slice,
                *selection, *qr.query_result, *attrs_to_get);
        if (opt_item && (!flt || flt.check(*opt_item))) {
            ++matched_count;
            if (select != select_type::count) {
                if (select == select_type::projection) {
                    // A filter caused us to fall through here instead of
                    // taking the projection early-exit above. Reconstruct
                    // the key-only item from the full item we fetched.
                    rjson::value key_item = rjson::empty_object();
                    for (const column_definition& cdef : base_schema->partition_key_columns()) {
                        if (const rjson::value* v = rjson::find(*opt_item, cdef.name_as_text())) {
                            rjson::add_with_string_name(key_item, cdef.name_as_text(), rjson::copy(*v));
                        }
                    }
                    for (const column_definition& cdef : base_schema->clustering_key_columns()) {
                        if (const rjson::value* v = rjson::find(*opt_item, cdef.name_as_text())) {
                            rjson::add_with_string_name(key_item, cdef.name_as_text(), rjson::copy(*v));
                        }
                    }
                    rjson::push_back(items_json, std::move(key_item));
                } else {
                    // When a filter caused us to fetch the full item, apply the
                    // requested projection (attrs_to_get_opt) before returning it.
                    // This mirrors describe_items_visitor::end_row() which removes
                    // extra filter attributes from the returned item.
                    if (flt && attrs_to_get_opt) {
                        for (const auto& [attr_name, subpath] : *attrs_to_get_opt) {
                            if (!subpath.has_value()) {
                                if (rjson::value* toplevel = rjson::find(*opt_item, attr_name)) {
                                    if (!hierarchy_filter(*toplevel, subpath)) {
                                        rjson::remove_member(*opt_item, attr_name);
                                    }
                                }
                            }
                        }
                        std::vector<std::string> to_remove;
                        for (auto it = opt_item->MemberBegin(); it != opt_item->MemberEnd(); ++it) {
                            std::string key(it->name.GetString(), it->name.GetStringLength());
                            if (!attrs_to_get_opt->contains(key)) {
                                to_remove.push_back(std::move(key));
                            }
                        }
                        for (const auto& key : to_remove) {
                            rjson::remove_member(*opt_item, key);
                        }
                    }
                    rjson::push_back(items_json, std::move(*opt_item));
                }
            }
        }
    }

    rjson::value response = rjson::empty_object();
    if (select == select_type::count) {
        rjson::add(response, "Count", rjson::value(matched_count));
    } else {
        rjson::add(response, "Count", rjson::value(static_cast<int>(items_json.Size())));
        rjson::add(response, "Items", std::move(items_json));
    }
    rjson::add(response, "ScannedCount", rjson::value(static_cast<int>(pkeys.size())));
    co_return rjson::print(std::move(response));
}

future<executor::request_return_type> executor::query(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.query++;
    elogger.trace("Querying {}", request);

    if (rjson::find(request, "VectorSearch")) {
        // If vector search is requested, we have a separate code path.
        // IndexName must be given and must refer to a vector index - not
        // to a GSI or LSI as the code below assumes.
        return query_vector(_proxy, _vsc, std::move(request), client_state, trace_state, std::move(permit),
                _enforce_authorization, _warn_authorization, _stats, *_parsed_expression_cache);
    }

    auto [schema, table_type] = get_table_or_view(_proxy, request);
    db::consistency_level cl = get_read_consistency(request);
    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(), schema->cf_name(), "Query", request, cl);

    get_stats_from_schema(_proxy, *schema)->api_operations.query++;
    tracing::add_alternator_table_name(trace_state, schema->cf_name());

    rjson::value* exclusive_start_key = rjson::find(request, "ExclusiveStartKey");
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
                        used_attribute_names, *_parsed_expression_cache);

    filter filter(*_parsed_expression_cache, request, filter::request_type::QUERY,
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

    auto attrs_to_get = calculate_attrs_to_get(request, *_parsed_expression_cache, used_attribute_names, select);
    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "Query");
    verify_all_are_used(expression_attribute_values, used_attribute_values, "ExpressionAttributeValues", "Query");
    query::partition_slice::option_set opts;
    opts.set_if<query::partition_slice::option::reversed>(!forward);
    return do_query(_proxy, schema, exclusive_start_key, std::move(partition_ranges), std::move(ck_bounds), std::move(attrs_to_get), limit, cl,
            std::move(filter), opts, client_state, _stats, std::move(trace_state), std::move(permit), _enforce_authorization, _warn_authorization);
}

// Converts a multi-row selection result to JSON compatible with DynamoDB.
// For each row, this method calls item_callback, which takes the size of
// the item as the parameter.
static future<std::vector<rjson::value>> describe_multi_item(schema_ptr schema,
        const query::partition_slice&& slice,
        shared_ptr<cql3::selection::selection> selection,
        foreign_ptr<lw_shared_ptr<query::result>> query_result,
        shared_ptr<const std::optional<attrs_to_get>> attrs_to_get,
        noncopyable_function<void(uint64_t)> item_callback) {
    cql3::selection::result_set_builder builder(*selection, gc_clock::now());
    query::result_view::consume(*query_result, slice, cql3::selection::result_set_builder::visitor(builder, *schema, *selection));
    auto result_set = builder.build();
    std::vector<rjson::value> ret;
    for (auto& result_row : result_set->rows()) {
        rjson::value item = rjson::empty_object();
        uint64_t item_length_in_bytes = 0;
        describe_single_item(*selection, result_row, *attrs_to_get, item, &item_length_in_bytes);
        if (item_callback) {
            item_callback(item_length_in_bytes);
        }
        ret.push_back(std::move(item));
        co_await coroutine::maybe_yield();
    }
    co_return ret;
}

// describe_item() wraps the result of describe_single_item() by a map
// as needed by the GetItem request. It should not be used for other purposes,
// use describe_single_item() instead.
static rjson::value describe_item(schema_ptr schema,
        const query::partition_slice& slice,
        const cql3::selection::selection& selection,
        const query::result& query_result,
        const std::optional<attrs_to_get>& attrs_to_get,
        consumed_capacity_counter& consumed_capacity,
        uint64_t& metric) {
    std::optional<rjson::value> opt_item = describe_single_item(std::move(schema), slice, selection, std::move(query_result), attrs_to_get, &consumed_capacity._total_bytes);
    rjson::value item_descr = rjson::empty_object();
    if (opt_item) {
        rjson::add(item_descr, "Item", std::move(*opt_item));
    }
    consumed_capacity.add_consumed_capacity_to_response_if_needed(item_descr);
    metric += consumed_capacity.get_half_units();
    return item_descr;
}

future<executor::request_return_type> executor::get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
    _stats.api_operations.get_item++;
    auto start_time = std::chrono::steady_clock::now();
    elogger.trace("Getting item {}", request);

    schema_ptr schema = get_table(_proxy, request);
    lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *schema);
    per_table_stats->api_operations.get_item++;
    tracing::add_alternator_table_name(trace_state, schema->cf_name());

    rjson::value& query_key = request["Key"];
    db::consistency_level cl = get_read_consistency(request);

    maybe_audit(audit_info, audit::statement_category::QUERY, schema->ks_name(), schema->cf_name(), "GetItem", request, cl);

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
    auto regular_columns =
            schema->regular_columns() | std::views::transform(&column_definition::id)
            | std::ranges::to<query::column_id_vector>();

    auto selection = cql3::selection::selection::wildcard(schema);

    auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
    auto command = ::make_lw_shared<query::read_command>(schema->id(), schema->version(), partition_slice, _proxy.get_max_result_size(partition_slice),
            query::tombstone_limit(_proxy.get_tombstone_limit()));

    std::unordered_set<std::string> used_attribute_names;
    auto attrs_to_get = calculate_attrs_to_get(request, *_parsed_expression_cache, used_attribute_names);
    const rjson::value* expression_attribute_names = rjson::find(request, "ExpressionAttributeNames");
    verify_all_are_used(expression_attribute_names, used_attribute_names, "ExpressionAttributeNames", "GetItem");
    rcu_consumed_capacity_counter add_capacity(request, cl == db::consistency_level::LOCAL_QUORUM);
    co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, schema, auth::permission::SELECT, _stats);
    service::storage_proxy::coordinator_query_result qr =
        co_await _proxy.query(
            schema, std::move(command), std::move(partition_ranges), cl,
            service::storage_proxy::coordinator_query_options(executor::default_timeout(), std::move(permit), client_state, trace_state));
    per_table_stats->api_operations.get_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    _stats.api_operations.get_item_latency.mark(std::chrono::steady_clock::now() - start_time);
    uint64_t rcu_half_units = 0;
    rjson::value res = describe_item(schema, partition_slice, *selection, *qr.query_result, std::move(attrs_to_get), add_capacity, rcu_half_units);
    per_table_stats->rcu_half_units_total += rcu_half_units;
    _stats.rcu_half_units_total += rcu_half_units;
    // Update item size metrics only if we found an item.
    if (qr.query_result->row_count().value_or(0) > 0) {
        per_table_stats->operation_sizes.get_item_op_size_kb.add(bytes_to_kb_ceil(add_capacity._total_bytes));
    }
    co_return rjson::print(std::move(res));
}

future<executor::request_return_type> executor::batch_get_item(client_state& client_state, tracing::trace_state_ptr trace_state, service_permit permit, rjson::value request, std::unique_ptr<audit::audit_info_alternator>& audit_info) {
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
    bool should_add_rcu = rcu_consumed_capacity_counter::should_add_capacity(request);
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
    uint batch_size = 0;
    for (auto it = request_items.MemberBegin(); it != request_items.MemberEnd(); ++it) {
        table_requests rs(get_table_from_batch_request(_proxy, it));
        tracing::add_alternator_table_name(trace_state, rs.schema->cf_name());
        rs.cl = get_read_consistency(it->value);
        std::unordered_set<std::string> used_attribute_names;
        rs.attrs_to_get = ::make_shared<const std::optional<attrs_to_get>>(calculate_attrs_to_get(it->value, *_parsed_expression_cache, used_attribute_names));
        const rjson::value* expression_attribute_names = rjson::find(it->value, "ExpressionAttributeNames");
        verify_all_are_used(expression_attribute_names, used_attribute_names,"ExpressionAttributeNames", "GetItem");
        auto& keys = (it->value)["Keys"];
        for (rjson::value& key : keys.GetArray()) {
            rs.add(key);
            check_key(key, rs.schema);
        }
        batch_size += rs.requests.size();
        requests.emplace_back(std::move(rs));
    }

    for (const table_requests& tr : requests) {
        co_await verify_permission(_enforce_authorization, _warn_authorization, client_state, tr.schema, auth::permission::SELECT, _stats);
    }

    _stats.api_operations.batch_get_item_batch_total += batch_size;
    _stats.api_operations.batch_get_item_histogram.add(batch_size);
    // If we got here, all "requests" are valid, so let's start the
    // requests for the different partitions all in parallel.
    std::vector<future<std::vector<rjson::value>>> response_futures;
    std::vector<uint64_t> consumed_rcu_half_units_per_table(requests.size());
    for (size_t i = 0; i < requests.size(); i++) {
        const table_requests& rs = requests[i];
        bool is_quorum = rs.cl == db::consistency_level::LOCAL_QUORUM;
        lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *rs.schema);
        per_table_stats->api_operations.batch_get_item_histogram.add(rs.requests.size());
        for (const auto& [pk, cks] : rs.requests) {
            dht::partition_range_vector partition_ranges{dht::partition_range(dht::decorate_key(*rs.schema, pk))};
            std::vector<query::clustering_range> bounds;
            if (rs.schema->clustering_key_size() == 0) {
                bounds.push_back(query::clustering_range::make_open_ended_both_sides());
            } else {
                for (auto& ck : cks) {
                    bounds.push_back(query::clustering_range::make_singular(ck.first));
                }
            }
            auto regular_columns =
                    rs.schema->regular_columns() | std::views::transform(&column_definition::id)
                    | std::ranges::to<query::column_id_vector>();
            auto selection = cql3::selection::selection::wildcard(rs.schema);
            auto partition_slice = query::partition_slice(std::move(bounds), {}, std::move(regular_columns), selection->get_query_options());
            auto command = ::make_lw_shared<query::read_command>(rs.schema->id(), rs.schema->version(), partition_slice, _proxy.get_max_result_size(partition_slice),
                    query::tombstone_limit(_proxy.get_tombstone_limit()));
            command->allow_limit = db::allow_per_partition_rate_limit::yes;
            const auto item_callback = [is_quorum, per_table_stats, &rcus_per_table = consumed_rcu_half_units_per_table[i]](uint64_t size) {
                rcus_per_table += rcu_consumed_capacity_counter::get_half_units(size, is_quorum);
                // Update item size only if the item exists.
                if (size > 0) {
                    per_table_stats->operation_sizes.batch_get_item_op_size_kb.add(bytes_to_kb_ceil(size));
                }
            };
            future<std::vector<rjson::value>> f = _proxy.query(rs.schema, std::move(command), std::move(partition_ranges), rs.cl,
                    service::storage_proxy::coordinator_query_options(executor::default_timeout(), permit, client_state, trace_state)).then(
                    [schema = rs.schema, partition_slice = std::move(partition_slice), selection = std::move(selection), attrs_to_get = rs.attrs_to_get, item_callback = std::move(item_callback)] (service::storage_proxy::coordinator_query_result qr) mutable {
                utils::get_local_injector().inject("alternator_batch_get_item", [] { throw std::runtime_error("batch_get_item injection"); });
                return describe_multi_item(std::move(schema), std::move(partition_slice), std::move(selection), std::move(qr.query_result), std::move(attrs_to_get), std::move(item_callback));
            });
            response_futures.push_back(std::move(f));
        }
    }

    // Wait for all requests to complete, and then return the response.
    // In case of full failure (no reads succeeded), an arbitrary error
    // from one of the operations will be returned.
    bool some_succeeded = false;
    std::exception_ptr eptr;
    std::set<sstring> table_names; // for auditing
    // FIXME: will_log() here doesn't pass keyspace/table, so keyspace-level audit
    // filtering is bypassed — a batch spanning multiple tables is audited as a whole.
    bool should_audit = _audit.local_is_initialized() && _audit.local().will_log(audit::statement_category::QUERY);
    rjson::value response = rjson::empty_object();
    rjson::add(response, "Responses", rjson::empty_object());
    rjson::add(response, "UnprocessedKeys", rjson::empty_object());
    auto fut_it = response_futures.begin();
    rjson::value consumed_capacity = rjson::empty_array();
    for (size_t i = 0; i < requests.size(); i++) {
        const table_requests& rs = requests[i];
        std::string table = rs.schema->cf_name();
        if (should_audit) {
            table_names.insert(table);
        }
        for (const auto& [_, cks] : rs.requests) {
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
        uint64_t rcu_half_units = consumed_rcu_half_units_per_table[i];
        _stats.rcu_half_units_total += rcu_half_units;
        lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *rs.schema);
        per_table_stats->rcu_half_units_total += rcu_half_units;
        if (should_add_rcu) {
            rjson::value entry = rjson::empty_object();
            rjson::add(entry, "TableName", table);
            rjson::add(entry, "CapacityUnits", rcu_half_units*0.5);
            rjson::push_back(consumed_capacity, std::move(entry));
        }
    }

    if (should_add_rcu) {
        rjson::add(response, "ConsumedCapacity", std::move(consumed_capacity));
    }
    elogger.trace("Unprocessed keys: {}", response["UnprocessedKeys"]);
    // NOTE: Each table in the batch has its own CL (set by get_read_consistency()),
    // but the audit entry records a single CL for the whole batch. We use ANY as a
    // placeholder to indicate "mixed / not applicable".
    // FIXME: Auditing is executed only for a complete success
    maybe_audit(audit_info, audit::statement_category::QUERY, "",
                print_names_for_audit(table_names), "BatchGetItem", request, db::consistency_level::ANY);
    if (!some_succeeded && eptr) {
        co_await coroutine::return_exception_ptr(std::move(eptr));
    }
    auto duration = std::chrono::steady_clock::now() - start_time;
    _stats.api_operations.batch_get_item_latency.mark(duration);
    for (const table_requests& rs : requests) {
        lw_shared_ptr<stats> per_table_stats = get_stats_from_schema(_proxy, *rs.schema);
        per_table_stats->api_operations.batch_get_item_latency.mark(duration);
    }
    if (is_big(response)) {
        co_return make_streamed(std::move(response));
    } else {
        co_return rjson::print(std::move(response));
    }
}

} // namespace alternator
