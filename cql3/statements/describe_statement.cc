/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#include <memory>
#include <optional>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/iterator_range_core.hpp>

#include "cdc/cdc_options.hh"
#include "cdc/log.hh"
#include "cql3/column_specification.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/statements/prepared_statement.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "service/client_state.hh"
#include "types/types.hh"
#include "cql3/query_processor.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/raw/describe_statement.hh"
#include "cql3/statements/describe_statement.hh"
#include <seastar/core/shared_ptr.hh>
#include "transport/messages/result_message.hh"
#include "transport/messages/result_message_base.hh"
#include "service/query_state.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "data_dictionary/data_dictionary.hh"
#include "service/storage_proxy.hh"
#include "cql3/ut_name.hh"
#include "cql3/util.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "cql3/functions/functions.hh"
#include "view_info.hh"
#include "index/secondary_index_manager.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/user_aggregate.hh"
#include "utils/overloaded_functor.hh"
#include "data_dictionary/keyspace_element.hh"
#include "db/system_keyspace.hh"
#include "db/extensions.hh"
#include "utils/sorting.hh"
#include "replica/database.hh"

static logging::logger dlogger("describe");

bool is_internal_keyspace(std::string_view name);

namespace replica {
class database;
}

namespace cql3 {

namespace statements {

using keyspace_element = data_dictionary::keyspace_element;

namespace {

// Represents description of keyspace_element
struct description {
    sstring _keyspace;
    sstring _type;
    sstring _name;
    std::optional<sstring> _create_statement;

    // Description without create_statement
    description(replica::database& db, const keyspace_element& element)
        : _keyspace(util::maybe_quote(element.keypace_name()))
        , _type(element.element_type(db))
        , _name(util::maybe_quote(element.element_name()))
        , _create_statement(std::nullopt) {}

    // Description with create_statement
    description(replica::database& db, const keyspace_element& element, bool with_internals)
        : _keyspace(util::maybe_quote(element.keypace_name()))
        , _type(element.element_type(db))
        , _name(util::maybe_quote(element.element_name()))
    {
        std::ostringstream os;
        element.describe(db, os, with_internals);
        _create_statement = os.str();
    }

    description(replica::database& db, const keyspace_element& element, sstring create_statement)
        : _keyspace(util::maybe_quote(element.keypace_name()))
        , _type(element.element_type(db))
        , _name(util::maybe_quote(element.element_name()))
        , _create_statement(std::move(create_statement)) {}

    std::vector<bytes_opt> serialize() const {
        auto desc = std::vector<bytes_opt>{
            {to_bytes(_keyspace)},
            {to_bytes(_type)},
            {to_bytes(_name)}
        };

        if (_create_statement) {
            desc.push_back({to_bytes(*_create_statement)});
        }

        return desc;
    }
};

future<std::vector<description>> generate_descriptions(
    replica::database& db, 
    std::vector<shared_ptr<const keyspace_element>> elements,
    std::optional<bool> with_internals = std::nullopt,
    bool sort_by_name = true) 
{
    std::vector<description> descs;
    descs.reserve(elements.size());
    if (sort_by_name) {
        boost::sort(elements, [] (const auto& a, const auto& b) {
            return a->element_name() < b->element_name();
        });
    }

    for (auto& e: elements) {
        auto desc = (with_internals.has_value()) 
            ? description(db, *e, *with_internals) 
            : description(db, *e);
        descs.push_back(desc);

        co_await coroutine::maybe_yield();
    }
    co_return descs;
}

bool is_index(const data_dictionary::database& db, const schema_ptr& schema) {
    return db.find_column_family(schema->view_info()->base_id()).get_index_manager().is_index(*schema);
}

/**
 *  DESCRIBE FUNCTIONS
 *  
 *  - "plular" functions (types/functions/aggregates/tables) 
 *  Return list of all elements in a given keyspace. Returned descriptions
 *  don't contain create statements.
 *  
 *  - "singular" functions (keyspace/type/function/aggregate/view/index/table)
 *  Return description of element. The description contain create_statement.
 *  Those functions throw `invalid_request_exception` if element cannot be found.
 *  Note:
 *    - `table()` returns description of the table and its indexes and views
 *    - `function()` and `aggregate()` might return multiple descriptions 
 *      since keyspace and name don't identify function uniquely
 */



description keyspace(replica::database& db, const lw_shared_ptr<keyspace_metadata>& ks, bool with_stmt = false) {
    return (with_stmt) ? description(db, *ks, true) : description(db, *ks);
}

description type(replica::database& db, const lw_shared_ptr<keyspace_metadata>& ks, const sstring& name) {
    auto udt_meta = ks->user_types();
    if (!udt_meta.has_type(to_bytes(name))) {
        throw exceptions::invalid_request_exception(format("Type '{}' not found in keyspace '{}'", name, ks->name()));
    }

    auto udt = udt_meta.get_type(to_bytes(name));
    return description(db, *udt, true);
}

// Because UDTs can depend on each other, we need to sort them topologically
future<std::vector<shared_ptr<const keyspace_element>>> get_sorted_types(const lw_shared_ptr<keyspace_metadata>& ks) {
    struct udts_comparator {
        inline bool operator()(const user_type& a, const user_type& b) const {
            return a->get_name_as_string() < b->get_name_as_string();
        }
    };

    std::vector<user_type> all_udts;
    std::multimap<user_type, user_type, udts_comparator> adjacency;

    for (auto& [_, udt]: ks->user_types().get_all_types()) {
        all_udts.push_back(udt);
        for (auto& ref_udt: udt->get_all_referenced_user_types()) {
            adjacency.insert({ref_udt, udt});
        }
    }

    auto sorted = co_await utils::topological_sort(all_udts, adjacency);
    co_return boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(sorted);
}

future<std::vector<description>> types(replica::database& db, const lw_shared_ptr<keyspace_metadata>& ks, bool with_stmt = false) {
    auto udts = co_await get_sorted_types(ks);
    co_return co_await generate_descriptions(db, udts, (with_stmt) ? std::optional(true) : std::nullopt, false);
}
    
future<std::vector<description>> function(replica::database& db, const sstring& ks, const sstring& name) {
    auto fs = functions::instance().find(functions::function_name(ks, name));
    if (fs.empty()) {
        throw exceptions::invalid_request_exception(format("Function '{}' not found in keyspace '{}'", name, ks));
    }

    auto udfs = boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(fs | boost::adaptors::transformed([] (const auto& f) {
        return dynamic_pointer_cast<const functions::user_function>(f.second);
    }) | boost::adaptors::filtered([] (const auto& f) {
        return f != nullptr;
    }));
    if (udfs.empty()) {
        throw exceptions::invalid_request_exception(format("Function '{}' not found in keyspace '{}'", name, ks));
    }

    co_return co_await generate_descriptions(db, udfs, true);
}

future<std::vector<description>> functions(replica::database& db,const sstring& ks, bool with_stmt = false) {
    auto udfs = cql3::functions::instance().get_user_functions(ks);
    auto elements = boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(udfs);

    co_return co_await generate_descriptions(db, elements, (with_stmt) ? std::optional(true) : std::nullopt);
}

future<std::vector<description>> aggregate(replica::database& db, const sstring& ks, const sstring& name) {
    auto fs = functions::instance().find(functions::function_name(ks, name));
    if (fs.empty()) {
        throw exceptions::invalid_request_exception(format("Aggregate '{}' not found in keyspace '{}'", name, ks));
    }

    auto udas = boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(fs | boost::adaptors::transformed([] (const auto& f) {
        return dynamic_pointer_cast<const functions::user_aggregate>(f.second);
    }) | boost::adaptors::filtered([] (const auto& f) {
        return f != nullptr;
    }));
    if (udas.empty()) {
        throw exceptions::invalid_request_exception(format("Aggregate '{}' not found in keyspace '{}'", name, ks));
    }

    co_return co_await generate_descriptions(db, udas, true);
}

future<std::vector<description>> aggregates(replica::database& db, const sstring& ks, bool with_stmt = false) {
    auto udas = functions::instance().get_user_aggregates(ks);
    auto elements = boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(udas);

    co_return co_await generate_descriptions(db, elements, (with_stmt) ? std::optional(true) : std::nullopt);
}

description view(const data_dictionary::database& db, const sstring& ks, const sstring& name, bool with_internals) {
    auto view = db.try_find_table(ks, name);
    if (!view || !view->schema()->is_view()) {
        throw exceptions::invalid_request_exception(format("Materialized view '{}' not found in keyspace '{}'", name, ks));
    }

    return description(db.real_database(), *view->schema(), with_internals);
}

description index(const data_dictionary::database& db, const sstring& ks, const sstring& name, bool with_internals) {
    auto schema = db.find_indexed_table(ks, name);
    if (!schema) {
        throw exceptions::invalid_request_exception(format("Table for existing index '{}' not found in '{}'", name, ks));
    }

    std::optional<view_ptr> idx;
    auto views = db.find_table(ks, schema->cf_name()).views();
    for (const auto& view: views) {
        if (is_index(db, view) && secondary_index::index_name_from_table_name(view->cf_name()) == name) {
            idx = view;
            break;
        }
    }

    return description(db.real_database(), **idx, with_internals);
}

// `base_name` should be a table with enabled cdc
std::optional<description> describe_cdc_log_table(const data_dictionary::database& db, const sstring& ks, const sstring& base_name) {
    auto table = db.try_find_table(ks, cdc::log_name(base_name));
    if (!table) {
        dlogger.warn("Couldn't find cdc log table for base table {}.{}", ks, base_name);
        return std::nullopt;
    }

    std::ostringstream os;
    auto schema = table->schema();
    schema->describe_alter_with_properties(db.real_database(), os);
    return description(db.real_database(), *schema, std::move(os.str()));
}

future<std::vector<description>> table(const data_dictionary::database& db, const sstring& ks, const sstring& name, bool with_internals) {
    auto table = db.try_find_table(ks, name);
    if (!table) {
        throw exceptions::invalid_request_exception(format("Table '{}' not found in keyspace '{}'", name, ks));
    }
    if (cdc::is_log_for_some_table(db.real_database(), ks, name)) {
        // we want to hide cdc log table from the user
        throw exceptions::invalid_request_exception(format("{}.{} is a cdc log table and it cannot be described directly. Try `DESC TABLE {}.{}` to describe cdc base table and it's log table.", ks, name, ks, cdc::base_name(name)));
    }

    auto schema = table->schema();
    auto idxs = table->get_index_manager().list_indexes();
    auto views = table->views();
    std::vector<description> result;

    // table
    result.push_back(description(db.real_database(), *schema, with_internals));

    // indexes
    boost::sort(idxs, [] (const auto& a, const auto& b) {
        return a.metadata().name() < b.metadata().name();
    });
    for (const auto& idx: idxs) {
        result.push_back(index(db, ks, idx.metadata().name(), with_internals));
        co_await coroutine::maybe_yield();
    }

    //views
    boost::sort(views, [] (const auto& a, const auto& b) {
        return a->cf_name() < b->cf_name();
    });
    for (const auto& v: views) {
        if(!is_index(db, v)) {
            result.push_back(view(db, ks, v->cf_name(), with_internals));
        }
        co_await coroutine::maybe_yield();
    }

    if (schema->cdc_options().enabled()) {
        auto cdc_log_alter = describe_cdc_log_table(db, ks, name);
        if (cdc_log_alter) {
            result.push_back(*cdc_log_alter);
        }
    }

    co_return result;
}

future<std::vector<description>> tables(const data_dictionary::database& db, const lw_shared_ptr<keyspace_metadata>& ks, std::optional<bool> with_internals = std::nullopt) {
    auto& replica_db = db.real_database();
    auto tables = boost::copy_range<std::vector<schema_ptr>>(ks->tables() | boost::adaptors::filtered([&replica_db] (const schema_ptr& s) {
        return !cdc::is_log_for_some_table(replica_db, s->ks_name(), s->cf_name());
    }));
    boost::sort(tables, [] (const auto& a, const auto& b) {
        return a->cf_name() < b->cf_name();
    });

    if (with_internals) {
        std::vector<description> result;
        for (const auto& t: tables) {
            auto tables_desc = co_await table(db, ks->name(), t->cf_name(), *with_internals);
            result.insert(result.end(), tables_desc.begin(), tables_desc.end());
            co_await coroutine::maybe_yield();
        }

        co_return result;
    }

    co_return boost::copy_range<std::vector<description>>(tables | boost::adaptors::transformed([&replica_db] (auto&& t) {
        return description(replica_db, *t);
    }));
}

// DESCRIBE UTILITY
// Various utility functions to make description easier.

/**
 *  Wrapper over `data_dictionary::database::find_keyspace()`
 *
 *  @return `data_dictionary::keyspace_metadata` for specified keyspace name
 *  @throw `invalid_request_exception` if there is no such keyspace
 */
lw_shared_ptr<keyspace_metadata> get_keyspace_metadata(const data_dictionary::database& db, const sstring& keyspace) {
    auto ks = db.try_find_keyspace(keyspace);
    if (!ks) {
        throw exceptions::invalid_request_exception(format("'{}' not found in keyspaces", keyspace));
    }

    return ks->metadata();
}

/**
 *  Lists `keyspace_element` for given keyspace
 *
 *  @return vector of `description` for specified `keyspace_element` in specified keyspace.
            Descriptions don't contain create_statements.
 *  @throw `invalid_request_exception` if there is no such keyspace
 */
future<std::vector<description>> list_elements(const data_dictionary::database& db, const sstring& ks, element_type element) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();

    switch (element) {
    case element_type::type:         co_return co_await types(replica_db, ks_meta);
    case element_type::function:     co_return co_await functions(replica_db, ks);
    case element_type::aggregate:    co_return co_await aggregates(replica_db, ks);
    case element_type::table:        co_return co_await tables(db, ks_meta);
    case element_type::keyspace:     co_return std::vector{keyspace(replica_db, ks_meta)}; //std::vector{description(ks, "keyspace", ks)};
    case element_type::view:
    case element_type::index:
        on_internal_error(dlogger, "listing of views and indexes is unsupported");
    }
}


/**
 *  Describe specified `keyspace_element` in given keyspace
 *
 *  @return `description` of specified `keyspace_element` in specified keyspace.
            Description contains create_statement.
 *  @throw `invalid_request_exception` if there is no such keyspace or there is no element with given name
 */
future<std::vector<description>> describe_element(const data_dictionary::database& db, const sstring& ks, element_type element, const sstring& name, bool with_internals) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();

    switch (element) {
    case element_type::type:             co_return std::vector{type(replica_db, ks_meta, name)};
    case element_type::function:         co_return co_await function(replica_db, ks, name);
    case element_type::aggregate:        co_return co_await aggregate(replica_db, ks, name);
    case element_type::table:            co_return co_await table(db, ks, name, with_internals);
    case element_type::index:            co_return std::vector{index(db, ks, name, with_internals)};
    case element_type::view:             co_return std::vector{view(db, ks, name, with_internals)};
    case element_type::keyspace:         co_return std::vector{keyspace(replica_db, ks_meta, true)};
    }
}

/**
 *  Describe all elements in given keyspace.
 *  Elements order: keyspace, user-defined types, user-defined functions, user-defined aggregates, tables
 *  Table description contains: table, indexes, views
 *
 *  @return `description`s of all elements in specified keyspace.
            Descriptions contain create_statements.
 *  @throw `invalid_request_exception` if there is no such keyspace or there is no element with given name
 */
future<std::vector<description>> describe_all_keyspace_elements(const data_dictionary::database& db, const sstring& ks, bool with_internals) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();
    std::vector<description> result;

    auto inserter = [&result] (std::vector<description>&& elements) mutable {
        result.insert(result.end(), elements.begin(), elements.end());
    };

    result.push_back(keyspace(replica_db, ks_meta, true));
    inserter(co_await types(replica_db, ks_meta, true));
    inserter(co_await functions(replica_db, ks, true));
    inserter(co_await aggregates(replica_db, ks, true));
    inserter(co_await tables(db, ks_meta, with_internals));

    co_return result; 
}

}

// Generic column specification for element describe statement
std::vector<lw_shared_ptr<column_specification>> get_listing_column_specifications() {
    return std::vector<lw_shared_ptr<column_specification>> {
        make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("keyspace_name", true), utf8_type),
        make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("type", true), utf8_type),
        make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("name", true), utf8_type)
    };
}

// Generic column specification for listing describe statement
std::vector<lw_shared_ptr<column_specification>> get_element_column_specifications() {
    auto col_specs = get_listing_column_specifications();
    col_specs.push_back(make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("create_statement", true), utf8_type));
    return col_specs;
}

std::vector<std::vector<bytes_opt>> serialize_descriptions(std::vector<description>&& descs) {
    return boost::copy_range<std::vector<std::vector<bytes_opt>>>(descs | boost::adaptors::transformed([] (const description& desc) {
        return desc.serialize();
    }));
}


// DESCRIBE STATEMENT
describe_statement::describe_statement() : cql_statement(&timeout_config::other_timeout) {}

uint32_t describe_statement::get_bound_terms() const { return 0; }

bool describe_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const { return false; }

future<> describe_statement::check_access(query_processor& qp, const service::client_state& state) const {
    state.validate_login();
    co_return;
}

seastar::shared_ptr<const metadata> describe_statement::get_result_metadata() const {
    return ::make_shared<const metadata>(get_column_specifications());
}

seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
describe_statement::execute(cql3::query_processor& qp, service::query_state& state, const query_options& options,  std::optional<service::group0_guard> guard) const {
    auto& client_state = state.get_client_state();

    auto descriptions = co_await describe(qp, client_state);
    auto result = std::make_unique<result_set>(get_column_specifications(qp.proxy().local_db(), client_state));

    for (auto&& row: descriptions) {
        result->add_row(std::move(row));
    }
    co_return ::make_shared<cql_transport::messages::result_message::rows>(cql3::result(std::move(result)));
}

// CLUSTER DESCRIBE STATEMENT
std::pair<data_type, data_type> range_ownership_type() {
    auto list_type = list_type_impl::get_instance(utf8_type, false);
    auto map_type = map_type_impl::get_instance(
        utf8_type, 
        list_type, 
        false
    );

    return {list_type, map_type};
}

cluster_describe_statement::cluster_describe_statement() : describe_statement() {}

std::vector<lw_shared_ptr<column_specification>> cluster_describe_statement::get_column_specifications() const {
    return std::vector<lw_shared_ptr<column_specification>> {
        make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("cluster", true), utf8_type),
        make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("partitioner", true), utf8_type),
        make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("snitch", true), utf8_type)
    };
}

std::vector<lw_shared_ptr<column_specification>> cluster_describe_statement::get_column_specifications(replica::database& db, const service::client_state& client_state) const {
    auto spec = get_column_specifications();
    if (should_add_range_ownership(db, client_state)) {
        auto map_type = range_ownership_type().second;
        spec.push_back(
            make_lw_shared<column_specification>("system", "describe", ::make_shared<column_identifier>("range_ownership", true), map_type)
        );
    }
    
    return spec;
}

bool cluster_describe_statement::should_add_range_ownership(replica::database& db, const service::client_state& client_state) const {
    auto ks = client_state.get_raw_keyspace();
    //TODO: produce range ownership for tables using tablets too
    bool uses_tablets = false;
    try {
        uses_tablets = !ks.empty() && db.find_keyspace(ks).uses_tablets();
    } catch (const data_dictionary::no_such_keyspace&) {
        // ignore
    }
    return !ks.empty() && !is_system_keyspace(ks) && !is_internal_keyspace(ks) && !uses_tablets;
}

future<bytes_opt> cluster_describe_statement::range_ownership(const service::storage_proxy& proxy, const sstring& ks) const {
    auto [list_type, map_type] = range_ownership_type();

    auto ranges = co_await proxy.describe_ring(ks);
    boost::sort(ranges, [] (const dht::token_range_endpoints& l, const dht::token_range_endpoints& r) {
        return std::stol(l._start_token) < std::stol(r._start_token);
    });

    auto ring_ranges = boost::copy_range<std::vector<std::pair<data_value, data_value>>>(ranges | boost::adaptors::transformed([list_type = std::move(list_type)] (auto& range) {
        auto token_end = data_value(range._end_token);
        auto endpoints = boost::copy_range<std::vector<data_value>>(range._endpoints | boost::adaptors::transformed([] (const auto& endpoint) {
            return data_value(endpoint);
        }));
        auto endpoints_list = make_list_value(list_type, endpoints);

        return std::pair(token_end, endpoints_list);
    }));

    co_return make_map_value(map_type, map_type_impl::native_type(
        std::make_move_iterator(ring_ranges.begin()),
        std::make_move_iterator(ring_ranges.end())
    )).serialize();
}

future<std::vector<std::vector<bytes_opt>>> cluster_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {   
    auto db = qp.db();
    auto& proxy = qp.proxy();

    auto cluster = to_bytes(db.get_config().cluster_name());
    auto partitioner = to_bytes(db.get_config().partitioner());
    auto snitch = to_bytes(db.get_config().endpoint_snitch());

    std::vector<bytes_opt> row {
        {cluster},
        {partitioner},
        {snitch}
    };

    if (should_add_range_ownership(proxy.local_db(), client_state)) {
        auto ro_map = co_await range_ownership(proxy, client_state.get_raw_keyspace());
        row.push_back(std::move(ro_map));
    }
    
    std::vector<std::vector<bytes_opt>> result {std::move(row)};
    co_return result;
}

// SCHEMA DESCRIBE STATEMENT
schema_describe_statement::schema_describe_statement(bool full_schema, bool with_internals)
    : describe_statement()
    , _config(schema_desc{full_schema})
    , _with_internals(with_internals) {}

schema_describe_statement::schema_describe_statement(std::optional<sstring> keyspace, bool only, bool with_internals)
    : describe_statement()
    , _config(keyspace_desc{keyspace, only})
    , _with_internals(with_internals) {}

std::vector<lw_shared_ptr<column_specification>> schema_describe_statement::get_column_specifications() const {
    return get_element_column_specifications();
}

future<std::vector<std::vector<bytes_opt>>> schema_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto db = qp.db();

    auto result = co_await std::visit(overloaded_functor{
        [&] (const schema_desc& config) -> future<std::vector<description>> {
            auto keyspaces = config.full_schema ? db.get_all_keyspaces() : db.get_user_keyspaces();
            std::vector<description> schema_result;

            for (auto&& ks: keyspaces) {
                if (!config.full_schema && db.extensions().is_extension_internal_keyspace(ks)) {
                    continue;
                }
                auto ks_result = co_await describe_all_keyspace_elements(db, ks, _with_internals);
                schema_result.insert(schema_result.end(), ks_result.begin(), ks_result.end());
            }

            co_return schema_result;
        },
        [&] (const keyspace_desc& config) -> future<std::vector<description>> {
            auto ks = client_state.get_raw_keyspace();
            if (config.keyspace) {
                ks = *config.keyspace;
            }

            if (config.only_keyspace) {
                auto ks_meta = get_keyspace_metadata(db, ks);
                auto ks_desc = keyspace(db.real_database(), ks_meta, true);

                co_return std::vector{ks_desc};
            } else {
                co_return co_await describe_all_keyspace_elements(db, ks, _with_internals);
            }
        }
    }, _config);

    co_return serialize_descriptions(std::move(result));
}

// LISTING DESCRIBE STATEMENT
listing_describe_statement::listing_describe_statement(element_type element, bool with_internals) 
    : describe_statement()
    , _element(element)
    , _with_internals(with_internals) {}

std::vector<lw_shared_ptr<column_specification>> listing_describe_statement::get_column_specifications() const {
    return get_listing_column_specifications();

}

future<std::vector<std::vector<bytes_opt>>> listing_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto db = qp.db();
    auto raw_ks = client_state.get_raw_keyspace();

    std::vector<sstring> keyspaces;
    if (!raw_ks.empty()) {
        keyspaces.push_back(raw_ks);
    } else {
        keyspaces = db.get_all_keyspaces();
        boost::sort(keyspaces);
    }

    std::vector<description> result;
    for (auto&& ks: keyspaces) {
        auto ks_result = co_await list_elements(db, ks, _element);
        result.insert(result.end(), ks_result.begin(), ks_result.end());
        co_await coroutine::maybe_yield();
    }

    co_return serialize_descriptions(std::move(result));
}

// ELEMENT DESCRIBE STATEMENT
element_describe_statement::element_describe_statement(element_type element, std::optional<sstring> keyspace, sstring name, bool with_internals) 
    : describe_statement()
    , _element(element)
    , _keyspace(std::move(keyspace))
    , _name(std::move(name))
    , _with_internals(with_internals) {}

std::vector<lw_shared_ptr<column_specification>> element_describe_statement::get_column_specifications() const {
    return get_element_column_specifications();
}

future<std::vector<std::vector<bytes_opt>>> element_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto ks = client_state.get_raw_keyspace();
    if (_keyspace) {
        ks = *_keyspace;
    }
    if (ks.empty()) {
        throw exceptions::invalid_request_exception("No keyspace specified and no current keyspace");
    }
    
    co_return serialize_descriptions(co_await describe_element(qp.db(), ks, _element, _name, _with_internals));
}

//GENERIC DESCRIBE STATEMENT
generic_describe_statement::generic_describe_statement(std::optional<sstring> keyspace, sstring name, bool with_internals)
    : describe_statement()
    , _keyspace(std::move(keyspace))
    , _name(std::move(name))
    , _with_internals(with_internals) {}

std::vector<lw_shared_ptr<column_specification>> generic_describe_statement::get_column_specifications() const {
    return get_element_column_specifications();
}

future<std::vector<std::vector<bytes_opt>>> generic_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto db = qp.db();
    auto& replica_db = db.real_database();
    auto raw_ks = client_state.get_raw_keyspace();
    auto ks_name = (_keyspace) ? *_keyspace : raw_ks;

    if (!_keyspace) {
        auto ks = db.try_find_keyspace(_name);

        if (ks) {
            co_return serialize_descriptions(co_await describe_all_keyspace_elements(db, ks->metadata()->name(), _with_internals));
        } else if (raw_ks.empty()) {
            throw exceptions::invalid_request_exception(format("'{}' not found in keyspaces", _name));
        }
    }
    
    auto ks = db.try_find_keyspace(ks_name);
    if (!ks) {
        throw exceptions::invalid_request_exception(format("'{}' not found in keyspaces", _name));
    }
    auto ks_meta = ks->metadata();

    auto tbl = db.try_find_table(ks_name, _name);
    if (tbl) {
        if (tbl->schema()->is_view()) {
            co_return serialize_descriptions({view(db, ks_name, _name, _with_internals)});
        } else {
            co_return serialize_descriptions(co_await table(db, ks_name, _name, _with_internals));
        }
    }

    auto idx_tbl = db.find_indexed_table(ks_name, _name);
    if (idx_tbl) {
        co_return serialize_descriptions({index(db, ks_name, _name, _with_internals)});
    }

    auto udt_meta = ks_meta->user_types();
    if (udt_meta.has_type(to_bytes(_name))) {
        co_return serialize_descriptions({type(replica_db, ks_meta, _name)});
    }

    auto uf = functions::instance().find(functions::function_name(ks_name, _name));
    if (!uf.empty()) {
        auto udfs = boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(uf | boost::adaptors::transformed([] (const auto& f) {
            return dynamic_pointer_cast<const functions::user_function>(f.second);
        }) | boost::adaptors::filtered([] (const auto& f) {
            return f != nullptr;
        }));
        if (!udfs.empty()) {
            co_return serialize_descriptions(co_await generate_descriptions(replica_db, udfs, true));
        }

        auto udas = boost::copy_range<std::vector<shared_ptr<const keyspace_element>>>(uf | boost::adaptors::transformed([] (const auto& f) {
            return dynamic_pointer_cast<const functions::user_aggregate>(f.second);
        }) | boost::adaptors::filtered([] (const auto& f) {
            return f != nullptr;
        }));
        if (!udas.empty()) {
            co_return serialize_descriptions(co_await generate_descriptions(replica_db, udas, true));
        }
    }

    throw exceptions::invalid_request_exception(format("'{}' not found in keyspace '{}'", _name, ks_name));
}

namespace raw {

using ds = describe_statement;

describe_statement::describe_statement(ds::describe_config config) : _config(std::move(config)), _with_internals(false) {}

void describe_statement::with_internals_details() {
    _with_internals = internals(true);
}

std::unique_ptr<prepared_statement> describe_statement::prepare(data_dictionary::database db, cql_stats &stats) {
    bool internals = bool(_with_internals);
    auto desc_stmt = std::visit(overloaded_functor{
        [] (const describe_cluster&) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<cluster_describe_statement>();
        },
        [&] (const describe_schema& cfg) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<schema_describe_statement>(cfg.full_schema, internals);
        },
        [&] (const describe_keyspace& cfg) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<schema_describe_statement>(std::move(cfg.keyspace), cfg.only_keyspace, internals);
        },
        [&] (const describe_listing& cfg) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<listing_describe_statement>(cfg.element_type, internals);
        },
        [&] (const describe_element& cfg) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<element_describe_statement>(cfg.element_type, std::move(cfg.keyspace), std::move(cfg.name), internals);
        },
        [&] (const describe_generic& cfg) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<generic_describe_statement>(std::move(cfg.keyspace), std::move(cfg.name), internals);
        }
    }, _config);
    return std::make_unique<prepared_statement>(desc_stmt);
}

std::unique_ptr<describe_statement> describe_statement::cluster() {
    return std::make_unique<describe_statement>(ds::describe_cluster());
}

std::unique_ptr<describe_statement> describe_statement::schema(bool full) {
    return std::make_unique<describe_statement>(ds::describe_schema{.full_schema = full});
}

std::unique_ptr<describe_statement> describe_statement::keyspaces() {
    return std::make_unique<describe_statement>(ds::describe_listing{.element_type = ds::element_type::keyspace});
}

std::unique_ptr<describe_statement> describe_statement::keyspace(std::optional<sstring> keyspace, bool only) {
    return std::make_unique<describe_statement>(ds::describe_keyspace{.keyspace = keyspace, .only_keyspace = only});
}

std::unique_ptr<describe_statement> describe_statement::tables() {
    return std::make_unique<describe_statement>(ds::describe_listing{.element_type = ds::element_type::table});
}

std::unique_ptr<describe_statement> describe_statement::table(const cf_name& cf_name) {
    auto ks = (cf_name.has_keyspace()) ? std::optional<sstring>(cf_name.get_keyspace()) : std::nullopt;
    return std::make_unique<describe_statement>(ds::describe_element{.element_type = ds::element_type::table, .keyspace = ks, .name = cf_name.get_column_family()});
}

std::unique_ptr<describe_statement> describe_statement::index(const cf_name& cf_name) {
    auto ks = (cf_name.has_keyspace()) ? std::optional<sstring>(cf_name.get_keyspace()) : std::nullopt;
    return std::make_unique<describe_statement>(ds::describe_element{.element_type = ds::element_type::index, .keyspace = ks, .name = cf_name.get_column_family()});
}

std::unique_ptr<describe_statement> describe_statement::view(const cf_name& cf_name) {
    auto ks = (cf_name.has_keyspace()) ? std::optional<sstring>(cf_name.get_keyspace()) : std::nullopt;
    return std::make_unique<describe_statement>(ds::describe_element{.element_type = ds::element_type::view, .keyspace = ks, .name = cf_name.get_column_family()});
}

std::unique_ptr<describe_statement> describe_statement::types() {
    return std::make_unique<describe_statement>(ds::describe_listing{.element_type = ds::element_type::type});
}

std::unique_ptr<describe_statement> describe_statement::type(const ut_name& ut_name) {
    auto ks = (ut_name.has_keyspace()) ? std::optional<sstring>(ut_name.get_keyspace()) : std::nullopt;
    return std::make_unique<describe_statement>(ds::describe_element{.element_type = ds::element_type::type, .keyspace = ks, .name = ut_name.get_string_type_name()});
}

std::unique_ptr<describe_statement> describe_statement::functions() {
    return std::make_unique<describe_statement>(ds::describe_listing{.element_type = ds::element_type::function});
}

std::unique_ptr<describe_statement> describe_statement::function(const functions::function_name& fn_name) {
    auto ks = (fn_name.has_keyspace()) ? std::optional<sstring>(fn_name.keyspace) : std::nullopt;
    return std::make_unique<describe_statement>(ds::describe_element{.element_type = ds::element_type::function, .keyspace = ks, .name = fn_name.name});
}

std::unique_ptr<describe_statement> describe_statement::aggregates() {
    return std::make_unique<describe_statement>(ds::describe_listing{.element_type = ds::element_type::aggregate});
}

std::unique_ptr<describe_statement> describe_statement::aggregate(const functions::function_name& fn_name) {
    auto ks = (fn_name.has_keyspace()) ? std::optional<sstring>(fn_name.keyspace) : std::nullopt;
    return std::make_unique<describe_statement>(ds::describe_element{.element_type = ds::element_type::aggregate, .keyspace = ks, .name = fn_name.name});
}

std::unique_ptr<describe_statement> describe_statement::generic(std::optional<sstring> keyspace, const sstring& name) {
    return std::make_unique<describe_statement>(ds::describe_generic{.keyspace = keyspace, .name = name});
}

} // namespace raw

} // namespace statements

} // namespace cql3
