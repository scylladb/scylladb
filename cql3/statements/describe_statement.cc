/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>

#include "cdc/cdc_options.hh"
#include "cdc/log.hh"
#include "cql3/column_specification.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/statements/prepared_statement.hh"
#include "exceptions/exceptions.hh"
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/exception.hh>
#include "service/client_state.hh"
#include "types/types.hh"
#include "cql3/query_processor.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/raw/describe_statement.hh"
#include "cql3/statements/describe_statement.hh"
#include <seastar/core/shared_ptr.hh>
#include <sstream>
#include "transport/messages/result_message.hh"
#include "transport/messages/result_message_base.hh"
#include "service/query_state.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "data_dictionary/data_dictionary.hh"
#include "service/storage_proxy.hh"
#include "cql3/ut_name.hh"
#include "types/map.hh"
#include "types/list.hh"
#include "cql3/functions/functions.hh"
#include "types/user.hh"
#include "view_info.hh"
#include "validation.hh"
#include "index/secondary_index_manager.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/user_aggregate.hh"
#include "utils/overloaded_functor.hh"
#include "db/config.hh"
#include "db/system_keyspace.hh"
#include "db/extensions.hh"
#include "utils/sorting.hh"
#include "replica/database.hh"
#include "cql3/description.hh"
#include "replica/schema_describe_helper.hh"

static logging::logger dlogger("describe");

bool is_internal_keyspace(std::string_view name);

namespace replica {
class database;
}

namespace cql3 {

namespace statements {

namespace {

template <typename Range, typename Describer>
    requires std::is_invocable_r_v<description, Describer, std::ranges::range_value_t<Range>>
future<std::vector<description>> generate_descriptions(Range&& range, const Describer& describer, bool sort_by_name = true) {
    std::vector<description> result{};
    if constexpr (std::ranges::sized_range<Range>) {
        result.reserve(std::ranges::size(range));
    }

    for (const auto& element : range) {
        result.push_back(describer(element));
        co_await coroutine::maybe_yield();
    }

    if (sort_by_name) {
        std::ranges::sort(result, std::less<>{}, std::mem_fn(&description::name));
    }

    co_return result;
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

description type(replica::database& db, const lw_shared_ptr<keyspace_metadata>& ks, const sstring& name) {
    auto udt_meta = ks->user_types();
    if (!udt_meta.has_type(to_bytes(name))) {
        throw exceptions::invalid_request_exception(format("Type '{}' not found in keyspace '{}'", name, ks->name()));
    }

    auto udt = udt_meta.get_type(to_bytes(name));
    return udt->describe(with_create_statement::yes);
}

// Because UDTs can depend on each other, we need to sort them topologically
future<std::vector<user_type>> get_sorted_types(const lw_shared_ptr<keyspace_metadata>& ks) {
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

    co_return co_await utils::topological_sort(all_udts, adjacency);
}

future<std::vector<description>> types(replica::database& db, const lw_shared_ptr<keyspace_metadata>& ks, with_create_statement with_stmt) {
    auto udts = co_await get_sorted_types(ks);
    auto describer = [with_stmt] (const user_type udt) -> cql3::description {
        return udt->describe(with_stmt);
    };
    co_return co_await generate_descriptions(udts, describer, false);
}
    
future<std::vector<description>> function(replica::database& db, const sstring& ks, const sstring& name) {
    auto fs = functions::instance().find(functions::function_name(ks, name));
    if (fs.empty()) {
        throw exceptions::invalid_request_exception(format("Function '{}' not found in keyspace '{}'", name, ks));
    }

    auto udfs = fs | std::views::transform([] (const auto& f) {
        const auto& [function_name, function_ptr] = f;
        return dynamic_pointer_cast<const functions::user_function>(function_ptr);
    }) | std::views::filter([] (shared_ptr<const functions::user_function> f) {
        return f != nullptr;
    });
    if (udfs.empty()) {
        throw exceptions::invalid_request_exception(format("Function '{}' not found in keyspace '{}'", name, ks));
    }

    auto describer = [] (shared_ptr<const functions::user_function> udf) {
        return udf->describe(cql3::with_create_statement::yes);
    };
    co_return co_await generate_descriptions(udfs, describer, true);
}

future<std::vector<description>> functions(replica::database& db,const sstring& ks, with_create_statement with_stmt) {
    auto udfs = cql3::functions::instance().get_user_functions(ks);
    auto describer = [with_stmt] (shared_ptr<const functions::user_function> udf) {
        return udf->describe(with_stmt);
    };
    co_return co_await generate_descriptions(udfs, describer, true);
}

future<std::vector<description>> aggregate(replica::database& db, const sstring& ks, const sstring& name) {
    auto fs = functions::instance().find(functions::function_name(ks, name));
    if (fs.empty()) {
        throw exceptions::invalid_request_exception(format("Aggregate '{}' not found in keyspace '{}'", name, ks));
    }

    auto udas = fs | std::views::transform([] (const auto& f) {
        return dynamic_pointer_cast<const functions::user_aggregate>(f.second);
    }) | std::views::filter([] (shared_ptr<const functions::user_aggregate> f) {
        return f != nullptr;
    });
    if (udas.empty()) {
        throw exceptions::invalid_request_exception(format("Aggregate '{}' not found in keyspace '{}'", name, ks));
    }

    auto describer = [] (shared_ptr<const functions::user_aggregate> uda) {
        return uda->describe(with_create_statement::yes);
    };
    co_return co_await generate_descriptions(udas, describer, true);
}

future<std::vector<description>> aggregates(replica::database& db, const sstring& ks, with_create_statement with_stmt) {
    auto udas = functions::instance().get_user_aggregates(ks);
    auto describer = [with_stmt] (shared_ptr<const functions::user_aggregate> uda) {
        return uda->describe(with_stmt);
    };
    co_return co_await generate_descriptions(udas, describer, true);
}

description view(const data_dictionary::database& db, const sstring& ks, const sstring& name, bool with_internals) {
    auto view = db.try_find_table(ks, name);
    if (!view || !view->schema()->is_view()) {
        throw exceptions::invalid_request_exception(format("Materialized view '{}' not found in keyspace '{}'", name, ks));
    }

    replica::schema_describe_helper describe_helper{db};
    return view->schema()->describe(describe_helper, with_internals ? describe_option::STMTS_AND_INTERNALS : describe_option::STMTS);
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

    replica::schema_describe_helper describe_helper{db};
    return (**idx).describe(describe_helper, with_internals ? describe_option::STMTS_AND_INTERNALS : describe_option::STMTS);
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
    replica::schema_describe_helper describe_helper{db};
    schema->describe_alter_with_properties(describe_helper, os);

    auto schema_desc = schema->describe(describe_helper, describe_option::NO_STMTS);
    schema_desc.create_statement = std::move(os).str();
    return schema_desc;
}

future<std::vector<description>> table(const data_dictionary::database& db, const sstring& ks, const sstring& name, bool with_internals) {
    auto table = db.try_find_table(ks, name);
    if (!table) {
        throw exceptions::invalid_request_exception(format("Table '{}' not found in keyspace '{}'", name, ks));
    }
    
    auto s = validation::validate_column_family(db, ks, name);
    if (s->is_view()) { 
        throw exceptions::invalid_request_exception("Cannot use DESC TABLE on materialized View");
    }

    auto schema = table->schema();
    auto idxs = table->get_index_manager().list_indexes();
    auto views = table->views();
    std::vector<description> result;

    // table
    replica::schema_describe_helper describe_helper{db};
    auto table_desc = schema->describe(describe_helper, with_internals ? describe_option::STMTS_AND_INTERNALS : describe_option::STMTS);
    if (cdc::is_log_for_some_table(db.real_database(), ks, name)) {
        // If the table the user wants to describe is a CDC log table, we want to print it as a CQL comment.
        // This way, the user learns about the internals of the table, but they're also told not to execute it.
        table_desc.create_statement = seastar::format(
                "/* Do NOT execute this statement! It's only for informational purposes.\n"
                "   A CDC log table is created automatically when the base is created.\n"
                "\n{}\n"
                "*/",
                *table_desc.create_statement);
    }
    result.push_back(std::move(table_desc));

    // indexes
    std::ranges::sort(idxs, std::ranges::less(), [] (const auto& a) {
        return a.metadata().name();
    });
    for (const auto& idx: idxs) {
        result.push_back(index(db, ks, idx.metadata().name(), with_internals));
        co_await coroutine::maybe_yield();
    }

    //views
    std::ranges::sort(views, std::ranges::less(), std::mem_fn(&schema::cf_name));
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
    auto tables = ks->tables() | std::views::filter([&replica_db] (const schema_ptr& s) {
        return !cdc::is_log_for_some_table(replica_db, s->ks_name(), s->cf_name());
    }) | std::ranges::to<std::vector<schema_ptr>>();
    std::ranges::sort(tables, std::ranges::less(), std::mem_fn(&schema::cf_name));

    if (with_internals) {
        std::vector<description> result;
        for (const auto& t: tables) {
            auto tables_desc = co_await table(db, ks->name(), t->cf_name(), *with_internals);
            result.insert(result.end(), tables_desc.begin(), tables_desc.end());
            co_await coroutine::maybe_yield();
        }

        co_return result;
    }

    replica::schema_describe_helper describe_helper{db};
    co_return tables | std::views::transform([&describe_helper] (auto&& t) {
        return t->describe(describe_helper, describe_option::NO_STMTS);
    }) | std::ranges::to<std::vector>();
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
 *  Lists keyspace elements for given keyspace
 *
 *  @return vector of `description` for the specified keyspace element type in the specified keyspace.
            Descriptions don't contain create_statements.
 *  @throw `invalid_request_exception` if there is no such keyspace
 */
future<std::vector<description>> list_elements(const data_dictionary::database& db, const sstring& ks, element_type element) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();

    switch (element) {
    case element_type::type:         co_return co_await types(replica_db, ks_meta, with_create_statement::no);
    case element_type::function:     co_return co_await functions(replica_db, ks, with_create_statement::no);
    case element_type::aggregate:    co_return co_await aggregates(replica_db, ks, with_create_statement::no);
    case element_type::table:        co_return co_await tables(db, ks_meta);
    case element_type::keyspace:     co_return std::vector{ks_meta->describe(replica_db, with_create_statement::no)};
    case element_type::view:
    case element_type::index:
        on_internal_error(dlogger, "listing of views and indexes is unsupported");
    }
}


/**
 *  Describe specified keyspace element type in given keyspace
 *
 *  @return `description` of the specified keyspace element type in the specified keyspace.
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
    case element_type::keyspace:         co_return std::vector{ks_meta->describe(replica_db, with_create_statement::yes)};
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

    result.push_back(ks_meta->describe(replica_db, with_create_statement::yes));
    inserter(co_await types(replica_db, ks_meta, with_create_statement::yes));
    inserter(co_await functions(replica_db, ks, with_create_statement::yes));
    inserter(co_await aggregates(replica_db, ks, with_create_statement::yes));
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

std::vector<std::vector<bytes_opt>> serialize_descriptions(std::vector<description>&& descs, bool serialize_create_statement = true) {
    return descs | std::views::transform([serialize_create_statement] (const description& desc) {
        return desc.serialize(serialize_create_statement);
    }) | std::ranges::to<std::vector>();
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
    std::ranges::sort(ranges, std::ranges::less(), [] (const dht::token_range_endpoints& r) {
        return std::stol(r._start_token);
    });

    auto ring_ranges = ranges | std::views::transform([list_type = std::move(list_type)] (auto& range) {
        auto token_end = data_value(range._end_token);
        auto endpoints = range._endpoints | std::views::transform([] (const auto& endpoint) {
            return data_value(endpoint);
        }) | std::ranges::to<std::vector>();
        auto endpoints_list = make_list_value(list_type, endpoints);

        return std::pair(token_end, endpoints_list);
    }) | std::ranges::to<std::vector>();

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
schema_describe_statement::schema_describe_statement(bool full_schema, bool with_hashed_passwords, bool with_internals)
    : describe_statement()
    , _config(schema_desc{.full_schema = full_schema, .with_hashed_passwords = with_hashed_passwords})
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
            auto& auth_service = *client_state.get_auth_service();

            if (config.with_hashed_passwords) {
                const auto maybe_user = client_state.user();
                if (!maybe_user || !co_await auth::has_superuser(auth_service, *maybe_user)) {
                    co_await coroutine::return_exception(exceptions::unauthorized_exception(
                            "DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS can only be issued by a superuser"));
                }
            }

            auto keyspaces = config.full_schema ? db.get_all_keyspaces() : db.get_user_keyspaces();
            std::vector<description> schema_result;

            for (auto&& ks: keyspaces) {
                if (!config.full_schema && db.extensions().is_extension_internal_keyspace(ks)) {
                    continue;
                }
                auto ks_result = co_await describe_all_keyspace_elements(db, ks, _with_internals);
                schema_result.insert(schema_result.end(), ks_result.begin(), ks_result.end());
            }

            if (_with_internals) {
                // The order is important here. We need to first restore auth
                // because we can only attach a service level to an existing role.
                auto auth_descs = co_await auth_service.describe_auth(config.with_hashed_passwords);
                auto service_level_descs = co_await client_state.get_service_level_controller().describe_service_levels();

                schema_result.insert(schema_result.end(), std::make_move_iterator(auth_descs.begin()),
                        std::make_move_iterator(auth_descs.end()));
                schema_result.insert(schema_result.end(), std::make_move_iterator(service_level_descs.begin()),
                        std::make_move_iterator(service_level_descs.end()));
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
                auto ks_desc = ks_meta->describe(db.real_database(), with_create_statement::yes);

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
        std::ranges::sort(keyspaces);
    }

    std::vector<description> result;
    for (auto&& ks: keyspaces) {
        auto ks_result = co_await list_elements(db, ks, _element);
        result.insert(result.end(), ks_result.begin(), ks_result.end());
        co_await coroutine::maybe_yield();
    }

    co_return serialize_descriptions(std::move(result), false);
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
        auto udfs = uf | std::views::transform([] (const auto& f) {
            const auto& [function_name, function_ptr] = f;
            return dynamic_pointer_cast<const functions::user_function>(function_ptr);
        }) | std::views::filter([] (shared_ptr<const functions::user_function> f) {
            return f != nullptr;
        });

        auto udf_describer = [] (shared_ptr<const functions::user_function> udf) {
            return udf->describe(with_create_statement::yes);
        };

        if (!udfs.empty()) {
            co_return serialize_descriptions(co_await generate_descriptions(udfs, udf_describer, true));
        }

        auto udas = uf | std::views::transform([] (const auto& f) {
            const auto& [function_name, function_ptr] = f;
            return dynamic_pointer_cast<const functions::user_aggregate>(function_ptr);
        }) | std::views::filter([] (shared_ptr<const functions::user_aggregate> f) {
            return f != nullptr;
        });

        auto uda_describer = [] (shared_ptr<const functions::user_aggregate> uda) {
            return uda->describe(with_create_statement::yes);
        };

        if (!udas.empty()) {
            co_return serialize_descriptions(co_await generate_descriptions(udas, uda_describer, true));
        }
    }

    throw exceptions::invalid_request_exception(format("'{}' not found in keyspace '{}'", _name, ks_name));
}

namespace raw {

using ds = describe_statement;

describe_statement::describe_statement(ds::describe_config config) : _config(std::move(config)), _with_internals(false) {}

void describe_statement::with_internals_details(bool with_hashed_passwords) {
    _with_internals = internals(true);

    if (with_hashed_passwords && !std::holds_alternative<describe_schema>(_config)) {
        throw exceptions::invalid_request_exception{"Option WITH PASSWORDS is only allowed with DESC SCHEMA"};
    }

    if (std::holds_alternative<describe_schema>(_config)) {
        std::get<describe_schema>(_config).with_hashed_passwords = with_hashed_passwords;
    }
}

audit::statement_category
describe_statement::category() const {
    return audit::statement_category::QUERY;
}

audit::audit_info_ptr
describe_statement::audit_info() const {
    return audit::audit::create_audit_info(category(), "system", "");
}

std::unique_ptr<prepared_statement> describe_statement::prepare(data_dictionary::database db, cql_stats &stats) {
    bool internals = bool(_with_internals);
    auto desc_stmt = std::visit(overloaded_functor{
        [] (const describe_cluster&) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<cluster_describe_statement>();
        },
        [&] (const describe_schema& cfg) -> ::shared_ptr<statements::describe_statement> {
            return ::make_shared<schema_describe_statement>(cfg.full_schema, cfg.with_hashed_passwords, internals);
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
    return std::make_unique<prepared_statement>(audit_info(), desc_stmt);
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
