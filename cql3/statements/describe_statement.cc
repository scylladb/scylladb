/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */
#include <algorithm>
#include <iterator>
#include <memory>
#include <optional>
#include <set>

#include "cdc/cdc_options.hh"
#include "cdc/log.hh"
#include "cql3/column_specification.hh"
#include "cql3/functions/function_name.hh"
#include "cql3/statements/prepared_statement.hh"
#include "exceptions/exceptions.hh"
#include <ranges>
#include <seastar/core/on_internal_error.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/exception.hh>
#include "index/vector_index.hh"
#include "schema/schema.hh"
#include "service/client_state.hh"
#include "service/paxos/paxos_state.hh"
#include "types/types.hh"
#include "cql3/query_processor.hh"
#include "cql3/cql_statement.hh"
#include "cql3/statements/raw/describe_statement.hh"
#include "cql3/statements/describe_statement.hh"
#include "cql3/statements/cluster_config_read.hh"
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
#include "utils/managed_string.hh"
#include "view_info.hh"
#include "validation.hh"
#include "index/secondary_index_manager.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/user_aggregate.hh"
#include "utils/overloaded_functor.hh"
#include "db/config.hh"
#include "db/cluster_config_registry.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "db/extensions.hh"
#include "utils/chunked_vector.hh"
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

// Collects the registry options visible at `s` scope, so DESCRIBE keeps emitting the full set
// as the registry grows instead of naming one option. Options above the cluster's registry
// version are excluded, mirroring the write side: what ALTER refuses to store, DESCRIBE does
// not describe. With no version (feature not enabled) no option is visible.
std::vector<std::string_view> config_option_names_for_scope(db::cluster_config_registry::scope s,
        std::optional<db::cluster_config_registry::version> current_version) {
    std::vector<std::string_view> names;
    if (!current_version) {
        return names;
    }
    for (const auto& opt : db::cluster_config_registry::options()) {
        if (opt.min_version <= *current_version && db::cluster_config_registry::supports_scope(opt, s)) {
            names.push_back(opt.name);
        }
    }
    return names;
}

template <typename Range, typename Describer>
    requires std::is_invocable_r_v<description, Describer, std::ranges::range_value_t<Range>>
future<utils::chunked_vector<description>> generate_descriptions(Range&& range, const Describer& describer, bool sort_by_name = true) {
    utils::chunked_vector<description> result{};
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

// `cql3::description` is a move-only type at the moment, and so we cannot initialize a vector passing
// its instances as elements of an initializer list. This function serves as an intermediary to avoid
// unnecessarily bloating some parts of the code below.
utils::chunked_vector<description> wrap_in_vector(description desc) {
    utils::chunked_vector<description> result{};
    result.reserve(1);
    result.push_back(std::move(desc));
    return result;
}

bool is_index(const data_dictionary::database& db, const schema_ptr& schema) {
    return db.find_column_family(schema->view_info()->base_id()).get_index_manager().is_index(*schema);
}

future<std::map<sstring, sstring>> table_config_options(query_processor& qp, const sstring& ks, const sstring& table) {
    return read_configs_column(qp,
            format("SELECT configs FROM system_schema.{} WHERE keyspace_name = ? AND table_name = ?", db::schema_tables::SCYLLA_TABLES),
            {ks, table});
}

future<std::map<sstring, sstring>> keyspace_config_options(query_processor& qp, const sstring& ks) {
    return read_configs_column(qp,
            format("SELECT configs FROM system_schema.{} WHERE keyspace_name = ?", db::schema_tables::SCYLLA_KEYSPACES),
            {ks});
}

future<std::map<sstring, sstring>> cluster_config_options(query_processor& qp) {
    return read_configs_column(qp,
            format("SELECT configs FROM system_schema.{} WHERE cluster_name = ?", db::schema_tables::SCYLLA_CLUSTERS),
            {sstring(db::schema_tables::CLUSTER_CONFIG_SINGLETON_KEY)});
}

std::optional<sstring> get_config_option(const std::map<sstring, sstring>& configs, std::string_view config_name) {
    if (auto it = configs.find(sstring(config_name)); it != configs.end()) {
        return it->second;
    }
    return std::nullopt;
}

// Memoizes the config-table rows read during one DESCRIBE execution. The per-option loops
// below would otherwise re-read the same cluster/keyspace/table configs row once per option
// per described object — over a schema dump of K keyspaces, N tables and M options that is
// 3*M*N + 2*M*K internal queries for three distinct rows per table. Each row is read at most
// once per execution, which also makes one DESCRIBE internally consistent: every object is
// annotated against the same config state. Reads are lazy, so a DESCRIBE that emits no
// config options issues no queries. The accessors return a reference into the snapshot,
// valid for the snapshot's lifetime (map nodes are stable across later inserts).
class config_snapshot {
    query_processor& _qp;
    std::optional<std::map<sstring, sstring>> _cluster;
    std::map<sstring, std::map<sstring, sstring>> _keyspaces;
    std::map<std::pair<sstring, sstring>, std::map<sstring, sstring>> _tables;
public:
    explicit config_snapshot(query_processor& qp) : _qp(qp) {}

    future<std::reference_wrapper<const std::map<sstring, sstring>>> cluster() {
        if (!_cluster) {
            _cluster = co_await cluster_config_options(_qp);
        }
        co_return std::cref(*_cluster);
    }

    future<std::reference_wrapper<const std::map<sstring, sstring>>> keyspace(const sstring& ks) {
        auto it = _keyspaces.find(ks);
        if (it == _keyspaces.end()) {
            auto configs = co_await keyspace_config_options(_qp, ks);
            it = _keyspaces.emplace(ks, std::move(configs)).first;
        }
        co_return std::cref(it->second);
    }

    future<std::reference_wrapper<const std::map<sstring, sstring>>> table(const sstring& ks, const sstring& table) {
        auto key = std::pair(ks, table);
        auto it = _tables.find(key);
        if (it == _tables.end()) {
            auto configs = co_await table_config_options(_qp, ks, table);
            it = _tables.emplace(std::move(key), std::move(configs)).first;
        }
        co_return std::cref(it->second);
    }
};

// A scope in a table-oriented option's resolution chain, used to annotate
// DESCRIBE output with the value's provenance.
enum class config_origin {
    cluster,
    keyspace,
    table,
};

std::string_view config_origin_name(config_origin origin) {
    switch (origin) {
    case config_origin::cluster:  return "cluster";
    case config_origin::keyspace: return "keyspace";
    case config_origin::table:    return "table";
    }
    return "unknown";
}

// One scope in an option's domain chain together with the override stored at
// exactly that scope (std::nullopt when no override is stored there).
struct config_scope_value {
    config_origin scope;
    std::optional<sstring> stored;
};

// Render a config value as it must appear on the right-hand side of a CQL
// property assignment. Numeric and boolean options are bare tokens; a text
// option must be a single-quoted CQL string literal (with embedded quotes
// doubled), otherwise replaying the DESCRIBE output fails to parse. The option
// is guaranteed to be registered here since the value came from the registry
// scope iteration; default to quoting if it somehow is not.
sstring format_config_option_value(std::string_view name, const sstring& value) {
    const auto* opt = db::cluster_config_registry::find(name);
    if (opt && opt->type != db::cluster_config_registry::value_type::text) {
        return value;
    }
    std::string escaped;
    for (char c : value) {
        if (c == '\'') {
            escaped += "''";
        } else {
            escaped += c;
        }
    }
    return format("'{}'", escaped);
}

// The provenance of one option: the formatted effective value together with the inline
// comment trailer that annotates the emitted property line:
// "from <scope> (<scope>=<value|NULL>, ...)". The chain lists every scope in the option's
// domain chain visible at the described scope, most specific first, unconditionally; each
// entry shows the override stored at exactly that scope, or NULL. The effective value is
// the resolution of exactly the printed chain (the leftmost non-NULL entry) and the scope
// after "from" names where it comes from, so the two can never disagree. Values are
// rendered as CQL literals (text quoted) so the effective value can sit on the right-hand
// side of a property assignment. The trailer contains neither ';' nor a newline, so
// appending it after "-- " keeps the whole tail one valid inline comment, both before and
// after a user uncomments the property it annotates. All-NULL chains never reach this
// function (such options emit nothing).
struct config_provenance {
    sstring effective;
    sstring trailer;
};

config_provenance format_config_provenance(std::string_view config_name, const std::vector<config_scope_value>& chain) {
    std::optional<sstring> effective;
    std::string_view source_scope = "unknown";
    for (const auto& entry : chain) {
        if (entry.stored) {
            effective = format_config_option_value(config_name, *entry.stored);
            source_scope = config_origin_name(entry.scope);
            break;
        }
    }
    sstring trailer = "from ";
    trailer += sstring(source_scope);
    trailer += " (";
    bool first = true;
    for (const auto& entry : chain) {
        if (!std::exchange(first, false)) {
            trailer += ", ";
        }
        trailer += sstring(config_origin_name(entry.scope));
        trailer += "=";
        trailer += entry.stored ? format_config_option_value(config_name, *entry.stored) : sstring("NULL");
    }
    trailer += ")";
    return {effective.value_or(sstring("NULL")), std::move(trailer)};
}

// One described option: the override stored at the described object's own scope (absent
// when nothing is stored there: the plain tier then emits the option as a commented-out
// property, and WITH INTERNALS emits nothing), the effective value, and the provenance
// trailer.
struct described_config_option {
    std::string_view name;
    std::optional<sstring> stored;
    sstring effective;
    sstring provenance;
};

// Returns nullopt when no scope in the chain stores a value: such an option produces
// no output at all and resolves to its registry default.
std::optional<described_config_option> make_described_config_option(std::string_view config_name, const std::vector<config_scope_value>& chain) {
    for (const auto& entry : chain) {
        if (entry.stored) {
            auto provenance = format_config_provenance(config_name, chain);
            return described_config_option{config_name, chain.front().stored, std::move(provenance.effective), std::move(provenance.trailer)};
        }
    }
    return std::nullopt;
}

future<std::optional<described_config_option>> described_keyspace_config_option(config_snapshot& snapshot, const sstring& ks, std::string_view config_name) {
    const auto& keyspace_configs = (co_await snapshot.keyspace(ks)).get();
    const auto& cluster_configs = (co_await snapshot.cluster()).get();

    std::vector<config_scope_value> chain{
        {config_origin::keyspace, get_config_option(keyspace_configs, config_name)},
        {config_origin::cluster, get_config_option(cluster_configs, config_name)},
    };
    co_return make_described_config_option(config_name, chain);
}

future<std::optional<described_config_option>> described_table_config_option(config_snapshot& snapshot, const sstring& ks, const sstring& table, std::string_view config_name) {
    const auto& table_configs = (co_await snapshot.table(ks, table)).get();
    const auto& keyspace_configs = (co_await snapshot.keyspace(ks)).get();
    const auto& cluster_configs = (co_await snapshot.cluster()).get();

    std::vector<config_scope_value> chain{
        {config_origin::table, get_config_option(table_configs, config_name)},
        {config_origin::keyspace, get_config_option(keyspace_configs, config_name)},
        {config_origin::cluster, get_config_option(cluster_configs, config_name)},
    };
    co_return make_described_config_option(config_name, chain);
}

void append_create_statement_options(description& desc, const std::vector<described_config_option>& options, bool with_comments) {
    if (!desc.create_statement || options.empty()) {
        return;
    }

    auto create_statement = desc.create_statement->linearize();
    while (!create_statement.empty() && std::isspace(static_cast<unsigned char>(create_statement.back()))) {
        create_statement.resize(create_statement.size() - 1);
    }
    if (with_comments) {
        // Plain tier: one property line per option, in WITH-clause position. An option
        // stored at the described object's own scope is a live property; an inherited one
        // is the same property commented out ("-- AND <option> = <effective>"). Folding an
        // inherited value into a live property instead would, on replay, invent an
        // override at this scope and detach the object from later broader-scope changes;
        // the commented form keeps replay-as-is inheritance-preserving, while erasing just
        // the leading comment marker pins the currently effective value here as a
        // deliberate one-keystroke edit. Every line carries a trailing inline comment with
        // the value's provenance; it stays a valid comment after uncommenting. The
        // terminating ';' moves to its own line: the COMMENT lexer token requires a
        // newline terminator (Cql.g), so a trailing comment at the very end of the
        // statement would make it fail to parse as a single request.
        if (!create_statement.empty() && create_statement.back() == ';') {
            create_statement.resize(create_statement.size() - 1);
        }
        for (const auto& option : options) {
            create_statement += option.stored ? "\n    AND " : "\n    -- AND ";
            create_statement += sstring(option.name);
            create_statement += " = ";
            create_statement += option.effective;
            create_statement += "  -- ";
            create_statement += option.provenance;
        }
        create_statement += "\n;";
    } else {
        // WITH INTERNALS stays pure replayable CQL: only overrides stored at the described
        // object's own scope appear, with no comment lines, so replaying a full dump
        // reproduces the stored configs maps at every scope exactly.
        const bool has_stored = std::ranges::any_of(options, [] (const described_config_option& option) { return bool(option.stored); });
        if (has_stored) {
            if (!create_statement.empty() && create_statement.back() == ';') {
                create_statement.resize(create_statement.size() - 1);
            }
            const auto separator = create_statement.contains('\n') ? sstring("\n    AND ") : sstring(" AND ");
            for (const auto& option : options) {
                if (!option.stored) {
                    continue;
                }
                create_statement += separator;
                create_statement += sstring(option.name);
                create_statement += " = ";
                create_statement += format_config_option_value(option.name, *option.stored);
            }
            create_statement += sstring(";");
        }
    }
    desc.create_statement = managed_string(create_statement);
}

// One scope of a scope object's resolution chain together with the full stored `configs`
// map at that scope. The first entry of a chain is the described scope itself.
struct scope_chain_entry {
    config_origin scope;
    const std::map<sstring, sstring>& configs;
};

// Builds the create_statement for a scope object (cluster, datacenter, rack, or node):
// one `<alter_target> WITH <option> = <value>;` line per option, since these scopes have
// no CREATE form. In the plain tier an option stored at the described scope itself is a
// live statement, and an option only inherited from a broader scope is the same statement
// commented out, so erasing the leading comment marker pins the effective value here;
// every line carries a trailing "-- from <scope> (<chain>)" provenance comment, and the
// output keeps its final newline because the COMMENT lexer token requires a newline
// terminator (Cql.g). Live statements come from the stored map, not the registry listing,
// so a dump reproduces stored state faithfully even for options this node's registry does
// not know. Inherited (commented) lines appear only for options that support
// `described_scope`: the shared cluster map also holds table-oriented options, and those
// do not resolve through the node-oriented chain, so annotating them here would be
// misleading. An option unknown to this node's registry gets a line only if it is stored
// at the described scope itself (its scope mask cannot be checked, but its presence
// proves a registry that knew it accepted it here). WITH INTERNALS output stays pure
// replayable CQL: stored options only, no comments. Returns an empty string when there is
// nothing to say.
managed_string make_scope_create_statement(const sstring& alter_target, db::cluster_config_registry::scope described_scope,
        const std::vector<scope_chain_entry>& chain, bool with_comments) {
    std::string result;
    if (with_comments) {
        std::set<sstring> option_names;
        for (const auto& [option_name, value] : chain.front().configs) {
            option_names.insert(option_name);
        }
        for (const auto& entry : chain) {
            for (const auto& [option_name, value] : entry.configs) {
                const auto* opt = db::cluster_config_registry::find(option_name);
                if (opt && db::cluster_config_registry::supports_scope(*opt, described_scope)) {
                    option_names.insert(option_name);
                }
            }
        }
        for (const auto& option_name : option_names) {
            std::vector<config_scope_value> values;
            for (const auto& entry : chain) {
                values.push_back({entry.scope, get_config_option(entry.configs, option_name)});
            }
            auto provenance = format_config_provenance(option_name, values);
            if (!values.front().stored) {
                result += "-- ";
            }
            result += alter_target;
            result += " WITH ";
            result += option_name;
            result += " = ";
            result += provenance.effective;
            result += ";  -- ";
            result += provenance.trailer;
            result += "\n";
        }
    } else {
        for (const auto& [option_name, value] : chain.front().configs) {
            result += alter_target;
            result += " WITH ";
            result += option_name;
            result += " = ";
            result += format_config_option_value(option_name, value);
            result += ";\n";
        }
        if (!result.empty() && result.back() == '\n') {
            result.resize(result.size() - 1);
        }
    }
    return managed_string(result);
}

future<description> keyspace_description(config_snapshot& snapshot, const lw_shared_ptr<keyspace_metadata>& ks_meta, const replica::database& db, with_create_statement with_stmt, bool with_internals) {
    auto desc = ks_meta->describe(db, with_stmt);
    if (!with_stmt || !desc.create_statement) {
        co_return desc;
    }

    std::vector<described_config_option> options;
    auto version = db::cluster_config_registry::current_version(db.features());
    for (const auto& config_name : config_option_names_for_scope(db::cluster_config_registry::scope::keyspace, version)) {
        if (auto described = co_await described_keyspace_config_option(snapshot, ks_meta->name(), config_name)) {
            options.push_back(std::move(*described));
        }
    }

    append_create_statement_options(desc, options, !with_internals);
    co_return desc;
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

future<utils::chunked_vector<description>> types(replica::database& db, const lw_shared_ptr<keyspace_metadata>& ks, with_create_statement with_stmt) {
    auto udts = co_await get_sorted_types(ks);
    auto describer = [with_stmt] (const user_type udt) -> cql3::description {
        return udt->describe(with_stmt);
    };
    co_return co_await generate_descriptions(udts, describer, false);
}
    
future<utils::chunked_vector<description>> function(replica::database& db, const sstring& ks, const sstring& name) {
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

future<utils::chunked_vector<description>> functions(replica::database& db,const sstring& ks, with_create_statement with_stmt) {
    auto udfs = cql3::functions::instance().get_user_functions(ks);
    auto describer = [with_stmt] (shared_ptr<const functions::user_function> udf) {
        return udf->describe(with_stmt);
    };
    co_return co_await generate_descriptions(udfs, describer, true);
}

future<utils::chunked_vector<description>> aggregate(replica::database& db, const sstring& ks, const sstring& name) {
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

future<utils::chunked_vector<description>> aggregates(replica::database& db, const sstring& ks, with_create_statement with_stmt) {
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

    auto helper = replica::make_schema_describe_helper(view->schema(), db);
    // We want to get a `CREATE MATERIALIZED VIEW` statement, not a `CREATE INDEX` one.
    const auto actual_type = std::exchange(helper.type, schema_describe_helper::type::view);

    auto result = view->schema()->describe(helper, with_internals ? describe_option::STMTS_AND_INTERNALS : describe_option::STMTS);

    // However, if the view is the underlying materialized view of a secondary index,
    // we'd like to comment it out to implicitly convey that the statement should not
    // be executed to restore the schema entity. The user should restore the index instead.
    // It's the same for CDC log tables.
    if (actual_type == schema_describe_helper::type::index) {
        fragmented_ostringstream os{};

        fmt::format_to(os.to_iter(),
                "/* Do NOT execute this statement! It's only for informational purposes.\n"
                "   This materialized view is the underlying materialized view of a secondary\n"
                "   index. It can be restored via restoring the index.\n"
                "\n{}\n"
                "*/",
                *result.create_statement);

        result.create_statement = std::move(os).to_managed_string();
    }

    return result;
}

description index(const data_dictionary::database& db, const sstring& ks, const sstring& name, bool with_internals) {
    auto schema = db.find_indexed_table(ks, name);
    if (!schema) {
        throw exceptions::invalid_request_exception(format("Table for existing index '{}' not found in '{}'", name, ks));
    }
    
    index_metadata im = schema->all_indices().find(name)->second;

    auto custom_class = secondary_index::secondary_index_manager::get_custom_class(im);

    if (custom_class) {
        auto desc = (*custom_class)->describe(im, *schema);
        if (desc) {
            return std::move(*desc);
        }
    }

    std::optional<view_ptr> idx;
    auto views = db.find_table(ks, schema->cf_name()).views();
    for (const auto& view: views) {
        if (is_index(db, view) && secondary_index::index_name_from_table_name(view->cf_name()) == name) {
            idx = view;
            break;
        }
    }

    return (**idx).describe(replica::make_schema_describe_helper(*idx, db), with_internals ? describe_option::STMTS_AND_INTERNALS : describe_option::STMTS);
}

// `base_name` should be a table with enabled cdc or a vector index
std::optional<description> describe_cdc_log_table(const data_dictionary::database& db, const sstring& ks, const sstring& base_name) {
    auto table = db.try_find_table(ks, cdc::log_name(base_name));
    if (!table) {
        dlogger.warn("Couldn't find cdc log table for base table {}.{}", ks, base_name);
        return std::nullopt;
    }

    fragmented_ostringstream os;
    auto schema = table->schema();
    auto describe_helper = replica::make_schema_describe_helper(schema, db);
    schema->describe_alter_with_properties(describe_helper, os);

    auto schema_desc = schema->describe(describe_helper, describe_option::NO_STMTS);
    schema_desc.create_statement = std::move(os).to_managed_string();
    return schema_desc;
}

future<utils::chunked_vector<description>> table(config_snapshot& snapshot, const data_dictionary::database& db, const lw_shared_ptr<keyspace_metadata>& ks_meta, const sstring& name, bool with_internals) {
    auto table = db.try_find_table(ks_meta->name(), name);
    if (!table) {
        throw exceptions::invalid_request_exception(format("Table '{}' not found in keyspace '{}'", name, ks_meta->name()));
    }
    
    auto s = validation::validate_column_family(db, ks_meta->name(), name);
    if (s->is_view()) { 
        throw exceptions::invalid_request_exception("Cannot use DESC TABLE on materialized View. (Did you mean DESC MATERIALIZED VIEW)?");
    }

    auto schema = table->schema();
    auto idxs = table->get_index_manager().list_indexes();
    auto views = table->views();
    utils::chunked_vector<description> result;

    // table
    auto describe_helper = replica::make_schema_describe_helper(schema, db);
    auto table_desc = schema->describe(describe_helper, with_internals ? describe_option::STMTS_AND_INTERNALS : describe_option::STMTS);
    std::vector<described_config_option> table_config_options;
    auto version = db::cluster_config_registry::current_version(db.features());
    for (const auto& config_name : config_option_names_for_scope(db::cluster_config_registry::scope::table, version)) {
        if (auto described = co_await described_table_config_option(snapshot, ks_meta->name(), name, config_name)) {
            table_config_options.push_back(std::move(*described));
        }
    }
    if (!table_config_options.empty()) {
        append_create_statement_options(table_desc, table_config_options, !with_internals);
    }
    if (cdc::is_log_for_some_table(db.real_database(), ks_meta->name(), name)) {
        // If the table the user wants to describe is a CDC log table, we want to print it as a CQL comment.
        // This way, the user learns about the internals of the table, but they're also told not to execute it.
        fragmented_ostringstream os{};

        fmt::format_to(os.to_iter(),
                "/* Do NOT execute this statement! It's only for informational purposes.\n"
                "   A CDC log table is created automatically when creating the base with CDC\n"
                "   enabled option or creating the vector index on the base table's vector column.\n"
                "\n{}\n"
                "*/",
                *table_desc.create_statement);

        table_desc.create_statement = std::move(os).to_managed_string();
    } else if (service::paxos::paxos_store::try_get_base_table(name)) {
        // Paxos state table is internally managed by Scylla and it shouldn't be exposed to the user.
        // The table is allowed to be described as a comment to ease administrative work but it's hidden from all listings.
        fragmented_ostringstream os{};

        fmt::format_to(os.to_iter(),
                "/* Do NOT execute this statement! It's only for informational purposes.\n"
                "   A paxos state table is created automatically when enabling LWT on a base table.\n"
                "\n{}\n"
                "*/",
                *table_desc.create_statement);

        table_desc.create_statement = std::move(os).to_managed_string();
    }
    result.push_back(std::move(table_desc));

    // indexes
    std::ranges::sort(idxs, std::ranges::less(), [] (const auto& a) {
        return a.metadata().name();
    });
    for (const auto& idx: idxs) {
        result.push_back(index(db, ks_meta->name(), idx.metadata().name(), with_internals));
        co_await coroutine::maybe_yield();
    }

    //views
    std::ranges::sort(views, std::ranges::less(), std::mem_fn(&schema::cf_name));
    for (const auto& v: views) {
        if(!is_index(db, v)) {
            result.push_back(view(db, ks_meta->name(), v->cf_name(), with_internals));
        }
        co_await coroutine::maybe_yield();
    }

    if (cdc::cdc_enabled(*schema)) {
        auto cdc_log_alter = describe_cdc_log_table(db, ks_meta->name(), name);
        if (cdc_log_alter) {
            result.push_back(std::move(*cdc_log_alter));
        }
    }

    co_return result;
}

future<utils::chunked_vector<description>> tables(config_snapshot& snapshot, const data_dictionary::database& db, const lw_shared_ptr<keyspace_metadata>& ks, std::optional<bool> with_internals = std::nullopt) {
    auto& replica_db = db.real_database();
    auto tables = ks->tables() | std::views::filter([&replica_db] (const schema_ptr& s) {
        return !cdc::is_log_for_some_table(replica_db, s->ks_name(), s->cf_name()) && !service::paxos::paxos_store::try_get_base_table(s->cf_name());
    }) | std::ranges::to<std::vector<schema_ptr>>();
    std::ranges::sort(tables, std::ranges::less(), std::mem_fn(&schema::cf_name));

    if (with_internals) {
        utils::chunked_vector<description> result;
        for (const auto& t: tables) {
            auto tables_desc = co_await table(snapshot, db, ks, t->cf_name(), *with_internals);
            result.insert(result.end(), std::make_move_iterator(tables_desc.begin()), std::make_move_iterator(tables_desc.end()));
            co_await coroutine::maybe_yield();
        }

        co_return result;
    }

    co_return tables | std::views::transform([&db] (auto&& t) {
        return t->describe(replica::make_schema_describe_helper(t, db), describe_option::NO_STMTS);
    }) | std::ranges::to<utils::chunked_vector<description>>();
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
future<utils::chunked_vector<description>> list_elements(config_snapshot& snapshot, const data_dictionary::database& db, const sstring& ks, element_type element) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();

    switch (element) {
    case element_type::type:         co_return co_await types(replica_db, ks_meta, with_create_statement::no);
    case element_type::function:     co_return co_await functions(replica_db, ks, with_create_statement::no);
    case element_type::aggregate:    co_return co_await aggregates(replica_db, ks, with_create_statement::no);
    case element_type::table:        co_return co_await tables(snapshot, db, ks_meta);
    case element_type::keyspace:     co_return wrap_in_vector(co_await keyspace_description(snapshot, ks_meta, replica_db, with_create_statement::no, false));
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
future<utils::chunked_vector<description>> describe_element(config_snapshot& snapshot, const data_dictionary::database& db, const sstring& ks, element_type element, const sstring& name, bool with_internals) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();

    switch (element) {
    case element_type::type:             co_return wrap_in_vector(type(replica_db, ks_meta, name));
    case element_type::function:         co_return co_await function(replica_db, ks, name);
    case element_type::aggregate:        co_return co_await aggregate(replica_db, ks, name);
    case element_type::table:            co_return co_await table(snapshot, db, ks_meta, name, with_internals);
    case element_type::index:            co_return wrap_in_vector(index(db, ks, name, with_internals));
    case element_type::view:             co_return wrap_in_vector(view(db, ks, name, with_internals));
    case element_type::keyspace:         co_return wrap_in_vector(co_await keyspace_description(snapshot, ks_meta, replica_db, with_create_statement::yes, with_internals));
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
future<utils::chunked_vector<description>> describe_all_keyspace_elements(config_snapshot& snapshot, const data_dictionary::database& db, const sstring& ks, bool with_internals) {
    auto ks_meta = get_keyspace_metadata(db, ks);
    auto& replica_db = db.real_database();
    utils::chunked_vector<description> result;

    auto inserter = [&result] (utils::chunked_vector<description>&& elements) mutable {
        result.insert(result.end(), std::make_move_iterator(elements.begin()), std::make_move_iterator(elements.end()));
    };

    result.push_back(co_await keyspace_description(snapshot, ks_meta, replica_db, with_create_statement::yes, with_internals));
    inserter(co_await types(replica_db, ks_meta, with_create_statement::yes));
    inserter(co_await functions(replica_db, ks, with_create_statement::yes));
    inserter(co_await aggregates(replica_db, ks, with_create_statement::yes));
    inserter(co_await tables(snapshot, db, ks_meta, with_internals));

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

std::vector<std::vector<managed_bytes_opt>> serialize_descriptions(utils::chunked_vector<description>&& descs, bool serialize_create_statement = true) {
    return descs | std::views::as_rvalue | std::views::transform([serialize_create_statement] (description desc) {
        return std::move(desc).serialize(serialize_create_statement);
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

future<managed_bytes_opt> cluster_describe_statement::range_ownership(const service::storage_proxy& proxy, const sstring& ks) const {
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

future<std::vector<std::vector<managed_bytes_opt>>> cluster_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {   
    auto& proxy = qp.proxy();
    auto& ss = qp.storage_service();

    auto cluster_info = ss.describe_cluster();
    std::vector<managed_bytes_opt> row {
        {to_managed_bytes(cluster_info.cluster_name)},
        {to_managed_bytes(cluster_info.partitioner)},
        {to_managed_bytes(cluster_info.snitch_name)}
    };

    if (should_add_range_ownership(proxy.local_db(), client_state)) {
        auto ro_map = co_await range_ownership(proxy, client_state.get_raw_keyspace());
        row.push_back(std::move(ro_map));
    }
    
    std::vector<std::vector<managed_bytes_opt>> result {std::move(row)};
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

future<std::vector<std::vector<managed_bytes_opt>>> schema_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto db = qp.db();
    // One config snapshot per DESCRIBE execution: every described object is annotated
    // against the same config state, read at most once per config-tables row.
    config_snapshot snapshot(qp);

    auto result = co_await std::visit(overloaded_functor{
        [&] (const schema_desc& config) -> future<utils::chunked_vector<description>> {
            auto& auth_service = *client_state.get_auth_service();

            if (config.with_hashed_passwords) {
                if (!co_await client_state.has_superuser()) {
                    co_await coroutine::return_exception(exceptions::unauthorized_exception(
                            "DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS can only be issued by a superuser"));
                }
            }

            auto keyspaces = config.full_schema ? db.get_all_keyspaces() : db.get_user_keyspaces();
            utils::chunked_vector<description> schema_result;

            // Cluster-scope config leads the dump, before any keyspace and in all tiers:
            // it is portable operator state that lower scopes inherit from, and emitting it
            // only under INTERNALS would make the default dump silently lossy.
            if (db::cluster_config_registry::current_version(db.features())) {
                const auto& cluster_configs = (co_await snapshot.cluster()).get();
                if (!cluster_configs.empty()) {
                    std::vector<scope_chain_entry> chain{{config_origin::cluster, cluster_configs}};
                    description cluster_desc;
                    cluster_desc.keyspace = std::nullopt;
                    cluster_desc.type = "cluster_config";
                    cluster_desc.name = "cluster";
                    cluster_desc.create_statement = make_scope_create_statement("ALTER CLUSTER", db::cluster_config_registry::scope::cluster, chain, !_with_internals);
                    schema_result.push_back(std::move(cluster_desc));
                }
            }

            for (auto&& ks: keyspaces) {
                if (!config.full_schema && db.extensions().is_extension_internal_keyspace(ks)) {
                    continue;
                }
                auto ks_result = co_await describe_all_keyspace_elements(snapshot, db, ks, _with_internals);
                schema_result.insert(schema_result.end(), std::make_move_iterator(ks_result.begin()), std::make_move_iterator(ks_result.end()));
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
        [&] (const keyspace_desc& config) -> future<utils::chunked_vector<description>> {
            auto ks = client_state.get_raw_keyspace();
            if (config.keyspace) {
                ks = *config.keyspace;
            }

            if (config.only_keyspace) {
                auto ks_meta = get_keyspace_metadata(db, ks);
                auto ks_desc = co_await keyspace_description(snapshot, ks_meta, db.real_database(), with_create_statement::yes, _with_internals);

                co_return wrap_in_vector(std::move(ks_desc));
            } else {
                co_return co_await describe_all_keyspace_elements(snapshot, db, ks, _with_internals);
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

future<std::vector<std::vector<managed_bytes_opt>>> listing_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto db = qp.db();
    std::vector<sstring> keyspaces;
    // For most describe statements we should limit the results to the USEd
    // keyspace (client_state.get_raw_keyspace()), if any. However for DESC
    // KEYSPACES we must list all keyspaces, not just the USEd one.
    if (_element != element_type::keyspace && !client_state.get_raw_keyspace().empty()) {
        keyspaces.push_back(client_state.get_raw_keyspace());
    } else {
        keyspaces = db.get_all_keyspaces();
        std::ranges::sort(keyspaces);
    }

    utils::chunked_vector<description> result;
    config_snapshot snapshot(qp);
    for (auto&& ks: keyspaces) {
        auto ks_result = co_await list_elements(snapshot, db, ks, _element);
        result.insert(result.end(), std::make_move_iterator(ks_result.begin()), std::make_move_iterator(ks_result.end()));
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

future<std::vector<std::vector<managed_bytes_opt>>> element_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto ks = client_state.get_raw_keyspace();
    if (_keyspace) {
        ks = *_keyspace;
    }
    if (ks.empty()) {
        throw exceptions::invalid_request_exception("No keyspace specified and no current keyspace");
    }
    
    config_snapshot snapshot(qp);
    co_return serialize_descriptions(co_await describe_element(snapshot, qp.db(), ks, _element, _name, _with_internals));
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

future<std::vector<std::vector<managed_bytes_opt>>> generic_describe_statement::describe(cql3::query_processor& qp, const service::client_state& client_state) const {
    auto db = qp.db();
    auto& replica_db = db.real_database();
    auto raw_ks = client_state.get_raw_keyspace();
    auto ks_name = (_keyspace) ? *_keyspace : raw_ks;
    config_snapshot snapshot(qp);

    if (!_keyspace) {
        auto ks = db.try_find_keyspace(_name);

        if (ks) {
            co_return serialize_descriptions(co_await describe_all_keyspace_elements(snapshot, db, ks->metadata()->name(), _with_internals));
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
            co_return serialize_descriptions(wrap_in_vector(view(db, ks_name, _name, _with_internals)));
        } else {
            co_return serialize_descriptions(co_await table(snapshot, db, ks_meta, _name, _with_internals));
        }
    }

    auto idx_tbl = db.find_indexed_table(ks_name, _name);
    if (idx_tbl) {
        co_return serialize_descriptions(wrap_in_vector(index(db, ks_name, _name, _with_internals)));
    }

    auto udt_meta = ks_meta->user_types();
    if (udt_meta.has_type(to_bytes(_name))) {
        co_return serialize_descriptions(wrap_in_vector(type(replica_db, ks_meta, _name)));
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

std::unique_ptr<prepared_statement> describe_statement::prepare(data_dictionary::database db, cql_stats &stats, const cql_config& cfg) {
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
