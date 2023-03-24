/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <seastar/core/fstream.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/util/closeable.hh>
#include <seastar/util/short_streams.hh>

#include "cdc/cdc_partitioner.hh"
#include "cdc/log.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/create_keyspace_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/create_type_statement.hh"
#include "cql3/statements/update_statement.hh"
#include "db/cql_type_parser.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "replica/database.hh"
#include "data_dictionary/impl.hh"
#include "data_dictionary/data_dictionary.hh"
#include "gms/feature_service.hh"
#include "locator/abstract_replication_strategy.hh"
#include "tools/schema_loader.hh"

namespace {

class data_dictionary_impl;
struct keyspace;
struct table;

struct database {
    db::extensions extensions;
    db::config& cfg;
    gms::feature_service& features;
    std::list<keyspace> keyspaces;
    std::list<table> tables;

    database(db::config& cfg, gms::feature_service& features) : cfg(cfg), features(features)
    { }
};

struct keyspace {
    lw_shared_ptr<keyspace_metadata> metadata;

    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata) : metadata(std::move(metadata))
    { }
};

struct table {
    keyspace& ks;
    schema_ptr schema;
    secondary_index::secondary_index_manager secondary_idx_man;

    table(data_dictionary_impl& impl, keyspace& ks, schema_ptr schema);
};

class data_dictionary_impl : public data_dictionary::impl {
public:
    const data_dictionary::database wrap(const database& db) const {
        return make_database(this, &db);
    }
    data_dictionary::keyspace wrap(const keyspace& ks) const {
        return make_keyspace(this, &ks);
    }
    data_dictionary::table wrap(const table& t) const {
        return make_table(this, &t);
    }
    static const database& unwrap(data_dictionary::database db) {
        return *static_cast<const database*>(extract(db));
    }
    static const keyspace& unwrap(data_dictionary::keyspace ks) {
        return *static_cast<const keyspace*>(extract(ks));
    }
    static const table& unwrap(data_dictionary::table t) {
        return *static_cast<const table*>(extract(t));
    }

private:
    virtual std::optional<data_dictionary::keyspace> try_find_keyspace(data_dictionary::database db, std::string_view name) const override {
        auto& keyspaces = unwrap(db).keyspaces;
        auto it = std::find_if(keyspaces.begin(), keyspaces.end(), [name] (const keyspace& ks) { return ks.metadata->name() == name; });
        if (it == keyspaces.end()) {
            return {};
        }
        return wrap(*it);
    }
    virtual std::vector<data_dictionary::keyspace> get_keyspaces(data_dictionary::database db) const override {
        return boost::copy_range<std::vector<data_dictionary::keyspace>>(unwrap(db).keyspaces | boost::adaptors::transformed([this] (const keyspace& ks) { return wrap(ks); }));
    }
    virtual std::vector<sstring> get_user_keyspaces(data_dictionary::database db) const override {
        return boost::copy_range<std::vector<sstring>>(
            unwrap(db).keyspaces 
            | boost::adaptors::transformed([] (const keyspace& ks) { return ks.metadata->name(); })
            | boost::adaptors::filtered([] (const sstring& ks) {return !is_internal_keyspace(ks); })
        );
    }
    virtual std::vector<sstring> get_all_keyspaces(data_dictionary::database db) const override {
        return boost::copy_range<std::vector<sstring>>(unwrap(db).keyspaces | boost::adaptors::transformed([] (const keyspace& ks) { return ks.metadata->name(); }));
    }
    virtual std::vector<data_dictionary::table> get_tables(data_dictionary::database db) const override {
        return boost::copy_range<std::vector<data_dictionary::table>>(unwrap(db).tables | boost::adaptors::transformed([this] (const table& ks) { return wrap(ks); }));
    }
    virtual std::optional<data_dictionary::table> try_find_table(data_dictionary::database db, std::string_view ks, std::string_view tab) const override {
        auto& tables = unwrap(db).tables;
        auto it = std::find_if(tables.begin(), tables.end(), [ks, tab] (const table& tbl) { return tbl.schema->ks_name() == ks && tbl.schema->cf_name() == tab; });
        if (it == tables.end()) {
            return {};
        }
        return wrap(*it);
    }
    virtual std::optional<data_dictionary::table> try_find_table(data_dictionary::database db, table_id id) const override {
        auto& tables = unwrap(db).tables;
        auto it = std::find_if(tables.begin(), tables.end(), [id] (const table& tbl) { return tbl.schema->id() == id; });
        if (it == tables.end()) {
            return {};
        }
        return wrap(*it);
    }
    virtual const secondary_index::secondary_index_manager& get_index_manager(data_dictionary::table t) const override {
        return unwrap(t).secondary_idx_man;
    }
    virtual schema_ptr get_table_schema(data_dictionary::table t) const override {
        return unwrap(t).schema;
    }
    virtual lw_shared_ptr<keyspace_metadata> get_keyspace_metadata(data_dictionary::keyspace ks) const override {
        return unwrap(ks).metadata;
    }
    virtual bool is_internal(data_dictionary::keyspace ks) const override {
        return is_system_keyspace(unwrap(ks).metadata->name());
    }
    virtual const locator::abstract_replication_strategy& get_replication_strategy(data_dictionary::keyspace ks) const override {
        throw std::bad_function_call();
    }
    virtual const std::vector<view_ptr>& get_table_views(data_dictionary::table t) const override {
        static const std::vector<view_ptr> empty;
        return empty;
    }
    virtual sstring get_available_index_name(data_dictionary::database db, std::string_view ks_name, std::string_view table_name,
            std::optional<sstring> index_name_root) const override {
        throw std::bad_function_call();
    }
    virtual std::set<sstring> existing_index_names(data_dictionary::database db, std::string_view ks_name, std::string_view cf_to_exclude = {}) const override {
        return {};
    }
    virtual schema_ptr find_indexed_table(data_dictionary::database db, std::string_view ks_name, std::string_view index_name) const override {
        return {};
    }
    virtual schema_ptr get_cdc_base_table(data_dictionary::database db, const schema&) const override {
        return {};
    }
    virtual const db::config& get_config(data_dictionary::database db) const override {
        return unwrap(db).cfg;
    }
    virtual const db::extensions& get_extensions(data_dictionary::database db) const override {
        return unwrap(db).extensions;
    }
    virtual const gms::feature_service& get_features(data_dictionary::database db) const override {
        return unwrap(db).features;
    }
    virtual replica::database& real_database(data_dictionary::database db) const override {
        throw std::bad_function_call();
    }
};

class user_types_storage : public data_dictionary::user_types_storage {
    database& _db;
public:
    user_types_storage(database& db) noexcept : _db(db) {}

    virtual const data_dictionary::user_types_metadata& get(const sstring& name) const override {
        for (const auto& ks : _db.keyspaces) {
            if (ks.metadata->name() == name) {
                return ks.metadata->user_types();
            }
        }
        throw data_dictionary::no_such_keyspace(name);
    }
};

table::table(data_dictionary_impl& impl, keyspace& ks, schema_ptr schema) :
    ks(ks), schema(std::move(schema)), secondary_idx_man(impl.wrap(*this))
{ }

sstring read_file(std::filesystem::path path) {
    auto file = open_file_dma(path.native(), open_flags::ro).get();
    auto fstream = make_file_input_stream(file);
    return util::read_entire_stream_contiguous(fstream).get();
}

std::vector<schema_ptr> do_load_schemas(std::string_view schema_str) {
    cql3::cql_stats cql_stats;

    db::config cfg;
    cfg.enable_cache(false);
    cfg.volatile_system_keyspace_for_testing(true);

    gms::feature_service feature_service(gms::feature_config_from_db_config(cfg));
    feature_service.enable(feature_service.supported_feature_set()).get();
    sharded<locator::shared_token_metadata> token_metadata;

    utils::fb_utilities::set_broadcast_address(gms::inet_address("localhost"));
    token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, locator::token_metadata::config{}).get();
    auto stop_token_metadata = deferred_stop(token_metadata);

    data_dictionary_impl dd_impl;
    database real_db(cfg, feature_service);
    auto db = dd_impl.wrap(real_db);

    // Mock system_schema keyspace to be able to parse modification statements
    // against system_schema.dropped_columns.
    real_db.keyspaces.emplace_back(make_lw_shared<keyspace_metadata>(
                db::schema_tables::NAME,
                "org.apache.cassandra.locator.LocalStrategy",
                std::map<sstring, sstring>{},
                false));
    real_db.tables.emplace_back(dd_impl, real_db.keyspaces.back(), db::schema_tables::dropped_columns());

    std::vector<schema_ptr> schemas;

    auto find_or_create_keyspace = [&] (const sstring& name) -> data_dictionary::keyspace {
        try {
            return db.find_keyspace(name);
        } catch (replica::no_such_keyspace&) {
            // fall-though to below
        }
        auto raw_statement = cql3::query_processor::parse_statement(
                fmt::format("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}", name));
        auto prepared_statement = raw_statement->prepare(db, cql_stats);
        auto* statement = prepared_statement->statement.get();
        auto p = dynamic_cast<cql3::statements::create_keyspace_statement*>(statement);
        assert(p);
        real_db.keyspaces.emplace_back(p->get_keyspace_metadata(*token_metadata.local().get()));
        return db.find_keyspace(name);
    };


    std::vector<std::unique_ptr<cql3::statements::raw::parsed_statement>> raw_statements;
    try {
        raw_statements = cql3::query_processor::parse_statements(schema_str);
    } catch (...) {
        throw std::runtime_error(format("tools:do_load_schemas(): failed to parse CQL statements: {}", std::current_exception()));
    }
    for (auto& raw_statement : raw_statements) {
        auto cf_statement = dynamic_cast<cql3::statements::raw::cf_statement*>(raw_statement.get());
        if (!cf_statement) {
            continue; // we don't support any non-cf statements here
        }
        auto ks = find_or_create_keyspace(cf_statement->keyspace());
        auto prepared_statement = cf_statement->prepare(db, cql_stats);
        auto* statement = prepared_statement->statement.get();

        if (auto p = dynamic_cast<cql3::statements::create_keyspace_statement*>(statement)) {
            real_db.keyspaces.emplace_back(p->get_keyspace_metadata(*token_metadata.local().get()));
        } else if (auto p = dynamic_cast<cql3::statements::create_type_statement*>(statement)) {
            dd_impl.unwrap(ks).metadata->add_user_type(p->create_type(db));
        } else if (auto p = dynamic_cast<cql3::statements::create_table_statement*>(statement)) {
            schemas.push_back(p->get_cf_meta_data(db));
            // CDC tables use a custom partitioner, which is not reflected when
            // dumping the schema to schema.cql, so we have to manually set it here.
            if (cdc::is_log_name(schemas.back()->cf_name())) {
                schema_builder b(std::move(schemas.back()));
                b.with_partitioner(cdc::cdc_partitioner::classname);
                schemas.back() = b.build();
            }
        } else if (auto p = dynamic_cast<cql3::statements::update_statement*>(statement)) {
            if (p->keyspace() != db::schema_tables::NAME && p->column_family() != db::schema_tables::DROPPED_COLUMNS) {
                throw std::runtime_error(fmt::format("tools::do_load_schemas(): expected modification statement to be against {}.{}, but it is against {}.{}",
                            db::schema_tables::NAME, db::schema_tables::DROPPED_COLUMNS, p->keyspace(), p->column_family()));
            }
            auto schema = db::schema_tables::dropped_columns();
            cql3::statements::modification_statement::json_cache_opt json_cache{};
            cql3::update_parameters params(schema, cql3::query_options::DEFAULT, api::new_timestamp(), schema->default_time_to_live(), cql3::update_parameters::prefetch_data(schema));
            auto pkeys = p->build_partition_keys(cql3::query_options::DEFAULT, json_cache);
            auto ckranges = p->create_clustering_ranges(cql3::query_options::DEFAULT, json_cache);
            auto updates = p->apply_updates(pkeys, ckranges, params, json_cache);
            if (updates.size() != 1) {
                throw std::runtime_error(fmt::format("tools::do_load_schemas(): expected one update per statement for {}.{}, got: {}",
                            db::schema_tables::NAME, db::schema_tables::DROPPED_COLUMNS, updates.size()));
            }
            auto& mut = updates.front();
            if (!mut.partition().row_tombstones().empty()) {
                throw std::runtime_error(fmt::format("tools::do_load_schemas(): expected only update against {}.{}, not deletes",
                            db::schema_tables::NAME, db::schema_tables::DROPPED_COLUMNS));
            }
            query::result_set rs(mut);
            for (auto& row : rs.rows()) {
                const auto keyspace_name = row.get_nonnull<sstring>("keyspace_name");
                const auto table_name = row.get_nonnull<sstring>("table_name");
                auto it = std::find_if(schemas.begin(), schemas.end(), [&] (schema_ptr s) {
                    return s->ks_name() == keyspace_name && s->cf_name() == table_name;
                });
                if (it == schemas.end()) {
                    throw std::runtime_error(fmt::format("tools::do_load_schemas(): failed applying update to {}.{}, the table it applies to is not found: {}.{}",
                            db::schema_tables::NAME, db::schema_tables::DROPPED_COLUMNS, keyspace_name, table_name));
                }
                auto name = row.get_nonnull<sstring>("column_name");
                auto type = db::cql_type_parser::parse(keyspace_name, row.get_nonnull<sstring>("type"), user_types_storage(real_db));
                auto time = row.get_nonnull<db_clock::time_point>("dropped_time");
                *it = schema_builder(*it).without_column(std::move(name), std::move(type), time.time_since_epoch().count()).build();
            }
        } else {
            throw std::runtime_error(fmt::format("tools::do_load_schemas(): expected statement to be one of (create keyspace, create type, create table), got: {}",
                        typeid(statement).name()));
        }
    }

    return schemas;
}

} // anonymous namespace

namespace tools {

future<std::vector<schema_ptr>> load_schemas(std::string_view schema_str) {
    return async([schema_str] () mutable {
        return do_load_schemas(schema_str);
    });
}

future<schema_ptr> load_one_schema_from_file(std::filesystem::path path) {
    return async([path] () mutable {
        auto schemas = do_load_schemas(read_file(path));
        if (schemas.size() != 1) {
            throw std::runtime_error(fmt::format("Schema file {} expected to contain exactly 1 schema, actually has {}", path.native(), schemas.size()));
        }
        return std::move(schemas.front());
    });
}

schema_ptr load_system_schema(std::string_view keyspace, std::string_view table) {
    db::config cfg;
    cfg.experimental.set(true);
    cfg.experimental_features.set(db::experimental_features_t::all());
    const std::unordered_map<std::string_view, std::vector<schema_ptr>> schemas{
        {db::schema_tables::NAME, db::schema_tables::all_tables(db::schema_features::full())},
        {db::system_keyspace::NAME, db::system_keyspace::all_tables(cfg)},
        {db::system_distributed_keyspace::NAME, db::system_distributed_keyspace::all_distributed_tables()},
        {db::system_distributed_keyspace::NAME_EVERYWHERE, db::system_distributed_keyspace::all_everywhere_tables()},
    };
    auto ks_it = schemas.find(keyspace);
    if (ks_it == schemas.end()) {
        throw std::invalid_argument(fmt::format("unknown system keyspace: {}", keyspace));
    }
    auto tb_it = boost::find_if(ks_it->second, [&] (const schema_ptr& s) {
        return s->cf_name() == table;
    });
    if (tb_it == ks_it->second.end()) {
        throw std::invalid_argument(fmt::format("unknown table {} in system keyspace: {}", table, keyspace));
    }
    return *tb_it;
}

} // namespace tools
