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
#include "cql3/statements/create_view_statement.hh"
#include "cql3/statements/create_index_statement.hh"
#include "cql3/statements/update_statement.hh"
#include "db/cql_type_parser.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "db/large_data_handler.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/schema_tables.hh"
#include "db/system_keyspace.hh"
#include "partition_slice_builder.hh"
#include "readers/combined.hh"
#include "replica/database.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_directory.hh"
#include "types/list.hh"
#include "data_dictionary/impl.hh"
#include "data_dictionary/data_dictionary.hh"
#include "gms/feature_service.hh"
#include "locator/abstract_replication_strategy.hh"
#include "tools/schema_loader.hh"
#include "view_info.hh"

namespace {

logging::logger sllog("schema_loader");

class data_dictionary_impl;
struct keyspace;
struct table;

struct database {
    db::extensions extensions;
    const db::config& cfg;
    gms::feature_service& features;
    std::list<keyspace> keyspaces;
    std::list<table> tables;

    database(const db::config& cfg, gms::feature_service& features) : cfg(cfg), features(features)
    { }
    database(database&&) = delete;
};

struct keyspace {
    lw_shared_ptr<keyspace_metadata> metadata;

    explicit keyspace(lw_shared_ptr<keyspace_metadata> metadata) : metadata(std::move(metadata))
    { }
    keyspace(keyspace&&) = delete;
};

struct table {
    const keyspace& ks;
    schema_ptr schema;
    secondary_index::secondary_index_manager secondary_idx_man;
    bool user;

    table(data_dictionary_impl& impl, const keyspace& ks, schema_ptr schema, bool user);
    table(table&&) = delete;
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
    virtual const table_schema_version& get_version(data_dictionary::database) const override {
        throw std::bad_function_call();
    }
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
    virtual sstring get_available_index_name(data_dictionary::database db, std::string_view ks_name, std::string_view cf_name,
            std::optional<sstring> index_name_root) const override {
        auto has_schema = [&] (std::string_view ks_name, std::string_view table_name) {
            const auto& tables = unwrap(db).tables;
            return std::find_if(tables.begin(), tables.end(), [&] (const table& t) {
                return t.schema->ks_name() == ks_name && t.schema->cf_name() == table_name;
            }) != tables.end();
        };
        return secondary_index::get_available_index_name(ks_name, cf_name, index_name_root, existing_index_names(db, ks_name), has_schema);
    }
    virtual std::set<sstring> existing_index_names(data_dictionary::database db, std::string_view ks_name, std::string_view cf_to_exclude = {}) const override {
        auto tables = boost::copy_range<std::vector<schema_ptr>>(unwrap(db).tables
                | boost::adaptors::filtered([ks_name] (const table& t) { return t.schema->ks_name() == ks_name; })
                | boost::adaptors::transformed([] (const table& t) { return t.schema; }));
        return secondary_index::existing_index_names(tables, cf_to_exclude);
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

table::table(data_dictionary_impl& impl, const keyspace& ks, schema_ptr schema, bool user) :
    ks(ks), schema(std::move(schema)), secondary_idx_man(impl.wrap(*this)), user(user)
{ }

sstring read_file(std::filesystem::path path) {
    auto file = open_file_dma(path.native(), open_flags::ro).get();
    auto fstream = make_file_input_stream(file);
    return util::read_entire_stream_contiguous(fstream).get();
}

std::vector<schema_ptr> do_load_schemas(const db::config& cfg, std::string_view schema_str) {
    cql3::cql_stats cql_stats;

    gms::feature_service feature_service(gms::feature_config_from_db_config(cfg));
    feature_service.enable(feature_service.supported_feature_set()).get();
    sharded<locator::shared_token_metadata> token_metadata;

    auto my_address = gms::inet_address("localhost");
    locator::token_metadata::config tm_cfg;
    tm_cfg.topo_cfg.this_endpoint = my_address;
    tm_cfg.topo_cfg.this_cql_address = my_address;
    token_metadata.start([] () noexcept { return db::schema_tables::hold_merge_lock(); }, tm_cfg).get();
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
                std::nullopt,
                false));
    real_db.tables.emplace_back(dd_impl, real_db.keyspaces.back(), db::schema_tables::dropped_columns(), false);

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
        real_db.keyspaces.emplace_back(p->get_keyspace_metadata(*token_metadata.local().get(), feature_service));
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
            real_db.keyspaces.emplace_back(p->get_keyspace_metadata(*token_metadata.local().get(), feature_service));
        } else if (auto p = dynamic_cast<cql3::statements::create_type_statement*>(statement)) {
            dd_impl.unwrap(ks).metadata->add_user_type(p->create_type(db));
        } else if (auto p = dynamic_cast<cql3::statements::create_table_statement*>(statement)) {
            auto schema = p->get_cf_meta_data(db);
            // CDC tables use a custom partitioner, which is not reflected when
            // dumping the schema to schema.cql, so we have to manually set it here.
            if (cdc::is_log_name(schema->cf_name())) {
                schema_builder b(std::move(schema));
                b.with_partitioner(cdc::cdc_partitioner::classname);
                schema = b.build();
            }
            real_db.tables.emplace_back(dd_impl, dd_impl.unwrap(ks), std::move(schema), true);
        } else if (auto p = dynamic_cast<cql3::statements::create_view_statement*>(statement)) {
            auto&& [view, warnings] = p->prepare_view(db);
            auto it = std::find_if(real_db.tables.begin(), real_db.tables.end(), [&] (const table& t) { return t.schema->ks_name() == view->ks_name() && t.schema->cf_name() == view->cf_name(); });
            if (it != real_db.tables.end()) {
                continue; // view already exists
            }
            real_db.tables.emplace_back(dd_impl, dd_impl.unwrap(ks), view, true);
        } else if (auto p = dynamic_cast<cql3::statements::create_index_statement*>(statement)) {
            auto res = p->build_index_schema(db);
            if (!res) {
                continue; // index already exists
            }
            auto [new_base_schema, index] = *res;
            auto it = std::find_if(real_db.tables.begin(), real_db.tables.end(), [&] (const table& t) { return t.schema->id() == new_base_schema->id(); });
            if (it == real_db.tables.end()) { // shouldn't happen but let's handle it
                throw std::runtime_error(fmt::format("tools::do_load_schemas(): failed to look up base table {}.{}, while creating index on it", new_base_schema->ks_name(), new_base_schema->cf_name()));
            }
            it->schema = std::move(new_base_schema);
            it->secondary_idx_man.reload();
            const bool new_token_column_computation = db.features().correct_idx_token_in_secondary_index;
            auto view = it->secondary_idx_man.create_view_for_index(index, new_token_column_computation);
            real_db.tables.emplace_back(dd_impl, dd_impl.unwrap(ks), view, true);
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
                auto it = std::find_if(real_db.tables.begin(), real_db.tables.end(), [&] (const table& t) {
                    auto& s = t.schema;
                    return s->ks_name() == keyspace_name && s->cf_name() == table_name;
                });
                if (it == real_db.tables.end()) {
                    throw std::runtime_error(fmt::format("tools::do_load_schemas(): failed applying update to {}.{}, the table it applies to is not found: {}.{}",
                            db::schema_tables::NAME, db::schema_tables::DROPPED_COLUMNS, keyspace_name, table_name));
                }
                auto name = row.get_nonnull<sstring>("column_name");
                auto type = db::cql_type_parser::parse(keyspace_name, row.get_nonnull<sstring>("type"), user_types_storage(real_db));
                auto time = row.get_nonnull<db_clock::time_point>("dropped_time");
                it->schema = schema_builder(std::move(it->schema)).without_column(std::move(name), std::move(type), time.time_since_epoch().count()).build();
            }
        } else {
            throw std::runtime_error(fmt::format("tools::do_load_schemas(): expected statement to be one of (create keyspace, create type, create table), got: {}",
                        typeid(statement).name()));
        }
    }

    return boost::copy_range<std::vector<schema_ptr>>(
            real_db.tables |
            boost::adaptors::filtered([] (const table& t) { return t.user; }) |
            boost::adaptors::transformed([] (const table& t) { return t.schema; }));
}

struct sstable_manager_service {
    db::nop_large_data_handler large_data_handler;
    gms::feature_service feature_service;
    cache_tracker tracker;
    sstables::directory_semaphore dir_sem;
    sstables::sstables_manager sst_man;

    explicit sstable_manager_service(const db::config& dbcfg)
        : feature_service(gms::feature_config_from_db_config(dbcfg))
        , dir_sem(1)
        , sst_man("schema_loader", large_data_handler, dbcfg, feature_service, tracker, memory::stats().total_memory(), dir_sem, []{ return locator::host_id{}; }) {
    }

    future<> stop() {
        return sst_man.close();
    }
};

mutation_opt read_schema_table_mutation(sharded<sstable_manager_service>& sst_man, std::filesystem::path schema_table_data_path,
        std::function<schema_ptr()> schema_factory, reader_permit permit, std::string_view keyspace, std::vector<std::string_view> ck_strings) {
    if (sllog.level() == log_level::trace) {
        auto schema = schema_factory();
        sllog.trace("read_schema_table_mutation(): path={}, table={}.{}, pk={}, ck={}",
                schema_table_data_path.native(),
                schema->ks_name(),
                schema->cf_name(),
                keyspace,
                ck_strings);
    }

    sharded<sstables::sstable_directory> sst_dirs;
    sst_dirs.start(
        sharded_parameter([&sst_man] { return std::ref(sst_man.local().sst_man); }),
        sharded_parameter([&schema_factory] { return schema_factory(); }),
        sharded_parameter([&] { return std::ref(schema_factory()->get_sharder()); }),
        sharded_parameter([] { return make_lw_shared<const data_dictionary::storage_options>(); }),
        schema_table_data_path.native(), sstables::sstable_state::normal,
        sharded_parameter([] { return default_io_error_handler_gen(); })).get();
    auto stop_sst_dirs = deferred_stop(sst_dirs);

    auto sstable_open_infos = sst_dirs.map_reduce0(
            [] (sstables::sstable_directory& sst_dir) -> future<std::vector<sstables::foreign_sstable_open_info>> {
               co_await sst_dir.process_sstable_dir(sstables::sstable_directory::process_flags{ .sort_sstables_according_to_owner = false });
               const auto& unsorted_ssts = sst_dir.get_unsorted_sstables();
               std::vector<sstables::foreign_sstable_open_info> open_infos;
               open_infos.reserve(unsorted_ssts.size());
               for (auto& sst : unsorted_ssts) {
                   open_infos.push_back(co_await sst->get_open_info());
               }
               co_return open_infos;
            },
            std::vector<sstables::foreign_sstable_open_info>{},
            [] (std::vector<sstables::foreign_sstable_open_info> a, std::vector<sstables::foreign_sstable_open_info> b) {
                std::move(b.begin(), b.end(), std::back_inserter(a));
                return a;
            }).get();

    auto schema_table_schema = schema_factory();

    if (sstable_open_infos.empty()) {
        sllog.debug("read_schema_table_mutation(): no sstables found for path={}, table={}.{}, pk={}, ck={}",
                schema_table_data_path.native(),
                schema_table_schema->ks_name(),
                schema_table_schema->cf_name(),
                keyspace,
                ck_strings);
        return {};
    }

    std::vector<sstables::shared_sstable> sstables;
    sstables.reserve(sstable_open_infos.size());
    for (auto& open_info : sstable_open_infos) {
        sstables.push_back(sst_dirs.local().load_foreign_sstable(open_info).get());
    }

    auto pk = partition_key::from_deeply_exploded(*schema_table_schema, {data_value(keyspace)});
    auto dk = dht::decorate_key(*schema_table_schema, pk);
    auto pr = dht::partition_range::make_singular(dk);

    std::vector<data_value> raw_ck_values;
    raw_ck_values.reserve(ck_strings.size());
    for (const auto& ck_str : ck_strings) {
        raw_ck_values.push_back(data_value(ck_str));
    }
    auto ck = clustering_key::from_deeply_exploded(*schema_table_schema, raw_ck_values);
    auto cr = query::clustering_range::make({ck, true}, {ck, true});
    auto ps = partition_slice_builder(*schema_table_schema)
            .with_range(cr)
            .build();

    sllog.trace("read_schema_table_mutation(): preparing to read {} sstables for table={}.{}, pr={} ({}), cr={} ({})\n{}",
            sstable_open_infos.size(),
            schema_table_schema->ks_name(),
            schema_table_schema->cf_name(),
            pr,
            keyspace,
            cr,
            ck_strings,
            sstables);

    std::vector<flat_mutation_reader_v2> readers;
    readers.reserve(sstables.size());
    for (const auto& sst : sstables) {
        readers.emplace_back(sst->make_reader(schema_table_schema, permit, pr, ps));
    }
    auto reader = make_combined_reader(schema_table_schema, permit, std::move(readers));
    auto close_reader = deferred_close(reader);

    auto mut_opt = read_mutation_from_flat_mutation_reader(reader).get();

    if (mut_opt) {
        sllog.debug("read_schema_table_mutation(): read mutation for table={}.{}, pr={} ({}), cr={} ({})\n{}",
                schema_table_schema->ks_name(),
                schema_table_schema->cf_name(),
                pr,
                keyspace,
                cr,
                ck_strings,
                *mut_opt);
    } else {
        sllog.debug("read_schema_table_mutation(): read empty mutation for table={}.{}, pr={} ({}), cr={} ({})",
                schema_table_schema->ks_name(),
                schema_table_schema->cf_name(),
                pr,
                keyspace,
                cr,
                ck_strings);
    }

    return mut_opt;
}

class single_keyspace_user_types_storage : public data_dictionary::user_types_storage {
    data_dictionary::user_types_metadata _utm;
public:
    single_keyspace_user_types_storage(data_dictionary::user_types_metadata utm) : _utm(std::move(utm)) { }
    virtual const data_dictionary::user_types_metadata& get(const sstring& ks) const override {
        return _utm;
    }
};

std::unordered_map<schema_ptr, std::string> get_schema_table_directories(std::filesystem::path scylla_data_path) {
    const std::vector<schema_ptr> schemas{
            db::schema_tables::types(),
            db::schema_tables::tables(),
            db::schema_tables::views(),
            db::schema_tables::columns(),
            db::schema_tables::view_virtual_columns(),
            db::schema_tables::computed_columns(),
            db::schema_tables::indexes(),
            db::schema_tables::dropped_columns(),
            db::schema_tables::scylla_tables()};

    std::unordered_map<schema_ptr, std::string> schema_table_table_dir;

    auto schema_tables_path = scylla_data_path / db::schema_tables::NAME;
    auto schema_tables_dir = open_directory(schema_tables_path.native()).get();

    schema_tables_dir.list_directory([&] (directory_entry de) -> future<> {
        auto dash_pos = de.name.find_last_of('-');
        auto table_name = de.name.substr(0, dash_pos);

        auto it = boost::find_if(schemas, [&] (const schema_ptr& s) {
            return s->cf_name() == table_name;
        });

        if (it != schemas.end()) {
            if (!de.type) {
                throw std::runtime_error(fmt::format("failed loading schema tables from {}: keyspace directory entry {} has unrecognized type", scylla_data_path.native(), de.name));
            } else if (*de.type != directory_entry_type::directory) {
                throw std::runtime_error(fmt::format("failed loading schema tables from {}: keyspace directory entry {} has unrecognized type {}", scylla_data_path.native(), de.name, static_cast<int>(*de.type)));
            }
            auto s = *it;
            schema_table_table_dir[s] = de.name;
        }
        return make_ready_future<>();
    }).done().get();

    if (schema_table_table_dir.size() != schemas.size()) {
        throw std::runtime_error(fmt::format("failed loading schema tables from {}: couldn't find table directory for all require schema tables", scylla_data_path.native()));
    }

    return schema_table_table_dir;
}

schema_ptr do_load_schema_from_schema_tables(const db::config& dbcfg, std::filesystem::path scylla_data_path, std::string_view keyspace, std::string_view table) {
    reader_concurrency_semaphore rcs_sem(reader_concurrency_semaphore::no_limits{}, __FUNCTION__, reader_concurrency_semaphore::register_metrics::no);
    auto stop_semaphore = deferred_stop(rcs_sem);

    sharded<sstable_manager_service> sst_man;
    sst_man.start(std::ref(dbcfg)).get();
    auto stop_sst_man_service = deferred_stop(sst_man);

    auto schema_table_table_dir = get_schema_table_directories(scylla_data_path);
    auto schema_tables_path = scylla_data_path / db::schema_tables::NAME;

    auto empty = [] (const mutation_opt& mopt) {
        return !mopt || !mopt->partition().row_count();
    };
    auto do_load = [&] (std::function<const schema_ptr()> schema_factory) {
        auto s = schema_factory();
        return read_schema_table_mutation(
                sst_man,
                schema_tables_path / schema_table_table_dir[s],
                schema_factory,
                rcs_sem.make_tracking_only_permit(s, "schema_mutation", db::no_timeout, {}),
                keyspace,
                {table});
    };
    mutation_opt tables = do_load(db::schema_tables::tables);
    mutation_opt views = do_load(db::schema_tables::views);
    mutation_opt columns = do_load(db::schema_tables::columns);
    mutation_opt view_virtual_columns = do_load(db::schema_tables::view_virtual_columns);
    mutation_opt computed_columns = do_load(db::schema_tables::computed_columns);
    mutation_opt indexes = do_load(db::schema_tables::indexes);
    mutation_opt dropped_columns = do_load(db::schema_tables::dropped_columns);
    mutation_opt scylla_tables = do_load([] () { return db::schema_tables::scylla_tables(); });

    if ((empty(tables) && empty(views)) || empty(columns)) {
        throw std::runtime_error(fmt::format("Failed to find {}.{} in schema tables", keyspace, table));
    }

    data_dictionary::user_types_metadata utm;

    auto types_mut = read_schema_table_mutation(
            sst_man,
            schema_tables_path / schema_table_table_dir[db::schema_tables::types()],
            db::schema_tables::types,
            rcs_sem.make_tracking_only_permit(db::schema_tables::types(), "types_mutation", db::no_timeout, {}),
            keyspace,
            {});
    if (types_mut) {
        query::result_set result(*types_mut);

        auto ks = make_lw_shared<keyspace_metadata>(keyspace, "org.apache.cassandra.locator.LocalStrategy", std::map<sstring, sstring>{}, std::nullopt, false);
        db::cql_type_parser::raw_builder ut_builder(*ks);

        auto get_list = [] (const query::result_set_row& row, const char* name) {
            return boost::copy_range<std::vector<sstring>>(
                    row.get_nonnull<const list_type_impl::native_type&>(name)
                    | boost::adaptors::transformed([] (const data_value& v) { return value_cast<sstring>(v); }));
        };

        for (const auto& row : result.rows()) {
            const auto name = row.get_nonnull<sstring>("type_name");
            const auto field_names = get_list(row, "field_names");
            const auto field_types = get_list(row, "field_types");
            ut_builder.add(name, field_names, field_types);
        }

        for (auto&& ut : ut_builder.build()) {
            utm.add_type(std::move(ut));
        }
    }

    auto user_type_storage = std::make_shared<single_keyspace_user_types_storage>(std::move(utm));
    gms::feature_service features(gms::feature_config_from_db_config(dbcfg));
    db::schema_ctxt ctxt(dbcfg, user_type_storage, features);

    if (empty(tables)) {
        tables = std::move(views);
    }

    schema_mutations muts(std::move(*tables), std::move(*columns), std::move(view_virtual_columns), std::move(computed_columns), std::move(indexes),
            std::move(dropped_columns), std::move(scylla_tables));
    if (muts.is_view()) {
        return db::schema_tables::create_view_from_mutations(ctxt, muts);
    } else {
        return db::schema_tables::create_table_from_mutations(ctxt, muts);
    }
}

} // anonymous namespace

namespace tools {

future<std::vector<schema_ptr>> load_schemas(const db::config& dbcfg, std::string_view schema_str) {
    return async([&dbcfg, schema_str] () mutable {
        return do_load_schemas(dbcfg, schema_str);
    });
}

future<schema_ptr> load_one_schema_from_file(const db::config& dbcfg, std::filesystem::path path) {
    return async([&dbcfg, path] () mutable {
        auto schemas = do_load_schemas(dbcfg, read_file(path));
        if (schemas.size() == 1) {
            return std::move(schemas.front());
        } else if (schemas.size() == 2) {
            // We expect a base table at index 0 and a view/index on it at index 1
            if (!schemas[0]->is_view() && schemas[1]->is_view() && schemas[0]->id() == schemas[1]->view_info()->base_id()) {
                return std::move(schemas[1]);
            }
        }
        throw std::runtime_error(fmt::format(
                    "Schema file {} expected to contain exactly 1 schema or 2 schemas (base table and view), actually has {} non-related schemas",
                    path.native(),
                    schemas.size()));
    });
}

schema_ptr load_system_schema(const db::config& cfg, std::string_view keyspace, std::string_view table) {
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

future<schema_ptr> load_schema_from_schema_tables(const db::config& cfg, std::filesystem::path scylla_data_path, std::string_view keyspace, std::string_view table) {
    return async([=, &cfg] () mutable {
        return do_load_schema_from_schema_tables(cfg, scylla_data_path, keyspace, table);
    });
}

} // namespace tools
