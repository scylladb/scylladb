/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <seastar/core/fstream.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/util/closeable.hh>

#include "cql3/query_processor.hh"
#include "cql3/statements/create_keyspace_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/create_type_statement.hh"
#include "cql3/statements/update_statement.hh"
#include "db/cql_type_parser.hh"
#include "db/config.hh"
#include "db/schema_tables.hh"
#include "database.hh"
#include "gms/feature_service.hh"
#include "locator/token_metadata.hh"
#include "locator/snitch_base.hh"
#include "tools/schema_loader.hh"
#include "utils/fb_utilities.hh"
#include "schema_registry.hh"

namespace {

sstring read_file(std::filesystem::path path) {
    auto file = open_file_dma(path.native(), open_flags::ro).get();
    auto fstream = make_file_input_stream(file);
    return httpd::read_entire_stream_contiguous(fstream).get();
}

std::vector<schema_ptr> do_load_schemas(std::string_view schema_str) {
    cql3::cql_stats cql_stats;

    db::config cfg;
    cfg.enable_cache(false);
    cfg.volatile_system_keyspace_for_testing(true);

    database_config dbcfg;
    dbcfg.available_memory = 1'000'000'000; // 1G
    service::migration_notifier migration_notifier;
    gms::feature_service feature_service(gms::feature_config_from_db_config(cfg));
    feature_service.enable(feature_service.known_feature_set());
    locator::shared_token_metadata token_metadata([] () noexcept { return db::schema_tables::hold_merge_lock(); });
    abort_source as;
    sharded<semaphore> sst_dir_sem;

    sst_dir_sem.start(1).get();
    auto sst_dir_sem_stop = deferred_stop(sst_dir_sem);

    utils::fb_utilities::set_broadcast_address(gms::inet_address(0x7f000001)); // 127.0.0.1
    if (!locator::i_endpoint_snitch::snitch_instance().local_is_initialized()) {
        locator::i_endpoint_snitch::create_snitch(cfg.endpoint_snitch()).get();
    }

    schema_registry registry;

    database db(cfg, dbcfg, migration_notifier, feature_service, token_metadata, registry, as, sst_dir_sem);
    auto stop_db = deferred_stop(db);

    // Mock system_schema keyspace to be able to parse modification statements
    // against system_schema.dropped_columns.
    db.create_keyspace(make_lw_shared<keyspace_metadata>(
                db::schema_tables::NAME,
                "org.apache.cassandra.locator.LocalStrategy",
                std::map<sstring, sstring>{},
                false)).get();
    db.add_column_family(db.find_keyspace(db::schema_tables::NAME), db::schema_tables::dropped_columns(db.get_schema_registry()), {});

    std::vector<schema_ptr> schemas;

    auto find_or_create_keyspace = [&] (const sstring& name) -> keyspace& {
        try {
            return db.find_keyspace(name);
        } catch (no_such_keyspace&) {
            // fall-though to below
        }
        auto raw_statement = cql3::query_processor::parse_statement(
                fmt::format("CREATE KEYSPACE {} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}", name));
        auto prepared_statement = raw_statement->prepare(db, cql_stats);
        auto* statement = prepared_statement->statement.get();
        auto p = dynamic_cast<cql3::statements::create_keyspace_statement*>(statement);
        assert(p);
        db.create_keyspace(p->get_keyspace_metadata(*token_metadata.get())).get();
        return db.find_keyspace(name);
    };


    std::vector<std::unique_ptr<cql3::statements::raw::parsed_statement>> raw_statements;
    try {
        raw_statements = cql3::query_processor::parse_statements(schema_str);
    } catch (...) {
        throw std::runtime_error(format("tools:do_load_schemas(): failed to parse CQL statements: {}", std::current_exception()));
    }
    for (auto& raw_statement : raw_statements) {
        auto prepared_statement = raw_statement->prepare(db, cql_stats);
        auto* statement = prepared_statement->statement.get();

        if (auto p = dynamic_cast<cql3::statements::create_keyspace_statement*>(statement)) {
            db.create_keyspace(p->get_keyspace_metadata(*token_metadata.get())).get();
        } else if (auto p = dynamic_cast<cql3::statements::create_type_statement*>(statement)) {
            auto type = p->create_type(db);
            find_or_create_keyspace(p->keyspace()).add_user_type(std::move(type));
        } else if (auto p = dynamic_cast<cql3::statements::create_table_statement*>(statement)) {
            schemas.push_back(p->get_cf_meta_data(db));
        } else if (auto p = dynamic_cast<cql3::statements::update_statement*>(statement)) {
            if (p->keyspace() != db::schema_tables::NAME && p->column_family() != db::schema_tables::DROPPED_COLUMNS) {
                throw std::runtime_error(fmt::format("tools::do_load_schemas(): expected modification statement to be against {}.{}, but it is against {}.{}",
                            db::schema_tables::NAME, db::schema_tables::DROPPED_COLUMNS, p->keyspace(), p->column_family()));
            }
            auto schema = db::schema_tables::dropped_columns(db.get_schema_registry());
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
                auto type = db::cql_type_parser::parse(keyspace_name, row.get_nonnull<sstring>("type"));
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

future<schema_ptr> load_one_schema(std::string_view schema_str) {
    return async([schema_str] () mutable {
        auto schemas = do_load_schemas(schema_str);
        if (schemas.size() != 1) {
            throw std::runtime_error(fmt::format("Schema string expected to contain exactly 1 schema, actually has {}", schemas.size()));
        }
        return std::move(schemas.front());
    });

}

future<std::vector<schema_ptr>> load_schemas_from_file(std::filesystem::path path) {
    return async([path] () mutable {
        return do_load_schemas(read_file(path));
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

} // namespace tools
