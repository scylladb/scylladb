/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
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

#include "redis/keyspace_utils.hh"
#include "schema_builder.hh"
#include "types.hh"
#include "exceptions/exceptions.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "seastar/core/future.hh"
#include <memory>
#include "log.hh"
#include "db/query_context.hh"
#include "auth/service.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"
#include "transport/server.hh"
#include "db/system_keyspace.hh"
#include "schema.hh"
#include "database.hh"
#include "gms/gossiper.hh"
#include <seastar/core/print.hh>

using namespace seastar;

namespace redis {

static logging::logger logger("keyspace_utils");
schema_ptr strings_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::STRINGs), ks_name, redis::STRINGs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save STRINGs for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr lists_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::LISTs), ks_name, redis::LISTs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", bytes_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save LISTs for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr hashes_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::HASHes), ks_name, redis::HASHes,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save HASHes for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr sets_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::SETs), ks_name, redis::SETs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", utf8_type}},
     // regular columns
     {},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save SETs for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr zsets_schema(sstring ks_name) {
     schema_builder builder(make_lw_shared(schema(generate_legacy_id(ks_name, redis::ZSETs), ks_name, redis::ZSETs,
     // partition key
     {{"pkey", utf8_type}},
     // clustering key
     {{"ckey", double_type}},
     // regular columns
     {{"data", utf8_type}},
     // static columns
     {},
     // regular column name type
     utf8_type,
     // comment
     "save ZSETs for redis"
    )));
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

future<> create_keyspace_if_not_exists_impl(db::config& config, int default_replication_factor) {
    auto keyspace_replication_strategy_options = config.redis_keyspace_replication_strategy_options();
    if (keyspace_replication_strategy_options.count("class") == 0) {
        keyspace_replication_strategy_options["class"] = "SimpleStrategy";
        keyspace_replication_strategy_options["replication_factor"] = sprint("%d", default_replication_factor);
    }
    auto keyspace_gen = [&config, keyspace_replication_strategy_options = std::move(keyspace_replication_strategy_options)]  (sstring name) {
        auto& proxy = service::get_local_storage_proxy();
        if (proxy.get_db().local().has_keyspace(name)) {
            return make_ready_future<>();
        }
        auto attrs = make_shared<cql3::statements::ks_prop_defs>();
        attrs->add_property(cql3::statements::ks_prop_defs::KW_DURABLE_WRITES, "true");
        std::map<sstring, sstring> replication_properties;
        for (auto&& option : keyspace_replication_strategy_options) {
            replication_properties.emplace(option.first, option.second);
        }
        attrs->add_property(cql3::statements::ks_prop_defs::KW_REPLICATION, replication_properties); 
        attrs->validate();
        const auto& tm = proxy.get_token_metadata();
        return service::get_local_migration_manager().announce_new_keyspace(attrs->as_ks_metadata(name, tm), false);
    };
    auto table_gen = [] (sstring ks_name, sstring cf_name, schema_ptr schema) {
        auto& proxy = service::get_local_storage_proxy();
        if (proxy.get_db().local().has_schema(ks_name, cf_name)) {
            return make_ready_future<>();
        }
        logger.info("Create keyspace: {}, table: {} for redis.", ks_name, cf_name);
        return service::get_local_migration_manager().announce_new_column_family(schema, false);
    };
    // create 16 default database for redis.
    return parallel_for_each(boost::irange<unsigned>(0, 16), [keyspace_gen = std::move(keyspace_gen), table_gen = std::move(table_gen)] (auto c) {
        auto ks_name = sprint("REDIS_%d", c);
        return keyspace_gen(ks_name).then([ks_name, table_gen] {
            return when_all_succeed(
                table_gen(ks_name, redis::STRINGs, strings_schema(ks_name)),
                table_gen(ks_name, redis::LISTs, lists_schema(ks_name)),
                table_gen(ks_name, redis::SETs, sets_schema(ks_name)),
                table_gen(ks_name, redis::HASHes, hashes_schema(ks_name)),
                table_gen(ks_name, redis::ZSETs, zsets_schema(ks_name))
            ).discard_result();
        });
    });
}

future<> maybe_create_keyspace(db::config& config) {
    return gms::get_up_endpoint_count().then([&config] (auto live_endpoint_count) {
        int replication_factor = 3;
        if (live_endpoint_count < replication_factor) {
            replication_factor = 1;
            logger.warn("Creating keyspace for redis with unsafe, live endpoint nodes count: {}.", live_endpoint_count);
        }
        return create_keyspace_if_not_exists_impl(config, replication_factor);
    });
}

}
