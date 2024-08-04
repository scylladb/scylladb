/*
 * Copyright (C) 2019 pengjian.uestc @ gmail.com
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "redis/keyspace_utils.hh"
#include "schema/schema_builder.hh"
#include "types/types.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include <seastar/core/future.hh>
#include "log.hh"
#include "auth/service.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"
#include "service/client_state.hh"
#include "transport/server.hh"
#include "db/system_keyspace.hh"
#include "schema/schema.hh"
#include "gms/gossiper.hh"
#include <seastar/core/print.hh>
#include "db/config.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include <boost/algorithm/cxx11/all_of.hpp>

using namespace seastar;

namespace redis {

static logging::logger logger("keyspace_utils");
schema_ptr strings_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::STRINGs), ks_name, redis::STRINGs,
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
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr lists_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::LISTs), ks_name, redis::LISTs,
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
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr hashes_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::HASHes), ks_name, redis::HASHes,
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
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr sets_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::SETs), ks_name, redis::SETs,
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
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

schema_ptr zsets_schema(sstring ks_name) {
     schema_builder builder(generate_legacy_id(ks_name, redis::ZSETs), ks_name, redis::ZSETs,
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
    );
    builder.set_gc_grace_seconds(0);
    builder.with(schema_builder::compact_storage::yes);
    builder.with_version(db::system_keyspace::generate_schema_version(builder.uuid()));
    return builder.build(schema_builder::compact_storage::yes);
}

future<> create_keyspace_if_not_exists_impl(seastar::sharded<service::storage_proxy>& proxy, data_dictionary::database db, seastar::sharded<service::migration_manager>& mm, db::config& config, int default_replication_factor) {
    SCYLLA_ASSERT(this_shard_id() == 0);
    auto keyspace_replication_strategy_options = config.redis_keyspace_replication_strategy_options();
    if (!keyspace_replication_strategy_options.contains("class")) {
        keyspace_replication_strategy_options["class"] = "SimpleStrategy";
        keyspace_replication_strategy_options["replication_factor"] = fmt::format("{}", default_replication_factor);
    }

    struct table {
        const char* name;
        std::function<schema_ptr(sstring)> schema;
    };

    static std::array tables{table{redis::STRINGs, strings_schema},
                             table{redis::LISTs, lists_schema},
                             table{redis::SETs, sets_schema},
                             table{redis::HASHes, hashes_schema},
                             table{redis::ZSETs, zsets_schema}};

    auto ks_names = boost::copy_range<std::vector<sstring>>(
            boost::irange<unsigned>(0, config.redis_database_count()) |
            boost::adaptors::transformed([] (unsigned i) { return fmt::format("REDIS_{}", i); }));

    while (true) {
        bool schema_ok = boost::algorithm::all_of(ks_names, [&] (auto& ks_name) {
            auto check = [&] (table t) {
                return db.has_schema(ks_name, t.name);
            };
            return db.has_keyspace(ks_name) && boost::algorithm::all_of(tables, check);
        });

        if (schema_ok) {
            logger.info("Redis schema is already up-to-date");
            co_return; // if schema is created already do nothing
        }

        auto& mml = mm.local();
        auto tm = proxy.local().get_token_metadata_ptr();

        std::vector<lw_shared_ptr<keyspace_metadata>> ksms;
        for (auto& ks_name: ks_names) {
            cql3::statements::ks_prop_defs attrs;
            attrs.add_property(cql3::statements::ks_prop_defs::KW_DURABLE_WRITES, "true");
            std::map<sstring, sstring> replication_properties;
            for (auto&& option : keyspace_replication_strategy_options) {
                replication_properties.emplace(option.first, option.second);
            }
            attrs.add_property(cql3::statements::ks_prop_defs::KW_REPLICATION, replication_properties);
            attrs.validate();

            ksms.push_back(attrs.as_ks_metadata(ks_name, *tm, proxy.local().features()));
        }

        auto group0_guard = co_await mml.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        std::vector<mutation> mutations;

        for (auto ksm: ksms) {
            if (db.has_keyspace(ksm->name())) {
                continue;
            }

            auto muts = service::prepare_new_keyspace_announcement(db.real_database(), ksm, ts);
            std::move(muts.begin(), muts.end(), std::back_inserter(mutations));
        }

        auto table_gen = std::bind_front(
                [] (data_dictionary::database db, service::storage_proxy& sp, std::vector<mutation>& mutations,
                    api::timestamp_type ts, const keyspace_metadata& ksm, sstring cf_name, schema_ptr schema) -> future<> {
            if (db.has_schema(ksm.name(), cf_name)) {
                co_return;
            }

            logger.info("Create keyspace: {}, table: {} for redis.", ksm.name(), cf_name);
            co_await service::prepare_new_column_family_announcement(mutations, sp, ksm, schema, ts);
        }, db, std::ref(proxy.local()), std::ref(mutations), ts);

        co_await coroutine::parallel_for_each(ksms, [table_gen = std::move(table_gen)] (const lw_shared_ptr<keyspace_metadata> ksm) mutable {
            return parallel_for_each(tables, [ksm, table_gen = std::move(table_gen)] (table t) {
                return table_gen(*ksm, t.name, t.schema(ksm->name()));
            }).discard_result();
        });

        if (mutations.empty()) {
            co_return;
        }

        try {
            co_return co_await mml.announce(std::move(mutations), std::move(group0_guard),
                    "keyspace-utils: create default keyspaces and databases for redis");
        } catch (service::group0_concurrent_modification&) {
            logger.info("Concurrent operation is detected while creating default databases for redis, retrying.");
        }
    }
}

future<> maybe_create_keyspace(seastar::sharded<service::storage_proxy>& proxy, data_dictionary::database db, seastar::sharded<service::migration_manager>& mm, db::config& config, sharded<gms::gossiper>& gossiper) {
    auto live_endpoint_count = gossiper.local().get_up_endpoint_count();
    int replication_factor = 3;
    if (live_endpoint_count < replication_factor) {
        replication_factor = 1;
        logger.warn("Creating keyspace for redis with unsafe, live endpoint nodes count: {}.", live_endpoint_count);
    }
    return create_keyspace_if_not_exists_impl(proxy, db, mm, config, replication_factor);
}

}
