/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "audit/audit_cf_storage_helper.hh"

#include "cql3/query_processor.hh"
#include "data_dictionary/keyspace_metadata.hh"
#include "utils/UUID_gen.hh"
#include "utils/class_registrator.hh"
#include "cql3/query_options.hh"
#include "cql3/statements/ks_prop_defs.hh"
#include "service/migration_manager.hh"
#include "service/storage_proxy.hh"

namespace audit {

const sstring audit_cf_storage_helper::KEYSPACE_NAME("audit");
const sstring audit_cf_storage_helper::TABLE_NAME("audit_log");

audit_cf_storage_helper::audit_cf_storage_helper(cql3::query_processor& qp, service::migration_manager& mm)
    : _qp(qp)
    , _mm(mm)
    , _table(KEYSPACE_NAME, TABLE_NAME,
             fmt::format("CREATE TABLE IF NOT EXISTS {}.{} ("
                       "date timestamp, "
                       "node inet, "
                       "event_time timeuuid, "
                       "category text, "
                       "consistency text, "
                       "table_name text, "
                       "keyspace_name text, "
                       "operation text, "
                       "source inet, "
                       "username text, "
                       "error boolean, "
                       "PRIMARY KEY ((date, node), event_time))",
                       KEYSPACE_NAME, TABLE_NAME),
             fmt::format("INSERT INTO {}.{} ("
                       "date,"
                       "node,"
                       "event_time,"
                       "category,"
                       "consistency,"
                       "table_name,"
                       "keyspace_name,"
                       "operation,"
                       "source,"
                       "username,"
                       "error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       KEYSPACE_NAME, TABLE_NAME))
    , _dummy_query_state(service::client_state::for_internal_calls(), empty_service_permit())
{
}

future<> audit_cf_storage_helper::migrate_audit_table(service::group0_guard group0_guard) {
    while (true) {
        auto const ks = _qp.db().try_find_keyspace(KEYSPACE_NAME);
        if (ks && ks->metadata()->strategy_name() == "org.apache.cassandra.locator.SimpleStrategy") {
            data_dictionary::database db = _qp.db();
            cql3::statements::ks_prop_defs old_ks_prop_defs;
            auto old_ks_metadata = old_ks_prop_defs.as_ks_metadata_update(
                    ks->metadata(), *_qp.proxy().get_token_metadata_ptr(), db.features());
            std::map<sstring, sstring> strategy_opts;
            for (const auto &dc: _qp.proxy().get_token_metadata_ptr()->get_topology().get_datacenters())
                strategy_opts[dc] = "3";

            auto new_ks_metadata = keyspace_metadata::new_keyspace(KEYSPACE_NAME,
                                                                   "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                                   strategy_opts,
                                                                   std::nullopt, // initial_tablets
                                                                   old_ks_metadata->durable_writes(),
                                                                   old_ks_metadata->get_storage_options(),
                                                                   old_ks_metadata->tables());
            auto ts = group0_guard.write_timestamp();
            try {
                co_await _mm.announce(
                        service::prepare_keyspace_update_announcement(db.real_database(), new_ks_metadata, ts),
                        std::move(group0_guard), format("audit: Alter {} keyspace", KEYSPACE_NAME));
                break;
            } catch (::service::group0_concurrent_modification &) {
                logger.info("Concurrent operation is detected while altering {} keyspace, retrying.", KEYSPACE_NAME);
            }
            group0_guard = co_await _mm.start_group0_operation();
        } else {
            co_return;
        }
    }
}

future<> audit_cf_storage_helper::start(const db::config &cfg) {
    if (this_shard_id() != 0) {
        co_return;
    }

    if (auto ks = _qp.db().try_find_keyspace(KEYSPACE_NAME);
            !ks ||
            ks->metadata()->strategy_name() == "org.apache.cassandra.locator.SimpleStrategy") {

        auto group0_guard = co_await _mm.start_group0_operation();
        if (ks = _qp.db().try_find_keyspace(KEYSPACE_NAME); !ks) {
            // releasing, because table_helper::setup_keyspace creates a raft guard of its own
            service::release_guard(std::move(group0_guard));
            co_return co_await table_helper::setup_keyspace(_qp, _mm, KEYSPACE_NAME,
                                                            "org.apache.cassandra.locator.NetworkTopologyStrategy",
                                                            "3", _dummy_query_state, {&_table});
        } else if (ks->metadata()->strategy_name() == "org.apache.cassandra.locator.SimpleStrategy") {
            // We want to migrate the old (pre-Scylla 6.0) SimpleStrategy to a newer one.
            // The migrate_audit_table() function will do nothing if it races with another strategy change:
            // - either by another node doing the same thing in parallel,
            // - or a user manually changing the strategy of the same table.
            // Note we only check the strategy, not the replication factor.
            co_return co_await migrate_audit_table(std::move(group0_guard));
        } else {
            co_return;
        }
    }
}

future<> audit_cf_storage_helper::stop() {
    return make_ready_future<>();
}

future<> audit_cf_storage_helper::write(const audit_info* audit_info,
                                    socket_address node_ip,
                                    socket_address client_ip,
                                    db::consistency_level cl,
                                    const sstring& username,
                                    bool error) {
    return _table.insert(_qp, _mm, _dummy_query_state, make_data, audit_info, node_ip, client_ip, cl, username, error);
}

future<> audit_cf_storage_helper::write_login(const sstring& username,
                                              socket_address node_ip,
                                              socket_address client_ip,
                                              bool error) {
    return _table.insert(_qp, _mm, _dummy_query_state, make_login_data, node_ip, client_ip, username, error);
}

cql3::query_options audit_cf_storage_helper::make_data(const audit_info* audit_info,
                                                       socket_address node_ip,
                                                       socket_address client_ip,
                                                       db::consistency_level cl,
                                                       const sstring& username,
                                                       bool error) {
    auto time = std::chrono::system_clock::now();
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count();
    auto ticks_per_day = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::hours(24)).count();
    auto date = millis_since_epoch / ticks_per_day * ticks_per_day;
    thread_local static int64_t last_nanos = 0;
    auto time_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(last_nanos, time));
    auto consistency_level = fmt::format("{}", cl);
    std::vector<cql3::raw_value> values {
        cql3::raw_value::make_value(timestamp_type->decompose(date)),
        cql3::raw_value::make_value(inet_addr_type->decompose(node_ip.addr())),
        cql3::raw_value::make_value(uuid_type->decompose(time_id)),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->category_string())),
        cql3::raw_value::make_value(utf8_type->decompose(sstring(consistency_level))),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->table())),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->keyspace())),
        cql3::raw_value::make_value(utf8_type->decompose(audit_info->query())),
        cql3::raw_value::make_value(inet_addr_type->decompose(client_ip.addr())),
        cql3::raw_value::make_value(utf8_type->decompose(username)),
        cql3::raw_value::make_value(boolean_type->decompose(error)),
    };
    return cql3::query_options(cql3::default_cql_config, db::consistency_level::ONE, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT);
}

cql3::query_options audit_cf_storage_helper::make_login_data(socket_address node_ip,
                                                             socket_address client_ip,
                                                             const sstring& username,
                                                             bool error) {
    auto time = std::chrono::system_clock::now();
    auto millis_since_epoch = std::chrono::duration_cast<std::chrono::milliseconds>(time.time_since_epoch()).count();
    auto ticks_per_day = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::hours(24)).count();
    auto date = millis_since_epoch / ticks_per_day * ticks_per_day;
    thread_local static int64_t last_nanos = 0;
    auto time_id = utils::UUID_gen::get_time_UUID(table_helper::make_monotonic_UUID_tp(last_nanos, time));
    std::vector<cql3::raw_value> values {
            cql3::raw_value::make_value(timestamp_type->decompose(date)),
            cql3::raw_value::make_value(inet_addr_type->decompose(node_ip.addr())),
            cql3::raw_value::make_value(uuid_type->decompose(time_id)),
            cql3::raw_value::make_value(utf8_type->decompose(sstring("AUTH"))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring(""))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring(""))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring(""))),
            cql3::raw_value::make_value(utf8_type->decompose(sstring("LOGIN"))),
            cql3::raw_value::make_value(inet_addr_type->decompose(client_ip.addr())),
            cql3::raw_value::make_value(utf8_type->decompose(username)),
            cql3::raw_value::make_value(boolean_type->decompose(error)),
    };
    return cql3::query_options(cql3::default_cql_config, db::consistency_level::ONE, std::nullopt, std::move(values), false, cql3::query_options::specific_options::DEFAULT);
}

using registry = class_registrator<storage_helper, audit_cf_storage_helper, cql3::query_processor&, service::migration_manager&>;
static registry registrator1("audit_cf_storage_helper");

}
