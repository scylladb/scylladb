/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "utils/assert.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "table_helper.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"

static logging::logger tlogger("table_helper");

static schema_ptr parse_new_cf_statement(cql3::query_processor& qp, const sstring& create_cql) {
    auto db = qp.db();

    auto parsed = cql3::query_processor::parse_statement(create_cql);

    cql3::statements::raw::cf_statement* parsed_cf_stmt = static_cast<cql3::statements::raw::cf_statement*>(parsed.get());
    (void)parsed_cf_stmt->keyspace(); // This will SCYLLA_ASSERT if cql statement did not contain keyspace
    ::shared_ptr<cql3::statements::create_table_statement> statement =
                    static_pointer_cast<cql3::statements::create_table_statement>(
                                    parsed_cf_stmt->prepare(db, qp.get_cql_stats())->statement);
    auto schema = statement->get_cf_meta_data(db);

    // Generate the CF UUID based on its KF names. This is needed to ensure that
    // all Nodes that create it would create it with the same UUID and we don't
    // hit the #420 issue.
    auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    return b.build();
}

future<> table_helper::setup_table(cql3::query_processor& qp, service::migration_manager& mm, const sstring& create_cql) {
    auto db = qp.db();

    auto schema = parse_new_cf_statement(qp, create_cql);

    if (db.has_schema(schema->ks_name(), schema->cf_name())) {
        co_return;
    }

    auto group0_guard = co_await mm.start_group0_operation();
    auto ts = group0_guard.write_timestamp();

    if (db.has_schema(schema->ks_name(), schema->cf_name())) { // re-check after read barrier
        co_return;
    }

    // We don't care it it fails really - this may happen due to concurrent
    // "CREATE TABLE" invocation on different Nodes.
    // The important thing is that it will converge eventually (some traces may
    // be lost in a process but that's ok).
    try {
        co_return co_await mm.announce(co_await service::prepare_new_column_family_announcement(qp.proxy(), schema, ts),
                std::move(group0_guard), format("table_helper: create {} table", schema->cf_name()));
    } catch (...) {}
}

future<bool> table_helper::try_prepare(bool fallback, cql3::query_processor& qp, service::query_state& qs) {
    // Note: `_insert_cql_fallback` is known to be engaged if `fallback` is true, see cache_table_info below.
    auto& stmt = fallback ? _insert_cql_fallback.value() : _insert_cql;
    try {
        shared_ptr<cql_transport::messages::result_message::prepared> msg_ptr = co_await qp.prepare(stmt, qs.get_client_state());
        _prepared_stmt = std::move(msg_ptr->get_prepared());
        shared_ptr<cql3::cql_statement> cql_stmt = _prepared_stmt->statement;
        _insert_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
        _is_fallback_stmt = fallback;
        co_return true;
    } catch (exceptions::invalid_request_exception& eptr) {
        // the non-fallback statement can't be prepared, and there is no possible fallback
        if (!fallback && !_insert_cql_fallback) {
            throw;
        }
        // We're trying to prepare the fallback statement, but it can't be prepared; signal an
        // unrecoverable error
        if (fallback) {
            throw;
        }

        // There's still a chance to prepare the fallback statement
        co_return false;
    }
}

future<> table_helper::cache_table_info(cql3::query_processor& qp, service::migration_manager& mm, service::query_state& qs) {
    if (!_prepared_stmt) {
        // if prepared statement has been invalidated - drop cached pointers
        _insert_stmt = nullptr;
    } else if (!_is_fallback_stmt) {
        // we've already prepared the non-fallback statement
        co_return;
    }

    try {
        bool success = co_await try_prepare(false, qp, qs);
        if (_is_fallback_stmt && _prepared_stmt) {
            co_return;
        }
        if (!success) {
            co_await try_prepare(true, qp, qs); // Can only return true or exception when preparing the fallback statement
        }
    } catch (...) {
        auto eptr = std::current_exception();

        // One of the possible causes for an error here could be the table that doesn't exist.
        //FIXME: discarded future.
        (void)qp.container().invoke_on(0, [&mm = mm.container(), create_cql = _create_cql] (cql3::query_processor& qp) -> future<> {
            co_return co_await table_helper::setup_table(qp, mm.local(), create_cql);
        });

        // We throw the bad_column_family exception because the caller
        // expects and accounts this type of errors.
        try {
            std::rethrow_exception(eptr);
        } catch (std::exception& e) {
            throw bad_column_family(_keyspace, _name, e);
        } catch (...) {
            throw bad_column_family(_keyspace, _name);
        }
    }
}

future<> table_helper::insert(cql3::query_processor& qp, service::migration_manager& mm, service::query_state& qs, noncopyable_function<cql3::query_options ()> opt_maker) {
    co_await cache_table_info(qp, mm, qs);
    auto opts = opt_maker();
    opts.prepare(_prepared_stmt->bound_names);
    co_await _insert_stmt->execute(qp, qs, opts, std::nullopt);
}

future<> table_helper::setup_keyspace(cql3::query_processor& qp, service::migration_manager& mm, std::string_view keyspace_name, sstring replication_factor, service::query_state& qs, std::vector<table_helper*> tables) {
    if (this_shard_id() != 0) {
        co_return;
    }

    // FIXME: call `announce` once (`announce` keyspace and tables together)
    //
    // Note that the CQL code in `parse_new_cf_statement` assumes that the keyspace exists.
    // To solve this problem, we could, for example, use `schema_builder` instead of the
    // CQL statements to create tables in `table_helper`.

    if (std::any_of(tables.begin(), tables.end(), [&] (table_helper* t) { return t->_keyspace != keyspace_name; })) {
        throw std::invalid_argument("setup_keyspace called with table_helper for different keyspace");
    }

    data_dictionary::database db = qp.db();

    std::map<sstring, sstring> opts;
    opts["replication_factor"] = replication_factor;
    auto ksm = keyspace_metadata::new_keyspace(keyspace_name, "org.apache.cassandra.locator.SimpleStrategy", std::move(opts), std::nullopt);

    while (!db.has_keyspace(keyspace_name)) {
        auto group0_guard = co_await mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();

        if (!db.has_keyspace(keyspace_name)) {
            try {
                co_await mm.announce(service::prepare_new_keyspace_announcement(db.real_database(), ksm, ts),
                        std::move(group0_guard), format("table_helper: create {} keyspace", keyspace_name));
            } catch (service::group0_concurrent_modification&) {
                tlogger.info("Concurrent operation is detected while creating {} keyspace, retrying.", keyspace_name);
            }
        }
    }

    qs.get_client_state().set_keyspace(db.real_database(), keyspace_name);

    while (std::any_of(tables.begin(), tables.end(), [db] (table_helper* t) { return !db.has_schema(t->_keyspace, t->_name); })) {
        auto group0_guard = co_await mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();
        std::vector<mutation> table_mutations;

        co_await coroutine::parallel_for_each(tables, [&] (auto&& table) -> future<> {
            auto schema = parse_new_cf_statement(qp, table->_create_cql);
            if (!db.has_schema(schema->ks_name(), schema->cf_name())) {
                co_return co_await service::prepare_new_column_family_announcement(table_mutations, qp.proxy(), *ksm, schema, ts);
            }
        });

        if (table_mutations.empty()) {
            co_return;
        }

        try {
            co_return co_await mm.announce(std::move(table_mutations), std::move(group0_guard),
                    format("table_helper: create tables for {} keyspace", keyspace_name));
        } catch (service::group0_concurrent_modification&) {
            tlogger.info("Concurrent operation is detected while creating tables for {} keyspace, retrying.", keyspace_name);
        }
    }
}
