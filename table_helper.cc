/*
 * Copyright (C) 2017-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include "table_helper.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"

future<> table_helper::setup_table(cql3::query_processor& qp, service::migration_manager& mm, const sstring& create_cql) {
    auto db = qp.db();

    auto parsed = cql3::query_processor::parse_statement(create_cql);

    cql3::statements::raw::cf_statement* parsed_cf_stmt = static_cast<cql3::statements::raw::cf_statement*>(parsed.get());
    (void)parsed_cf_stmt->keyspace(); // This will assert if cql statement did not contain keyspace
    ::shared_ptr<cql3::statements::create_table_statement> statement =
                    static_pointer_cast<cql3::statements::create_table_statement>(
                                    parsed_cf_stmt->prepare(db, qp.get_cql_stats())->statement);
    auto schema = statement->get_cf_meta_data(db);

    if (db.has_schema(schema->ks_name(), schema->cf_name())) {
        co_return;
    }

    auto group0_guard = co_await mm.start_group0_operation();
    auto ts = group0_guard.write_timestamp();

    if (db.has_schema(schema->ks_name(), schema->cf_name())) { // re-check after read barrier
        co_return;
    }

    // Generate the CF UUID based on its KF names. This is needed to ensure that
    // all Nodes that create it would create it with the same UUID and we don't
    // hit the #420 issue.
    auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);

    // We don't care it it fails really - this may happen due to concurrent
    // "CREATE TABLE" invocation on different Nodes.
    // The important thing is that it will converge eventually (some traces may
    // be lost in a process but that's ok).
    try {
        co_return co_await mm.announce(co_await service::prepare_new_column_family_announcement(qp.proxy(), b.build(), ts),
                std::move(group0_guard), format("table_helper: create {} table", schema->cf_name()));
    } catch (...) {}
}

future<> table_helper::cache_table_info(cql3::query_processor& qp, service::migration_manager& mm, service::query_state& qs) {
    if (!_prepared_stmt) {
        // if prepared statement has been invalidated - drop cached pointers
        _insert_stmt = nullptr;
    } else if (!_is_fallback_stmt) {
        // we've already prepared the non-fallback statement
        return now();
    }

    return qp.prepare(_insert_cql, qs.get_client_state(), false)
            .then([this] (shared_ptr<cql_transport::messages::result_message::prepared> msg_ptr) noexcept {
        _prepared_stmt = std::move(msg_ptr->get_prepared());
        shared_ptr<cql3::cql_statement> cql_stmt = _prepared_stmt->statement;
        _insert_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
        _is_fallback_stmt = false;
    }).handle_exception_type([this, &qs, &qp] (exceptions::invalid_request_exception& eptr) {
        // the non-fallback statement can't be prepared
        if (!_insert_cql_fallback) {
            return make_exception_future(eptr);
        }
        if (_is_fallback_stmt && _prepared_stmt) {
            // we have already prepared the fallback statement
            return now();
        }
        return qp.prepare(_insert_cql_fallback.value(), qs.get_client_state(), false)
                .then([this] (shared_ptr<cql_transport::messages::result_message::prepared> msg_ptr) noexcept {
            _prepared_stmt = std::move(msg_ptr->get_prepared());
            shared_ptr<cql3::cql_statement> cql_stmt = _prepared_stmt->statement;
            _insert_stmt = dynamic_pointer_cast<cql3::statements::modification_statement>(cql_stmt);
            _is_fallback_stmt = true;
        });
    }).handle_exception([this, &qp, &mm] (auto eptr) {
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
    });
}

future<> table_helper::insert(cql3::query_processor& qp, service::migration_manager& mm, service::query_state& qs, noncopyable_function<cql3::query_options ()> opt_maker) {
    return cache_table_info(qp, mm, qs).then([this, &qp, &qs, opt_maker = std::move(opt_maker)] () mutable {
        return do_with(opt_maker(), [this, &qp, &qs] (auto& opts) {
            opts.prepare(_prepared_stmt->bound_names);
            return _insert_stmt->execute(qp, qs, opts, std::nullopt);
        });
    }).discard_result();
}

future<> table_helper::setup_keyspace(cql3::query_processor& qp, service::migration_manager& mm, std::string_view keyspace_name, sstring replication_factor, service::query_state& qs, std::vector<table_helper*> tables) {
    if (this_shard_id() != 0) {
        co_return;
    }

    // FIXME: call `announce` once (collapse the calls here and in `setup_table`)

    if (std::any_of(tables.begin(), tables.end(), [&] (table_helper* t) { return t->_keyspace != keyspace_name; })) {
        throw std::invalid_argument("setup_keyspace called with table_helper for different keyspace");
    }

    data_dictionary::database db = qp.db();

    if (!db.has_keyspace(keyspace_name)) {
        auto group0_guard = co_await mm.start_group0_operation();
        auto ts = group0_guard.write_timestamp();

        if (!db.has_keyspace(keyspace_name)) {
            std::map<sstring, sstring> opts;
            opts["replication_factor"] = replication_factor;
            auto ksm = keyspace_metadata::new_keyspace(keyspace_name, "org.apache.cassandra.locator.SimpleStrategy", std::move(opts), true);
            co_await mm.announce(service::prepare_new_keyspace_announcement(db.real_database(), ksm, ts),
                    std::move(group0_guard), format("table_helper: create {} keyspace", keyspace_name));
        }
    }

    qs.get_client_state().set_keyspace(db.real_database(), keyspace_name);

    // Create tables
    co_await coroutine::parallel_for_each(tables, [&qp, &mm] (table_helper* t) {
        return table_helper::setup_table(qp, mm, t->_create_cql);
    });
}
