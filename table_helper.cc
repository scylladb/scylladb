/*
 * Copyright (C) 2017-present ScyllaDB
 *
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

#include "table_helper.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/create_table_statement.hh"
#include "cql3/statements/modification_statement.hh"
#include "database.hh"

future<> table_helper::setup_table(cql3::query_processor& qp) const {
    auto& db = qp.db();

    if (db.has_schema(_keyspace, _name)) {
        return make_ready_future<>();
    }

    auto parsed = cql3::query_processor::parse_statement(_create_cql);

    cql3::statements::raw::cf_statement* parsed_cf_stmt = static_cast<cql3::statements::raw::cf_statement*>(parsed.get());
    parsed_cf_stmt->prepare_keyspace(_keyspace);
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

    // We don't care it it fails really - this may happen due to concurrent
    // "CREATE TABLE" invocation on different Nodes.
    // The important thing is that it will converge eventually (some traces may
    // be lost in a process but that's ok).
    return qp.get_migration_manager().announce_new_column_family(b.build()).discard_result().handle_exception([this] (auto ep) {});;
}

future<> table_helper::cache_table_info(cql3::query_processor& qp, service::query_state& qs) {
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
    }).handle_exception([this, &qp] (auto eptr) {
        // One of the possible causes for an error here could be the table that doesn't exist.
        //FIXME: discarded future.
        (void)this->setup_table(qp).discard_result();

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

future<> table_helper::insert(cql3::query_processor& qp, service::query_state& qs, noncopyable_function<cql3::query_options ()> opt_maker) {
    return cache_table_info(qp, qs).then([this, &qp, &qs, opt_maker = std::move(opt_maker)] () mutable {
        return do_with(opt_maker(), [this, &qp, &qs] (auto& opts) {
            opts.prepare(_prepared_stmt->bound_names);
            return _insert_stmt->execute(qp, qs, opts);
        });
    }).discard_result();
}

future<> table_helper::setup_keyspace(cql3::query_processor& qp, const sstring& keyspace_name, sstring replication_factor, service::query_state& qs, std::vector<table_helper*> tables) {
    if (this_shard_id() == 0) {
        size_t n = tables.size();
        for (size_t i = 0; i < n; ++i) {
            if (tables[i]->_keyspace != keyspace_name) {
                throw std::invalid_argument("setup_keyspace called with table_helper for different keyspace");
            }
        }
        return seastar::async([&qp, &keyspace_name, replication_factor, &qs, tables] {
            database& db = qp.db();

            // Create a keyspace
            if (!db.has_keyspace(keyspace_name)) {
                std::map<sstring, sstring> opts;
                opts["replication_factor"] = replication_factor;
                auto ksm = keyspace_metadata::new_keyspace(keyspace_name, "org.apache.cassandra.locator.SimpleStrategy", std::move(opts), true);
                // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments. See issue #2129.
                qp.get_migration_manager().announce_new_keyspace(ksm, api::min_timestamp).get();
            }

            qs.get_client_state().set_keyspace(db, keyspace_name);


            // Create tables
            size_t n = tables.size();
            for (size_t i = 0; i < n; ++i) {
                tables[i]->setup_table(qp).get();
            }
        });
    } else {
        return make_ready_future<>();
    }
}
