/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2016 ScyllaDB
 *
 * Modified by ScyllaDB
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

#include <chrono>

#include <seastar/core/sleep.hh>

#include <seastar/core/distributed.hh>

#include "auth.hh"
#include "authenticator.hh"
#include "authorizer.hh"
#include "common.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "db/config.hh"
#include "delayed_tasks.hh"
#include "permissions_cache.hh"
#include "service/migration_manager.hh"
#include "utils/loading_cache.hh"
#include "utils/hash.hh"

static const sstring USER_NAME("name");
static const sstring SUPER("super");

static logging::logger alogger("auth");

// TODO: configurable
using namespace std::chrono_literals;
static const std::chrono::milliseconds SUPERUSER_SETUP_DELAY = 10000ms;

class auth_migration_listener : public service::migration_listener {
    void on_create_keyspace(const sstring& ks_name) override {}
    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_create_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_create_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_create_view(const sstring& ks_name, const sstring& view_name) override {}

    void on_update_keyspace(const sstring& ks_name) override {}
    void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool) override {}
    void on_update_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_update_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_update_view(const sstring& ks_name, const sstring& view_name, bool columns_changed) override {}

    void on_drop_keyspace(const sstring& ks_name) override {
        auth::authorizer::get().revoke_all(auth::data_resource(ks_name));
    }
    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        auth::authorizer::get().revoke_all(auth::data_resource(ks_name, cf_name));
    }
    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_drop_view(const sstring& ks_name, const sstring& view_name) override {}
};

static auth_migration_listener auth_migration;

static sharded<auth::permissions_cache> perm_cache;

static future<> start_permission_cache() {
    auto& db_config = cql3::get_local_query_processor().db().local().get_config();

    auth::permissions_cache_config c;
    c.max_entries = db_config.permissions_cache_max_entries();
    c.validity_period = std::chrono::milliseconds(db_config.permissions_validity_in_ms());
    c.update_period = std::chrono::milliseconds(db_config.permissions_update_interval_in_ms());

    return perm_cache.start(c, std::ref(auth::authorizer::get()), std::ref(alogger));
}

static delayed_tasks<>& get_local_delayed_tasks() {
    static thread_local delayed_tasks<> instance;
    return instance;
}

void auth::auth::schedule_when_up(scheduled_func f) {
    get_local_delayed_tasks().schedule_after(SUPERUSER_SETUP_DELAY, std::move(f));
}

future<> auth::auth::setup() {
    auto& db = cql3::get_local_query_processor().db().local();
    auto& cfg = db.get_config();

    qualified_name authenticator_name(meta::AUTH_PACKAGE_NAME, cfg.authenticator()),
                    authorizer_name(meta::AUTH_PACKAGE_NAME, cfg.authorizer());

    if (allow_all_authenticator_name() == authenticator_name && allow_all_authorizer_name() == authorizer_name) {
        return authenticator::setup(authenticator_name).then([authorizer_name = std::move(authorizer_name)] {
            return authorizer::setup(authorizer_name);
        }).then([] {
            return start_permission_cache();
        });
    }

    future<> f = make_ready_future<>();

    if (!db.has_keyspace(meta::AUTH_KS)) {
        std::map<sstring, sstring> opts;
        opts["replication_factor"] = "1";
        auto ksm = keyspace_metadata::new_keyspace(meta::AUTH_KS, "org.apache.cassandra.locator.SimpleStrategy", opts, true);
        // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments. See issue #2129.
        f = service::get_local_migration_manager().announce_new_keyspace(ksm, api::min_timestamp, false);
    }

    return f.then([] {
        return setup_table(meta::USERS_CF, sprint("CREATE TABLE %s.%s (%s text, %s boolean, PRIMARY KEY(%s)) WITH gc_grace_seconds=%d",
                                              meta::AUTH_KS, meta::USERS_CF, USER_NAME, SUPER, USER_NAME,
                                        90 * 24 * 60 * 60)); // 3 months.
    }).then([authenticator_name = std::move(authenticator_name)] {
        return authenticator::setup(authenticator_name);
    }).then([authorizer_name = std::move(authorizer_name)] {
        return authorizer::setup(authorizer_name);
    }).then([] {
        return start_permission_cache();
    }).then([] {
        service::get_local_migration_manager().register_listener(&auth_migration); // again, only one shard...
        // instead of once-timer, just schedule this later
        schedule_when_up([] {
            // setup default super user
            return has_existing_users(meta::USERS_CF, meta::DEFAULT_SUPERUSER_NAME, USER_NAME).then([](bool exists) {
                if (!exists) {
                    auto query = sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?) USING TIMESTAMP 0",
                                    meta::AUTH_KS, meta::USERS_CF, USER_NAME, SUPER);
                    cql3::get_local_query_processor().process(query, db::consistency_level::ONE, {meta::DEFAULT_SUPERUSER_NAME, true}).then([](auto) {
                        alogger.info("Created default superuser '{}'", meta::DEFAULT_SUPERUSER_NAME);
                    }).handle_exception([](auto ep) {
                        try {
                            std::rethrow_exception(ep);
                        } catch (exceptions::request_execution_exception&) {
                            alogger.warn("Skipped default superuser setup: some nodes were not ready");
                        }
                    });
                }
            });
        });
    });
}

future<> auth::auth::shutdown() {
    // just make sure we don't have pending tasks.
    // this is mostly relevant for test cases where
    // db-env-shutdown != process shutdown
    return smp::invoke_on_all([] {
        get_local_delayed_tasks().cancel_all();
    }).then([] {
        return perm_cache.stop();
    });
}

future<auth::permission_set> auth::auth::get_permissions(::shared_ptr<authenticated_user> user, data_resource resource) {
    return perm_cache.local().get(std::move(user), std::move(resource));
}

static db::consistency_level consistency_for_user(const sstring& username) {
    if (username == auth::meta::DEFAULT_SUPERUSER_NAME) {
        return db::consistency_level::QUORUM;
    }
    return db::consistency_level::LOCAL_ONE;
}

static future<::shared_ptr<cql3::untyped_result_set>> select_user(const sstring& username) {
    // Here was a thread local, explicit cache of prepared statement. In normal execution this is
    // fine, but since we in testing set up and tear down system over and over, we'd start using
    // obsolete prepared statements pretty quickly.
    // Rely on query processing caching statements instead, and lets assume
    // that a map lookup string->statement is not gonna kill us much.
    return cql3::get_local_query_processor().process(
                    sprint("SELECT * FROM %s.%s WHERE %s = ?",
                                    auth::meta::AUTH_KS, auth::meta::USERS_CF,
                                    USER_NAME), consistency_for_user(username),
                    { username }, true);
}

future<bool> auth::auth::is_existing_user(const sstring& username) {
    return select_user(username).then(
                    [](::shared_ptr<cql3::untyped_result_set> res) {
                        return make_ready_future<bool>(!res->empty());
                    });
}

future<bool> auth::auth::is_super_user(const sstring& username) {
    return select_user(username).then(
                    [](::shared_ptr<cql3::untyped_result_set> res) {
                        return make_ready_future<bool>(!res->empty() && res->one().get_as<bool>(SUPER));
                    });
}

future<> auth::auth::insert_user(const sstring& username, bool is_super) {
    return cql3::get_local_query_processor().process(sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?)",
                    meta::AUTH_KS, meta::USERS_CF, USER_NAME, SUPER),
                    consistency_for_user(username), { username, is_super }).discard_result();
}

future<> auth::auth::delete_user(const sstring& username) {
    return cql3::get_local_query_processor().process(sprint("DELETE FROM %s.%s WHERE %s = ?",
                    meta::AUTH_KS, meta::USERS_CF, USER_NAME),
                    consistency_for_user(username), { username }).discard_result();
}

future<> auth::auth::setup_table(const sstring& name, const sstring& cql) {
    auto& qp = cql3::get_local_query_processor();
    auto& db = qp.db().local();

    if (db.has_schema(meta::AUTH_KS, name)) {
        return make_ready_future();
    }

    ::shared_ptr<cql3::statements::raw::cf_statement> parsed = static_pointer_cast<
                    cql3::statements::raw::cf_statement>(cql3::query_processor::parse_statement(cql));
    parsed->prepare_keyspace(meta::AUTH_KS);
    ::shared_ptr<cql3::statements::create_table_statement> statement =
                    static_pointer_cast<cql3::statements::create_table_statement>(
                                    parsed->prepare(db, qp.get_cql_stats())->statement);
    auto schema = statement->get_cf_meta_data();
    auto uuid = generate_legacy_id(schema->ks_name(), schema->cf_name());

    schema_builder b(schema);
    b.set_uuid(uuid);
    return service::get_local_migration_manager().announce_new_column_family(b.build(), false);
}

future<bool> auth::auth::has_existing_users(const sstring& cfname, const sstring& def_user_name, const sstring& name_column) {
    auto default_user_query = sprint("SELECT * FROM %s.%s WHERE %s = ?", meta::AUTH_KS, cfname, name_column);
    auto all_users_query = sprint("SELECT * FROM %s.%s LIMIT 1", meta::AUTH_KS, cfname);

    return cql3::get_local_query_processor().process(default_user_query, db::consistency_level::ONE, { def_user_name }).then([=](::shared_ptr<cql3::untyped_result_set> res) {
        if (!res->empty()) {
            return make_ready_future<bool>(true);
        }
        return cql3::get_local_query_processor().process(default_user_query, db::consistency_level::QUORUM, { def_user_name }).then([all_users_query](::shared_ptr<cql3::untyped_result_set> res) {
            if (!res->empty()) {
                return make_ready_future<bool>(true);
            }
            return cql3::get_local_query_processor().process(all_users_query, db::consistency_level::QUORUM).then([](::shared_ptr<cql3::untyped_result_set> res) {
                return make_ready_future<bool>(!res->empty());
            });
        });
    });
}

