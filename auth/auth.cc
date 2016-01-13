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
 * Copyright 2016 Cloudius Systems
 *
 * Modified by Cloudius Systems
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
#include <seastar/core/sleep.hh>

#include "auth.hh"
#include "authenticator.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/cf_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "db/config.hh"
#include "service/migration_manager.hh"

const sstring auth::auth::DEFAULT_SUPERUSER_NAME("cassandra");
const sstring auth::auth::AUTH_KS("system_auth");
const sstring auth::auth::USERS_CF("users");

static const sstring USER_NAME("name");
static const sstring SUPER("super");

static logging::logger logger("auth");

// TODO: configurable
using namespace std::chrono_literals;
const std::chrono::milliseconds auth::auth::SUPERUSER_SETUP_DELAY = 10000ms;

class auth_migration_listener : public service::migration_listener {
    void on_create_keyspace(const sstring& ks_name) override {}
    void on_create_column_family(const sstring& ks_name, const sstring& cf_name) override {}
    void on_create_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_create_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_create_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}

    void on_update_keyspace(const sstring& ks_name) override {}
    void on_update_column_family(const sstring& ks_name, const sstring& cf_name, bool) override {}
    void on_update_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_update_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_update_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}

    void on_drop_keyspace(const sstring& ks_name) override {
        // TODO:
        //DatabaseDescriptor.getAuthorizer().revokeAll(DataResource.keyspace(ksName));

    }
    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        // TODO:
        //DatabaseDescriptor.getAuthorizer().revokeAll(DataResource.columnFamily(ksName, cfName));
    }
    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
};

static auth_migration_listener auth_migration;

bool auth::auth::is_class_type(const sstring& type, const sstring& classname) {
    if (type == classname) {
        return true;
    }
    auto i = classname.find_last_of('.');
    return classname.compare(i + 1, sstring::npos, type) == 0;
}

future<> auth::auth::setup() {
    auto& db = cql3::get_local_query_processor().db().local();
    auto& cfg = db.get_config();
    auto type = cfg.authenticator();

    if (is_class_type(type, authenticator::ALLOW_ALL_AUTHENTICATOR_NAME)) {
        return authenticator::setup(type).discard_result(); // just create the object
    }

    future<> f = make_ready_future();

    if (!db.has_keyspace(AUTH_KS)) {
        std::map<sstring, sstring> opts;
        opts["replication_factor"] = "1";
        auto ksm = keyspace_metadata::new_keyspace(AUTH_KS, "org.apache.cassandra.locator.SimpleStrategy", opts, true);
        f = service::get_local_migration_manager().announce_new_keyspace(ksm, false);
    }

    return f.then([] {
        return setup_table(USERS_CF, sprint("CREATE TABLE %s.%s (%s text, %s boolean, PRIMARY KEY(%s)) WITH gc_grace_seconds=%d",
                                        AUTH_KS, USERS_CF, USER_NAME, SUPER, USER_NAME,
                                        90 * 24 * 60 * 60)); // 3 months.
    }).then([type] {
        return authenticator::setup(type).discard_result();
    }).then([] {
        // TODO authorizer
    }).then([] {
        service::get_local_migration_manager().register_listener(&auth_migration); // again, only one shard...
        // instead of once-timer, just schedule this later
        sleep(SUPERUSER_SETUP_DELAY).then([] {
            // setup default super user
            return has_existing_users(USERS_CF, DEFAULT_SUPERUSER_NAME, USER_NAME).then([](bool exists) {
                if (!exists) {
                    auto query = sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?) USING TIMESTAMP 0",
                                    AUTH_KS, USERS_CF, USER_NAME, SUPER);
                    cql3::get_local_query_processor().process(query, db::consistency_level::ONE, {DEFAULT_SUPERUSER_NAME, true}).then([](auto) {
                        logger.info("Created default superuser '{}'", DEFAULT_SUPERUSER_NAME);
                    }).handle_exception([](auto ep) {
                        try {
                            std::rethrow_exception(ep);
                        } catch (exceptions::request_execution_exception&) {
                            logger.warn("Skipped default superuser setup: some nodes were not ready");
                        }
                    });
                }
            });
        });
    });
}

static db::consistency_level consistency_for_user(const sstring& username) {
    if (username == auth::auth::DEFAULT_SUPERUSER_NAME) {
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
                                    auth::auth::AUTH_KS, auth::auth::USERS_CF,
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

future<> auth::auth::insert_user(const sstring& username, bool is_super)
                throw (exceptions::request_execution_exception) {
    return cql3::get_local_query_processor().process(sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?)",
                    AUTH_KS, USERS_CF, USER_NAME, SUPER),
                    consistency_for_user(username), { username, is_super }).discard_result();
}

future<> auth::auth::delete_user(const sstring& username) throw(exceptions::request_execution_exception) {
    return cql3::get_local_query_processor().process(sprint("DELETE FROM %s.%s WHERE %s = ?",
                    AUTH_KS, USERS_CF, USER_NAME),
                    consistency_for_user(username), { username }).discard_result();
}

future<> auth::auth::setup_table(const sstring& name, const sstring& cql) {
    auto& qp = cql3::get_local_query_processor();
    auto& db = qp.db().local();

    if (db.has_schema(AUTH_KS, name)) {
        return make_ready_future();
    }

    ::shared_ptr<cql3::statements::cf_statement> parsed = static_pointer_cast<
                    cql3::statements::cf_statement>(cql3::query_processor::parse_statement(cql));
    parsed->prepare_keyspace(AUTH_KS);
    ::shared_ptr<cql3::statements::create_table_statement> statement =
                    static_pointer_cast<cql3::statements::create_table_statement>(
                                    parsed->prepare(db)->statement);
    // Origin sets "Legacy Cf Id" for the new table. We have no need to be
    // pre-2.1 compatible (afaik), so lets skip a whole lotta hoolaballo
    return statement->announce_migration(qp.proxy(), false).then([statement](bool) {});
}

future<bool> auth::auth::has_existing_users(const sstring& cfname, const sstring& def_user_name, const sstring& name_column) {
    auto default_user_query = sprint("SELECT * FROM %s.%s WHERE %s = ?", AUTH_KS, cfname, name_column);
    auto all_users_query = sprint("SELECT * FROM %s.%s LIMIT 1", AUTH_KS, cfname);

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

