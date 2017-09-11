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
#include <seastar/core/sleep.hh>

#include <seastar/core/distributed.hh>

#include "auth.hh"
#include "authenticator.hh"
#include "authorizer.hh"
#include "database.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/raw/cf_statement.hh"
#include "cql3/statements/create_table_statement.hh"
#include "db/config.hh"
#include "service/migration_manager.hh"
#include "utils/loading_cache.hh"
#include "utils/hash.hh"

const sstring auth::auth::DEFAULT_SUPERUSER_NAME("cassandra");
const sstring auth::auth::AUTH_KS("system_auth");
const sstring auth::auth::USERS_CF("users");

static const sstring USER_NAME("name");
static const sstring SUPER("super");

static logging::logger alogger("auth");

// TODO: configurable
using namespace std::chrono_literals;
const std::chrono::milliseconds auth::auth::SUPERUSER_SETUP_DELAY = 10000ms;

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

namespace std {
template <>
struct hash<auth::data_resource> {
    size_t operator()(const auth::data_resource & v) const {
        return v.hash_value();
    }
};

template <>
struct hash<auth::authenticated_user> {
    size_t operator()(const auth::authenticated_user & v) const {
        return utils::tuple_hash()(v.name(), v.is_anonymous());
    }
};
}

class auth::auth::permissions_cache {
public:
    typedef utils::loading_cache<std::pair<authenticated_user, data_resource>, permission_set, utils::simple_entry_size<permission_set>, utils::tuple_hash> cache_type;
    typedef typename cache_type::key_type key_type;

    permissions_cache()
                    : permissions_cache(
                                    cql3::get_local_query_processor().db().local().get_config()) {
    }

    permissions_cache(const db::config& cfg)
                    : _cache(cfg.permissions_cache_max_entries(), std::chrono::milliseconds(cfg.permissions_validity_in_ms()), std::chrono::milliseconds(cfg.permissions_update_interval_in_ms()), alogger,
                        [] (const key_type& k) {
                            alogger.debug("Refreshing permissions for {}", k.first.name());
                            return authorizer::get().authorize(::make_shared<authenticated_user>(k.first), k.second);
                        }) {}

    future<> stop() {
        return _cache.stop();
    }

    future<permission_set> get(::shared_ptr<authenticated_user> user, data_resource resource) {
        return _cache.get(key_type(*user, std::move(resource)));
    }

private:
    cache_type _cache;
};

namespace std { // for ADL, yuch

std::ostream& operator<<(std::ostream& os, const std::pair<auth::authenticated_user, auth::data_resource>& p) {
    os << "{user: " << p.first.name() << ", data_resource: " << p.second << "}";
    return os;
}

}

static distributed<auth::auth::permissions_cache> perm_cache;

/**
 * Poor mans job schedule. For maximum 2 jobs. Sic.
 * Still does nothing more clever than waiting 10 seconds
 * like origin, then runs the submitted tasks.
 *
 * Only difference compared to sleep (from which this
 * borrows _heavily_) is that if tasks have not run by the time
 * we exit (and do static clean up) we delete the promise + cont
 *
 * Should be abstracted to some sort of global server function
 * probably.
 */
struct waiter {
    promise<> done;
    timer<> tmr;
    waiter() : tmr([this] {done.set_value();})
    {
        tmr.arm(auth::auth::SUPERUSER_SETUP_DELAY);
    }
    ~waiter() {
        if (tmr.armed()) {
            tmr.cancel();
            done.set_exception(std::runtime_error("shutting down"));
        }
        alogger.trace("Deleting scheduled task");
    }
    void kill() {
    }
};

typedef std::unique_ptr<waiter> waiter_ptr;

static std::vector<waiter_ptr> & thread_waiters() {
    static thread_local std::vector<waiter_ptr> the_waiters;
    return the_waiters;
}

void auth::auth::schedule_when_up(scheduled_func f) {
    alogger.trace("Adding scheduled task");

    auto & waiters = thread_waiters();

    waiters.emplace_back(std::make_unique<waiter>());
    auto* w = waiters.back().get();

    w->done.get_future().finally([w] {
        auto & waiters = thread_waiters();
        auto i = std::find_if(waiters.begin(), waiters.end(), [w](const waiter_ptr& p) {
                            return p.get() == w;
                        });
        if (i != waiters.end()) {
            waiters.erase(i);
        }
    }).then([f = std::move(f)] {
        alogger.trace("Running scheduled task");
        return f();
    }).handle_exception([](auto ep) {
        return make_ready_future();
    });
}

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

    future<> f = perm_cache.start();

    if (is_class_type(cfg.authenticator(),
                    authenticator::ALLOW_ALL_AUTHENTICATOR_NAME)
                    && is_class_type(cfg.authorizer(),
                                    authorizer::ALLOW_ALL_AUTHORIZER_NAME)
                                    ) {
        // just create the objects
        return f.then([&cfg] {
            return authenticator::setup(cfg.authenticator());
        }).then([&cfg] {
            return authorizer::setup(cfg.authorizer());
        });
    }

    if (!db.has_keyspace(AUTH_KS)) {
        std::map<sstring, sstring> opts;
        opts["replication_factor"] = "1";
        auto ksm = keyspace_metadata::new_keyspace(AUTH_KS, "org.apache.cassandra.locator.SimpleStrategy", opts, true);
        // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments. See issue #2129.
        f = service::get_local_migration_manager().announce_new_keyspace(ksm, api::min_timestamp, false);
    }

    return f.then([] {
        return setup_table(USERS_CF, sprint("CREATE TABLE %s.%s (%s text, %s boolean, PRIMARY KEY(%s)) WITH gc_grace_seconds=%d",
                                        AUTH_KS, USERS_CF, USER_NAME, SUPER, USER_NAME,
                                        90 * 24 * 60 * 60)); // 3 months.
    }).then([&cfg] {
        return authenticator::setup(cfg.authenticator());
    }).then([&cfg] {
        return authorizer::setup(cfg.authorizer());
    }).then([] {
        service::get_local_migration_manager().register_listener(&auth_migration); // again, only one shard...
        // instead of once-timer, just schedule this later
        schedule_when_up([] {
            // setup default super user
            return has_existing_users(USERS_CF, DEFAULT_SUPERUSER_NAME, USER_NAME).then([](bool exists) {
                if (!exists) {
                    auto query = sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?) USING TIMESTAMP 0",
                                    AUTH_KS, USERS_CF, USER_NAME, SUPER);
                    cql3::get_local_query_processor().process(query, db::consistency_level::ONE, {DEFAULT_SUPERUSER_NAME, true}).then([](auto) {
                        alogger.info("Created default superuser '{}'", DEFAULT_SUPERUSER_NAME);
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
        thread_waiters().clear();
    }).then([] {
        return perm_cache.stop();
    });
}

future<auth::permission_set> auth::auth::get_permissions(::shared_ptr<authenticated_user> user, data_resource resource) {
    return perm_cache.local().get(std::move(user), std::move(resource));
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

future<> auth::auth::insert_user(const sstring& username, bool is_super) {
    return cql3::get_local_query_processor().process(sprint("INSERT INTO %s.%s (%s, %s) VALUES (?, ?)",
                    AUTH_KS, USERS_CF, USER_NAME, SUPER),
                    consistency_for_user(username), { username, is_super }).discard_result();
}

future<> auth::auth::delete_user(const sstring& username) {
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

    ::shared_ptr<cql3::statements::raw::cf_statement> parsed = static_pointer_cast<
                    cql3::statements::raw::cf_statement>(cql3::query_processor::parse_statement(cql));
    parsed->prepare_keyspace(AUTH_KS);
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

