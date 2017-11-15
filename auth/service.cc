/*
 * Copyright (C) 2017 ScyllaDB
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

#include "auth/service.hh"

#include <map>

#include <seastar/core/future-util.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include "auth/allow_all_authenticator.hh"
#include "auth/allow_all_authorizer.hh"
#include "auth/common.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/config.hh"
#include "db/consistency_level.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "service/migration_listener.hh"
#include "utils/class_registrator.hh"

namespace auth {

namespace meta {

static const sstring user_name_col_name("name");
static const sstring superuser_col_name("super");

}

static logging::logger log("auth_service");

class auth_migration_listener final : public ::service::migration_listener {
    authorizer& _authorizer;

public:
    explicit auth_migration_listener(authorizer& a) : _authorizer(a) {
    }

private:
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
        _authorizer.revoke_all(auth::data_resource(ks_name));
    }

    void on_drop_column_family(const sstring& ks_name, const sstring& cf_name) override {
        _authorizer.revoke_all(auth::data_resource(ks_name, cf_name));
    }

    void on_drop_user_type(const sstring& ks_name, const sstring& type_name) override {}
    void on_drop_function(const sstring& ks_name, const sstring& function_name) override {}
    void on_drop_aggregate(const sstring& ks_name, const sstring& aggregate_name) override {}
    void on_drop_view(const sstring& ks_name, const sstring& view_name) override {}
};

static sharded<permissions_cache> sharded_permissions_cache{};

static db::consistency_level consistency_for_user(const sstring& name) {
    if (name == meta::DEFAULT_SUPERUSER_NAME) {
        return db::consistency_level::QUORUM;
    } else {
        return db::consistency_level::LOCAL_ONE;
    }
}

static future<::shared_ptr<cql3::untyped_result_set>> select_user(cql3::query_processor& qp, const sstring& name) {
    // Here was a thread local, explicit cache of prepared statement. In normal execution this is
    // fine, but since we in testing set up and tear down system over and over, we'd start using
    // obsolete prepared statements pretty quickly.
    // Rely on query processing caching statements instead, and lets assume
    // that a map lookup string->statement is not gonna kill us much.
    return qp.process(
            sprint(
                    "SELECT * FROM %s.%s WHERE %s = ?",
                    meta::AUTH_KS,
                    meta::USERS_CF,
                    meta::user_name_col_name),
            consistency_for_user(name),
            { name },
            true);
}

service_config service_config::from_db_config(const db::config& dc) {
    const qualified_name qualified_authorizer_name(meta::AUTH_PACKAGE_NAME, dc.authorizer());
    const qualified_name qualified_authenticator_name(meta::AUTH_PACKAGE_NAME, dc.authenticator());

    service_config c;
    c.authorizer_java_name = qualified_authorizer_name;
    c.authenticator_java_name = qualified_authenticator_name;

    return c;
}

service::service(
        permissions_cache_config c,
        cql3::query_processor& qp,
        ::service::migration_manager& mm,
        std::unique_ptr<authorizer> a,
        std::unique_ptr<authenticator> b)
            : _cache_config(std::move(c))
            , _qp(qp)
            , _migration_manager(mm)
            , _authorizer(std::move(a))
            , _authenticator(std::move(b))
            , _migration_listener(std::make_unique<auth_migration_listener>(*_authorizer)) {
}

service::service(
        permissions_cache_config cache_config,
        cql3::query_processor& qp,
        ::service::migration_manager& mm,
        const service_config& sc)
            : service(
                      std::move(cache_config),
                      qp,
                      mm,
                      create_object<authorizer>(sc.authorizer_java_name, qp, mm),
                      create_object<authenticator>(sc.authenticator_java_name, qp, mm)) {
}

bool service::should_create_metadata() const {
    const bool null_authorizer = _authorizer->qualified_java_name() == allow_all_authorizer_name();
    const bool null_authenticator = _authenticator->qualified_java_name() == allow_all_authenticator_name();
    return !null_authorizer || !null_authenticator;
}

future<> service::create_metadata_if_missing() {
    auto& db = _qp.db().local();

    auto f = make_ready_future<>();

    if (!db.has_keyspace(meta::AUTH_KS)) {
        std::map<sstring, sstring> opts{{"replication_factor", "1"}};

        auto ksm = keyspace_metadata::new_keyspace(
                meta::AUTH_KS,
                "org.apache.cassandra.locator.SimpleStrategy",
                opts,
                true);

        // We use min_timestamp so that default keyspace metadata will loose with any manual adjustments.
        // See issue #2129.
        f = _migration_manager.announce_new_keyspace(ksm, api::min_timestamp, false);
    }

    return f.then([this] {
        // 3 months.
        static const auto gc_grace_seconds = 90 * 24 * 60 * 60;

        static const sstring users_table_query = sprint(
                "CREATE TABLE %s.%s (%s text, %s boolean, PRIMARY KEY (%s)) WITH gc_grace_seconds=%s",
                meta::AUTH_KS,
                meta::USERS_CF,
                meta::user_name_col_name,
                meta::superuser_col_name,
                meta::user_name_col_name,
                gc_grace_seconds);

        return create_metadata_table_if_missing(
                meta::USERS_CF,
                _qp,
                users_table_query,
                _migration_manager);
    }).then([this] {
        delay_until_system_ready(_delayed, [this] {
            return has_existing_users().then([this](bool existing) {
                if (!existing) {
                    //
                    // Create default superuser.
                    //

                    static const sstring query = sprint(
                            "INSERT INTO %s.%s (%s, %s) VALUES (?, ?) USING TIMESTAMP 0",
                            meta::AUTH_KS,
                            meta::USERS_CF,
                            meta::user_name_col_name,
                            meta::superuser_col_name);

                    return _qp.process(
                            query,
                            db::consistency_level::ONE,
                            { meta::DEFAULT_SUPERUSER_NAME, true }).then([](auto&&) {
                        log.info("Created default superuser '{}'", meta::DEFAULT_SUPERUSER_NAME);
                    }).handle_exception([](auto exn) {
                        try {
                            std::rethrow_exception(exn);
                        } catch (const exceptions::request_execution_exception&) {
                            log.warn("Skipped default superuser setup: some nodes were not ready");
                        }
                    }).discard_result();
                }

                return make_ready_future<>();
            });
        });

        return make_ready_future<>();
    });
}

future<> service::start() {
    return once_among_shards([this] {
        if (should_create_metadata()) {
            return create_metadata_if_missing();
        }

        return make_ready_future<>();
    }).then([this] {
        return when_all_succeed(_authorizer->start(), _authenticator->start());
    }).then([this] {
        return once_among_shards([this] {
            _migration_manager.register_listener(_migration_listener.get());
            return sharded_permissions_cache.start(std::ref(_cache_config), std::ref(*this), std::ref(log));
        });
    });
}

future<> service::stop() {
    return once_among_shards([this] {
        _delayed.cancel_all();
        return sharded_permissions_cache.stop();
    }).then([this] {
        return when_all_succeed(_authorizer->stop(), _authenticator->stop());
    });
}

future<bool> service::has_existing_users() const {
    static const sstring default_user_query = sprint(
            "SELECT * FROM %s.%s WHERE %s = ?",
            meta::AUTH_KS,
            meta::USERS_CF,
            meta::user_name_col_name);

    static const sstring all_users_query = sprint(
            "SELECT * FROM %s.%s LIMIT 1",
            meta::AUTH_KS,
            meta::USERS_CF);

    // This logic is borrowed directly from Apache Cassandra. By first checking for the presence of the default user, we
    // can potentially avoid doing a range query with a high consistency level.

    return _qp.process(
            default_user_query,
            db::consistency_level::ONE,
            { meta::DEFAULT_SUPERUSER_NAME },
            true).then([this](auto results) {
        if (!results->empty()) {
            return make_ready_future<bool>(true);
        }

        return _qp.process(
                default_user_query,
                db::consistency_level::QUORUM,
                { meta::DEFAULT_SUPERUSER_NAME },
                true).then([this](auto results) {
            if (!results->empty()) {
                return make_ready_future<bool>(true);
            }

            return _qp.process(
                    all_users_query,
                    db::consistency_level::QUORUM).then([](auto results) {
                return make_ready_future<bool>(!results->empty());
            });
        });
    });
}

future<bool> service::is_existing_user(const sstring& name) const {
    return select_user(_qp, name).then([](auto results) {
        return !results->empty();
    });
}

future<bool> service::is_super_user(const sstring& name) const {
    return select_user(_qp, name).then([](auto results) {
        return !results->empty() && results->one().template get_as<bool>(meta::superuser_col_name);
    });
}

future<> service::insert_user(const sstring& name, bool is_superuser) {
    return _qp.process(
            sprint(
                    "INSERT INTO %s.%s (%s, %s) VALUES (?, ?)",
                    meta::AUTH_KS,
                    meta::USERS_CF,
                    meta::user_name_col_name,
                    meta::superuser_col_name),
            consistency_for_user(name),
            { name, is_superuser }).discard_result();
}

future<> service::delete_user(const sstring& name) {
    return _qp.process(
            sprint(
                    "DELETE FROM %s.%s WHERE %s = ?",
                    meta::AUTH_KS,
                    meta::USERS_CF,
                    meta::user_name_col_name),
            consistency_for_user(name),
            { name }).discard_result();
}

future<permission_set> service::get_permissions(::shared_ptr<authenticated_user> u, data_resource r) const {
    return sharded_permissions_cache.local().get(std::move(u), std::move(r));
}

//
// Free functions.
//

future<bool> is_super_user(const service& ser, const authenticated_user& u) {
    if (u.is_anonymous()) {
        return make_ready_future<bool>(false);
    }

    return ser.is_super_user(u.name());
}

}
