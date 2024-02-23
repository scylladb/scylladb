/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/default_authorizer.hh"
#include "db/system_auth_keyspace.hh"

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

#include <boost/algorithm/string/join.hpp>
#include <boost/range.hpp>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include "auth/common.hh"
#include "auth/permission.hh"
#include "auth/role_or_anonymous.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "utils/class_registrator.hh"

namespace auth {

std::string_view default_authorizer::qualified_java_name() const {
    return "org.apache.cassandra.auth.CassandraAuthorizer";
}

static constexpr std::string_view ROLE_NAME = "role";
static constexpr std::string_view RESOURCE_NAME = "resource";
static constexpr std::string_view PERMISSIONS_NAME = "permissions";
static constexpr std::string_view PERMISSIONS_CF = "role_permissions";

static logging::logger alogger("default_authorizer");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authorizer,
        default_authorizer,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> password_auth_reg("org.apache.cassandra.auth.CassandraAuthorizer");

default_authorizer::default_authorizer(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm)
        : _qp(qp)
        , _group0_client(g0)
        , _migration_manager(mm) {
}

default_authorizer::~default_authorizer() {
}

static const sstring legacy_table_name{"permissions"};

bool default_authorizer::legacy_metadata_exists() const {
    return _qp.db().has_schema(meta::legacy::AUTH_KS, legacy_table_name);
}

future<bool> default_authorizer::legacy_any_granted() const {
    static const sstring query = format("SELECT * FROM {}.{} LIMIT 1", meta::legacy::AUTH_KS, PERMISSIONS_CF);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            {},
            cql3::query_processor::cache_internal::yes).then([](::shared_ptr<cql3::untyped_result_set> results) {
        return !results->empty();
    });
}

future<> default_authorizer::migrate_legacy_metadata() {
    alogger.info("Starting migration of legacy permissions metadata.");
    static const sstring query = format("SELECT * FROM {}.{}", meta::legacy::AUTH_KS, legacy_table_name);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            cql3::query_processor::cache_internal::no).then([this](::shared_ptr<cql3::untyped_result_set> results) {
        return do_for_each(*results, [this](const cql3::untyped_result_set_row& row) {
            return do_with(
                    row.get_as<sstring>("username"),
                    parse_resource(row.get_as<sstring>(RESOURCE_NAME)),
                    [this, &row](const auto& username, const auto& r) {
                const permission_set perms = permissions::from_strings(row.get_set<sstring>(PERMISSIONS_NAME));
                return grant(username, perms, r);
            });
        }).finally([results] {});
    }).then([] {
        alogger.info("Finished migrating legacy permissions metadata.");
    }).handle_exception([](std::exception_ptr ep) {
        alogger.error("Encountered an error during migration!");
        std::rethrow_exception(ep);
    });
}

future<> default_authorizer::start() {
    static const sstring create_table = fmt::format(
            "CREATE TABLE {}.{} ("
            "{} text,"
            "{} text,"
            "{} set<text>,"
            "PRIMARY KEY({}, {})"
            ") WITH gc_grace_seconds={}",
            meta::legacy::AUTH_KS,
            PERMISSIONS_CF,
            ROLE_NAME,
            RESOURCE_NAME,
            PERMISSIONS_NAME,
            ROLE_NAME,
            RESOURCE_NAME,
            90 * 24 * 60 * 60); // 3 months.

    return once_among_shards([this] {
        return create_metadata_table_if_missing(
                PERMISSIONS_CF,
                _qp,
                create_table,
                _migration_manager).then([this] {
            _finished = do_after_system_ready(_as, [this] {
                return async([this] {
                    _migration_manager.wait_for_schema_agreement(_qp.db().real_database(), db::timeout_clock::time_point::max(), &_as).get();

                    if (legacy_metadata_exists()) {
                        if (!legacy_any_granted().get()) {
                            migrate_legacy_metadata().get();
                            return;
                        }

                        alogger.warn("Ignoring legacy permissions metadata since role permissions exist.");
                    }
                });
            });
        });
    });
}

future<> default_authorizer::stop() {
    _as.request_abort();
    return _finished.handle_exception_type([](const sleep_aborted&) {}).handle_exception_type([](const abort_requested_exception&) {});
}

future<permission_set>
default_authorizer::authorize(const role_or_anonymous& maybe_role, const resource& r) const {
    if (is_anonymous(maybe_role)) {
        co_return permissions::NONE;
    }

    const sstring query = format("SELECT {} FROM {}.{} WHERE {} = ? AND {} = ?",
            PERMISSIONS_NAME,
            get_auth_ks_name(_qp),
            PERMISSIONS_CF,
            ROLE_NAME,
            RESOURCE_NAME);

    const auto results = co_await _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            {*maybe_role.name, r.name()},
            cql3::query_processor::cache_internal::yes);
    if (results->empty()) {
        co_return permissions::NONE;
    }
    co_return permissions::from_strings(results->one().get_set<sstring>(PERMISSIONS_NAME));
}

future<>
default_authorizer::modify(
        std::string_view role_name,
        permission_set set,
        const resource& resource,
        std::string_view op) {
    const sstring query = format("UPDATE {}.{} SET {} = {} {} ? WHERE {} = ? AND {} = ?",
            get_auth_ks_name(_qp),
            PERMISSIONS_CF,
            PERMISSIONS_NAME,
            PERMISSIONS_NAME,
            op,
            ROLE_NAME,
            RESOURCE_NAME);
    if (legacy_mode(_qp)) {
        co_return co_await _qp.execute_internal(
                query,
                db::consistency_level::ONE,
                internal_distributed_query_state(),
                {permissions::to_strings(set), sstring(role_name), resource.name()},
                cql3::query_processor::cache_internal::no).discard_result();
    }
    co_return co_await announce_mutations(_qp, _group0_client, query,
        {permissions::to_strings(set), sstring(role_name), resource.name()}, &_as);
}


future<> default_authorizer::grant(std::string_view role_name, permission_set set, const resource& resource) {
    return modify(role_name, std::move(set), resource, "+");
}

future<> default_authorizer::revoke(std::string_view role_name, permission_set set, const resource& resource) {
    return modify(role_name, std::move(set), resource, "-");
}

future<std::vector<permission_details>> default_authorizer::list_all() const {
    const sstring query = format("SELECT {}, {}, {} FROM {}.{}",
            ROLE_NAME,
            RESOURCE_NAME,
            PERMISSIONS_NAME,
            get_auth_ks_name(_qp),
            PERMISSIONS_CF);

    const auto results = co_await _qp.execute_internal(
            query,
            db::consistency_level::ONE,
            internal_distributed_query_state(),
            {},
            cql3::query_processor::cache_internal::yes);

    std::vector<permission_details> all_details;
    for (const auto& row : *results) {
        if (row.has(PERMISSIONS_NAME)) {
            auto role_name = row.get_as<sstring>(ROLE_NAME);
            auto resource = parse_resource(row.get_as<sstring>(RESOURCE_NAME));
            auto perms = permissions::from_strings(row.get_set<sstring>(PERMISSIONS_NAME));
            all_details.push_back(permission_details{std::move(role_name), std::move(resource), std::move(perms)});
        }
    }
    co_return all_details;
}

future<> default_authorizer::revoke_all(std::string_view role_name) {
    try {
        const sstring query = format("DELETE FROM {}.{} WHERE {} = ?",
                get_auth_ks_name(_qp),
                PERMISSIONS_CF,
                ROLE_NAME);
        if (legacy_mode(_qp)) {
            co_await _qp.execute_internal(
                    query,
                    db::consistency_level::ONE,
                    internal_distributed_query_state(),
                    {sstring(role_name)},
                    cql3::query_processor::cache_internal::no).discard_result();
        } else {
            co_await announce_mutations(_qp, _group0_client, query, {sstring(role_name)}, &_as);
        }
    } catch (exceptions::request_execution_exception& e) {
        alogger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", role_name, e);
    }
}

future<> default_authorizer::revoke_all_legacy(const resource& resource) {
    static const sstring query = format("SELECT {} FROM {}.{} WHERE {} = ? ALLOW FILTERING",
            ROLE_NAME,
            get_auth_ks_name(_qp),
            PERMISSIONS_CF,
            RESOURCE_NAME);

    return _qp.execute_internal(
            query,
            db::consistency_level::LOCAL_ONE,
            {resource.name()},
            cql3::query_processor::cache_internal::no).then_wrapped([this, resource](future<::shared_ptr<cql3::untyped_result_set>> f) {
        try {
            auto res = f.get();
            return parallel_for_each(
                    res->begin(),
                    res->end(),
                    [this, res, resource](const cql3::untyped_result_set::row& r) {
                static const sstring query = format("DELETE FROM {}.{} WHERE {} = ? AND {} = ?",
                        get_auth_ks_name(_qp),
                        PERMISSIONS_CF,
                        ROLE_NAME,
                        RESOURCE_NAME);

                return _qp.execute_internal(
                        query,
                        db::consistency_level::LOCAL_ONE,
                        {r.get_as<sstring>(ROLE_NAME), resource.name()},
                        cql3::query_processor::cache_internal::no).discard_result().handle_exception(
                                [resource](auto ep) {
                    try {
                        std::rethrow_exception(ep);
                    } catch (exceptions::request_execution_exception& e) {
                        alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
                    }

                });
            });
        } catch (exceptions::request_execution_exception& e) {
            alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", resource, e);
            return make_ready_future();
        }
    });
}

future<> default_authorizer::revoke_all(const resource& resource) {
    if (legacy_mode(_qp)) {
        co_return co_await revoke_all_legacy(resource);
    }
    auto name = resource.name();
    try {
        auto gen = [this, name] (api::timestamp_type& t) -> mutations_generator {
            const sstring query = format("SELECT {} FROM {}.{} WHERE {} = ? ALLOW FILTERING",
                    ROLE_NAME,
                    get_auth_ks_name(_qp),
                    PERMISSIONS_CF,
                    RESOURCE_NAME);
            auto res = co_await _qp.execute_internal(
                    query,
                    db::consistency_level::LOCAL_ONE,
                    {name},
                    cql3::query_processor::cache_internal::no);
            for (const auto& r : *res) {
                const sstring query = format("DELETE FROM {}.{} WHERE {} = ? AND {} = ?",
                        get_auth_ks_name(_qp),
                        PERMISSIONS_CF,
                        ROLE_NAME,
                        RESOURCE_NAME);
                auto muts = co_await _qp.get_mutations_internal(
                        query,
                        internal_distributed_query_state(),
                        t,
                        {r.get_as<sstring>(ROLE_NAME), name});
                if (muts.size() != 1) {
                    on_internal_error(alogger,
                        format("expecting single delete mutation, got {}", muts.size()));
                }
                co_yield std::move(muts[0]);
            }
        };
        co_await announce_mutations_with_batching(
                _group0_client,
                [this](abort_source* as) { return _group0_client.start_operation(as); },
                std::move(gen),
                &_as);
    } catch (exceptions::request_execution_exception& e) {
        alogger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", name, e);
    }
}

const resource_set& default_authorizer::protected_resources() const {
    static const resource_set resources({ make_data_resource(meta::legacy::AUTH_KS, PERMISSIONS_CF) });
    return resources;
}

}
