/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "auth/default_authorizer.hh"
#include "db/system_keyspace.hh"

extern "C" {
#include <crypt.h>
#include <unistd.h>
}

#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>

#include "auth/common.hh"
#include "auth/permission.hh"
#include "auth/role_or_anonymous.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "exceptions/exceptions.hh"
#include "utils/log.hh"
#include "utils/class_registrator.hh"

namespace auth {

std::string_view default_authorizer::qualified_java_name() const {
    return "org.apache.cassandra.auth.CassandraAuthorizer";
}

static constexpr std::string_view ROLE_NAME = "role";
static constexpr std::string_view RESOURCE_NAME = "resource";
static constexpr std::string_view PERMISSIONS_NAME = "permissions";

static logging::logger alogger("default_authorizer");

// To ensure correct initialization order, we unfortunately need to use a string literal.
static const class_registrator<
        authorizer,
        default_authorizer,
        cql3::query_processor&> password_auth_reg("org.apache.cassandra.auth.CassandraAuthorizer");

default_authorizer::default_authorizer(cql3::query_processor& qp)
        : _qp(qp) {
}

default_authorizer::~default_authorizer() {
}

future<> default_authorizer::start() {
    return make_ready_future<>();
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

    const sstring query = seastar::format("SELECT {} FROM {}.{} WHERE {} = ? AND {} = ?",
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
        std::string_view op,
        ::service::group0_batch& mc) {
    const sstring query = seastar::format("UPDATE {}.{} SET {} = {} {} ? WHERE {} = ? AND {} = ?",
            get_auth_ks_name(_qp),
            PERMISSIONS_CF,
            PERMISSIONS_NAME,
            PERMISSIONS_NAME,
            op,
            ROLE_NAME,
            RESOURCE_NAME);
    co_await collect_mutations(_qp, mc, query,
            {permissions::to_strings(set), sstring(role_name), resource.name()});
}


future<> default_authorizer::grant(std::string_view role_name, permission_set set, const resource& resource, ::service::group0_batch& mc) {
    return modify(role_name, std::move(set), resource, "+", mc);
}

future<> default_authorizer::revoke(std::string_view role_name, permission_set set, const resource& resource, ::service::group0_batch& mc) {
    return modify(role_name, std::move(set), resource, "-", mc);
}

future<std::vector<permission_details>> default_authorizer::list_all() const {
    const sstring query = seastar::format("SELECT {}, {}, {} FROM {}.{}",
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

future<> default_authorizer::revoke_all(std::string_view role_name, ::service::group0_batch& mc) {
    try {
        const sstring query = seastar::format("DELETE FROM {}.{} WHERE {} = ?",
                get_auth_ks_name(_qp),
                PERMISSIONS_CF,
                ROLE_NAME);
        co_await collect_mutations(_qp, mc, query, {sstring(role_name)});
    } catch (const exceptions::request_execution_exception& e) {
        alogger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", role_name, e);
    }
}

future<> default_authorizer::revoke_all(const resource& resource, ::service::group0_batch& mc) {
    if (resource.kind() == resource_kind::data &&
            data_resource_view(resource).is_keyspace()) {
        revoke_all_keyspace_resources(resource, mc);
        co_return;
    }

    auto name = resource.name();
    auto gen = [this, name] (api::timestamp_type t) -> ::service::mutations_generator {
        const sstring query = seastar::format("SELECT {} FROM {}.{} WHERE {} = ? ALLOW FILTERING",
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
            const sstring query = seastar::format("DELETE FROM {}.{} WHERE {} = ? AND {} = ?",
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
                    seastar::format("expecting single delete mutation, got {}", muts.size()));
            }
            co_yield std::move(muts[0]);
        }
    };
    mc.add_generator(std::move(gen), "default_authorizer::revoke_all");
}

void default_authorizer::revoke_all_keyspace_resources(const resource& ks_resource, ::service::group0_batch& mc) {
    auto ks_name = ks_resource.name();
    auto gen = [this, ks_name] (api::timestamp_type t) -> ::service::mutations_generator {
        const sstring query = seastar::format("SELECT {}, {} FROM {}.{}",
                ROLE_NAME,
                RESOURCE_NAME,
                get_auth_ks_name(_qp),
                PERMISSIONS_CF);
        auto res = co_await _qp.execute_internal(
                query,
                db::consistency_level::LOCAL_ONE,
                {},
                cql3::query_processor::cache_internal::no);
        auto ks_prefix = ks_name + "/";
        for (const auto& r : *res) {
            auto name = r.get_as<sstring>(RESOURCE_NAME);
            if (name != ks_name && !name.starts_with(ks_prefix)) {
                // r doesn't represent resource related to ks_resource
                continue;
            }
            const sstring query = seastar::format("DELETE FROM {}.{} WHERE {} = ? AND {} = ?",
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
    mc.add_generator(std::move(gen), "default_authorizer::revoke_all_keyspace_resources");
}

const resource_set& default_authorizer::protected_resources() const {
    static const resource_set resources({ make_data_resource(meta::legacy::AUTH_KS, PERMISSIONS_CF) });
    return resources;
}

}
