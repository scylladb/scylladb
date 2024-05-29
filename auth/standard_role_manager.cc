/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/standard_role_manager.hh"

#include <optional>
#include <unordered_set>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <seastar/core/future-util.hh>
#include <seastar/core/on_internal_error.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

#include "auth/common.hh"
#include "auth/role_manager.hh"
#include "auth/roles-metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "seastar/core/loop.hh"
#include "seastar/coroutine/maybe_yield.hh"
#include "service/raft/raft_group0_client.hh"
#include "utils/class_registrator.hh"
#include "service/migration_manager.hh"
#include "password_authenticator.hh"

namespace auth {

namespace meta {

namespace role_members_table {

constexpr std::string_view name{"role_members" , 12};

}

namespace role_attributes_table {
constexpr std::string_view name{"role_attributes", 15};

static std::string_view creation_query() noexcept {
    static const sstring instance = format(
            "CREATE TABLE {}.{} ("
            "  role text,"
            "  name text,"
            "  value text,"
            "  PRIMARY KEY(role, name)"
            ")",
            meta::legacy::AUTH_KS,
            name);

    return instance;
}
}
}

static logging::logger log("standard_role_manager");

static const class_registrator<
        role_manager,
        standard_role_manager,
        cql3::query_processor&,
        ::service::raft_group0_client&,
        ::service::migration_manager&> registration("org.apache.cassandra.auth.CassandraRoleManager");

struct record final {
    sstring name;
    bool is_superuser;
    bool can_login;
    role_set member_of;
};

static db::consistency_level consistency_for_role(std::string_view role_name) noexcept {
    if (role_name == meta::DEFAULT_SUPERUSER_NAME) {
        return db::consistency_level::QUORUM;
    }

    return db::consistency_level::LOCAL_ONE;
}

static future<std::optional<record>> find_record(cql3::query_processor& qp, std::string_view role_name) {
    const sstring query = format("SELECT * FROM {}.{} WHERE {} = ?",
            get_auth_ks_name(qp),
            meta::roles_table::name,
            meta::roles_table::role_col_name);

    const auto results = co_await qp.execute_internal(
            query,
            consistency_for_role(role_name),
            internal_distributed_query_state(),
            {sstring(role_name)},
            cql3::query_processor::cache_internal::yes);
    if (results->empty()) {
        co_return std::optional<record>();
    }

    const cql3::untyped_result_set_row& row = results->one();
    co_return std::make_optional(record{
            row.get_as<sstring>(sstring(meta::roles_table::role_col_name)),
            row.get_or<bool>("is_superuser", false),
            row.get_or<bool>("can_login", false),
            (row.has("member_of")
                        ? row.get_set<sstring>("member_of")
                        : role_set())});
}

static future<record> require_record(cql3::query_processor& qp, std::string_view role_name) {
    return find_record(qp, role_name).then([role_name](std::optional<record> mr) {
        if (!mr) {
            throw nonexistant_role(role_name);
        }

        return make_ready_future<record>(*mr);
   });
}

static bool has_can_login(const cql3::untyped_result_set_row& row) {
    return row.has("can_login") && !(boolean_type->deserialize(row.get_blob("can_login")).is_null());
}

standard_role_manager::standard_role_manager(cql3::query_processor& qp, ::service::raft_group0_client& g0, ::service::migration_manager& mm)
    : _qp(qp)
    , _group0_client(g0)
    , _migration_manager(mm)
    , _stopped(make_ready_future<>())
    , _superuser(password_authenticator::default_superuser(qp.db().get_config()))
{}

std::string_view standard_role_manager::qualified_java_name() const noexcept {
    return "org.apache.cassandra.auth.CassandraRoleManager";
}

const resource_set& standard_role_manager::protected_resources() const {
    static const resource_set resources({
            make_data_resource(meta::legacy::AUTH_KS, meta::roles_table::name),
            make_data_resource(meta::legacy::AUTH_KS, meta::role_members_table::name)});

    return resources;
}

future<> standard_role_manager::create_legacy_metadata_tables_if_missing() const {
    static const sstring create_role_members_query = fmt::format(
            "CREATE TABLE {}.{} ("
            "  role text,"
            "  member text,"
            "  PRIMARY KEY (role, member)"
            ")",
            meta::legacy::AUTH_KS,
            meta::role_members_table::name);


    return when_all_succeed(
            create_legacy_metadata_table_if_missing(
                    meta::roles_table::name,
                    _qp,
                    meta::roles_table::creation_query(),
                    _migration_manager),
            create_legacy_metadata_table_if_missing(
                    meta::role_members_table::name,
                    _qp,
                    create_role_members_query,
                    _migration_manager),
            create_legacy_metadata_table_if_missing(
                    meta::role_attributes_table::name,
                    _qp,
                    meta::role_attributes_table::creation_query(),
                    _migration_manager)).discard_result();
}

future<> standard_role_manager::create_default_role_if_missing() {
    try {
        const auto exists = co_await default_role_row_satisfies(_qp, &has_can_login, _superuser);
        if (exists) {
            co_return;
        }
        const sstring query = format("INSERT INTO {}.{} ({}, is_superuser, can_login) VALUES (?, true, true)",
                get_auth_ks_name(_qp),
                meta::roles_table::name,
                meta::roles_table::role_col_name);
        if (legacy_mode(_qp)) {
            co_await _qp.execute_internal(
                    query,
                    db::consistency_level::QUORUM,
                    internal_distributed_query_state(),
                    {_superuser},
                    cql3::query_processor::cache_internal::no).discard_result();
        } else {
            co_await announce_mutations(_qp, _group0_client, query, {_superuser}, _as, ::service::raft_timeout{});
        }
        log.info("Created default superuser role '{}'.", _superuser);
    } catch(const exceptions::unavailable_exception& e) {
        log.warn("Skipped default role setup: some nodes were not ready; will retry");
        throw e;
    }
}

static const sstring legacy_table_name{"users"};

bool standard_role_manager::legacy_metadata_exists() {
    return _qp.db().has_schema(meta::legacy::AUTH_KS, legacy_table_name);
}

future<> standard_role_manager::migrate_legacy_metadata() {
    log.info("Starting migration of legacy user metadata.");
    static const sstring query = format("SELECT * FROM {}.{}", meta::legacy::AUTH_KS, legacy_table_name);

    return _qp.execute_internal(
            query,
            db::consistency_level::QUORUM,
            internal_distributed_query_state(),
            cql3::query_processor::cache_internal::no).then([this](::shared_ptr<cql3::untyped_result_set> results) {
        return do_for_each(*results, [this](const cql3::untyped_result_set_row& row) {
            role_config config;
            config.is_superuser = row.get_or<bool>("super", false);
            config.can_login = true;

            return do_with(
                    row.get_as<sstring>("name"),
                    std::move(config),
                    ::service::group0_batch::unused(),
                    [this](const auto& name, const auto& config, auto& mc) {
                return create_or_replace(name, config, mc);
            });
        }).finally([results] {});
    }).then([] {
        log.info("Finished migrating legacy user metadata.");
    }).handle_exception([](std::exception_ptr ep) {
        log.error("Encountered an error during migration!");
        std::rethrow_exception(ep);
    });
}

future<> standard_role_manager::start() {
    return once_among_shards([this] {
        return futurize_invoke([this] () {
            if (legacy_mode(_qp)) {
                return create_legacy_metadata_tables_if_missing();
            }
            return make_ready_future<>();
        }).then([this] {
            _stopped = auth::do_after_system_ready(_as, [this] {
                return seastar::async([this] {
                    if (legacy_mode(_qp)) {
                        _migration_manager.wait_for_schema_agreement(_qp.db().real_database(), db::timeout_clock::time_point::max(), &_as).get();

                        if (any_nondefault_role_row_satisfies(_qp, &has_can_login).get()) {
                            if (legacy_metadata_exists()) {
                                log.warn("Ignoring legacy user metadata since nondefault roles already exist.");
                            }

                            return;
                        }

                        if (legacy_metadata_exists()) {
                            migrate_legacy_metadata().get();
                            return;
                        }
                    }
                    create_default_role_if_missing().get();
                });
            });
        });
    });
}

future<> standard_role_manager::stop() {
    _as.request_abort();
    return _stopped.handle_exception_type([] (const sleep_aborted&) { }).handle_exception_type([](const abort_requested_exception&) {});;
}

future<> standard_role_manager::create_or_replace(std::string_view role_name, const role_config& c, ::service::group0_batch& mc) {
    const sstring query = format("INSERT INTO {}.{} ({}, is_superuser, can_login) VALUES (?, ?, ?)",
            get_auth_ks_name(_qp),
            meta::roles_table::name,
            meta::roles_table::role_col_name);
    if (legacy_mode(_qp)) {
        co_await _qp.execute_internal(
                query,
                consistency_for_role(role_name),
                internal_distributed_query_state(),
                {sstring(role_name), c.is_superuser, c.can_login},
                cql3::query_processor::cache_internal::yes).discard_result();
    } else {
        co_await collect_mutations(_qp, mc,  query, {sstring(role_name), c.is_superuser, c.can_login});
    }
}

future<>
standard_role_manager::create(std::string_view role_name, const role_config& c, ::service::group0_batch& mc) {
    return exists(role_name).then([this, role_name, &c, &mc](bool role_exists) {
        if (role_exists) {
            throw role_already_exists(role_name);
        }

        return create_or_replace(role_name, c, mc);
    });
}

future<>
standard_role_manager::alter(std::string_view role_name, const role_config_update& u, ::service::group0_batch& mc) {
    static const auto build_column_assignments = [](const role_config_update& u) -> sstring {
        std::vector<sstring> assignments;

        if (u.is_superuser) {
            assignments.push_back(sstring("is_superuser = ") + (*u.is_superuser ? "true" : "false"));
        }

        if (u.can_login) {
            assignments.push_back(sstring("can_login = ") + (*u.can_login ? "true" : "false"));
        }

        return boost::algorithm::join(assignments, ", ");
    };

    return require_record(_qp, role_name).then([this, role_name, &u, &mc](record) {
        if (!u.is_superuser && !u.can_login) {
            return make_ready_future<>();
        }
        const sstring query = format("UPDATE {}.{} SET {} WHERE {} = ?",
            get_auth_ks_name(_qp),
            meta::roles_table::name,
            build_column_assignments(u),
            meta::roles_table::role_col_name);
        if (legacy_mode(_qp)) {
            return _qp.execute_internal(
                    std::move(query),
                    consistency_for_role(role_name),
                    internal_distributed_query_state(),
                    {sstring(role_name)},
                    cql3::query_processor::cache_internal::no).discard_result();
        } else {
            return collect_mutations(_qp, mc, std::move(query), {sstring(role_name)});
        }
    });
}

future<> standard_role_manager::drop(std::string_view role_name, ::service::group0_batch& mc) {
    if (!co_await exists(role_name)) {
        throw nonexistant_role(role_name);
    }
    // First, revoke this role from all roles that are members of it.
    const auto revoke_from_members = [this, role_name, &mc] () -> future<> {
        const sstring query = format("SELECT member FROM {}.{} WHERE role = ?",
                get_auth_ks_name(_qp),
                meta::role_members_table::name);
        const auto members = co_await _qp.execute_internal(
                query,
                consistency_for_role(role_name),
                internal_distributed_query_state(),
                {sstring(role_name)},
                cql3::query_processor::cache_internal::no);
        co_await parallel_for_each(
                members->begin(),
                members->end(),
                [this, role_name, &mc] (const cql3::untyped_result_set_row& member_row) -> future<> {
                    const sstring member = member_row.template get_as<sstring>("member");
                    co_await modify_membership(member, role_name, membership_change::remove, mc);
                }
        );
    };
    // In parallel, revoke all roles that this role is members of.
    const auto revoke_members_of = [this, grantee = role_name, &mc] () -> future<> {
        const role_set granted_roles = co_await query_granted(
                grantee,
                recursive_role_query::no);
        co_await parallel_for_each(
                granted_roles.begin(),
                granted_roles.end(),
                [this, grantee, &mc](const sstring& role_name) {
            return modify_membership(grantee, role_name, membership_change::remove, mc);
        });
    };
    // Delete all attributes for that role
    const auto remove_attributes_of = [this, role_name, &mc] () -> future<> {
        const sstring query = format("DELETE FROM {}.{} WHERE role = ?",
                get_auth_ks_name(_qp),
                meta::role_attributes_table::name);
        if (legacy_mode(_qp)) {
            co_await _qp.execute_internal(query, {sstring(role_name)},
                cql3::query_processor::cache_internal::yes).discard_result();
        } else {
            co_await collect_mutations(_qp, mc, query, {sstring(role_name)});
        }
    };
    // Finally, delete the role itself.
    const auto delete_role = [this, role_name, &mc] () -> future<> {
        const sstring query = format("DELETE FROM {}.{} WHERE {} = ?",
                get_auth_ks_name(_qp),
                meta::roles_table::name,
                meta::roles_table::role_col_name);

        if (legacy_mode(_qp)) {
            co_await _qp.execute_internal(
                    query,
                    consistency_for_role(role_name),
                    internal_distributed_query_state(),
                    {sstring(role_name)},
                    cql3::query_processor::cache_internal::no).discard_result();
        } else {
            co_await collect_mutations(_qp, mc, query, {sstring(role_name)});
        }
    };

    co_await when_all_succeed(revoke_from_members, revoke_members_of, remove_attributes_of);
    co_await delete_role();
}

future<>
standard_role_manager::legacy_modify_membership(
        std::string_view grantee_name,
        std::string_view role_name,
        membership_change ch) {
    const auto modify_roles = [this, role_name, grantee_name, ch] () -> future<> {
        const auto query = format(
                "UPDATE {}.{} SET member_of = member_of {} ? WHERE {} = ?",
                get_auth_ks_name(_qp),
                meta::roles_table::name,
                (ch == membership_change::add ? '+' : '-'),
                meta::roles_table::role_col_name);
        co_await _qp.execute_internal(
                query,
                consistency_for_role(grantee_name),
                internal_distributed_query_state(),
                {role_set{sstring(role_name)}, sstring(grantee_name)},
                cql3::query_processor::cache_internal::no).discard_result();
    };

    const auto modify_role_members = [this, role_name, grantee_name, ch] () -> future<> {
        switch (ch) {
            case membership_change::add: {
                const sstring insert_query = format("INSERT INTO {}.{} (role, member) VALUES (?, ?)",
                        get_auth_ks_name(_qp),
                        meta::role_members_table::name);
                co_return co_await _qp.execute_internal(
                        insert_query,
                        consistency_for_role(role_name),
                        internal_distributed_query_state(),
                        {sstring(role_name), sstring(grantee_name)},
                        cql3::query_processor::cache_internal::no).discard_result();
            }

            case membership_change::remove: {
                const sstring delete_query = format("DELETE FROM {}.{} WHERE role = ? AND member = ?",
                        get_auth_ks_name(_qp),
                        meta::role_members_table::name);
                co_return co_await _qp.execute_internal(
                        delete_query,
                        consistency_for_role(role_name),
                        internal_distributed_query_state(),
                        {sstring(role_name), sstring(grantee_name)},
                        cql3::query_processor::cache_internal::no).discard_result();
            }
        }
    };

    co_await when_all_succeed(modify_roles, modify_role_members).discard_result();
}

future<>
standard_role_manager::modify_membership(
        std::string_view grantee_name,
        std::string_view role_name,
        membership_change ch,
        ::service::group0_batch& mc) {
    if (legacy_mode(_qp)) {
        co_return co_await legacy_modify_membership(grantee_name, role_name, ch);
    }

    const auto modify_roles = format(
            "UPDATE {}.{} SET member_of = member_of {} ? WHERE {} = ?",
            get_auth_ks_name(_qp),
            meta::roles_table::name,
            (ch == membership_change::add ? '+' : '-'),
            meta::roles_table::role_col_name);
    co_await collect_mutations(_qp, mc, modify_roles,
            {role_set{sstring(role_name)}, sstring(grantee_name)});

    sstring modify_role_members;
    switch (ch) {
    case membership_change::add:
        modify_role_members = format("INSERT INTO {}.{} (role, member) VALUES (?, ?)",
                get_auth_ks_name(_qp),
                meta::role_members_table::name);
        break;
    case membership_change::remove:
        modify_role_members = format("DELETE FROM {}.{} WHERE role = ? AND member = ?",
                get_auth_ks_name(_qp),
                meta::role_members_table::name);
        break;
    default:
        on_internal_error(log, format("unknown membership_change value: {}", int(ch)));
    }
    co_await collect_mutations(_qp, mc, modify_role_members,
            {sstring(role_name), sstring(grantee_name)});
}

future<>
standard_role_manager::grant(std::string_view grantee_name, std::string_view role_name, ::service::group0_batch& mc) {
    const auto check_redundant = [this, role_name, grantee_name] {
        return query_granted(
                grantee_name,
                recursive_role_query::yes).then([role_name, grantee_name](role_set roles) {
            if (roles.contains(sstring(role_name))) {
                throw role_already_included(grantee_name, role_name);
            }

            return make_ready_future<>();
        });
    };

    const auto check_cycle = [this, role_name, grantee_name] {
        return query_granted(
                role_name,
                recursive_role_query::yes).then([role_name, grantee_name](role_set roles) {
            if (roles.contains(sstring(grantee_name))) {
                throw role_already_included(role_name, grantee_name);
            }

            return make_ready_future<>();
        });
    };

    return when_all_succeed(check_redundant(), check_cycle()).then_unpack([this, role_name, grantee_name, &mc] {
        return modify_membership(grantee_name, role_name, membership_change::add, mc);
    });
}

future<>
standard_role_manager::revoke(std::string_view revokee_name, std::string_view role_name, ::service::group0_batch& mc) {
    return exists(role_name).then([role_name](bool role_exists) {
        if (!role_exists) {
            throw nonexistant_role(sstring(role_name));
        }
    }).then([this, revokee_name, role_name, &mc] {
        return query_granted(
                revokee_name,
                recursive_role_query::no).then([revokee_name, role_name](role_set roles) {
            if (!roles.contains(sstring(role_name))) {
                throw revoke_ungranted_role(revokee_name, role_name);
            }

            return make_ready_future<>();
        }).then([this, revokee_name, role_name, &mc] {
            return modify_membership(revokee_name, role_name, membership_change::remove, mc);
        });
    });
}

static future<> collect_roles(
        cql3::query_processor& qp,
        std::string_view grantee_name,
        bool recurse,
        role_set& roles) {
    return require_record(qp, grantee_name).then([&qp, &roles, recurse](record r) {
        return do_with(std::move(r.member_of), [&qp, &roles, recurse](const role_set& memberships) {
            return do_for_each(memberships.begin(), memberships.end(), [&qp, &roles, recurse](const sstring& role_name) {
                roles.insert(role_name);

                if (recurse) {
                    return collect_roles(qp, role_name, true, roles);
                }

                return make_ready_future<>();
            });
        });
    });
}

future<role_set> standard_role_manager::query_granted(std::string_view grantee_name, recursive_role_query m) {
    const bool recurse = (m == recursive_role_query::yes);

    return do_with(
            role_set{sstring(grantee_name)},
            [this, grantee_name, recurse](role_set& roles) {
        return collect_roles(_qp, grantee_name, recurse, roles).then([&roles] { return roles; });
    });
}

future<role_to_directly_granted_map> standard_role_manager::query_all_directly_granted() {
    const sstring query = format("SELECT * FROM {}.{}",
            get_auth_ks_name(_qp),
            meta::role_members_table::name);

    role_to_directly_granted_map roles_map;
    co_await _qp.query_internal(query, [&roles_map] (const cql3::untyped_result_set_row& row) -> future<stop_iteration> {
        roles_map.insert({row.get_as<sstring>("member"), row.get_as<sstring>("role")});
        co_return stop_iteration::no;
    });

    co_return roles_map;
}

future<role_set> standard_role_manager::query_all() {
    const sstring query = format("SELECT {} FROM {}.{}",
            meta::roles_table::role_col_name,
            get_auth_ks_name(_qp),
            meta::roles_table::name);

    // To avoid many copies of a view.
    static const auto role_col_name_string = sstring(meta::roles_table::role_col_name);

    const auto results = co_await _qp.execute_internal(
            query,
            db::consistency_level::QUORUM,
            internal_distributed_query_state(),
            cql3::query_processor::cache_internal::yes);

    role_set roles;
    std::transform(
            results->begin(),
            results->end(),
            std::inserter(roles, roles.begin()),
            [] (const cql3::untyped_result_set_row& row) {
                return row.get_as<sstring>(role_col_name_string);}
    );
    co_return roles;
}

future<bool> standard_role_manager::exists(std::string_view role_name) {
    return find_record(_qp, role_name).then([](std::optional<record> mr) {
        return static_cast<bool>(mr);
    });
}

future<bool> standard_role_manager::is_superuser(std::string_view role_name) {
    return require_record(_qp, role_name).then([](record r) {
        return r.is_superuser;
    });
}

future<bool> standard_role_manager::can_login(std::string_view role_name) {
    return require_record(_qp, role_name).then([](record r) {
        return r.can_login;
    });
}

future<std::optional<sstring>> standard_role_manager::get_attribute(std::string_view role_name, std::string_view attribute_name) {
    const sstring query = format("SELECT name, value FROM {}.{} WHERE role = ? AND name = ?",
            get_auth_ks_name(_qp),
            meta::role_attributes_table::name);
    const auto result_set = co_await _qp.execute_internal(query, {sstring(role_name), sstring(attribute_name)}, cql3::query_processor::cache_internal::yes);
    if (!result_set->empty()) {
        const cql3::untyped_result_set_row &row = result_set->one();
        co_return std::optional<sstring>(row.get_as<sstring>("value"));
    }
    co_return std::optional<sstring>{};
}

future<role_manager::attribute_vals> standard_role_manager::query_attribute_for_all (std::string_view attribute_name) {
    return query_all().then([this, attribute_name] (role_set roles) {
        return do_with(attribute_vals{}, [this, attribute_name, roles = std::move(roles)] (attribute_vals &role_to_att_val) {
            return parallel_for_each(roles.begin(), roles.end(), [this, &role_to_att_val, attribute_name] (sstring role) {
                return get_attribute(role, attribute_name).then([&role_to_att_val, role] (std::optional<sstring> att_val) {
                    if (att_val) {
                        role_to_att_val.emplace(std::move(role), std::move(*att_val));
                    }
                });
            }).then([&role_to_att_val] () {
                return make_ready_future<attribute_vals>(std::move(role_to_att_val));
            });
        });
    });
}

future<> standard_role_manager::set_attribute(std::string_view role_name, std::string_view attribute_name, std::string_view attribute_value, ::service::group0_batch& mc) {
    if (!co_await exists(role_name)) {
        throw auth::nonexistant_role(role_name);
    }
    const sstring query = format("INSERT INTO {}.{} (role, name, value)  VALUES (?, ?, ?)",
            get_auth_ks_name(_qp),
            meta::role_attributes_table::name);
    if (legacy_mode(_qp)) {
        co_await _qp.execute_internal(query, {sstring(role_name), sstring(attribute_name), sstring(attribute_value)}, cql3::query_processor::cache_internal::yes).discard_result();
    } else {
        co_await collect_mutations(_qp, mc, query,
                {sstring(role_name), sstring(attribute_name), sstring(attribute_value)});
    }
}

future<> standard_role_manager::remove_attribute(std::string_view role_name, std::string_view attribute_name, ::service::group0_batch& mc) {
    if (!co_await exists(role_name)) {
        throw auth::nonexistant_role(role_name);
    }
    const sstring query = format("DELETE FROM {}.{} WHERE role = ? AND name = ?",
            get_auth_ks_name(_qp),
            meta::role_attributes_table::name);
    if (legacy_mode(_qp)) {
        co_await _qp.execute_internal(query, {sstring(role_name), sstring(attribute_name)}, cql3::query_processor::cache_internal::yes).discard_result();
    } else {
        co_await collect_mutations(_qp, mc, query,
                {sstring(role_name), sstring(attribute_name)});
    }
}
}
