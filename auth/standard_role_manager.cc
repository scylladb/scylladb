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

#include "auth/standard_role_manager.hh"

#include <optional>
#include <unordered_set>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <seastar/core/future-util.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

#include "auth/common.hh"
#include "auth/roles-metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "utils/class_registrator.hh"
#include "database.hh"
#include "schema_builder.hh"
#include "service/migration_manager.hh"

namespace auth {

namespace meta {

namespace role_members_table {

constexpr std::string_view name{"role_members" , 12};
constexpr std::string_view qualified_name("system_auth.role_members");

}

}

static logging::logger log("standard_role_manager");

static const class_registrator<
        role_manager,
        standard_role_manager,
        cql3::query_processor&,
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
    static const sstring query = format("SELECT * FROM {} WHERE {} = ?",
            meta::roles_table::qualified_name,
            meta::roles_table::role_col_name);

    return qp.execute_internal(
            query,
            consistency_for_role(role_name),
            internal_distributed_query_state(),
            {sstring(role_name)},
            true).then([](::shared_ptr<cql3::untyped_result_set> results) {
        if (results->empty()) {
            return std::optional<record>();
        }

        const cql3::untyped_result_set_row& row = results->one();

        return std::make_optional(
                record{
                        row.get_as<sstring>(sstring(meta::roles_table::role_col_name)),
                        row.get_or<bool>("is_superuser", false),
                        row.get_or<bool>("can_login", false),
                        (row.has("member_of")
                                 ? row.get_set<sstring>("member_of")
                                 : role_set())});
    });
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

std::string_view standard_role_manager::qualified_java_name() const noexcept {
    return "org.apache.cassandra.auth.CassandraRoleManager";
}

const resource_set& standard_role_manager::protected_resources() const {
    static const resource_set resources({
            make_data_resource(meta::AUTH_KS, meta::roles_table::name),
            make_data_resource(meta::AUTH_KS, meta::role_members_table::name)});

    return resources;
}

static future<> add_options_column_if_missing(database& db, ::service::migration_manager& mm) noexcept {
    try {
        auto schema = db.find_schema(sstring(meta::AUTH_KS), sstring(meta::roles_table::name));
        bytes options_name = to_bytes(meta::roles_table::options_col_name.data());
        if (schema->get_column_definition(options_name)) {
            return make_ready_future<>();
        }
        schema_builder b(schema);
        b.with_column(options_name,
                map_type_impl::get_instance(utf8_type, utf8_type, false),
                column_kind::regular_column);
        schema_ptr table = b.build();
        return mm.announce_column_family_update(table, false, {}, false, api::timestamp_type(0)).handle_exception([] (const std::exception_ptr&) {});
    } catch (...) {
        log.warn("Failed to update options column in the roles table: {}", std::current_exception());
        return make_ready_future<>();
    }
}

future<> standard_role_manager::create_metadata_tables_if_missing() const {
    static const sstring create_role_members_query = sprint(
            "CREATE TABLE %s ("
            "  role text,"
            "  member text,"
            "  PRIMARY KEY (role, member)"
            ")",
            meta::role_members_table::qualified_name);


    return when_all_succeed(
            create_metadata_table_if_missing(
                    meta::roles_table::name,
                    _qp,
                    meta::roles_table::creation_query(),
                    _migration_manager),
            create_metadata_table_if_missing(
                    meta::role_members_table::name,
                    _qp,
                    create_role_members_query,
                    _migration_manager)).discard_result().then([this] {
        return add_options_column_if_missing(_qp.db(), _migration_manager);
    });
}

future<> standard_role_manager::create_default_role_if_missing() const {
    return default_role_row_satisfies(_qp, &has_can_login).then([this](bool exists) {
        if (!exists) {
            static const sstring query = format("INSERT INTO {} ({}, is_superuser, can_login) VALUES (?, true, true)",
                    meta::roles_table::qualified_name,
                    meta::roles_table::role_col_name);

            return _qp.execute_internal(
                    query,
                    db::consistency_level::QUORUM,
                    internal_distributed_query_state(),
                    {meta::DEFAULT_SUPERUSER_NAME}).then([](auto&&) {
                log.info("Created default superuser role '{}'.", meta::DEFAULT_SUPERUSER_NAME);
                return make_ready_future<>();
            });
        }

        return make_ready_future<>();
    }).handle_exception_type([](const exceptions::unavailable_exception& e) {
        log.warn("Skipped default role setup: some nodes were not ready; will retry");
        return make_exception_future<>(e);
    });
}

static const sstring legacy_table_name{"users"};

bool standard_role_manager::legacy_metadata_exists() const {
    return _qp.db().has_schema(meta::AUTH_KS, legacy_table_name);
}

future<> standard_role_manager::migrate_legacy_metadata() const {
    log.info("Starting migration of legacy user metadata.");
    static const sstring query = format("SELECT * FROM {}.{}", meta::AUTH_KS, legacy_table_name);

    return _qp.execute_internal(
            query,
            db::consistency_level::QUORUM,
            internal_distributed_query_state()).then([this](::shared_ptr<cql3::untyped_result_set> results) {
        return do_for_each(*results, [this](const cql3::untyped_result_set_row& row) {
            role_config config;
            config.is_superuser = row.get_or<bool>("super", false);
            config.can_login = true;

            return do_with(
                    row.get_as<sstring>("name"),
                    std::move(config),
                    [this](const auto& name, const auto& config) {
                return this->create_or_replace(name, config);
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
        return this->create_metadata_tables_if_missing().then([this] {
            _stopped = auth::do_after_system_ready(_as, [this] {
                return seastar::async([this] {
                    wait_for_schema_agreement(_migration_manager, _qp.db(), _as).get0();

                    if (any_nondefault_role_row_satisfies(_qp, &has_can_login).get0()) {
                        if (this->legacy_metadata_exists()) {
                            log.warn("Ignoring legacy user metadata since nondefault roles already exist.");
                        }

                        return;
                    }

                    if (this->legacy_metadata_exists()) {
                        this->migrate_legacy_metadata().get0();
                        return;
                    }

                    create_default_role_if_missing().get0();
                });
            });
        });
    });
}

future<> standard_role_manager::stop() {
    _as.request_abort();
    return _stopped.handle_exception_type([] (const sleep_aborted&) { }).handle_exception_type([](const abort_requested_exception&) {});;
}

future<> standard_role_manager::create_or_replace(std::string_view role_name, const role_config& c) const {
    static const sstring query = format("INSERT INTO {} ({}, is_superuser, can_login) VALUES (?, ?, ?)",
            meta::roles_table::qualified_name,
            meta::roles_table::role_col_name);

    return _qp.execute_internal(
            query,
            consistency_for_role(role_name),
            internal_distributed_query_state(),
            {sstring(role_name), c.is_superuser, c.can_login},
            true).discard_result();
}

future<>
standard_role_manager::create(std::string_view role_name, const role_config& c) const {
    return this->exists(role_name).then([this, role_name, &c](bool role_exists) {
        if (role_exists) {
            throw role_already_exists(role_name);
        }

        return this->create_or_replace(role_name, c);
    });
}

future<>
standard_role_manager::alter(std::string_view role_name, const role_config_update& u) const {
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

    return require_record(_qp, role_name).then([this, role_name, &u](record) {
        if (!u.is_superuser && !u.can_login) {
            return make_ready_future<>();
        }

        return _qp.execute_internal(
                format("UPDATE {} SET {} WHERE {} = ?",
                        meta::roles_table::qualified_name,
                        build_column_assignments(u),
                        meta::roles_table::role_col_name),
                consistency_for_role(role_name),
                internal_distributed_query_state(),
                {sstring(role_name)}).discard_result();
    });
}

future<> standard_role_manager::update_custom_options(std::string_view role_name, const custom_options& options) const {
    static const sstring query = format("UPDATE {} SET {} = ? WHERE {} = ?",
            meta::roles_table::qualified_name,
            meta::roles_table::options_col_name,
            meta::roles_table::role_col_name);
    std::vector<std::pair<data_value, data_value>> entries;
    for (const auto& entry : options) {
        entries.push_back({data_value(entry.first), data_value(entry.second)});
    }
    auto map_value = make_map_value(map_type_impl::get_instance(utf8_type, utf8_type, false), entries);

    return _qp.execute_internal(
            query,
            consistency_for_role(role_name),
            internal_distributed_query_state(),
            {std::move(map_value), sstring(role_name)}).discard_result();
}

future<custom_options> standard_role_manager::query_custom_options(std::string_view role_name) const {
    static const sstring query = format("SELECT {} FROM {} WHERE {} = ?",
            meta::roles_table::options_col_name,
            meta::roles_table::qualified_name,
            meta::roles_table::role_col_name);

    return _qp.execute_internal(
            query, consistency_for_role(role_name),
            internal_distributed_query_state(),
            {sstring(role_name)}).then([](::shared_ptr<cql3::untyped_result_set> rs) {
        custom_options opts;
        const auto& row = rs->one();
        if (row.has(meta::roles_table::options_col_name)) {
            row.get_map_data<sstring, sstring>(meta::roles_table::options_col_name, std::inserter(opts, opts.end()), utf8_type, utf8_type);
        }
        return opts;
    });
}

future<> standard_role_manager::drop(std::string_view role_name) const {
    return this->exists(role_name).then([this, role_name](bool role_exists) {
        if (!role_exists) {
            throw nonexistant_role(role_name);
        }

        // First, revoke this role from all roles that are members of it.
        const auto revoke_from_members = [this, role_name] {
            static const sstring query = format("SELECT member FROM {} WHERE role = ?",
                    meta::role_members_table::qualified_name);

            return _qp.execute_internal(
                    query,
                    consistency_for_role(role_name),
                    internal_distributed_query_state(),
                    {sstring(role_name)}).then([this, role_name](::shared_ptr<cql3::untyped_result_set> members) {
                return parallel_for_each(
                        members->begin(),
                        members->end(),
                        [this, role_name](const cql3::untyped_result_set_row& member_row) {
                    const sstring member = member_row.template get_as<sstring>("member");
                    return this->modify_membership(member, role_name, membership_change::remove);
                }).finally([members] {});
            });
        };

        // In parallel, revoke all roles that this role is members of.
        const auto revoke_members_of = [this, grantee = role_name] {
            return this->query_granted(
                    grantee,
                    recursive_role_query::no).then([this, grantee](role_set granted_roles) {
                return do_with(
                        std::move(granted_roles),
                        [this, grantee](const role_set& granted_roles) {
                    return parallel_for_each(
                            granted_roles.begin(),
                            granted_roles.end(),
                            [this, grantee](const sstring& role_name) {
                        return this->modify_membership(grantee, role_name, membership_change::remove);
                    });
                });
            });
        };

        // Finally, delete the role itself.
        auto delete_role = [this, role_name] {
            static const sstring query = format("DELETE FROM {} WHERE {} = ?",
                    meta::roles_table::qualified_name,
                    meta::roles_table::role_col_name);

            return _qp.execute_internal(
                    query,
                    consistency_for_role(role_name),
                    internal_distributed_query_state(),
                    {sstring(role_name)}).discard_result();
        };

        return when_all_succeed(revoke_from_members(), revoke_members_of()).then_unpack([delete_role = std::move(delete_role)] {
            return delete_role();
        });
    });
}

future<>
standard_role_manager::modify_membership(
        std::string_view grantee_name,
        std::string_view role_name,
        membership_change ch) const {


    const auto modify_roles = [this, role_name, grantee_name, ch] {
        const auto query = format(
                "UPDATE {} SET member_of = member_of {} ? WHERE {} = ?",
                meta::roles_table::qualified_name,
                (ch == membership_change::add ? '+' : '-'),
                meta::roles_table::role_col_name);

        return _qp.execute_internal(
                query,
                consistency_for_role(grantee_name),
                internal_distributed_query_state(),
                {role_set{sstring(role_name)}, sstring(grantee_name)}).discard_result();
    };

    const auto modify_role_members = [this, role_name, grantee_name, ch] {
        switch (ch) {
            case membership_change::add:
                return _qp.execute_internal(
                        format("INSERT INTO {} (role, member) VALUES (?, ?)",
                                meta::role_members_table::qualified_name),
                        consistency_for_role(role_name),
                        internal_distributed_query_state(),
                        {sstring(role_name), sstring(grantee_name)}).discard_result();

            case membership_change::remove:
                return _qp.execute_internal(
                        format("DELETE FROM {} WHERE role = ? AND member = ?",
                                meta::role_members_table::qualified_name),
                        consistency_for_role(role_name),
                        internal_distributed_query_state(),
                        {sstring(role_name), sstring(grantee_name)}).discard_result();
        }

        return make_ready_future<>();
    };

    return when_all_succeed(modify_roles(), modify_role_members).discard_result();
}

future<>
standard_role_manager::grant(std::string_view grantee_name, std::string_view role_name) const {
    const auto check_redundant = [this, role_name, grantee_name] {
        return this->query_granted(
                grantee_name,
                recursive_role_query::yes).then([role_name, grantee_name](role_set roles) {
            if (roles.contains(sstring(role_name))) {
                throw role_already_included(grantee_name, role_name);
            }

            return make_ready_future<>();
        });
    };

    const auto check_cycle = [this, role_name, grantee_name] {
        return this->query_granted(
                role_name,
                recursive_role_query::yes).then([role_name, grantee_name](role_set roles) {
            if (roles.contains(sstring(grantee_name))) {
                throw role_already_included(role_name, grantee_name);
            }

            return make_ready_future<>();
        });
    };

   return when_all_succeed(check_redundant(), check_cycle()).then_unpack([this, role_name, grantee_name] {
       return this->modify_membership(grantee_name, role_name, membership_change::add);
   });
}

future<>
standard_role_manager::revoke(std::string_view revokee_name, std::string_view role_name) const {
    return this->exists(role_name).then([this, revokee_name, role_name](bool role_exists) {
        if (!role_exists) {
            throw nonexistant_role(sstring(role_name));
        }
    }).then([this, revokee_name, role_name] {
        return this->query_granted(
                revokee_name,
                recursive_role_query::no).then([revokee_name, role_name](role_set roles) {
            if (!roles.contains(sstring(role_name))) {
                throw revoke_ungranted_role(revokee_name, role_name);
            }

            return make_ready_future<>();
        }).then([this, revokee_name, role_name] {
            return this->modify_membership(revokee_name, role_name, membership_change::remove);
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

future<role_set> standard_role_manager::query_granted(std::string_view grantee_name, recursive_role_query m) const {
    const bool recurse = (m == recursive_role_query::yes);

    return do_with(
            role_set{sstring(grantee_name)},
            [this, grantee_name, recurse](role_set& roles) {
        return collect_roles(_qp, grantee_name, recurse, roles).then([&roles] { return roles; });
    });
}

future<role_set> standard_role_manager::query_all() const {
    static const sstring query = format("SELECT {} FROM {}",
            meta::roles_table::role_col_name,
            meta::roles_table::qualified_name);

    // To avoid many copies of a view.
    static const auto role_col_name_string = sstring(meta::roles_table::role_col_name);

    return _qp.execute_internal(
            query,
            db::consistency_level::QUORUM,
            internal_distributed_query_state()).then([](::shared_ptr<cql3::untyped_result_set> results) {
        role_set roles;

        std::transform(
                results->begin(),
                results->end(),
                std::inserter(roles, roles.begin()),
                [](const cql3::untyped_result_set_row& row) {
            return row.get_as<sstring>(role_col_name_string);
        });

        return roles;
    });
}

future<bool> standard_role_manager::exists(std::string_view role_name) const  {
    return find_record(_qp, role_name).then([](std::optional<record> mr) {
        return static_cast<bool>(mr);
    });
}

future<bool> standard_role_manager::is_superuser(std::string_view role_name) const {
    return require_record(_qp, role_name).then([](record r) {
        return r.is_superuser;
    });
}

future<bool> standard_role_manager::can_login(std::string_view role_name) const {
    return require_record(_qp, role_name).then([](record r) {
        return r.can_login;
    });
}

}
