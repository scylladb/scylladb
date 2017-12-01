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

#include <experimental/optional>
#include <unordered_set>
#include <vector>

#include <boost/algorithm/string/join.hpp>
#include <seastar/core/future-util.hh>
#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

#include "auth/common.hh"
#include "auth/roles-metadata.hh"
#include "cql3/query_processor.hh"
#include "db/consistency_level_type.hh"
#include "exceptions/exceptions.hh"
#include "log.hh"
#include "utils/class_registrator.hh"

namespace auth {

namespace meta {

namespace roles_table {

stdx::string_view qualified_name() noexcept {
    static const sstring instance = AUTH_KS + "." + sstring(name);
    return instance;
}

}

namespace role_members_table {

constexpr stdx::string_view name{"role_members" , 12};

static stdx::string_view qualified_name() noexcept {
    static const sstring instance = AUTH_KS + "." + sstring(name);
    return instance;
}

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
    std::unordered_set<sstring> member_of;
};

static db::consistency_level consistency_for_role(stdx::string_view role_name) noexcept {
    if (role_name == meta::DEFAULT_SUPERUSER_NAME) {
        return db::consistency_level::QUORUM;
    }

    return db::consistency_level::LOCAL_ONE;
}

static future<stdx::optional<record>> find_record(cql3::query_processor& qp, stdx::string_view role_name) {
    static const sstring query = sprint(
            "SELECT * FROM %s WHERE %s = ?",
            meta::roles_table::qualified_name(),
            meta::roles_table::role_col_name);

    return qp.process(
            query,
            consistency_for_role(role_name),
            {sstring(role_name)},
            true).then([](::shared_ptr<cql3::untyped_result_set> results) {
        if (results->empty()) {
            return stdx::optional<record>();
        }

        const cql3::untyped_result_set_row& row = results->one();

        return stdx::make_optional(
                record{
                        row.get_as<sstring>(sstring(meta::roles_table::role_col_name)),
                        row.get_as<bool>("is_superuser"),
                        row.get_as<bool>("can_login"),
                        (row.has("member_of")
                                 ? row.get_set<sstring>("member_of")
                                 : std::unordered_set<sstring>()) });
    });
}

static future<record> require_record(cql3::query_processor& qp, stdx::string_view role_name) {
    return find_record(qp, role_name).then([role_name](stdx::optional<record> mr) {
        if (!mr) {
            throw nonexistant_role(sstring(role_name));
        }

        return make_ready_future<record>(*mr);
   });
}

stdx::string_view standard_role_manager_name() noexcept {
    static const sstring instance = meta::AUTH_PACKAGE_NAME + "CassandraRoleManager";
    return instance;
}

stdx::string_view standard_role_manager::qualified_java_name() const noexcept {
    return standard_role_manager_name();
}

future<> standard_role_manager::create_metadata_tables_if_missing() {
    static const sstring create_roles_query = sprint(
            "CREATE TABLE %s ("
            "  %s text PRIMARY KEY,"
            "  can_login boolean,"
            "  is_superuser boolean,"
            "  member_of set<text>,"
            "  salted_hash text"
            ")",
            meta::roles_table::qualified_name(),
            meta::roles_table::role_col_name);

    static const sstring create_role_members_query = sprint(
            "CREATE TABLE %s ("
            "  role text,"
            "  member text,"
            "  PRIMARY KEY (role, member)"
            ")",
            meta::role_members_table::qualified_name());


    return when_all_succeed(
            create_metadata_table_if_missing(
                    sstring(meta::roles_table::name),
                    _qp,
                    create_roles_query,
                    _migration_manager),
            create_metadata_table_if_missing(
                    sstring(meta::role_members_table::name),
                    _qp,
                    create_role_members_query,
                    _migration_manager));
}

// Must be called within the scope of a seastar thread
bool standard_role_manager::has_existing_roles() const {
    static const sstring default_role_query = sprint(
        "SELECT * FROM %s WHERE %s = ?",
        meta::roles_table::qualified_name(),
        meta::roles_table::role_col_name);

    static const sstring all_roles_query = sprint("SELECT * FROM %s LIMIT 1", meta::roles_table::qualified_name());

    // This logic is borrowed directly from Apache Cassandra. By first checking for the presence of the default role, we
    // can potentially avoid doing a range query with a high consistency level.

    const bool default_exists_one = !_qp.process(
            default_role_query,
            db::consistency_level::ONE,
            {meta::DEFAULT_SUPERUSER_NAME},
            true).get0()->empty();

    if (default_exists_one) {
        return true;
    }

    const bool default_exists_quorum = !_qp.process(
            default_role_query,
            db::consistency_level::QUORUM,
            {meta::DEFAULT_SUPERUSER_NAME},
            true).get0()->empty();

    if (default_exists_quorum) {
        return true;
    }

    const bool any_exists_quorum = !_qp.process(all_roles_query, db::consistency_level::QUORUM).get0()->empty();
    return any_exists_quorum;
}

future<> standard_role_manager::start() {
    return once_among_shards([this] {
        return this->create_metadata_tables_if_missing().then([this] {
            delay_until_system_ready(_delayed, [this] {
                return seastar::async([this] {
                    try {
                        if (this->has_existing_roles()) {
                            return;
                        }
                        // Create the default superuser.
                        _qp.process(
                                sprint(
                                        "INSERT INTO %s (%s, is_superuser, can_login) VALUES (?, true, true)",
                                        meta::roles_table::qualified_name(),
                                        meta::roles_table::role_col_name),
                                db::consistency_level::QUORUM,
                                {meta::DEFAULT_SUPERUSER_NAME}).get();
                        log.info("Created default superuser role '{}'.", meta::DEFAULT_SUPERUSER_NAME);
                    } catch (const exceptions::unavailable_exception& e) {
                        log.warn("Skipped default role setup: some nodes were ready; will retry");
                        throw e;
                    }
                });
            });
        });
    });
}

future<>
standard_role_manager::create(const authenticated_user& performer, stdx::string_view role_name, const role_config& c) {
    static const sstring query = sprint(
            "INSERT INTO %s (%s, is_superuser, can_login) VALUES (?, ?, ?)",
            meta::roles_table::qualified_name(),
            meta::roles_table::role_col_name);

    return this->exists(role_name).then([this, role_name, &c](bool role_exists) {
        if (role_exists) {
            throw role_already_exists(sstring(role_name));
        }

        return _qp.process(
                query,
                consistency_for_role(role_name),
                {sstring(role_name), c.is_superuser, c.can_login},
                true).discard_result();
    });
}

future<>
standard_role_manager::alter(const authenticated_user&, stdx::string_view role_name, const role_config_update& u) {
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
        return _qp.process(
                sprint(
                        "UPDATE %s SET %s WHERE %s = ?",
                        meta::roles_table::qualified_name(),
                        build_column_assignments(u),
                        meta::roles_table::role_col_name),
                consistency_for_role(role_name),
                {sstring(role_name)}).discard_result();
    });
}

future<> standard_role_manager::drop(const authenticated_user&, stdx::string_view role_name) {
    return this->exists(role_name).then([this, role_name](bool role_exists) {
        if (!role_exists) {
            throw nonexistant_role(sstring(role_name));
        }

        // First, revoke this role from all roles that are members of it.
        const auto revoke_from_members = [this, role_name] {
            static const sstring query = sprint(
                    "SELECT member FROM %s WHERE role = ?",
                    meta::role_members_table::qualified_name());

            return _qp.process(
                    query,
                    consistency_for_role(role_name),
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
                    recursive_role_query::no).then([this, grantee](std::unordered_set<sstring> granted_roles) {
                return do_with(
                        std::move(granted_roles),
                        [this, grantee](const std::unordered_set<sstring>& granted_roles) {
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
            static const sstring query = sprint(
                    "DELETE FROM %s WHERE %s = ?",
                    meta::roles_table::qualified_name(),
                    meta::roles_table::role_col_name);

            return _qp.process(
                    query,
                    consistency_for_role(role_name),
                    {sstring(role_name)}).discard_result();
        };

        return when_all_succeed(revoke_from_members(), revoke_members_of()).then([delete_role = std::move(delete_role)] {
            return delete_role();
        });
    });
}

future<>
standard_role_manager::modify_membership(
        stdx::string_view grantee_name,
        stdx::string_view role_name,
        membership_change ch) {


    const auto modify_roles = [this, role_name, grantee_name, ch] {
        const auto query = sprint(
                "UPDATE %s SET member_of = member_of %s ? WHERE %s = ?",
                meta::roles_table::qualified_name(),
                (ch == membership_change::add ? '+' : '-'),
                meta::roles_table::role_col_name);

        return _qp.process(
                query,
                consistency_for_role(grantee_name),
                {std::unordered_set<sstring>{sstring(role_name)}, sstring(grantee_name)}).discard_result();
    };

    const auto modify_role_members = [this, role_name, grantee_name, ch] {
        switch (ch) {
            case membership_change::add:
                return _qp.process(
                        sprint(
                                "INSERT INTO %s (role, member) VALUES (?, ?)",
                                meta::role_members_table::qualified_name()),
                        consistency_for_role(role_name),
                        {sstring(role_name), sstring(grantee_name)}).discard_result();

            case membership_change::remove:
                return _qp.process(
                        sprint(
                                "DELETE FROM %s WHERE role = ? AND member = ?",
                                meta::role_members_table::qualified_name()),
                        consistency_for_role(role_name),
                        {sstring(role_name), sstring(grantee_name)}).discard_result();
        }

        return make_ready_future<>();
    };

    return when_all_succeed(modify_roles(), modify_role_members());
}

future<>
standard_role_manager::grant(
        const authenticated_user&,
        stdx::string_view grantee_name,
        stdx::string_view role_name) {
    const auto check_redundant = [this, role_name, grantee_name] {
        return this->query_granted(
                grantee_name,
                recursive_role_query::yes).then([role_name, grantee_name](std::unordered_set<sstring> roles) {
            const sstring role_name_string = sstring(role_name);

            if (roles.count(role_name_string) != 0) {
                throw role_already_included(sstring(grantee_name), role_name_string);
            }

            return make_ready_future<>();
        });
    };

    const auto check_cycle = [this, role_name, grantee_name] {
        return this->query_granted(
                role_name,
                recursive_role_query::yes).then([role_name, grantee_name](std::unordered_set<sstring> roles) {
            const auto grantee_name_string = sstring(grantee_name);

            if (roles.count(grantee_name_string) != 0) {
                throw role_already_included(sstring(role_name), grantee_name_string);
            }

            return make_ready_future<>();
        });
    };

   return when_all_succeed(check_redundant(), check_cycle()).then([this, role_name, grantee_name] {
       return this->modify_membership(grantee_name, role_name, membership_change::add);
   });
}

future<>
standard_role_manager::revoke(
        const authenticated_user&,
        stdx::string_view revokee_name,
        stdx::string_view role_name) {
    return this->exists(role_name).then([this, revokee_name, role_name](bool role_exists) {
        if (!role_exists) {
            throw nonexistant_role(sstring(role_name));
        }
    }).then([this, revokee_name, role_name] {
        return this->query_granted(
                revokee_name,
                recursive_role_query::no).then([revokee_name, role_name](std::unordered_set<sstring> roles) {
            const auto role_name_string = sstring(role_name);

            if (roles.count(role_name_string) == 0) {
                throw revoke_ungranted_role(sstring(revokee_name), role_name_string);
            }

            return make_ready_future<>();
        }).then([this, revokee_name, role_name] {
            return this->modify_membership(revokee_name, role_name, membership_change::remove);
        });
    });
}

static future<> collect_roles(
        cql3::query_processor& qp,
        stdx::string_view grantee_name,
        bool recurse,
        std::unordered_set<sstring>& roles) {
    return require_record(qp, grantee_name).then([&qp, &roles, recurse](record r) {
        return do_for_each(r.member_of, [&qp, &roles, recurse](const sstring& role_name) {
            roles.insert(role_name);

            if (recurse) {
                return collect_roles(qp, role_name, true, roles);
            }

            return make_ready_future<>();
        });
    });
}

future<std::unordered_set<sstring>>
standard_role_manager::query_granted(stdx::string_view grantee_name, recursive_role_query m) const {
    const bool recurse = (m == recursive_role_query::yes);

    return do_with(
            std::unordered_set<sstring>{sstring(grantee_name)},
            [this, grantee_name, recurse](std::unordered_set<sstring>& roles) {
        return collect_roles(_qp, grantee_name, recurse, roles).then([&roles] { return roles; });
    });
}

future<std::unordered_set<sstring>> standard_role_manager::query_all() const {
    static const sstring query = sprint(
            "SELECT %s FROM %s",
            meta::roles_table::role_col_name,
            meta::roles_table::qualified_name());

    // To avoid many copies of a view.
    static const auto role_col_name_string = sstring(meta::roles_table::role_col_name);

    return _qp.process(query, db::consistency_level::QUORUM).then([](::shared_ptr<cql3::untyped_result_set> results) {
        std::unordered_set<sstring> roles;

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

future<bool> standard_role_manager::exists(stdx::string_view role_name) const  {
    return find_record(_qp, role_name).then([](stdx::optional<record> mr) {
        return static_cast<bool>(mr);
    });
}

future<bool> standard_role_manager::is_superuser(stdx::string_view role_name) const {
    return require_record(_qp, role_name).then([](record r) {
        return r.is_superuser;
    });
}

future<bool> standard_role_manager::can_login(stdx::string_view role_name) const {
    return require_record(_qp, role_name).then([](record r) {
        return r.can_login;
    });
}

}
