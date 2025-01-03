/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/standard_role_manager.hh"
#include "auth/ldap_role_manager.hh"
#include "auth/password_authenticator.hh"
#include "db/config.hh"

#include <fmt/format.h>
#include <fmt/ranges.h>
#include "utils/to_string.hh"
#include <seastar/testing/test_case.hh>

#include "test/lib/exception_utils.hh"
#include "test/lib/test_utils.hh"
#include "ldap_common.hh"
#include "service/migration_manager.hh"
#include "test/lib/cql_test_env.hh"

auto make_manager(cql_test_env& env) {
    auto stop_role_manager = [] (auth::standard_role_manager* m) {
        m->stop().get();
        std::default_delete<auth::standard_role_manager>()(m);
    };
    return std::unique_ptr<auth::standard_role_manager, decltype(stop_role_manager)>(
            new auth::standard_role_manager(env.local_qp(), env.get_raft_group0_client(),  env.migration_manager().local()),
            std::move(stop_role_manager));
}

SEASTAR_TEST_CASE(create_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, and verify its properties.
        //

        auth::role_config c;
        c.is_superuser = true;

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("admin", c, b).get();
        });
        BOOST_REQUIRE_EQUAL(m->exists("admin").get(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("admin").get(), false);
        BOOST_REQUIRE_EQUAL(m->is_superuser("admin").get(), true);

        BOOST_REQUIRE_EQUAL(
                m->query_granted("admin", auth::recursive_role_query::yes).get(),
                std::unordered_set<sstring>{"admin"});

        //
        // Creating a role that already exists is an error.
        //

        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->create("admin", c, b).get(), auth::role_already_exists);
        });
    });
}

SEASTAR_TEST_CASE(drop_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, then drop it, then verify it's gone.
        //

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("lord", auth::role_config(), b).get();
        });
        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->drop("lord", b).get();
        });
        BOOST_REQUIRE_EQUAL(m->exists("lord").get(), false);

        //
        // Dropping a role revokes it from other roles and revokes other roles from it.
        //

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("peasant", auth::role_config(), b).get();
            m->create("lord", auth::role_config(), b).get();
            m->create("king", auth::role_config(), b).get();
        });

        auth::role_config tim_config;
        tim_config.is_superuser = false;
        tim_config.can_login = true;
        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("tim", tim_config, b).get();
        });

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->grant("lord", "peasant", b).get();
            m->grant("king", "lord", b).get();
            m->grant("tim", "lord", b).get();
        });

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->drop("lord", b).get();
        });

        BOOST_REQUIRE_EQUAL(
                m->query_granted("tim", auth::recursive_role_query::yes).get(),
                std::unordered_set<sstring>{"tim"});

        BOOST_REQUIRE_EQUAL(
                m->query_granted("king", auth::recursive_role_query::yes).get(),
                std::unordered_set<sstring>{"king"});

        //
        // Dropping a role that does not exist is an error.
        //

        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->drop("emperor", b).get(), auth::nonexistant_role);
        });
    });
}

SEASTAR_TEST_CASE(grant_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        auth::role_config jsnow_config;
        jsnow_config.is_superuser = false;
        jsnow_config.can_login = true;
        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("jsnow", jsnow_config, b).get();

            m->create("lord", auth::role_config(), b).get();
            m->create("king", auth::role_config(), b).get();
        });

        //
        // All kings have the rights of lords, and 'jsnow' is a king.
        //

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->grant("king", "lord", b).get();
            m->grant("jsnow", "king", b).get();
        });

        BOOST_REQUIRE_EQUAL(
                m->query_granted("king", auth::recursive_role_query::yes).get(),
                (std::unordered_set<sstring>{"king", "lord"}));

        BOOST_REQUIRE_EQUAL(
                m->query_granted("jsnow", auth::recursive_role_query::no).get(),
               (std::unordered_set<sstring>{"jsnow", "king"}));

        BOOST_REQUIRE_EQUAL(
                m->query_granted("jsnow", auth::recursive_role_query::yes).get(),
                (std::unordered_set<sstring>{"jsnow", "king", "lord"}));

        // A non-existing role cannot be granted.
        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->grant("jsnow", "doctor", b).get(), auth::nonexistant_role);
        });

        // A role cannot be granted to a non-existing role.
        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->grant("hpotter", "lord", b).get(), auth::nonexistant_role);
        });
    });
}

SEASTAR_TEST_CASE(revoke_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        auth::role_config rrat_config;
        rrat_config.is_superuser = false;
        rrat_config.can_login = true;
        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("rrat", rrat_config, b).get();

            m->create("chef", auth::role_config(), b).get();
            m->create("sous_chef", auth::role_config(), b).get();
        });

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->grant("chef", "sous_chef", b).get();
            m->grant("rrat", "chef", b).get();
        });

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->revoke("chef", "sous_chef", b).get();
        });

        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get(),
                (std::unordered_set<sstring>{"chef", "rrat"}));

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->revoke("rrat", "chef", b).get();
        });
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get(),
                std::unordered_set<sstring>{"rrat"});

        // A non-existing role cannot be revoked.
        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->revoke("rrat", "taster", b).get(), auth::nonexistant_role);
        });

        // A role cannot be revoked from a non-existing role.
        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->revoke("ccasper", "chef", b).get(), auth::nonexistant_role);
        });

        // Revoking a role not granted is an error.
        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->revoke("rrat", "sous_chef", b).get(), auth::revoke_ungranted_role);
        });
    });
}

SEASTAR_TEST_CASE(alter_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        auth::role_config tsmith_config;
        tsmith_config.is_superuser = true;
        tsmith_config.can_login = true;
        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->create("tsmith", tsmith_config, b).get();
        });

        auth::role_config_update u;
        u.can_login = false;

        do_with_mc(env, [&] (::service::group0_batch& b) {
            m->alter("tsmith", u, b).get();
        });

        BOOST_REQUIRE_EQUAL(m->is_superuser("tsmith").get(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("tsmith").get(), false);

        // Altering a non-existing role is an error.
        do_with_mc(env, [&] (::service::group0_batch& b) {
            BOOST_REQUIRE_THROW(m->alter("hjones", u, b).get(), auth::nonexistant_role);
        });
    });
}

namespace {

const auto default_query_template = fmt::format(
        "ldap://localhost:{}/{}?cn?sub?(uniqueMember=uid={{USER}},ou=People,dc=example,dc=com)",
        ldap_port, base_dn);

const auto flaky_server_query_template = fmt::format(
        "ldap://localhost:{}/{}?cn?sub?(uniqueMember=uid={{USER}},ou=People,dc=example,dc=com)",
        std::stoi(ldap_port) + 2, base_dn);

auto make_ldap_manager(cql_test_env& env, sstring query_template = default_query_template) {
    auto stop_role_manager = [] (auth::ldap_role_manager* m) {
        m->stop().get();
        std::default_delete<auth::ldap_role_manager>()(m);
    };
    return std::unique_ptr<auth::ldap_role_manager, decltype(stop_role_manager)>(
            new auth::ldap_role_manager(query_template, /*target_attr=*/"cn", manager_dn, manager_password,
                                        env.local_qp(), env.get_raft_group0_client(), env.migration_manager().local()),
            std::move(stop_role_manager));
}

void create_ldap_roles(cql_test_env& env, auth::role_manager& rmgr) {
  do_with_mc(env, [&] (::service::group0_batch& b) {
    rmgr.create("jsmith", auth::role_config(), b).get();
    rmgr.create("role1", auth::role_config(), b).get();
    rmgr.create("role2", auth::role_config(), b).get();
  });
}

} // anonymous namespace

using auth::role_set;

SEASTAR_TEST_CASE(ldap_single_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        create_ldap_roles(env, *m);
        const role_set expected{"jsmith", "role1"};
        BOOST_REQUIRE_EQUAL(expected, m->query_granted("jsmith", auth::recursive_role_query::no).get());
    });
}

SEASTAR_TEST_CASE(ldap_two_roles) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        create_ldap_roles(env, *m);
        const role_set expected{"cassandra", "role1","role2"};
        BOOST_REQUIRE_EQUAL(expected, m->query_granted("cassandra", auth::recursive_role_query::no).get());
    });
}

SEASTAR_TEST_CASE(ldap_no_roles) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        create_ldap_roles(env, *m);
        BOOST_REQUIRE_EQUAL(role_set{"dontexist"},
                            m->query_granted("dontexist", auth::recursive_role_query::no).get());
    });
}

SEASTAR_TEST_CASE(ldap_wrong_role) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        create_ldap_roles(env, *m);
        BOOST_REQUIRE_EQUAL(role_set{"jdoe"}, m->query_granted("jdoe", auth::recursive_role_query::no).get());
    });
}

SEASTAR_TEST_CASE(ldap_reconnect) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, flaky_server_query_template);
        m->start().get();
        create_ldap_roles(env, *m);
        std::vector<future<role_set>> queries;
        constexpr int noq = 1000;
        queries.reserve(noq);
        for (int i = 0; i < noq; ++i) {
            queries.push_back(m->query_granted("jsmith", auth::recursive_role_query::no)
                              .handle_exception([] (std::exception_ptr) { return role_set{}; }));
        }
        when_all(queries.begin(), queries.end()).get();
    });
}

using exception_predicate::message_contains;

SEASTAR_TEST_CASE(ldap_wrong_url) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, "wrong:/UR?L");
        BOOST_REQUIRE_EXCEPTION(m->start().get(), std::runtime_error, message_contains("server address"));
    });
}

SEASTAR_TEST_CASE(ldap_wrong_server_name) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, "ldap://server.that.will.never.exist.scylladb.com");
        m->start().get();
        BOOST_REQUIRE_EXCEPTION(m->query_granted("jdoe", auth::recursive_role_query::no).get(),
                                std::runtime_error, message_contains("reconnect fail"));
    });
}

SEASTAR_TEST_CASE(ldap_wrong_port) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env, "ldap://localhost:2");
        m->start().get();
        BOOST_REQUIRE_EXCEPTION(m->query_granted("jdoe", auth::recursive_role_query::no).get(),
                                std::runtime_error, message_contains("reconnect fail"));
    });
}

SEASTAR_TEST_CASE(ldap_qualified_name) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        const sstring name(make_ldap_manager(env)->qualified_java_name());
        static const sstring suffix = "LDAPRoleManager";
        BOOST_REQUIRE_EQUAL(name.find(suffix), name.size() - suffix.size());
    });
}

SEASTAR_TEST_CASE(ldap_delegates_drop) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        create_ldap_roles(env, *m);
        BOOST_REQUIRE(m->exists("role1").get());
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->drop("role1", b).get();
        });
        BOOST_REQUIRE(!m->exists("role1").get());
    });
}

SEASTAR_TEST_CASE(ldap_delegates_query_all) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        create_ldap_roles(env, *m);
        const auto roles = m->query_all().get();
        BOOST_REQUIRE_EQUAL(1, roles.count("role1"));
        BOOST_REQUIRE_EQUAL(1, roles.count("role2"));
        BOOST_REQUIRE_EQUAL(1, roles.count("jsmith"));
    });
}

SEASTAR_TEST_CASE(ldap_delegates_config) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->create("super", auth::role_config{/*is_superuser=*/true, /*can_login=*/false}, b).get();
        });
        BOOST_REQUIRE(m->is_superuser("super").get());
        BOOST_REQUIRE(!m->can_login("super").get());
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->create("user", auth::role_config{/*is_superuser=*/false, /*can_login=*/true}, b).get();
        });
        BOOST_REQUIRE(!m->is_superuser("user").get());
        BOOST_REQUIRE(m->can_login("user").get());
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->alter("super", auth::role_config_update{/*is_superuser=*/true, /*can_login=*/true}, b).get();
        });
        BOOST_REQUIRE(m->can_login("super").get());
    });
}

SEASTAR_TEST_CASE(ldap_delegates_attributes) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->create("r", auth::role_config{}, b).get();
        });
        BOOST_REQUIRE(!m->get_attribute("r", "a").get());
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->set_attribute("r", "a", "3", b).get();
        });
        // TODO: uncomment when failure is fixed.
        //BOOST_REQUIRE_EQUAL("3", *m->get_attribute("r", "a").get());
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->remove_attribute("r", "a", b).get();
        });
        BOOST_REQUIRE(!m->get_attribute("r", "a").get());
    });
}

using exceptions::invalid_request_exception;

SEASTAR_TEST_CASE(ldap_forbids_grant) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        do_with_mc(env, [&] (service::group0_batch& b) {
            BOOST_REQUIRE_EXCEPTION(m->grant("a", "b", b).get(), invalid_request_exception,
                                message_contains("with LDAPRoleManager."));
        });
    });
}

SEASTAR_TEST_CASE(ldap_forbids_revoke) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        do_with_mc(env, [&] (service::group0_batch& b) {
            BOOST_REQUIRE_EXCEPTION(m->revoke("a", "b", b).get(), invalid_request_exception,
                                message_contains("with LDAPRoleManager."));
        });
    });
}

SEASTAR_TEST_CASE(ldap_autocreate_user) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        auto m = make_ldap_manager(env);
        m->start().get();
        bool jsmith_exists = m->exists("jsmith").get();
        // JSmith cannot be auto-created - he's not assigned any existing roles in the system.
        // He does belong to role1, but role1 was not explicitly created in Scylla yet.
        BOOST_REQUIRE(!jsmith_exists);
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->create("role1", auth::role_config(), b).get();
        });
        // JSmith is now assigned an existing role - role1 - so he can be auto-created.
        jsmith_exists = m->exists("jsmith").get();
        BOOST_REQUIRE(jsmith_exists);
        do_with_mc(env, [&] (service::group0_batch& b) {
            m->drop("jsmith", b).get();
            m->drop("role1", b).get();
        });
        // JSmith was revoked role1 (simulated by dropping it from Scylla, which is easier),
        // and his account was deleted. No auto-creation for you, Joe, move on.
        jsmith_exists = m->exists("jsmith").get();
        BOOST_REQUIRE(!jsmith_exists);
        // JDeer does not exist at all and is not assigned any roles, even in LDAP,
        // which translates to no auto-creation.
        bool jdeer_exists = m->exists("jdeer").get();
        BOOST_REQUIRE(!jdeer_exists);
    });
}

namespace {

shared_ptr<db::config> make_ldap_config() {
    auto p = make_shared<db::config>();
    p->role_manager("com.scylladb.auth.LDAPRoleManager");
    p->authenticator(sstring(auth::password_authenticator_name));
    p->ldap_url_template(default_query_template);
    p->ldap_attr_role("cn");
    p->ldap_bind_dn(manager_dn);
    p->ldap_bind_passwd(manager_password);
    return p;
}

} // anonymous namespace

SEASTAR_TEST_CASE(ldap_config) {
    return do_with_cql_env_thread([](cql_test_env& env) {
        const auto& svc = env.local_auth_service();
        BOOST_REQUIRE_EQUAL(role_set{"cassandra"}, svc.get_roles("cassandra").get());
        do_with_mc(env, [&] (service::group0_batch& b) {
            auth::create_role(svc, "role1", auth::role_config{}, auth::authentication_options{}, b).get();
        });
        const role_set expected{"cassandra", "role1"};
        BOOST_REQUIRE_EQUAL(expected, svc.get_roles("cassandra").get());
    },
        make_ldap_config());
}
