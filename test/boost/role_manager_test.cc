/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <fmt/ranges.h>

#include "auth/standard_role_manager.hh"

#include "service/raft/raft_group0_client.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include "test/lib/cql_test_env.hh"

auto make_manager(cql_test_env& env) {
    auto stop_role_manager = [] (auth::standard_role_manager* m) {
        m->stop().get();
        std::default_delete<auth::standard_role_manager>()(m);
    };
    return std::unique_ptr<auth::standard_role_manager, decltype(stop_role_manager)>(
            new auth::standard_role_manager(env.local_qp(), env.get_raft_group0_client(), env.migration_manager().local()),
            std::move(stop_role_manager));
}

SEASTAR_TEST_CASE(create_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, and verify its properties.
        //
        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config c;
            c.is_superuser = true;
            m->create("admin", c, mc).get(); 
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
        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config c;
            c.is_superuser = true;
            BOOST_REQUIRE_THROW(m->create("admin", c, mc).get(), auth::role_already_exists);
        });
    });
}

SEASTAR_TEST_CASE(drop_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, then drop it, then verify it's gone.
        //

        do_with_mc(env, [&m] (auto& mc) {
            m->create("lord", auth::role_config(), mc).get();
        });
        do_with_mc(env, [&m] (auto& mc) {
            m->drop("lord", mc).get();
        });
        BOOST_REQUIRE_EQUAL(m->exists("lord").get(), false);

        //
        // Dropping a role revokes it from other roles and revokes other roles from it.
        //

        do_with_mc(env, [&m] (auto& mc) {
            m->create("peasant", auth::role_config(), mc).get();
            m->create("lord", auth::role_config(), mc).get();
            m->create("king", auth::role_config(), mc).get();

            auth::role_config tim_config;
            tim_config.is_superuser = false;
            tim_config.can_login = true;
            m->create("tim", tim_config, mc).get();
        });

        do_with_mc(env, [&m] (auto& mc) {
            m->grant("lord", "peasant", mc).get();
            m->grant("king", "lord", mc).get();
            m->grant("tim", "lord", mc).get();
        });

        do_with_mc(env, [&m] (auto& mc) {
            m->drop("lord", mc).get();
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
        do_with_mc(env, [&m] (auto& mc) {
            BOOST_REQUIRE_THROW(m->drop("emperor", mc).get(), auth::nonexistant_role);
        });
    });
}

SEASTAR_TEST_CASE(grant_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config jsnow_config;
            jsnow_config.is_superuser = false;
            jsnow_config.can_login = true;
            m->create("jsnow", jsnow_config, mc).get();
            m->create("lord", auth::role_config(), mc).get();
            m->create("king", auth::role_config(), mc).get();
        });

        // All kings have the rights of lords, and 'jsnow' is a king.
        do_with_mc(env, [&m] (auto& mc) {
            m->grant("king", "lord", mc).get();
            m->grant("jsnow", "king", mc).get();
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

        do_with_mc(env, [&m] (auto& mc) {
            // A non-existing role cannot be granted.
            BOOST_REQUIRE_THROW(m->grant("jsnow", "doctor", mc).get(), auth::nonexistant_role);
            // A role cannot be granted to a non-existing role.
            BOOST_REQUIRE_THROW(m->grant("hpotter", "lord", mc).get(), auth::nonexistant_role);
        });
    });
}

SEASTAR_TEST_CASE(revoke_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config rrat_config;
            rrat_config.is_superuser = false;
            rrat_config.can_login = true;
            m->create("rrat", rrat_config, mc).get();
            m->create("chef", auth::role_config(), mc).get();
            m->create("sous_chef", auth::role_config(), mc).get();
        });

        do_with_mc(env, [&m] (auto& mc) {
            m->grant("chef", "sous_chef", mc).get();
            m->grant("rrat", "chef", mc).get();
        });

        do_with_mc(env, [&m] (auto& mc) {
            m->revoke("chef", "sous_chef", mc).get();
        });
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get(),
                (std::unordered_set<sstring>{"chef", "rrat"}));

        do_with_mc(env, [&m] (auto& mc) {
            m->revoke("rrat", "chef", mc).get();
        });
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get(),
                std::unordered_set<sstring>{"rrat"});

        do_with_mc(env, [&m] (auto& mc) {
            // A non-existing role cannot be revoked.
            BOOST_REQUIRE_THROW(m->revoke("rrat", "taster", mc).get(), auth::nonexistant_role);
            // A role cannot be revoked from a non-existing role.
            BOOST_REQUIRE_THROW(m->revoke("ccasper", "chef", mc).get(), auth::nonexistant_role);
            // Revoking a role not granted is an error.
            BOOST_REQUIRE_THROW(m->revoke("rrat", "sous_chef", mc).get(), auth::revoke_ungranted_role);
        });
    });
}

SEASTAR_TEST_CASE(alter_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get();

        const auto anon = auth::authenticated_user();

        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config tsmith_config;
            tsmith_config.is_superuser = true;
            tsmith_config.can_login = true;
            m->create("tsmith", tsmith_config, mc).get();
        });

        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config_update u;
            u.can_login = false;
            m->alter("tsmith", u, mc).get();
        });

        BOOST_REQUIRE_EQUAL(m->is_superuser("tsmith").get(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("tsmith").get(), false);

        // Altering a non-existing role is an error.
        do_with_mc(env, [&m] (auto& mc) {
            auth::role_config_update u;
            u.can_login = false;
            BOOST_REQUIRE_THROW(m->alter("hjones", u, mc).get(), auth::nonexistant_role);
        });
    });
}
