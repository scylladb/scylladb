/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/standard_role_manager.hh"

#include "test/lib/scylla_test_case.hh"

#include "service/migration_manager.hh"
#include "test/lib/cql_test_env.hh"

auto make_manager(cql_test_env& env) {
    auto stop_role_manager = [] (auth::standard_role_manager* m) {
        m->stop().get();
        std::default_delete<auth::standard_role_manager>()(m);
    };
    return std::unique_ptr<auth::standard_role_manager, decltype(stop_role_manager)>(
            new auth::standard_role_manager(env.local_qp(), env.migration_manager().local()),
            std::move(stop_role_manager));
}

SEASTAR_TEST_CASE(create_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, and verify its properties.
        //

        auth::role_config c;
        c.is_superuser = true;

        m->create("admin", c).get();
        BOOST_REQUIRE_EQUAL(m->exists("admin").get0(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("admin").get0(), false);
        BOOST_REQUIRE_EQUAL(m->is_superuser("admin").get0(), true);

        BOOST_REQUIRE_EQUAL(
                m->query_granted("admin", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"admin"});

        //
        // Creating a role that already exists is an error.
        //

        BOOST_REQUIRE_THROW(m->create("admin", c).get0(), auth::role_already_exists);
    });
}

SEASTAR_TEST_CASE(drop_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        //
        // Create a role, then drop it, then verify it's gone.
        //

        m->create("lord", auth::role_config()).get();
        m->drop("lord").get();
        BOOST_REQUIRE_EQUAL(m->exists("lord").get0(), false);

        //
        // Dropping a role revokes it from other roles and revokes other roles from it.
        //

        m->create("peasant", auth::role_config()).get0();
        m->create("lord", auth::role_config()).get0();
        m->create("king", auth::role_config()).get0();

        auth::role_config tim_config;
        tim_config.is_superuser = false;
        tim_config.can_login = true;
        m->create("tim", tim_config).get0();

        m->grant("lord", "peasant").get0();
        m->grant("king", "lord").get0();
        m->grant("tim", "lord").get0();

        m->drop("lord").get0();

        BOOST_REQUIRE_EQUAL(
                m->query_granted("tim", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"tim"});

        BOOST_REQUIRE_EQUAL(
                m->query_granted("king", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"king"});

        //
        // Dropping a role that does not exist is an error.
        //

        BOOST_REQUIRE_THROW(m->drop("emperor").get0(), auth::nonexistant_role);
    });
}

SEASTAR_TEST_CASE(grant_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        auth::role_config jsnow_config;
        jsnow_config.is_superuser = false;
        jsnow_config.can_login = true;
        m->create("jsnow", jsnow_config).get0();

        m->create("lord", auth::role_config()).get0();
        m->create("king", auth::role_config()).get0();

        //
        // All kings have the rights of lords, and 'jsnow' is a king.
        //

        m->grant("king", "lord").get0();
        m->grant("jsnow", "king").get0();

        BOOST_REQUIRE_EQUAL(
                m->query_granted("king", auth::recursive_role_query::yes).get0(),
                (std::unordered_set<sstring>{"king", "lord"}));

        BOOST_REQUIRE_EQUAL(
                m->query_granted("jsnow", auth::recursive_role_query::no).get0(),
               (std::unordered_set<sstring>{"jsnow", "king"}));

        BOOST_REQUIRE_EQUAL(
                m->query_granted("jsnow", auth::recursive_role_query::yes).get0(),
                (std::unordered_set<sstring>{"jsnow", "king", "lord"}));

        // A non-existing role cannot be granted.
        BOOST_REQUIRE_THROW(m->grant("jsnow", "doctor").get0(), auth::nonexistant_role);

        // A role cannot be granted to a non-existing role.
        BOOST_REQUIRE_THROW(m->grant("hpotter", "lord").get0(), auth::nonexistant_role);
    });
}

SEASTAR_TEST_CASE(revoke_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        auth::role_config rrat_config;
        rrat_config.is_superuser = false;
        rrat_config.can_login = true;
        m->create("rrat", rrat_config).get0();

        m->create("chef", auth::role_config()).get0();
        m->create("sous_chef", auth::role_config()).get0();

        m->grant("chef", "sous_chef").get0();
        m->grant("rrat", "chef").get0();

        m->revoke("chef", "sous_chef").get0();
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get0(),
                (std::unordered_set<sstring>{"chef", "rrat"}));

        m->revoke("rrat", "chef").get0();
        BOOST_REQUIRE_EQUAL(
                m->query_granted("rrat", auth::recursive_role_query::yes).get0(),
                std::unordered_set<sstring>{"rrat"});

        // A non-existing role cannot be revoked.
        BOOST_REQUIRE_THROW(m->revoke("rrat", "taster").get0(), auth::nonexistant_role);

        // A role cannot be revoked from a non-existing role.
        BOOST_REQUIRE_THROW(m->revoke("ccasper", "chef").get0(), auth::nonexistant_role);

        // Revoking a role not granted is an error.
        BOOST_REQUIRE_THROW(m->revoke("rrat", "sous_chef").get0(), auth::revoke_ungranted_role);
    });
}

SEASTAR_TEST_CASE(alter_role) {
    return do_with_cql_env_thread([](auto&& env) {
        auto m = make_manager(env);
        m->start().get0();

        const auto anon = auth::authenticated_user();

        auth::role_config tsmith_config;
        tsmith_config.is_superuser = true;
        tsmith_config.can_login = true;
        m->create("tsmith", tsmith_config).get0();

        auth::role_config_update u;
        u.can_login = false;

        m->alter("tsmith", u).get0();

        BOOST_REQUIRE_EQUAL(m->is_superuser("tsmith").get0(), true);
        BOOST_REQUIRE_EQUAL(m->can_login("tsmith").get0(), false);

        // Altering a non-existing role is an error.
        BOOST_REQUIRE_THROW(m->alter("hjones", u).get0(), auth::nonexistant_role);
    });
}
