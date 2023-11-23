/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <stdlib.h>
#include <iostream>

#include "auth/authentication_options.hh"
#include "auth/password_authenticator.hh"
#include "seastarx.hh"
#include "service/qos/qos_common.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/future-util.hh>
#include <algorithm>
#include "service/qos/service_level_controller.hh"
#include "service/qos/qos_configuration_change_subscriber.hh"
#include "auth/service.hh"
#include "utils/overloaded_functor.hh"
#include "test/lib/make_random_string.hh"
#include <seastar/core/sleep.hh>

using namespace qos;
struct before_add_op {
    sstring name;
    service_level_options slo;
    bool operator==(const before_add_op& other) const = default;
};

struct after_add_op {
    sstring name;
    service_level_options slo;
    bool operator==(const after_add_op& other) const = default;
};

struct remove_op {
    sstring name;
    bool operator==(const remove_op& other) const = default;
};

struct before_change_op {
    sstring name;
    service_level_options slo_before;
    service_level_options slo_after;
    bool operator==(const before_change_op& other) const = default;
};

struct after_change_op {
    sstring name;
    service_level_options slo_before;
    service_level_options slo_after;
    bool operator==(const after_change_op& other) const = default;
};

using service_level_op =  std::variant<before_add_op, after_add_op, remove_op, before_change_op, after_change_op>;

struct qos_configuration_change_suscriber_simple : public qos_configuration_change_subscriber {


    std::vector<service_level_op> ops;

    virtual future<> on_before_service_level_add(service_level_options slo, service_level_info sl_info) override {
        ops.push_back(before_add_op{sl_info.name, slo});
        return make_ready_future<>();
    }

    virtual future<> on_after_service_level_add(service_level_options slo, service_level_info sl_info) override {
        ops.push_back(after_add_op{sl_info.name, slo});
        return make_ready_future<>();
    }

    virtual future<> on_after_service_level_remove(service_level_info sl_info) override {
        ops.push_back(remove_op{sl_info.name});
        return make_ready_future<>();
    }

    virtual future<> on_before_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) override {
        ops.push_back(before_change_op{sl_info.name, slo_before, slo_after});
        return make_ready_future<>();
    }

    virtual future<> on_after_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) override {
        ops.push_back(after_change_op(sl_info.name, slo_before, slo_after));
        return make_ready_future<>();
    }
};

std::ostream& operator<<(std::ostream& os, const before_add_op& op) {
    return os << "Service Level: before add '" << op.name << "' with " << op.slo.workload;
}

std::ostream& operator<<(std::ostream& os, const after_add_op& op) {
    return os << "Service Level: after add '" << op.name << "' with " << op.slo.workload;
}

std::ostream& operator<<(std::ostream& os, const before_change_op& op) {
    return os << "Service Level: before change '" << op.name << "' from " << op.slo_before.workload << " to " << op.slo_after.workload;
}

std::ostream& operator<<(std::ostream& os, const after_change_op& op) {
    return os << "Service Level: after change '" << op.name << "' from " << op.slo_before.workload << " to " << op.slo_after.workload;
}

std::ostream& operator<<(std::ostream& os, const remove_op& op) {
    return os << "Service Level: remove '" << op.name << "'";
}

std::ostream& operator<<(std::ostream& os, const service_level_op& op) {
     std::visit(overloaded_functor {
            [&os] (const before_add_op& op) { os << op; },
            [&os] (const after_add_op& op) { os << op; },
            [&os] (const remove_op& op) { os << op; },
            [&os] (const before_change_op& op) { os << op; },
            [&os] (const after_change_op& op) { os << op; },
     }, op);
     return os;
}

SEASTAR_THREAD_TEST_CASE(subscriber_simple) {
    sharded<service_level_controller> sl_controller;
    sharded<auth::service> auth_service;
    sl_controller.start(std::ref(auth_service), service_level_options{}).get();
    qos_configuration_change_suscriber_simple ccss;
    sl_controller.local().register_subscriber(&ccss);
    sl_controller.local().add_service_level("sl1", service_level_options{}).get();
    sl_controller.local().add_service_level("sl2", service_level_options{}).get();
    service_level_options slo;
    slo.workload = service_level_options::workload_type::interactive;
    sl_controller.local().add_service_level("sl1", slo).get();
    sl_controller.local().remove_service_level("sl2", false).get();

    std::vector<service_level_op> expected_result = {
        before_add_op{"sl1", service_level_options{}},
        after_add_op{"sl1", service_level_options{}},
        before_add_op{"sl2", service_level_options{}},
        after_add_op{"sl2", service_level_options{}},
        before_change_op{"sl1", service_level_options{}, slo},
        after_change_op{"sl1", service_level_options{}, slo},
        remove_op{"sl2"},
    };

    sl_controller.local().unregister_subscriber(&ccss).get();
    BOOST_REQUIRE_EQUAL(ccss.ops, expected_result);
    sl_controller.stop().get();
}

static future<auth::authenticated_user>
authenticate(cql_test_env& env, std::string_view username, std::string_view password) {
    auto& c = env.local_client_state();
    auto& a = env.local_auth_service().underlying_authenticator();

    return do_with(
            auth::authenticator::credentials_map{
                    {auth::authenticator::USERNAME_KEY, sstring(username)},
                    {auth::authenticator::PASSWORD_KEY, sstring(password)}},
            [&a, &c](const auto& credentials) {
        return a.authenticate(credentials).then([&c](auth::authenticated_user u) {
            c.set_login(std::move(u));
            return c.check_user_can_login().then([&c] { 
                return c.maybe_update_per_service_level_params().then([&c] {
                    return *c.user(); 
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_client_state_params_update_on_sl_alter) {
    auto cfg = make_shared<db::config>();
    cfg->authenticator(sstring(auth::password_authenticator_name));

    return do_with_cql_env_thread([] (cql_test_env& env) {
        env.local_client_state().register_service_level_subscriber();

        sstring role_name = "user_" + make_random_numeric_string(8);
        auth::role_config config {
            .can_login = true,
        };
        auth::authentication_options opts {
            .password = "pass",
        };
        auth::create_role(env.local_auth_service(), role_name, config, opts).get();
        sstring sl_name = "sl_" + make_random_numeric_string(8);

        cquery_nofail(env, "CREATE SERVICE LEVEL " + sl_name + " WITH timeout = 10s");
        cquery_nofail(env, "ATTACH SERVICE LEVEL " + sl_name + " TO " + role_name);
        authenticate(env, role_name, "pass").get();
        BOOST_REQUIRE_EQUAL(env.local_client_state().get_timeout_config().read_timeout, std::chrono::seconds(10));
        BOOST_REQUIRE_EQUAL(env.local_client_state().get_workload_type(), qos::service_level_options::workload_type::unspecified);

        cquery_nofail(env, "ALTER SERVICE LEVEL " + sl_name + " WITH timeout = 2s AND workload_type = 'interactive'");
        BOOST_REQUIRE_EQUAL(env.local_client_state().get_timeout_config().read_timeout, std::chrono::seconds(2));
        BOOST_REQUIRE_EQUAL(env.local_client_state().get_workload_type(), qos::service_level_options::workload_type::interactive);
    }, cfg);
}
