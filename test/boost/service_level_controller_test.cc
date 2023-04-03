/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/test/unit_test.hpp>
#include <stdlib.h>
#include <iostream>

#include "seastarx.hh"
#include "test/lib/scylla_test_case.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/future-util.hh>
#include <algorithm>
#include "service/qos/service_level_controller.hh"
#include "service/qos/qos_configuration_change_subscriber.hh"
#include "auth/service.hh"
#include "utils/overloaded_functor.hh"

using namespace qos;
struct add_op {
    sstring name;
    service_level_options slo;
    bool operator==(const add_op& other) const = default;
};

struct remove_op {
    sstring name;
    bool operator==(const remove_op& other) const = default;
};

struct change_op {
    sstring name;
    service_level_options slo_before;
    service_level_options slo_after;
    bool operator==(const change_op& other) const = default;
};

using service_level_op =  std::variant<add_op, remove_op, change_op>;

struct qos_configuration_change_suscriber_simple : public qos_configuration_change_subscriber {


    std::vector<service_level_op> ops;

    virtual future<> on_before_service_level_add(service_level_options slo, service_level_info sl_info) override {
        ops.push_back(add_op{sl_info.name, slo});
        return make_ready_future<>();
    }

    virtual future<> on_after_service_level_add(service_level_options slo, service_level_info sl_info) override {
        return make_ready_future<>();
    }

    virtual future<> on_after_service_level_remove(service_level_info sl_info) override {
        ops.push_back(remove_op{sl_info.name});
        return make_ready_future<>();
    }

    virtual future<> on_before_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) override {
        ops.push_back(change_op{sl_info.name, slo_before, slo_after});
        return make_ready_future<>();
    }

    virtual future<> on_after_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) override {
        return make_ready_future<>();
    }
};

std::ostream& operator<<(std::ostream& os, const add_op& op) {
    return os << "Service Level: added '" << op.name << "' with " << op.slo.workload;
}

std::ostream& operator<<(std::ostream& os, const change_op& op) {
    return os << "Service Level: changed '" << op.name << "' from " << op.slo_before.workload << " to " << op.slo_after.workload;
}

std::ostream& operator<<(std::ostream& os, const remove_op& op) {
    return os << "Service Level: removed '" << op.name << "'";
}

std::ostream& operator<<(std::ostream& os, const service_level_op& op) {
     std::visit(overloaded_functor {
            [&os] (const add_op& op) { os << op; },
            [&os] (const remove_op& op) { os << op; },
            [&os] (const change_op& op) { os << op; },
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
        add_op{"sl1", service_level_options{}},
        add_op{"sl2", service_level_options{}},
        change_op{"sl1", service_level_options{}, slo},
        remove_op{"sl2"},
    };

    sl_controller.local().unregister_subscriber(&ccss).get();
    BOOST_REQUIRE_EQUAL(ccss.ops, expected_result);
    sl_controller.stop().get();
}
