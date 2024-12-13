/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include <boost/test/unit_test.hpp>
#include <fmt/std.h>
#include <fmt/ranges.h>
#include <stdlib.h>
#include <fmt/std.h>

#include <seastar/core/future.hh>
#include "seastarx.hh"
#include "service/qos/qos_common.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/future-util.hh>
#include "service/qos/service_level_controller.hh"
#include "service/qos/qos_configuration_change_subscriber.hh"
#include "locator/token_metadata.hh"
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

    virtual future<> on_after_service_level_remove(service_level_info sl_info) override {
        ops.push_back(remove_op{sl_info.name});
        return make_ready_future<>();
    }

    virtual future<> on_before_service_level_change(service_level_options slo_before, service_level_options slo_after, service_level_info sl_info) override {
        ops.push_back(change_op{sl_info.name, slo_before, slo_after});
        return make_ready_future<>();
    }

    virtual future<> on_effective_service_levels_cache_reloaded() override {
        return make_ready_future<>();
    }
};

template <> struct fmt::formatter<add_op> : fmt::formatter<string_view> {
    auto format(const add_op& op, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "Service Level: added '{}' with {}",
                              op.name, op.slo.workload);
    }
};

template <> struct fmt::formatter<change_op> : fmt::formatter<string_view> {
    auto format(const change_op& op, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "Service Level: changed '{}' from {} to {}",
                              op.name, op.slo_before.workload, op.slo_after.workload);
    }
};

template <> struct fmt::formatter<remove_op> : fmt::formatter<string_view> {
    auto format(const remove_op& op, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "Service Level: removed '{}'", op.name);
    }
};

template <> struct fmt::formatter<service_level_op> : fmt::formatter<string_view> {
    auto format(const service_level_op& op, fmt::format_context& ctx) const {
        return std::visit(overloaded_functor {
            [&ctx] (const auto& op) { return fmt::format_to(ctx.out(), "{}", op); }
     }, op);
    }
};

SEASTAR_THREAD_TEST_CASE(subscriber_simple) {
    sharded<service_level_controller> sl_controller;
    sharded<auth::service> auth_service;
    service_level_options sl_options;
    sl_options.shares.emplace<int32_t>(1000);
    scheduling_group default_scheduling_group = create_scheduling_group("sl_default_sg", 1.0).get();
    locator::shared_token_metadata tm({}, {locator::topology::config{ .local_dc_rack = locator::endpoint_dc_rack::default_location }});
    sharded<abort_source> as;
    as.start().get();
    auto stop_as = defer([&as] { as.stop().get(); });
    sl_controller.start(std::ref(auth_service), std::ref(tm), std::ref(as), sl_options, default_scheduling_group).get();
    qos_configuration_change_suscriber_simple ccss;
    sl_controller.local().register_subscriber(&ccss);
    sl_controller.local().add_service_level("sl1", sl_options).get();
    sl_controller.local().add_service_level("sl2", sl_options).get();
    sl_controller.local().add_service_level("sl3", service_level_options{}).get();
    service_level_options slo;
    slo.shares.emplace<int32_t>(500);
    slo.workload = service_level_options::workload_type::interactive;
    sl_controller.local().add_service_level("sl1", slo).get();
    sl_controller.local().remove_service_level("sl2", false).get();

    std::vector<service_level_op> expected_result = {
        add_op{"sl1", sl_options},
        add_op{"sl2", sl_options},
        add_op{"sl3", service_level_options{}},
        change_op{"sl1", sl_options, slo},
        remove_op{"sl2"},
    };

    sl_controller.local().unregister_subscriber(&ccss).get();
    BOOST_REQUIRE_EQUAL(ccss.ops, expected_result);
    as.invoke_on_all([] (auto& as) { as.request_abort(); }).get();
    sl_controller.stop().get();
}

SEASTAR_THREAD_TEST_CASE(too_many_service_levels) {
    class data_accessor : public service_level_controller::service_level_distributed_data_accessor {
    public:
        mutable service_levels_info configuration;
        future<qos::service_levels_info> get_service_levels(qos::query_context) const override {
            return make_ready_future<service_levels_info>(configuration);
        }
        future<qos::service_levels_info> get_service_level(sstring service_level_name) const override {
            service_levels_info ret;
            if (configuration.contains(service_level_name)) {
                ret[service_level_name] = configuration[service_level_name];
            }
            return make_ready_future<service_levels_info>(ret);
        }
        future<> set_service_level(sstring service_level_name, qos::service_level_options slo, service::group0_batch&) const override {
            configuration[service_level_name] = slo;
            return make_ready_future<>();
        }
        future<> drop_service_level(sstring service_level_name, service::group0_batch&) const override {
            if (configuration.contains(service_level_name)) {
                configuration.erase(service_level_name);
            }
            return make_ready_future<>();
        }    
        virtual bool is_v2() const override {
            return true;
        }
        virtual ::shared_ptr<service_level_distributed_data_accessor> upgrade_to_v2(cql3::query_processor& qp, service::raft_group0_client& group0_client) const override {
            return make_shared<data_accessor>();
        }
        virtual future<> commit_mutations(service::group0_batch&& mc, abort_source& as) const override {
            return make_ready_future<>();
        }

    };
    
    shared_ptr<data_accessor> test_accessor = make_shared<data_accessor>();
    sharded<service_level_controller> sl_controller;
    sharded<auth::service> auth_service;
    service_level_options sl_options;
    sl_options.shares.emplace<int32_t>(1000);
    sl_options.workload = service_level_options::workload_type::interactive;
    scheduling_group default_scheduling_group = create_scheduling_group("sl_default_sg1", 1.0).get();
    locator::shared_token_metadata tm({}, {locator::topology::config{ .local_dc_rack = locator::endpoint_dc_rack::default_location }});
    sharded<abort_source> as;
    as.start().get();
    auto stop_as = defer([&as] { as.stop().get(); });
    sl_controller.start(std::ref(auth_service), std::ref(tm), std::ref(as), sl_options, default_scheduling_group, true).get();
    sl_controller.local().set_distributed_data_accessor(test_accessor);
    int service_level_id = 0;
    unsigned service_level_count = 0;
    std::vector<sstring> expected_service_levels;
    while (service_level_count <= max_scheduling_groups()) {
        try {
            sstring sl_name = format("sl{:020}",service_level_id);
            sl_controller.local().add_service_level(sl_name, sl_options).get();
            test_accessor->configuration[sl_name] = sl_options;
            expected_service_levels.emplace_back(sl_name);
            // create the service levels with gaps, this will allow to later "push" another service
            // level between two others if odd id numbers are used.
            service_level_id+=2;
            service_level_count++;
        } catch (std::runtime_error) {
            break;
        }
    }
    // If we have failed to create at least 2 service levels the test can pass but it will
    // not really test anything. We know that there are a lot more available scheduling groups
    // than only two.
    BOOST_REQUIRE(service_level_count >= 2);
    // make sure the service levels we believe to be active really have been created.
    sl_controller.local().update_service_levels_cache().get();
    for (auto&& sl : expected_service_levels) {
        BOOST_REQUIRE(sl_controller.local().has_service_level(sl));
    }
    // Squize a service level betwin  id 0 and id 2 - only to the configuration since 
    // we know that a creation of another service level will fail.
    test_accessor->configuration[format("sl{:020}",1)] = sl_options;
    
    // do a config poll round
    // we expect a failure to apply the configuration since it contains more service levels
    // than available scheduling groups.
    try {
        sl_controller.local().update_service_levels_cache().get();
    } catch (std::runtime_error) {
    }
    expected_service_levels.clear();
    // Record the state of service levels after a configuration round (with a bad configuration).
    for (auto&& sl : test_accessor->configuration) {
        const auto& [sl_name, slo] = sl;
        if (sl_controller.local().has_service_level(sl_name)) {
            expected_service_levels.emplace_back(sl_name);
        }
    }
    sl_controller.stop().get();
    // Simulate a rebooted node which haven't "witnesed" the configuration change and only knows
    // the current configuration.
    sharded<service_level_controller> new_sl_controller;
    default_scheduling_group = create_scheduling_group("sl_default_sg2", 1.0).get();
    new_sl_controller.start(std::ref(auth_service), std::ref(tm), std::ref(as), sl_options, default_scheduling_group, true).get();
    new_sl_controller.local().set_distributed_data_accessor(test_accessor);
    try {
        new_sl_controller.local().update_service_levels_cache().get();
    } catch (std::runtime_error) {
    }
    // Finally, make sure that this rebooted node have the same service levels as the node
    // that did "witness" the configuration change.
    for (auto&& sl : expected_service_levels) {
        BOOST_REQUIRE(new_sl_controller.local().has_service_level(sl));
    }
    new_sl_controller.stop().get();
}

SEASTAR_THREAD_TEST_CASE(add_remove_bad_sequence) {
    sharded<service_level_controller> sl_controller;
    sharded<auth::service> auth_service;
    service_level_options sl_options;
    sl_options.shares.emplace<int32_t>(1000);
    scheduling_group default_scheduling_group = create_scheduling_group("sl_default_sg3", 1.0).get();
    locator::shared_token_metadata tm({}, {locator::topology::config{ .local_dc_rack = locator::endpoint_dc_rack::default_location }});
    sharded<abort_source> as;
    as.start().get();
    auto stop_as = defer([&as] { as.stop().get(); });
    sl_controller.start(std::ref(auth_service), std::ref(tm), std::ref(as), sl_options, default_scheduling_group, true).get();
    service_level_options slo;
    slo.shares.emplace<int32_t>(500);
    slo.workload = service_level_options::workload_type::interactive;
    sl_controller.local().add_service_level("a", slo).get();
    sl_controller.local().add_service_level("b", slo).get();
    sl_controller.local().remove_service_level("b", false).get();
    sl_controller.local().remove_service_level("a", false).get();
    sl_controller.local().add_service_level("a", slo).get();
    sl_controller.local().remove_service_level("a", false).get();
    sl_controller.stop().get();
}

SEASTAR_THREAD_TEST_CASE(verify_unset_shares_in_cache_when_service_level_created_without_shares) {
    using std::literals::chrono_literals::operator""ms;

    sharded<service_level_controller> sl_controller;
    sharded<auth::service> auth_service;

    service_level_options sl_options;
    sl_options.shares.emplace<int32_t>(1000);
    scheduling_group default_scheduling_group = create_scheduling_group("sl_default_sg", 1.0).get();
    locator::shared_token_metadata tm({}, {locator::topology::config{ .local_dc_rack = locator::endpoint_dc_rack::default_location }});
    sharded<abort_source> as;

    as.start().get();
    auto stop_as = defer([&as] { as.stop().get(); });
    sl_controller.start(std::ref(auth_service), std::ref(tm), std::ref(as), sl_options, default_scheduling_group).get();

    using timeout_duration = typename seastar::lowres_clock::duration;
    using workload_type = typename service_level_options::workload_type;

    std::pair<sstring, service_level_options> configs[] = {
        {"sl_all_default", service_level_options{}},
        {"sl_timeout_set", service_level_options{.timeout = timeout_duration(10ms)}},
        {"sl_workload_set", service_level_options{.workload = workload_type::batch}},
        {"sl_shares_set", service_level_options {.shares = 100}},
        {"sl_timeout_and_workload_set", service_level_options{.timeout = timeout_duration(100ms), .workload = workload_type::interactive}},
        {"sl_timeout_and_shares_set", service_level_options{.timeout = timeout_duration(200ms), .shares = 50}},
        {"sl_workload_and_shares_set", service_level_options{.workload = workload_type::interactive, .shares = 250}},
        {"sl_everything_set", service_level_options{.timeout = timeout_duration(50ms), .workload = workload_type::interactive, .shares = 700}}
    };

    for (const auto& [name, opts] : configs) {
        sl_controller.local().add_service_level(name, opts).get();
        const auto& sl = sl_controller.local().get_service_level(name);
        BOOST_REQUIRE_MESSAGE(opts == sl.slo, seastar::format("Comparing options of {}", name));
        sl_controller.local().remove_service_level(name, false).get();
    }

    as.invoke_on_all([] (auto& as) { as.request_abort(); }).get();
    sl_controller.stop().get();
}
