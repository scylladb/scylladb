/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include "core/app-template.hh"
#include "core/shared_ptr.hh"
#include "test-utils.hh"

SEASTAR_TEST_CASE(test_finally_is_called_on_success_and_failure) {
    auto finally1 = make_shared<bool>();
    auto finally2 = make_shared<bool>();

    return make_ready_future().then([] {
    }).finally([=] {
        *finally1 = true;
    }).then([] {
        throw std::runtime_error("");
    }).finally([=] {
        *finally2 = true;
    }).rescue([=] (auto get) {
        BOOST_REQUIRE(*finally1);
        BOOST_REQUIRE(*finally2);

        // Should be failed.
        try {
            get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_exception_from_finally_fails_the_target) {
    promise<> pr;

    auto f = pr.get_future().finally([=] {
        throw std::runtime_error("");
    }).then([] {
        BOOST_REQUIRE(false);
    }).rescue([] (auto get) {});

    pr.set_value();
    return f;
}

SEASTAR_TEST_CASE(test_exception_from_finally_fails_the_target_on_already_resolved) {
    return make_ready_future().finally([=] {
        throw std::runtime_error("");
    }).then([] {
        BOOST_REQUIRE(false);
    }).rescue([] (auto get) {});
}

SEASTAR_TEST_CASE(test_exception_thrown_from_rescue_causes_future_to_fail) {
    return make_ready_future().rescue([] (auto get) {
        throw std::runtime_error("");
    }).rescue([] (auto get) {
        try {
            get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_exception_thrown_from_rescue_causes_future_to_fail__async_case) {
    promise<> p;

    auto f = p.get_future().rescue([] (auto get) {
        throw std::runtime_error("");
    }).rescue([] (auto get) {
        try {
            get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });

    p.set_value();

    return f;
}

SEASTAR_TEST_CASE(test_failing_intermediate_promise_should_fail_the_master_future) {
    promise<> p1;
    promise<> p2;

    auto f = p1.get_future().then([f = std::move(p2.get_future())] () mutable {
        return std::move(f);
    }).then([] {
        BOOST_REQUIRE(false);
    });

    p1.set_value();
    p2.set_exception(std::runtime_error("boom"));

    return std::move(f).rescue([](auto get) {
        try {
            get();
            BOOST_REQUIRE(false);
        } catch (...) {}
    });
}

SEASTAR_TEST_CASE(test_future_forwarding__not_ready_to_unarmed) {
    promise<> p1;
    promise<> p2;

    auto f1 = p1.get_future();
    auto f2 = p2.get_future();

    f1.forward_to(std::move(p2));

    BOOST_REQUIRE(!f2.available());

    auto called = f2.then([] {});

    p1.set_value();
    return called;
}

SEASTAR_TEST_CASE(test_future_forwarding__not_ready_to_armed) {
    promise<> p1;
    promise<> p2;

    auto f1 = p1.get_future();
    auto f2 = p2.get_future();

    auto called = f2.then([] {});

    f1.forward_to(std::move(p2));

    BOOST_REQUIRE(!f2.available());

    p1.set_value();

    return called;
}

SEASTAR_TEST_CASE(test_future_forwarding__ready_to_unarmed) {
    promise<> p2;

    auto f1 = make_ready_future<>();
    auto f2 = p2.get_future();

    std::move(f1).forward_to(std::move(p2));
    BOOST_REQUIRE(f2.available());

    auto called = std::move(f2).then([] {});
    BOOST_REQUIRE(called.available());

    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_future_forwarding__ready_to_armed) {
    promise<> p2;

    auto f1 = make_ready_future<>();
    auto f2 = p2.get_future();

    auto called = std::move(f2).then([] {});

    BOOST_REQUIRE(f1.available());

    f1.forward_to(std::move(p2));
    return called;
}

static void forward_dead_unarmed_promise_with_dead_future_to(promise<>& p) {
    promise<> p2;
    p.get_future().forward_to(std::move(p2));
}

SEASTAR_TEST_CASE(test_future_forwarding__ready_to_unarmed_soon_to_be_dead) {
    promise<> p1;
    forward_dead_unarmed_promise_with_dead_future_to(p1);
    make_ready_future<>().forward_to(std::move(p1));
    return make_ready_future<>();
}
