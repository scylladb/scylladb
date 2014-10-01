/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#include <core/app-template.hh>
#include <core/shared_ptr.hh>
#include "test-utils.hh"

future<> test_finally_is_called_on_success_and_failure() {
    auto finally1 = make_shared<bool>();
    auto finally2 = make_shared<bool>();

    return make_ready_future().then([] {
        OK();
    }).finally([=] {
        *finally1 = true;
    }).then([] {
        throw std::runtime_error("");
    }).finally([=] {
        *finally2 = true;
    }).rescue([=] (auto get) {
        if (!*finally1) {
            BUG();
        }
        if (!*finally2) {
            BUG();
        }

        // Should be failed.
        try {
            get();
            BUG();
        } catch (...) {
            OK();
        }
    });
}

future<> test_exception_from_finally_fails_the_target() {
    promise<> pr;

    auto f = pr.get_future().finally([=] {
        OK();
        throw std::runtime_error("");
    }).then([] {
        BUG();
    }).rescue([] (auto get) {
        OK();
    });

    pr.set_value();
    return f;
}

future<> test_exception_from_finally_fails_the_target_on_already_resolved() {
    return make_ready_future().finally([=] {
        OK();
        throw std::runtime_error("");
    }).then([] {
        BUG();
    }).rescue([] (auto get) {
        OK();
    });
}

future<> test_exception_thrown_from_rescue_causes_future_to_fail()
{
    return make_ready_future().rescue([] (auto get) {
        throw std::runtime_error("");
    }).rescue([] (auto get) {
        try {
            get();
            BUG();
        } catch (...) {
            OK();
        }
    });
}

future<> test_exception_thrown_from_rescue_causes_future_to_fail__async_case()
{
    promise<> p;

    auto f = p.get_future().rescue([] (auto get) {
        throw std::runtime_error("");
    }).rescue([] (auto get) {
        try {
            get();
            BUG();
        } catch (...) {
            OK();
        }
    });

    p.set_value();

    return f;
}

int main(int ac, char **av)
{
    return app_template().run(ac, av, [] {
        run_tests(
            test_finally_is_called_on_success_and_failure()
            .then(test_exception_from_finally_fails_the_target)
            .then(test_exception_from_finally_fails_the_target_on_already_resolved)
            .then(test_exception_thrown_from_rescue_causes_future_to_fail)
            .then(test_exception_thrown_from_rescue_causes_future_to_fail__async_case)
        );
    });
}
