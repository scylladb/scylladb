/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Test for utils::on_internal_error() behavior during stack unwinding.
// See CUSTOMER-340: on_internal_error called during stack unwinding hides the
// original exception by throwing (which triggers std::terminate).

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>
#include <stdexcept>
#include <cstdlib>
#include <cstring>
#include <sys/wait.h>
#include <unistd.h>

#include "utils/on_internal_error.hh"
#include <seastar/core/on_internal_error.hh>

// A helper RAII class whose destructor calls on_internal_error during unwinding.
struct call_on_internal_error_in_destructor {
    ~call_on_internal_error_in_destructor() noexcept(false) {
        // This is called during stack unwinding due to the original exception.
        // Before the fix, this would throw and trigger std::terminate.
        // After the fix, this should abort (not throw), logging the original exception.
        utils::on_internal_error("on_internal_error called from destructor during unwinding");
    }
};

BOOST_AUTO_TEST_CASE(test_on_internal_error_throws_when_not_unwinding) {
    // When not unwinding, on_internal_error should throw (with abort disabled)
    seastar::set_abort_on_internal_error(false);
    BOOST_REQUIRE_THROW(utils::on_internal_error("test error"), std::runtime_error);
}

BOOST_AUTO_TEST_CASE(test_on_internal_error_aborts_during_unwinding) {
    // Fork a child process to test that on_internal_error aborts during unwinding
    // instead of throwing (which would cause std::terminate with no useful info).
    //
    // The child process:
    // 1. Throws an exception ("original exception")
    // 2. During unwinding, a destructor calls on_internal_error
    // 3. The fix should cause abort (SIGABRT) instead of std::terminate
    //
    // We verify the child is killed by SIGABRT.

    seastar::set_abort_on_internal_error(false);

    pid_t pid = fork();
    BOOST_REQUIRE(pid >= 0);

    if (pid == 0) {
        // Child process
        try {
            call_on_internal_error_in_destructor guard;
            throw std::runtime_error("original exception that should be logged");
        } catch (...) {
            // Should not reach here - the destructor should abort
            _exit(0);
        }
        _exit(0);
    }

    // Parent: wait for child
    int status = 0;
    waitpid(pid, &status, 0);

    // Child should have been killed by SIGABRT (from on_fatal_internal_error)
    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_REQUIRE_EQUAL(WTERMSIG(status), SIGABRT);
}

BOOST_AUTO_TEST_CASE(test_on_internal_error_logs_original_exception_during_unwinding) {
    // Fork a child process and capture stderr to verify that the unwinding
    // situation is logged before aborting.

    seastar::set_abort_on_internal_error(false);

    int pipefd[2];
    BOOST_REQUIRE(pipe(pipefd) == 0);

    pid_t pid = fork();
    BOOST_REQUIRE(pid >= 0);

    if (pid == 0) {
        // Child: redirect stderr to pipe
        close(pipefd[0]);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        try {
            call_on_internal_error_in_destructor guard;
            throw std::runtime_error("the_original_exception_message");
        } catch (...) {
            _exit(0);
        }
        _exit(0);
    }

    // Parent: read child's stderr
    close(pipefd[1]);

    std::string output;
    char buf[4096];
    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof(buf) - 1)) > 0) {
        buf[n] = '\0';
        output += buf;
    }
    close(pipefd[0]);

    int status = 0;
    waitpid(pid, &status, 0);

    // Child should abort
    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_REQUIRE_EQUAL(WTERMSIG(status), SIGABRT);

    // The output should mention that on_internal_error was called during
    // stack unwinding, so the developer has a clear indication of what happened.
    BOOST_REQUIRE_MESSAGE(output.find("during stack unwinding") != std::string::npos, "Expected 'during stack unwinding' in output, got: " + output);
}

// A helper that calls on_internal_error from within a catch block while another
// exception is unwinding above it. In this scenario std::current_exception()
// returns the caught exception, so we can log it.
struct call_on_internal_error_in_nested_catch_destructor {
    ~call_on_internal_error_in_nested_catch_destructor() noexcept(false) {
        try {
            throw std::runtime_error("inner_triggering_exception");
        } catch (...) {
            // Now std::current_exception() returns "inner_triggering_exception"
            // and std::uncaught_exceptions() > 0 because of the outer exception.
            utils::on_internal_error("on_internal_error in nested catch");
        }
    }
};

BOOST_AUTO_TEST_CASE(test_on_internal_error_logs_caught_exception_during_unwinding) {
    // When on_internal_error is called from a catch block during unwinding,
    // std::current_exception() is available and should be logged.

    seastar::set_abort_on_internal_error(false);

    int pipefd[2];
    BOOST_REQUIRE(pipe(pipefd) == 0);

    pid_t pid = fork();
    BOOST_REQUIRE(pid >= 0);

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[1]);

        try {
            call_on_internal_error_in_nested_catch_destructor guard;
            throw std::runtime_error("outer_exception");
        } catch (...) {
            _exit(0);
        }
        _exit(0);
    }

    close(pipefd[1]);

    std::string output;
    char buf[4096];
    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof(buf) - 1)) > 0) {
        buf[n] = '\0';
        output += buf;
    }
    close(pipefd[0]);

    int status = 0;
    waitpid(pid, &status, 0);

    BOOST_REQUIRE(WIFSIGNALED(status));
    BOOST_REQUIRE_EQUAL(WTERMSIG(status), SIGABRT);

    // Should log the unwinding situation
    BOOST_REQUIRE_MESSAGE(output.find("during stack unwinding") != std::string::npos, "Expected 'during stack unwinding' in output, got: " + output);
    // Should log the currently handled exception
    BOOST_REQUIRE_MESSAGE(output.find("inner_triggering_exception") != std::string::npos, "Expected 'inner_triggering_exception' in output, got: " + output);
}
