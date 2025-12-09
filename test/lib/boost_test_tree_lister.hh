/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <boost/test/tree/visitor.hpp>

#include <fmt/base.h>

#include <memory>

namespace internal {

using label_info = std::string;

/// Type representing a single Boost test case.
struct test_case_info {
    /// The name of the test case.
    std::string name;
    /// The labels the test was marked with.
    std::vector<label_info> labels;
};

/// Type representing a single Boost test suite within a single file.
///
/// Note that a single suite can span multiple files (as of Boost.Test 1.89.0); see:
/// https://www.boost.org/doc/libs/1_89_0/libs/test/doc/html/boost_test/tests_organization/test_tree/test_suite.html.
///
/// We turn away from that convention and list suites from different files separately.
/// However, that doesn't change the fact that it's still the same suite from the
/// perspective of Boost.Test. In particular, if a suite is marked with a label,
/// it's applied to it globally.
struct test_suite_info {
    std::string name;
    std::vector<std::unique_ptr<test_suite_info>> subsuites;
    /// The tests belonging directly to this suite.
    std::vector<test_case_info> tests;
};

struct test_file_info {
    std::vector<test_suite_info> suites;
    std::vector<test_case_info> free_tests;
};

struct test_file_forest {
    std::map<std::string, test_file_info, std::less<>> test_files;
};

} // namespace internal

using test_file_forest = internal::test_file_forest;

/// Implementation of the `boost::unit_test::test_tree_visitor` that
/// produces a similar result to running a Boost.Test executable with
/// `--list_content=HRF` or `--list_content=DOT`. This type results
/// in the JSON format of the output.
///
/// The crucial difference between this implementation and the built-in
/// HRF and DOT ones is that the result obtained by a call to `get_result()`
/// (after the traversal has finished) is going to have a different structure.
///
/// The type `boost_test_tree_lister` will treat the same suite from different
/// files as separate ones, even if they share the name. Boost.Test would treat
/// them as the same one and group the results by suites. In other words,
/// this type groups results by (in order):
///
/// 1. File
/// 2. Suite(s)
/// 3. Test cases
class boost_test_tree_lister : public boost::unit_test::test_tree_visitor {
private:
    struct impl;

private:
    std::unique_ptr<impl> _impl;

public:
    boost_test_tree_lister();
    ~boost_test_tree_lister() noexcept;

public:
    const test_file_forest& get_result() const;

private:
    virtual void visit(const boost::unit_test::test_case&) override;
    virtual bool test_suite_start(const boost::unit_test::test_suite&) override;
    virtual void test_suite_finish(const boost::unit_test::test_suite&) override;
};

template <>
struct fmt::formatter<internal::test_case_info> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const internal::test_case_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<internal::test_suite_info> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const internal::test_suite_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<internal::test_file_info> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const internal::test_file_info&, fmt::format_context& ctx) const -> decltype(ctx.out());
};

template <>
struct fmt::formatter<internal::test_file_forest> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const internal::test_file_forest&, fmt::format_context& ctx) const -> decltype(ctx.out());
};
