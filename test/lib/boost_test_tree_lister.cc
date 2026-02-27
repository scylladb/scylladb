/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "test/lib/boost_test_tree_lister.hh"

#include <boost/algorithm/string/replace.hpp>
#include <fmt/ranges.h>

#include <flat_set>
#include <memory>
#include <ranges>

namespace {

using label_info = internal::label_info;

using test_case_info = internal::test_case_info;
using test_suite_info = internal::test_suite_info;
using test_file_info = internal::test_file_info;

} // anonymous namespace

/// --------------------
///
/// Implementation notes
///
/// --------------------
///
/// The structure of the Boost.Test's test tree consists solely
/// of nodes representing test suites and test cases. It ignores
/// information like, for instance, the name of the file those
/// entities reside [1].
///
/// What's more, a test suite can span multiple files as long
/// as it has the same name [2].
///
/// We'd like to re-visualize the tree in a different manner:
/// have a forest, where each tree represents the internal structure
/// of a specific file. The non-leaf nodes represent test suites,
/// and the leaves -- test cases.
///
/// This type achieves that very goal (albeit in a bit ugly manner).
///
/// ---
///
/// Note that the implementation suffers from the same problems
/// Boost.Test itself does. For instance, when parametrizing tests
/// with `boost::unit_test::data`, the test will appear as a test suite,
/// while cases for each of the data instances -- as test cases.
/// There's no way to overcome that, so we're stuck with it.
///
/// -----------
///
/// Assumptions
///
/// -----------
///
/// We rely on the following assumptions:
///
/// 1. The tree traversal is performed pre-order. That's the case for
///    Boost.Test 1.89.0.
/// 2. If a test case TC belong to a test suite TS (directly or indirectly),
///    the following execution order holds:
///    i.   `test_suite_start(TC)`,
///    ii.  `visit(TC)`,
///    iii. `test_suite_finish(TC)`.
/// 3. If test suite TS1 is nested within test suite TS2, the following
///    execution order holds:
///    i.   `test_suite_start(TS1)`,
///    ii.  `test_suite_start(TS2)`,
///    iii. `test_suite_finish(TS2)`,
///    iv.  `test_suite_finish(TS1)`.
///
/// ----------
///
/// References
///
/// ----------
///
/// [1] https://www.boost.org/doc/libs/1_89_0/libs/test/doc/html/boost_test/tests_organization/test_tree.html
/// [2] https://www.boost.org/doc/libs/1_89_0/libs/test/doc/html/boost_test/tests_organization/test_tree/test_suite.html
///
/// ----------------------------
///
/// Example of high-level output
///
/// ----------------------------
///
/// Let's consider the following organization of tests.
///
/// TestFile1.cc:
/// - Suite A:
///   - Suite A1:
///     - Test A1.1 (labels: L1)
///     - Test A1.2
///   - Suite A2:
///     - Test A2.1
///   - Test A.1
/// - Suite B:
///   - Test B1
///   - Test B2 (labels: L2, L3)
/// - Test 1
///
/// TestFile2.cc:
/// - Suite A:
///   - Suite A3
///     - Test A3.1
///   - Test A.2
/// - Suite C:
///   - Test C.1
/// - Test 2
///
/// This structure will be translated into the following JSON (we're
/// omitting some details to make it cleaner and easier to read):
///
/// [
///   {
///     "file": "TestFile1.cc",
///     "content": {
///       "suites": [
///         {
///           "name": "A",
///           "suites": [
///             {
///               "name": "A1",
///               "suites": [],
///               "tests": [
///                 {
///                   "name": "Test A1.1",
///                   "labels": "L1"
///                 },
///                 {
///                   "name": "Test A1.2",
///                   "labels": ""
///                 }
///               ]
///             }
///           ],
///           "tests": [
///             {
///               "name": "Test1",
///               "labels": ""
///             }
///           ]
///         },
///         {
///           "name": "B",
///           "suites": [],
///           "tests": [
///             {
///               "name": "Test B1",
///               "labels": ""
///             },
///             {
///               "name": "Test B2",
///               "labels": "L2,L3"
///             },
///           ]
///         }
///       ],
///       "tests": [
///         {
///           "name": "Test 1",
///           "labels": ""
///         }
///       ]
///     }
///   },
///   {
///     "file": "TestFile2.cc",
///     "content": {
///       "suites": [
///         {
///           "name": "A",
///           "suites": [
///             {
///               "name": "A3",
///               "suites": [],
///               "tests": [
///                 {
///                   "name": "Test A3.1",
///                   "labels": ""
///                 }
///               ]
///             }
///           ],
///           "tests": [
///             {
///               "name": "Test A.2",
///               "labels": ""
///             }
///           ]
///         },
///         {
///           "name": "C",
///           "suites": [],
///           "tests": [
///             {
///               "name": "Test C.1",
///               "labels": ""
///             }
///           ]
///         }
///       ],
///       "tests": [
///         {
///           "name": "Test 2",
///           "labels": ""
///         }
///       ]
///     }
///   }
/// ]
///
/// Note that although Boost.Test treats Suite A in TestFile1.cc
/// and Suite A in TestFile2.cc as the SAME suite, we consider it
/// separately for each of the files it resides in.
struct boost_test_tree_lister::impl {
public:
    /// The final result we're building while traversing the test tree.
    test_file_forest file_forest;
    /// The path from the root to the current suite.
    std::vector<std::string> active_suites;

public:
    void process_test_case(const boost::unit_test::test_case& tc) {
        const std::string_view filename = {tc.p_file_name.begin(), tc.p_file_name.end()};
        test_file_info& test_file = get_file_info(filename);

        std::string test_name = tc.p_name;

        // Boost.Test duplicates the labels for some reason when we
        // traverse the tree from within a global fixture. This
        // is an ugly workaround until we find out a cleaner solution.
        auto labels = tc.p_labels.get()
                | std::ranges::to<std::flat_set<label_info>>()
                | std::ranges::to<std::vector<label_info>>();

        test_case_info test_info {.name = std::move(test_name), .labels = std::move(labels)};

        if (active_suites.empty()) {
            test_file.free_tests.push_back(std::move(test_info));
        } else {
            test_suite_info& suite_info = get_active_suite(filename);
            suite_info.tests.push_back(std::move(test_info));
        }
    }

    bool test_suite_start(const boost::unit_test::test_suite& ts) {
        // The suite is the master test suite, so let's ignore it
        // because it doesn't represent any actual test suite.
        if (ts.p_parent_id == boost::unit_test::INV_TEST_UNIT_ID) {
            assert(active_suites.empty());
            return true;
        }

        std::string suite_name = ts.p_name.value;
        add_active_suite(std::move(suite_name));

        return true;
    }

    void test_suite_finish(const boost::unit_test::test_suite& ts) {
        // The suite is the master test suite, so let's ignore it
        // because it doesn't represent any actual test suite.
        if (ts.p_parent_id == boost::unit_test::INV_TEST_UNIT_ID) {
            assert(active_suites.empty());
            return;
        }

        // If the suite doesn't have any children, that indicates one of
        // the following:
        //
        // * This suite represents an actual test suite that doesn't contain
        //   any tests.
        // * This suite corresponds to a test file that was compiled into
        //   the `test/boost/combined_tests` binary. In that case, the test
        //   file was empty, i.e. it didn't contain any suites or tests.
        //
        // In either situation, we still want to record the information that
        // the file/suite exists (e.g. to be able to tell if the file the user
        // wants to run even exists).
        if (ts.size() == 0) {
            const std::string_view filename = {ts.p_file_name.begin(), ts.p_file_name.end()};
            (void) get_active_suite(filename);
        }

        drop_active_suite();
    }

private:
    test_file_info& get_file_info(std::string_view filename) {
        auto& test_files = file_forest.test_files;

        auto it = test_files.find(filename);
        if (it == test_files.end()) {
            std::tie(it, std::ignore) = test_files.emplace(filename, std::vector<test_suite_info>{});
        }

        return it->second;
    }

    void add_active_suite(std::string suite_name) {
        active_suites.push_back(std::move(suite_name));
    }

    void drop_active_suite() {
        assert(!active_suites.empty());
        active_suites.pop_back();
    }

    test_suite_info& get_active_suite(std::string_view filename) {
        assert(!active_suites.empty());

        test_file_info& file_info = get_file_info(filename);
        test_suite_info* last = &get_root_suite(file_info, active_suites[0]);

        for (const auto& suite_name : active_suites | std::views::drop(1)) {
            last = &get_subsuite(*last, suite_name);
        }

        return *last;
    }

    test_suite_info& get_root_suite(test_file_info& file_info, std::string_view suite_name) {
        auto suite_it = std::ranges::find(file_info.suites, suite_name, &test_suite_info::name);
        if (suite_it != file_info.suites.end()) {
            return *suite_it;
        }

        test_suite_info suite_info {.name = std::string(suite_name)};
        file_info.suites.push_back(std::move(suite_info));

        return *file_info.suites.rbegin();
    }

    test_suite_info& get_subsuite(test_suite_info& parent, std::string_view suite_name) {
        auto suite_it = std::ranges::find(parent.subsuites, suite_name, [] (auto&& suite_ptr) -> std::string_view {
            return suite_ptr->name;
        });

        if (suite_it != parent.subsuites.end()) {
            return **suite_it;
        }

        auto suite = std::make_unique<test_suite_info>(std::string(suite_name));
        parent.subsuites.push_back(std::move(suite));

        return **parent.subsuites.rbegin();
    }
};

boost_test_tree_lister::boost_test_tree_lister() : _impl(std::make_unique<impl>()) {}
boost_test_tree_lister::~boost_test_tree_lister() noexcept = default;

const test_file_forest& boost_test_tree_lister::get_result() const {
    return _impl->file_forest;
}

void boost_test_tree_lister::visit(const boost::unit_test::test_case& tc) {
    return _impl->process_test_case(tc);
}

bool boost_test_tree_lister::test_suite_start(const boost::unit_test::test_suite& ts) {
    return _impl->test_suite_start(ts);
}

void boost_test_tree_lister::test_suite_finish(const boost::unit_test::test_suite& ts) {
    return _impl->test_suite_finish(ts);
}

// Replace every occurrenace of a double quotation mark (`"`) with a string `\"`.
static std::string escape_quotation_marks(std::string_view str) {
    const std::size_t double_quotation_count = std::ranges::count(str, '"');
    std::string result(str.size() + double_quotation_count, '\\');

    std::size_t offset = 0;
    for (std::size_t i = 0; i < str.size(); ++i) {
        if (str[i] == '"') {
            result[i + offset] = '\\';
            ++offset;
        }
        result[i + offset] = str[i];
    }

    return result;
}

auto fmt::formatter<internal::test_case_info>::format(
        const internal::test_case_info& test_info,
        fmt::format_context& ctx) const -> decltype(ctx.out())
{
    // Sanity check. The names of tests are expected to comprise only of alphanumeric characters.
    assert(std::ranges::count(test_info.name, '"') == 0);
    auto label_range = test_info.labels | std::views::transform(escape_quotation_marks);

    return fmt::format_to(ctx.out(), R"({{"name":"{}","labels":"{}"}})",
            test_info.name, fmt::join(label_range, ","));
}

auto fmt::formatter<internal::test_suite_info>::format(
        const internal::test_suite_info& suite_info,
        fmt::format_context& ctx) const -> decltype(ctx.out())
{
    auto actual_suite_range = suite_info.subsuites | std::views::transform([] (auto&& ptr) -> const test_suite_info& {
        return *ptr;
    });
    auto suite_range = fmt::join(actual_suite_range, ",");
    auto test_range = fmt::join(suite_info.tests, ",");
    return fmt::format_to(ctx.out(), R"({{"name":"{}","suites":[{}],"tests":[{}]}})",
            suite_info.name, std::move(suite_range), std::move(test_range));
}

auto fmt::formatter<internal::test_file_info>::format(
        const internal::test_file_info& file_info,
        fmt::format_context& ctx) const -> decltype(ctx.out())
{
    auto suite_range = fmt::join(file_info.suites, ",");
    auto test_range = fmt::join(file_info.free_tests, ",");
    return fmt::format_to(ctx.out(), R"({{"suites":[{}],"tests":[{}]}})",
            std::move(suite_range), std::move(test_range));
}

auto fmt::formatter<internal::test_file_forest>::format(
        const internal::test_file_forest& forest_info,
        fmt::format_context& ctx) const -> decltype(ctx.out())
{
    std::size_t files_left = forest_info.test_files.size();

    fmt::format_to(ctx.out(), "[");
    for (const auto& [file, content] : forest_info.test_files) {
        fmt::format_to(ctx.out(), R"({{"file":"{}","content":{}}})",
                file, content);
        if (files_left > 1) {
            fmt::format_to(ctx.out(), ",");
        }
        --files_left;

    }
    return fmt::format_to(ctx.out(), "]");
}
