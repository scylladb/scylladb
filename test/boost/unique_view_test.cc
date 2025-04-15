#define BOOST_TEST_MODULE test-ranges

#include <boost/test/unit_test.hpp>
#include <vector>
#include <list>
#include <string>
#include <ranges>

#include "utils/unique_view.hh"


BOOST_AUTO_TEST_CASE(test_empty_range) {
    std::vector<int> empty;
    auto view = empty | utils::views::unique;

    BOOST_CHECK(std::ranges::empty(view));
}

BOOST_AUTO_TEST_CASE(test_single_element) {
    std::vector<int> single{42};
    auto view = single | utils::views::unique;

    BOOST_CHECK_EQUAL(std::ranges::distance(view), 1);
    BOOST_CHECK_EQUAL(*view.begin(), 42);
}

BOOST_AUTO_TEST_CASE(test_all_same_elements) {
    std::vector<int> same{1, 1, 1, 1, 1};
    auto view = same | utils::views::unique;

    BOOST_CHECK_EQUAL(std::ranges::distance(view), 1);
    BOOST_CHECK_EQUAL(*view.begin(), 1);
}

BOOST_AUTO_TEST_CASE(test_all_different_elements) {
    std::vector<int> different{1, 2, 3, 4, 5};
    auto result = different | utils::views::unique | std::ranges::to<std::vector>();

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  different.begin(), different.end());
}

BOOST_AUTO_TEST_CASE(test_consecutive_duplicates) {
    std::vector<int> input{1, 1, 2, 2, 3, 3, 2, 2, 1, 1};
    auto result = input | utils::views::unique | std::ranges::to<std::vector>();

    std::vector<int> expected{1, 2, 3, 2, 1};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_string_elements) {
    std::vector<std::string> input{"hello", "hello", "world", "world", "hello"};
    auto result = input | utils::views::unique | std::ranges::to<std::vector>();

    std::vector<std::string> expected{"hello", "world", "hello"};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_different_container_type) {
    std::list<int> input{1, 1, 2, 2, 3, 3};
    auto result = input | utils::views::unique | std::ranges::to<std::vector>();

    std::vector<int> expected{1, 2, 3};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_const_range) {
    const std::vector<int> input{1, 1, 2, 2, 3};
    auto result = input | utils::views::unique | std::ranges::to<std::vector>();

    std::vector<int> expected{1, 2, 3};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_non_common_range) {
        auto result = std::views::iota(1)
        | utils::views::unique
        | std::views::take(3)
        | std::ranges::to<std::vector>();

    std::vector<int> expected{1, 2, 3};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_composition_with_other_views) {
    std::vector<int> input{1, 1, 2, 2, 3, 3, 4, 4, 5, 5};
    auto result = input
        | utils::views::unique
        | std::views::take(3)
        | std::ranges::to<std::vector>();

    std::vector<int> expected{1, 2, 3};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_function_call_syntax) {
    std::vector<int> input{1, 1, 2, 2, 3};
    auto result = utils::views::unique(input) | std::ranges::to<std::vector>();

    std::vector<int> expected{1, 2, 3};

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}
