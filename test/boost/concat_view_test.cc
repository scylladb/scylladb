#define BOOST_TEST_MODULE test-ranges

#include <boost/test/unit_test.hpp>
#include <list>
#include <vector>
#include <string>
#include <ranges>

#include "utils/concat_view.hh"

BOOST_AUTO_TEST_CASE(test_empty_ranges)
{
    std::vector<int> v1{};
    std::vector<int> v2{};

    auto result = utils::views::concat(v1, v2);
    BOOST_CHECK(std::ranges::empty(result));
    // auto result_vec = std::vector<int>(result.begin(), result.end());

    // BOOST_CHECK(result_vec.empty());
}

BOOST_AUTO_TEST_CASE(test_first_empty)
{
    std::vector<int> v1{};
    std::vector<int> v2{1, 2, 3};

    auto result = utils::views::concat(std::views::all(v1), std::views::all(v2));
    auto result_vec = std::vector<int>(result.begin(), result.end());

    BOOST_CHECK_EQUAL_COLLECTIONS(result_vec.begin(), result_vec.end(),
                                  v2.begin(), v2.end());
}

BOOST_AUTO_TEST_CASE(test_second_empty)
{
    std::vector<int> v1{1, 2, 3};
    std::vector<int> v2{};

    auto result = utils::views::concat(std::views::all(v1), std::views::all(v2));
    auto result_vec = std::vector<int>(result.begin(), result.end());

    BOOST_CHECK_EQUAL_COLLECTIONS(result_vec.begin(), result_vec.end(),
                                  v1.begin(), v1.end());
}

BOOST_AUTO_TEST_CASE(test_non_empty_ranges)
{
    std::vector<int> v1{1, 2, 3};
    std::vector<int> v2{4, 5, 6};
    std::vector<int> expected{1, 2, 3, 4, 5, 6};

    auto result = utils::views::concat(std::views::all(v1), std::views::all(v2));
    auto result_vec = std::vector<int>(result.begin(), result.end());

    BOOST_CHECK_EQUAL_COLLECTIONS(result_vec.begin(), result_vec.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_with_string_view)
{
    std::string_view s1 = "Hello";
    std::string_view s2 = "World";
    std::string expected = "HelloWorld";

    auto result = utils::views::concat(s1, s2);
    auto result_str = std::string(result.begin(), result.end());

    BOOST_CHECK_EQUAL(result_str, expected);
}

BOOST_AUTO_TEST_CASE(test_with_transform)
{
    std::vector<int> v1{1, 2, 3};
    std::vector<int> v2{4, 5, 6};
    std::vector<int> expected{2, 4, 6, 8, 10, 12};

    auto result = utils::views::concat(v1, v2)
        | std::views::transform([](int x) { return x * 2; })
        | std::ranges::to<std::vector>();

    BOOST_CHECK_EQUAL_COLLECTIONS(result.begin(), result.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_iterator_operations)
{
    std::vector<int> v1{1, 2};
    std::vector<int> v2{3, 4};

    auto result = utils::views::concat(std::views::all(v1), std::views::all(v2));
    auto it = result.begin();

    BOOST_CHECK_EQUAL(*it, 1);
    ++it;
    BOOST_CHECK_EQUAL(*it, 2);
    ++it;
    BOOST_CHECK_EQUAL(*it, 3);
    ++it;
    BOOST_CHECK_EQUAL(*it, 4);
    ++it;
    BOOST_CHECK(it == result.end());
}

BOOST_AUTO_TEST_CASE(test_different_range_types_same_element)
{
    // Different range types with int elements
    std::vector<int> v1{1, 2, 3};
    std::array<int, 3> arr{4, 5, 6};

    auto result = utils::views::concat(v1, arr);
    auto result_vec = std::vector<int>(result.begin(), result.end());

    std::vector<int> expected{1, 2, 3, 4, 5, 6};
    BOOST_CHECK_EQUAL_COLLECTIONS(result_vec.begin(), result_vec.end(),
                                  expected.begin(), expected.end());
}

BOOST_AUTO_TEST_CASE(test_vector_and_list_concatenation)
{
    std::vector<std::string> v1{"Hello", "World"};
    std::list<std::string> lst{"!"};

    auto result = utils::views::concat(v1, lst);
    auto result_vec = std::vector<std::string>(result.begin(), result.end());

    std::vector<std::string> expected{"Hello", "World", "!"};
    BOOST_CHECK_EQUAL_COLLECTIONS(result_vec.begin(), result_vec.end(),
                                  expected.begin(), expected.end());
}
