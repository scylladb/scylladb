/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#pragma once

#include <ranges>
#include <concepts>
#include <iterator>

#ifdef __cpp_lib_ranges_concat
#warning "use std::views::concat instead"
#endif
/**
 * @brief A view that concatenates two input ranges into a single view.
 *
 * @tparam First The type of the first input range (must be a view)
 * @tparam Second The type of the second input range (must be a view)
 *
 * This view allows treating two ranges as a single contiguous range,
 * iterating through the first range followed by the second range.
 *
 * @note Requires both input ranges to have the same value type
 * @note Satisfies the requirements of a standard view
 *
 * @warning Differences from C++26 std::views::concat:
 * - This implementation is more restrictive: both ranges must be of the same value type
 * - C++26's concat supports ranges with convertible value types
 * - C++26's concat can concatenate more than two ranges (variadic)
 * - C++26's concat offers operator[] if the concatenated ranges satisfy random_access_range
 * - This implementation is a simplified, pre-standard version
 *
 * @code{.cpp}
 * std::vector<int> v1{1, 2, 3};
 * std::vector<int> v2{4, 5, 6};
 *
 * auto result = utils::views::concat(v1, v2);
 * @endcode
 */
namespace utils {

template<std::ranges::input_range First, std::ranges::input_range Second>
requires std::ranges::view<First> && std::ranges::view<Second> &&
         std::same_as<std::ranges::range_value_t<First>, std::ranges::range_value_t<Second>>
class concat_view : public std::ranges::view_interface<concat_view<First, Second>> {
private:
    First _first;
    Second _second;

    class iterator {
    private:
        using first_iterator = std::ranges::iterator_t<First>;
        using second_iterator = std::ranges::iterator_t<Second>;

        concat_view* _parent = nullptr;
        first_iterator _first_it{};
        second_iterator _second_it{};

    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::ranges::range_value_t<First>;
        using difference_type = std::common_type_t<
            std::ranges::range_difference_t<First>,
            std::ranges::range_difference_t<Second>>;
        using pointer = value_type*;
        using reference = std::ranges::range_reference_t<First>;

        iterator() = default;

        iterator(concat_view& parent,
                 first_iterator first_it,
                 second_iterator second_it)
            : _parent(&parent)
            , _first_it(first_it)
            , _second_it(second_it) {}

        reference operator*() const {
            if (_first_it == std::ranges::end(_parent->_first)) {
                return *_second_it;
            }
            return *_first_it;
        }

        iterator& operator++() {
            if (_first_it != std::ranges::end(_parent->_first)) {
                ++_first_it;
            } else {
                ++_second_it;
            }
            return *this;
        }

        iterator operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        friend bool operator==(const iterator& x, const iterator& y) {
            return (x._first_it == y._first_it &&
                    x._second_it == y._second_it);
        }
    };

public:
    concat_view() = default;
    concat_view(First first, Second second)
        : _first(std::move(first))
        , _second(std::move(second)) {}

    iterator begin() {
        return {*this, std::ranges::begin(_first), std::ranges::begin(_second)};
    }

    iterator end() {
        return {*this, std::ranges::end(_first), std::ranges::end(_second)};
    }

    template<typename V1 = First, typename V2 = Second>
    requires std::ranges::sized_range<V1> && std::ranges::sized_range<V2>
    size_t size() const {
        return std::ranges::size(_first) && std::ranges::size(_second);
    }

    bool empty() const {
        return std::ranges::empty(_first) && std::ranges::empty(_second);
    }
};

template<class First, class Second>
concat_view(First&&, Second&&) -> concat_view<std::views::all_t<First>, std::views::all_t<Second>>;

namespace detail {

struct concat_fn {
    template<std::ranges::viewable_range First, std::ranges::viewable_range Second>
    constexpr auto operator()(First&& first, Second&& second) const {
        return concat_view(std::forward<First>(first), std::forward<Second>(second));
    }
};

} // namespace detail

namespace views {
    inline constexpr detail::concat_fn concat;
} // namespace views
} // namespace utils
