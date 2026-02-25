/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <concepts>
#include <iterator>
#include <ranges>
#include <type_traits>

/**
 * @brief A lazy view adapter that yields only the first element from every consecutive
 *        group of equal elements in the underlying range.
 *
 * This view adapter provides similar functionality to boost::adaptors::uniqued but
 * is compatible with C++20 ranges.
 *
 * @section implementations Related Implementations
 *
 * @subsection this_impl This implementation (unique_view)
 * - Creates a non-modifying view over the source range
 * - Lazily filters consecutive duplicates
 * - Compatible with C++20 ranges (satisfies view/range concepts)
 * - Can be composed with other range adaptors
 *
 * Example:
 * @code
 * range | unique_view{} | std::views::take(n)
 * @endcode
 *
 * @subsection boost_uniqued boost::adaptors::uniqued
 * - Functionally identical to this implementation
 * - Not compatible with C++20 ranges (doesn't satisfy required concepts)
 *
 * Example:
 * @code
 * range | boost::adaptors::uniqued
 * @endcode
 *
 * @subsection ranges_unique std::ranges::unique (C++23)
 * - Eager algorithm that modifies the source range in-place
 * - Returns iterator to new end after removing duplicates
 * - Cannot be used as a view or composed with other adaptors
 *
 * Example:
 * @code
 * auto r = std::ranges::unique(range); // range is modified
 * @endcode
 *
 * @subsection std_unique std::unique (pre-C++20)
 * - Like std::ranges::unique but with iterator-based interface
 * - Modifies source range in-place
 *
 * Example:
 * @code
 * auto it = std::unique(range.begin(), range.end());
 * @endcode
 *
 * @subsection boost_range_unique boost::range::unique
 * - Range-based wrapper around std::unique
 * - Modifies source range in-place
 *
 * @section future Future Standardization
 * std::views::unique is proposed for C++26 in P2214 and will provide
 * standardized lazy unique view functionality with expected API:
 * @code
 * range | std::views::unique
 * @endcode
 *
 * @section why Why This Implementation
 * 1. std::ranges::unique/std::unique modify the source range, whereas we need
 *    a non-destructive view
 * 2. boost::adaptors::uniqued is incompatible with C++20 ranges
 * 3. std::views::unique isn't standardized yet (targeting C++26)
 *
 * @section compat API Compatibility
 * - Provides pipe operator (|) for consistency with range adaptor patterns
 * - Can be used as drop-in replacement for boost::adaptors::uniqued in most cases
 * - Satisfies C++20 view/range concepts for compatibility with std::ranges
 *
 * @section usage Usage Example
 * @code
 * std::vector<int> v{1, 1, 2, 2, 3, 3};
 * auto unique = v | unique_view{};  // yields: 1, 2, 3
 * // v is unchanged, unique is a view
 * @endcode
 */
namespace utils {

template <std::ranges::input_range V>
requires std::ranges::view<V> && std::equality_comparable<std::ranges::range_reference_t<V>>
class unique_view : public std::ranges::view_interface<unique_view<V>> {
    V _base = V();

    class iterator;
    class sentinel {
        std::ranges::sentinel_t<V> _end = std::ranges::sentinel_t<V>();

    public:
        sentinel() = default;

        constexpr explicit sentinel(unique_view* parent)
            : _end(std::ranges::end(parent->_base)) {}

        friend constexpr bool operator==(const iterator& it, const sentinel& sent) {
            return it._current == sent._end;
        }
    };

    class iterator {
        friend class sentinel;
        using base_iterator = std::ranges::iterator_t<V>;
        base_iterator _current = base_iterator();
        base_iterator _next = base_iterator();
        std::ranges::sentinel_t<V> _end = std::ranges::sentinel_t<V>();
    public:
        using iterator_concept = std::forward_iterator_tag;
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::ranges::range_value_t<V>;
        using difference_type = std::ranges::range_difference_t<V>;

        iterator() requires std::default_initializable<base_iterator> = default;

        constexpr iterator(base_iterator current, std::ranges::sentinel_t<V> end)
            : _current(current)
            , _next(current)
            , _end(end) {
            if (_current != _end) {
                _next = std::next(_current);
                skip_duplicates();
            }
        }

        constexpr const std::ranges::range_reference_t<V> operator*() const {
            return *_current;
        }

        constexpr iterator& operator++() {
            _current = _next;
            if (_current != _end) {
                _next = std::next(_current);
                skip_duplicates();
            }
            return *this;
        }

        constexpr iterator operator++(int) {
            auto tmp = *this;
            ++*this;
            return tmp;
        }

        friend constexpr bool operator==(const iterator& x, const iterator& y)
        requires std::equality_comparable<base_iterator> {
            return x._current == y._current;
        }

    private:
        constexpr void skip_duplicates() {
            while (_next != _end && *_current == *_next) {
                ++_next;
            }
        }
    };

public:
    unique_view() requires std::default_initializable<V> = default;

    constexpr explicit unique_view(V base)
        : _base(std::move(base)) {}

    constexpr auto begin() {
        return iterator{std::ranges::begin(_base), std::ranges::end(_base)};
    }

    constexpr auto end() {
        return sentinel{this};
    }
};

template<class R>
unique_view(R&&) -> unique_view<std::views::all_t<R>>;

namespace detail {
struct unique_closure : std::ranges::range_adaptor_closure<unique_closure> {
    template<std::ranges::viewable_range R>
    constexpr auto operator()(R&& r) const {
        return unique_view(std::forward<R>(r));
    }
};
}
namespace views {
inline constexpr detail::unique_closure unique{};
}

} // namespace utils
