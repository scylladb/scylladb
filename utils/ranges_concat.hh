/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <iterator>
#include <ranges>
#include <utility>

#ifdef __cpp_lib_ranges_concat
#warning "use std::views::concat instead"
#endif

namespace utils {

// A custom implementation of the missing std::views::concat in C++23.
//
// Unlike std::views::concat (which would join multiple ranges), this implementation
// is specifically designed to join exactly two ranges of the same value type.
template<std::ranges::input_range V1, std::ranges::input_range V2>
class concat_view : public std::ranges::view_interface<concat_view<V1, V2>> {
private:
    V1 _first;
    V2 _second;

public:
    concat_view(V1 first, V2 second)
    : _first(std::move(first))
    , _second(std::move(second)) {}

    class iterator {
    private:
        enum class State { First, Second, End };

        std::ranges::iterator_t<V1> _first_iter;
        std::ranges::iterator_t<V2> _second_iter;
        concat_view* _parent = nullptr;
        State _current_state = State::First;

    public:
        using iterator_category = std::forward_iterator_tag;
        using iterator_concept = std::forward_iterator_tag;
        using value_type = std::common_type_t<
            std::iter_value_t<std::ranges::iterator_t<V1>>,
            std::iter_value_t<std::ranges::iterator_t<V2>>>;
        using difference_type = std::ptrdiff_t;
        using pointer = void;

        iterator() = default;

        iterator(std::ranges::iterator_t<V1> first_begin,
                 std::ranges::iterator_t<V2> second_begin,
                 concat_view* parent,
                 bool is_end)
            : _first_iter(first_begin)
            , _second_iter(second_begin)
            , _parent(parent)
            , _current_state(is_end ? State::End : State::First)
        {
            if (!is_end && _first_iter == std::ranges::end(_parent->_first)) {
                _current_state = State::Second;
            }
        }

        decltype(auto) operator*() const {
            return _current_state == State::First
                ? *_first_iter
                : *_second_iter;
        }

        iterator& operator++() {
            if (_current_state == State::First) {
                ++_first_iter;
                if (_first_iter == std::ranges::end(_parent->_first)) {
                    _current_state = State::Second;
                }
            } else {
                ++_second_iter;
                if (_second_iter == std::ranges::end(_parent->_second)) {
                    _current_state = State::End;
                }
            }
            return *this;
        }

        iterator operator++(int) {
            auto tmp = *this;
            ++(*this);
            return tmp;
        }

        friend bool operator==(const iterator& x, const iterator& y) {
            if (x._current_state != y._current_state) {
                return false;
            }
            switch (x._current_state) {
            case State::First:
                return x._first_iter == y._first_iter;
            case State::Second:
                return x._second_iter == y._second_iter;
            case State::End:
                return true;
            }
        }

        friend bool operator==(const iterator& x, std::default_sentinel_t) {
            switch (x._current_state) {
            case State::First:
                return false;
            case State::Second:
                return x._second_iter == std::ranges::end(x._parent->_second);
            case State::End:
                return true;
            }
        }
    };

    auto begin() {
        bool is_end = std::ranges::empty(_first) && std::ranges::empty(_second);
        return iterator(
            std::ranges::begin(_first),
            std::ranges::begin(_second),
            this,
            is_end
        );
    }

    auto end() {
        return std::default_sentinel;
    }
};

namespace views {

struct concat_fn {
    template<std::ranges::input_range V1, std::ranges::input_range V2>
    constexpr auto operator() (V1&& first, V2&& second) const {
        return concat_view(std::forward<V1>(first), std::forward<V2>(second));
    }
};

inline constexpr concat_fn concat;
}

} // namespace utils
