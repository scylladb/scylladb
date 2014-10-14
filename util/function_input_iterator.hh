/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef UTIL_FUNCTION_INPUT_ITERATOR_HH_
#define UTIL_FUNCTION_INPUT_ITERATOR_HH_

template <typename Function, typename State>
struct function_input_iterator {
    Function _func;
    State _state;
public:
    function_input_iterator(Function func, State state)
        : _func(func), _state(state) {
    }
    function_input_iterator(const function_input_iterator&) = default;
    function_input_iterator(function_input_iterator&&) = default;
    function_input_iterator& operator=(const function_input_iterator&) = default;
    function_input_iterator& operator=(function_input_iterator&&) = default;
    auto operator*() const {
        return _func();
    }
    function_input_iterator& operator++() {
        ++_state;
        return *this;
    }
    function_input_iterator operator++(int) {
        function_input_iterator ret{*this};
        ++_state;
        return *this;
    }
    bool operator==(const function_input_iterator& x) const {
        return _state == x._state;
    }
    bool operator!=(const function_input_iterator& x) const {
        return !operator==(x);
    }
};

template <typename Function, typename State>
inline
function_input_iterator<Function, State>
make_function_input_iterator(Function func, State state) {
    return function_input_iterator<Function, State>(func, state);
}

template <typename Function, typename State>
inline
function_input_iterator<Function, State>
make_function_input_iterator(Function&& func) {
    return function_input_iterator<Function, State>(func, State{});
}

#endif /* UTIL_FUNCTION_INPUT_ITERATOR_HH_ */
