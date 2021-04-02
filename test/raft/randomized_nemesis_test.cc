/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <seastar/testing/test_case.hh>

// A direct translaction of a mathematical definition of a state machine
// (see e.g. Wikipedia) as a C++ concept. Implementations of this concept
// do not store the state, they only define the types, the transition function
// (which is a pure function), and the initial state (which is a constant).
template <typename M> concept PureStateMachine =
requires (typename M::state_t s, typename M::input_t i) {
    // The type of all possible states.
    typename M::state_t;

    // The type of all possible inputs (commands).
    typename M::input_t;

    // The type of all possible outputs.
    typename M::output_t;

    // The transition function (a pure function - no side effects). It takes a state
    // and an input, and returns the next state and the output produced
    // by applying the input to the given state.
    { M::delta(s, i) } -> std::same_as<std::pair<typename M::state_t, typename M::output_t>>;

    // The initial state, of type `state_t`.
    M::init;
    requires std::is_same_v<const typename M::state_t, decltype(M::init)>;
};

SEASTAR_TEST_CASE(dummy_test) {
    return seastar::make_ready_future<>();
}
