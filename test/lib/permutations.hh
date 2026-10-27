/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <vector>

namespace tests {

// Returns all distinct orderings of the given elements. Useful when a test scenario should be
// run for every way of assigning some values to some variables, without an explicit list:
//
// ```
// for (auto [a, b] : tests::permutations(std::array{1, 2})) {
//     // Runs with (a, b) = (1, 2) and with (a, b) = (2, 1).
// }
// ```
template <typename Container>
std::vector<Container> permutations(Container elements) {
    std::ranges::sort(elements);
    std::vector<Container> ret;
    do {
        ret.push_back(elements);
    } while (std::ranges::next_permutation(elements).found);
    return ret;
}

}
