/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <vector>
#include <utility>
#include "utils/assert.hh"

#pragma once

// We'd like to test edge cases in the sstable reader,
// related to the handling of inexact index answers.
// (I.e. a BTI index query may return a result off by one partition
// and the reader has to handle that).
//
// Triggering those edge cases might require a specific sequence of calls to the reader
// (e.g. advancing the reader to a range, and then advancing it to a later range which
// touches the first one)
// combined with a specific sequence of answers from the index
// (e.g. first answer being accurate and second answer being off by one).
//
// Open-coding an exhaustive list of such scenarios is impractical.
// But we can write the test in a nondeterministic form:
//
// 1. Whenever queried, let the index return *some* answer within the contract.
// 2. Advance the reader to *some* range, check output.
// 3. If possible, advance the reader to *some* range later than the first one, check output.
//
// and drive this syntax by rerunning the test scenario with
// all possible values for *some* explored.
//
// This is an API for writing such nondeterministic tests.
//
// The intended usage is:
//
// ```
// nondeterministic_choice_stack ncs;
// do {
//     switch (ncs.choose_up_to(3)) {
//         // Fork the test scenario into 3 possible paths...
//         ...
//     }
// } while (ncs.rewind());
// ```
class nondeterministic_choice_stack {
    std::vector<std::pair<int, bool>> choices;
    int next_frame = 0;
public:
    // Nondeterministically choose a non-negative integer.
    // This should be followed by mark_last_choice() if
    // the returned integer is the maximum value which should be chosen.
    //
    // Can be used if the number of possible choices isn't known in advance.
    // For most uses, choose_up_to(n) is powerful enough, and is easier to use.
    int choose() {
        if (next_frame >= int(choices.size())) {
            choices.emplace_back(0, false);
        } else if (next_frame + 1 == int(choices.size())) {
            choices.back().first += 1;
        }
        return choices[next_frame++].first;
    }
    // Mark the last `choose()` fork as fully explored.
    void mark_last_choice() {
        SCYLLA_ASSERT(next_frame > 0);
        if (next_frame != int(choices.size())) {
            SCYLLA_ASSERT(choices[next_frame - 1].second);
        }
        choices[next_frame - 1].second = true;
    }
    // Nondeterministically choose an integer in range [0, n]
    int choose_up_to(int n) {
        int result = choose();
        if (result >= n) {
            mark_last_choice();
        }
        return result;
    }
    // Nondeterministically choose a boolean.
    bool choose_bool() {
        return choose_up_to(1);
    }
    // Proceed to the next iteration of the test.
    bool rewind() {
        while (!choices.empty() && choices.back().second) {
            choices.pop_back();
        }
        next_frame = 0;
        return choices.size() > 0;
    }
};
