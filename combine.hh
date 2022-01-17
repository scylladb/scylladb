/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <algorithm>

// combine two sorted uniqued sequences into a single sorted sequence
// unique elements are copied, duplicate elements are merged with a
// binary function.
template <typename InputIterator1,
          typename InputIterator2,
          typename OutputIterator,
          typename Compare,
          typename Merge>
OutputIterator
combine(InputIterator1 begin1, InputIterator1 end1,
        InputIterator2 begin2, InputIterator2 end2,
        OutputIterator out,
        Compare compare,
        Merge merge) {
    while (begin1 != end1 && begin2 != end2) {
        auto& e1 = *begin1;
        auto& e2 = *begin2;
        if (compare(e1, e2)) {
            *out++ = e1;
            ++begin1;
        } else if (compare(e2, e1)) {
            *out++ = e2;
            ++begin2;
        } else {
            *out++ = merge(e1, e2);
            ++begin1;
            ++begin2;
        }
    }
    out = std::copy(begin1, end1, out);
    out = std::copy(begin2, end2, out);
    return out;
}


