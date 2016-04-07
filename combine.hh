/*
 * Copyright (C) 2015 ScyllaDB
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

#pragma once

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


