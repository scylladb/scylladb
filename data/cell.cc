/*
 * Copyright (C) 2018 ScyllaDB
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

#include "data/cell.hh"

#include "types.hh"

thread_local imr::alloc::context_factory<data::cell::last_chunk_context> lcc;
thread_local imr::alloc::lsa_migrate_fn<data::cell::external_last_chunk,
        imr::alloc::context_factory<data::cell::last_chunk_context>> data::cell::lsa_last_chunk_migrate_fn(lcc);
thread_local imr::alloc::context_factory<data::cell::chunk_context> ecc;
thread_local imr::alloc::lsa_migrate_fn<data::cell::external_chunk,
        imr::alloc::context_factory<data::cell::chunk_context>> data::cell::lsa_chunk_migrate_fn(ecc);

int compare_unsigned(data::value_view lhs, data::value_view rhs) noexcept
{
    auto it1 = lhs.begin();
    auto it2 = rhs.begin();
    while (it1 != lhs.end() && it2 != rhs.end()) {
        auto r = ::compare_unsigned(*it1, *it2);
        if (r) {
            return r;
        }
        ++it1;
        ++it2;
    }
    if (it1 != lhs.end()) {
        return 1;
    } else if (it2 != rhs.end()) {
        return -1;
    }
    return 0;
}

