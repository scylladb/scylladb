/*
 * Copyright (C) 2017 ScyllaDB
 *
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

#include <utility>
#include <functional>
#include <unordered_set>
#include <seastar/core/shared_ptr.hh>

namespace sstables {

class sstable;

};

// Customize deleter so that lw_shared_ptr can work with an incomplete sstable class
namespace seastar {

template <>
struct lw_shared_ptr_deleter<sstables::sstable> {
    static void dispose(sstables::sstable* sst);
};

}

namespace sstables {

using shared_sstable = seastar::lw_shared_ptr<sstable>;
using sstable_list = std::unordered_set<shared_sstable>;

}


