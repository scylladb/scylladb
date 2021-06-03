/*
 * Copyright (C) 2020-present ScyllaDB
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

#include <seastar/core/future.hh>
#include "seastarx.hh"

namespace cql3 {

namespace statements {

    // In certain SELECT statements, such as statements with
    // aggregates or GROUP BYs, internal paging is done with
    // different paging size than specified in query options.
    //
    // set_internal_paging_size allows to override the default
    // paging_size of this type of paging.
    future<> set_internal_paging_size(int internal_paging_size);

    // reset_internal_paging_size sets internal_paging_size
    // to default value, which was set on startup - before 
    // any calls to set_internal_paging_size.
    future<> reset_internal_paging_size();

    // Wrapper around set_internal_paging_size to set it
    // to desired value on construction and reset it to 
    // the default value on destruction. Should only be
    // used inside Seastar thread.
    struct set_internal_paging_size_guard {
        set_internal_paging_size_guard(int internal_paging_size) {
            set_internal_paging_size(internal_paging_size).get();
        }

        ~set_internal_paging_size_guard() {
            reset_internal_paging_size().get();
        }
    };
}

}
