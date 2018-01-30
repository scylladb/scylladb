
/*
 * Copyright 2015 ScyllaDB
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


#include "managed_bytes.hh"

thread_local managed_bytes::linearization_context managed_bytes::_linearization_context;
thread_local std::unordered_map<const blob_storage*, std::unique_ptr<bytes_view::value_type[]>> managed_bytes::_lc_state;

void
managed_bytes::linearization_context::forget(const blob_storage* p) noexcept {
    _lc_state.erase(p);
}

const bytes_view::value_type*
managed_bytes::do_linearize() const {
    auto& lc = _linearization_context;
    assert(lc._nesting);
    lc._state_ptr = &_lc_state;
    auto b = _u.ptr;
    auto i = _lc_state.find(b);
    if (i == _lc_state.end()) {
        auto data = std::unique_ptr<bytes_view::value_type[]>(new bytes_view::value_type[b->size]);
        auto e = data.get();
        while (b) {
            e = std::copy_n(b->data, b->frag_size, e);
            b = b->next;
        }
        i = _lc_state.emplace(_u.ptr, std::move(data)).first;
    }
    return i->second.get();
}

