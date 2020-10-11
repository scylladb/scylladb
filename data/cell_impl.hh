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

#pragma once

#include "data/cell.hh"

namespace data {

template <typename T>
using value_writer = cell::value_writer<T>;

inline value_writer<empty_fragment_range> cell::variable_value::write(size_t value_size, bool force_internal) noexcept
{
    static_assert(imr::WriterAllocator<value_writer<empty_fragment_range>, structure>);
    return value_writer<empty_fragment_range>(empty_fragment_range(), value_size, force_internal);
}

template<typename FragmentRange>
inline value_writer<std::decay_t<FragmentRange>> cell::variable_value::write(FragmentRange&& value, bool force_internal) noexcept
{
    static_assert(imr::WriterAllocator<value_writer<std::decay_t<FragmentRange>>, structure>);
    return value_writer<std::decay_t<FragmentRange>>(std::forward<FragmentRange>(value), value.size_bytes(), force_internal);
}

inline auto cell::variable_value::write(bytes_view value, bool force_internal) noexcept
{
    return write(single_fragment_range(value), force_internal);
}

template<mutable_view is_mutable>
inline basic_value_view<is_mutable> cell::variable_value::do_make_view(structure::basic_view<is_mutable> view, bool external_storage)
{
    auto size = view.template get<tags::value_size>().load();
    context ctx(external_storage, size);
    return view.template get<tags::value_data>().visit(make_visitor(
            [&] (imr::pod<uint8_t*>::view ptr) {
                auto ex_ptr = static_cast<uint8_t*>(ptr.load());
                if (size > cell::effective_external_chunk_length) {
                    auto ex_ctx = chunk_context(ex_ptr);
                    auto ex_view = external_chunk::make_view(ex_ptr, ex_ctx);
                    auto next = static_cast<uint8_t*>(ex_view.get<tags::chunk_next>().load());
                    return basic_value_view<is_mutable>(ex_view.get<tags::chunk_data>(ex_ctx), size - cell::effective_external_chunk_length, next);
                } else {
                    auto ex_ctx = last_chunk_context(ex_ptr);
                    auto ex_view = external_last_chunk::make_view(ex_ptr, ex_ctx);
                    assert(ex_view.get<tags::chunk_data>(ex_ctx).size() == size);
                    return basic_value_view<is_mutable>(ex_view.get<tags::chunk_data>(ex_ctx), 0, nullptr);
                }
            },
            [] (imr::buffer<tags::data>::basic_view<is_mutable> data) {
                return basic_value_view<is_mutable>(data, 0, nullptr);
            }
    ), ctx);
}

}
