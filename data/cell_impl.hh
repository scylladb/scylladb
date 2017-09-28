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

template<typename FragmentRange>
class value_writer {
    FragmentRange _value;

    typename FragmentRange::const_iterator _value_it;
    typename FragmentRange::const_iterator _value_end;
    bytes_view _value_current;

    size_t _value_size;
    bool _force_internal;
private:
    // Distinguishes between cell::make_live_uninitialized() and other
    // cell::make_live*() variants.
    static constexpr bool initialize_value() {
        return !std::is_same_v<FragmentRange, empty_fragment_range>;
    }

    auto write_all_to_destination() {
        if constexpr (initialize_value()) {
            return [this] (uint8_t* out) noexcept {
                auto dst = reinterpret_cast<bytes_mutable_view::pointer>(out);
                while (_value_it != _value_end) {
                    _value_current = *_value_it++;
                    dst = std::copy_n(_value_current.data(), _value_current.size(), dst);
                }
            };
        } else {
            return [] (uint8_t*) noexcept { };
        }
    }

    auto write_to_destination(size_t n) {
        if constexpr (initialize_value()) {
            return [this, n] (uint8_t* out) mutable noexcept {
                auto dst = reinterpret_cast<bytes_mutable_view::pointer>(out);
                while (n) {
                    auto this_size = std::min(_value_current.size(), n);
                    dst = std::copy_n(_value_current.data(), this_size, dst);
                    _value_current.remove_prefix(this_size);
                    if (_value_current.empty()) {
                        ++_value_it;
                        _value_current = *_value_it;
                    }
                    n -= this_size;
                }
            };
        } else {
            return [] (uint8_t*) noexcept { };
        }
    }
public:
    value_writer(FragmentRange value, size_t value_size, bool force_internal)
        : _value(std::move(value))
        , _value_it(_value.begin())
        , _value_end(_value.end())
        , _value_current(_value.empty() ? bytes_view() : *_value_it)
        , _value_size(value_size)
        , _force_internal(force_internal)
    { }

    template<typename Serializer, typename Allocator>
    GCC6_CONCEPT(
        requires (imr::is_sizer_for_v<cell::variable_value::structure, Serializer>
                && std::is_same_v<Allocator, imr::alloc::object_allocator::sizer>)
            || (imr::is_serializer_for_v<cell::variable_value::structure, Serializer>
                && std::is_same_v<Allocator, imr::alloc::object_allocator::serializer>)
    )
    auto operator()(Serializer serializer, Allocator allocations) {
        auto after_size = serializer.serialize(_value_size);
        if (_force_internal || _value_size <= cell::maximum_internal_storage_length) {
            return after_size
                .template serialize_as<cell::tags::data>(_value_size, write_all_to_destination())
                .done();
        }

        imr::placeholder<imr::pod<uint8_t*>> next_pointer_phldr;
        auto next_pointer_position = after_size.position();
        auto cell_ser = after_size.template serialize_as<cell::tags::pointer>(next_pointer_phldr);

        auto offset = 0;
        auto migrate_fn_ptr = &cell::lsa_chunk_migrate_fn;
        while (_value_size - offset > cell::maximum_external_chunk_length) {
            imr::placeholder<imr::pod<uint8_t*>> phldr;
            auto chunk_ser = allocations.template allocate_nested<cell::external_chunk>(migrate_fn_ptr)
                    .serialize(next_pointer_position);
            next_pointer_position = chunk_ser.position();
            next_pointer_phldr.serialize(
                chunk_ser.serialize(phldr)
                        .serialize(cell::maximum_external_chunk_length,
                                   write_to_destination(cell::maximum_external_chunk_length))
                        .done()
            );
            next_pointer_phldr = phldr;
            offset += cell::maximum_external_chunk_length;
        }

        size_t left = _value_size - offset;
        auto ptr = allocations.template allocate_nested<cell::external_last_chunk>(&cell::lsa_last_chunk_migrate_fn)
                .serialize(next_pointer_position)
                .serialize(left)
                .serialize(left, write_to_destination(left))
                .done();
        next_pointer_phldr.serialize(ptr);
        return cell_ser.done();
    }
};

inline value_writer<empty_fragment_range> cell::variable_value::write(size_t value_size, bool force_internal) noexcept
{
    GCC6_CONCEPT(static_assert(imr::WriterAllocator<value_writer<empty_fragment_range>, structure>));
    return value_writer<empty_fragment_range>(empty_fragment_range(), value_size, force_internal);
}

template<typename FragmentRange>
inline value_writer<std::decay_t<FragmentRange>> cell::variable_value::write(FragmentRange&& value, bool force_internal) noexcept
{
    GCC6_CONCEPT(static_assert(imr::WriterAllocator<value_writer<std::decay_t<FragmentRange>>, structure>));
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
                if (size > maximum_external_chunk_length) {
                    auto ex_ctx = chunk_context(ex_ptr);
                    auto ex_view = external_chunk::make_view(ex_ptr, ex_ctx);
                    auto next = static_cast<uint8_t*>(ex_view.get<tags::chunk_next>().load());
                    return basic_value_view<is_mutable>(ex_view.get<tags::chunk_data>(ex_ctx), size - maximum_external_chunk_length, next);
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
