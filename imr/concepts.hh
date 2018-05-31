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

#include "imr/alloc.hh"
#include "imr/compound.hh"
#include "imr/fundamental.hh"

namespace imr {

/// Check if a type T is a sizer for Structure.
template<typename Structure, typename T>
struct is_sizer_for : std::false_type { };

template<typename Continuation, typename... Members>
struct is_sizer_for<structure<Members...>,
                    internal::structure_sizer<Continuation, Members...>>
                : std::true_type { };

template<typename Structure, typename T>
constexpr bool is_sizer_for_v = is_sizer_for<Structure, T>::value;

/// Check if a type T is a serializer for Structure.
template<typename Structure, typename T>
struct is_serializer_for : std::false_type { };

template<typename Continuation, typename... Members>
struct is_serializer_for<structure<Members...>,
                         internal::structure_serializer<Continuation, Members...>>
                    : std::true_type { };

template<typename Structure, typename T>
constexpr bool is_serializer_for_v = is_serializer_for<Structure, T>::value;

/// The default sizer for Structure.
template<typename Structure>
using default_sizer_t = decltype(Structure::get_sizer());

/// The default serializer for Structure.
template<typename Structure>
using default_serializer_t = decltype(Structure::get_serializer(nullptr));

GCC6_CONCEPT(

/// A simple writer that accepts only sizer or serializer as an argument.
template<typename Writer, typename Structure>
concept bool WriterSimple = requires(Writer writer, default_sizer_t<Structure> sizer,
                                     default_serializer_t<Structure> serializer)
{
    writer(sizer);
    writer(serializer);
};

/// A writer that accepts both sizer or serializer and a memory allocator.
template<typename Writer, typename Structure>
concept bool WriterAllocator = requires(Writer writer, default_sizer_t<Structure> sizer,
                                        default_serializer_t<Structure> serializer,
                                        imr::alloc::object_allocator::sizer alloc_sizer,
                                        imr::alloc::object_allocator::serializer alloc_serializer)
{
    writer(sizer, alloc_sizer);
    writer(serializer, alloc_serializer);
};

)

}
