/*
 * Copyright (C) 2023-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <functional>
#include <tuple>

namespace utils {
    template <typename T>
    constexpr auto tuple_ex_size_v = std::tuple_size_v<std::remove_reference_t<T>>;

    template<typename T>
    concept Tuple = requires(T item) {
        std::invoke([]<typename... U>(const std::tuple<U...>& t) {}, item);
    };

    // Insert a new item of type NewItem into SourceTuple to produce ResultTuple.
    // ResultTuple mirrors SourceTuple, with the inclusion of an extra element of type NewItem.
    // SourceTuple and ResultTuple can be arbitrary specialisations of std::tuple or rpc::tuple.
    template<Tuple ResultTuple, Tuple SourceTuple, typename NewItem>
    requires (tuple_ex_size_v<SourceTuple> + 1 == tuple_ex_size_v<ResultTuple>)
    static ResultTuple tuple_insert(SourceTuple&& source_tuple, NewItem&& new_item) {
        constexpr auto source_size = tuple_ex_size_v<SourceTuple>;
        constexpr auto result_size = tuple_ex_size_v<ResultTuple>;
        constexpr auto match = std::invoke([]<std::size_t... Is>(std::index_sequence<Is...>) {
            auto index = result_size;
            const auto count = (0 + ... + (std::is_same_v<std::tuple_element_t<Is, ResultTuple>, NewItem> ? (index = Is, 1) : 0));
            return std::pair(index, count);
        }, std::make_index_sequence<result_size>());
        static_assert(match.second == 1, "ResultTuple should contain exactly one NewItem");
        constexpr auto item_index = match.first;
        return std::invoke([&]<std::size_t... Left, std::size_t... Right>(std::index_sequence<Left...>, std::index_sequence<Right...>) {
            return ResultTuple {
                std::get<Left>(std::forward<SourceTuple>(source_tuple))...,
                std::forward<NewItem>(new_item),
                std::get<Right + item_index>(std::forward<SourceTuple>(source_tuple))...
            };
        }, std::make_index_sequence<item_index>(), std::make_index_sequence<source_size - item_index>());
    }
}
