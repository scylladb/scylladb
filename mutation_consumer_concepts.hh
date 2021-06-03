/*
 * Copyright (C) 2021-present ScyllaDB
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
#include "mutation_fragment.hh"

template<typename Consumer>
concept FlatMutationReaderConsumer =
    requires(Consumer c, mutation_fragment mf) {
        { c(std::move(mf)) } -> std::same_as<stop_iteration>;
    } || requires(Consumer c, mutation_fragment mf) {
        { c(std::move(mf)) } -> std::same_as<future<stop_iteration>>;
    };


template<typename T>
concept FlattenedConsumer =
    StreamedMutationConsumer<T> && requires(T obj, const dht::decorated_key& dk) {
        { obj.consume_new_partition(dk) };
        { obj.consume_end_of_partition() };
    };

template<typename T>
concept FlattenedConsumerFilter =
    requires(T filter, const dht::decorated_key& dk, const mutation_fragment& mf) {
        { filter(dk) } -> std::same_as<bool>;
        { filter(mf) } -> std::same_as<bool>;
        { filter.on_end_of_stream() } -> std::same_as<void>;
    };

