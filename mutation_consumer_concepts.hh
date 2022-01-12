/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once
#include "mutation_fragment_v2.hh"

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

template<typename Consumer>
concept FlatMutationReaderConsumerV2 =
    requires(Consumer c, mutation_fragment_v2 mf) {
        { c(std::move(mf)) } -> std::same_as<stop_iteration>;
    } || requires(Consumer c, mutation_fragment_v2 mf) {
        { c(std::move(mf)) } -> std::same_as<future<stop_iteration>>;
    };

template<typename T>
concept FlattenedConsumerV2 =
    StreamedMutationConsumerV2<T> && requires(T obj, const dht::decorated_key& dk) {
        { obj.consume_new_partition(dk) };
        { obj.consume_end_of_partition() };
    };

template<typename T>
concept FlattenedConsumerFilterV2 =
    requires(T filter, const dht::decorated_key& dk, const mutation_fragment_v2& mf) {
        { filter(dk) } -> std::same_as<bool>;
        { filter(mf) } -> std::same_as<bool>;
        { filter.on_end_of_stream() } -> std::same_as<void>;
    };
