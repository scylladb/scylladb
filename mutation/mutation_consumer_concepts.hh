/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
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


template<typename T, typename ReturnType>
concept FlattenedConsumerReturning =
    requires(T obj, const dht::decorated_key& dk, tombstone tomb, static_row sr, clustering_row cr, range_tombstone rt) {
        { obj.consume_new_partition(dk) };
        { obj.consume(tomb) };
        { obj.consume(std::move(sr)) } -> std::same_as<ReturnType>;
        { obj.consume(std::move(cr)) } -> std::same_as<ReturnType>;
        { obj.consume(std::move(rt)) } -> std::same_as<ReturnType>;
        { obj.consume_end_of_partition() };
        { obj.consume_end_of_stream() };
    };

template<typename T>
concept FlattenedConsumer =
    FlattenedConsumerReturning<T, stop_iteration> || FlattenedConsumerReturning<T, future<stop_iteration>>;

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

template<typename Consumer>
concept MutationConsumer =
    requires(Consumer c, mutation m) {
        { c(std::move(m)) } -> std::same_as<stop_iteration>;
    } || requires(Consumer c, mutation m) {
        { c(std::move(m)) } -> std::same_as<future<stop_iteration>>;
    };

template<typename T, typename ReturnType>
concept FlattenedConsumerReturningV2 =
    requires(T obj, const dht::decorated_key& dk, tombstone tomb, static_row sr, clustering_row cr, range_tombstone_change rt) {
        { obj.consume_new_partition(dk) };
        { obj.consume(tomb) };
        { obj.consume(std::move(sr)) } -> std::same_as<ReturnType>;
        { obj.consume(std::move(cr)) } -> std::same_as<ReturnType>;
        { obj.consume(std::move(rt)) } -> std::same_as<ReturnType>;
        { obj.consume_end_of_partition() };
        { obj.consume_end_of_stream() };
    };

template<typename T>
concept FlattenedConsumerV2 =
FlattenedConsumerReturningV2<T, stop_iteration> || FlattenedConsumerReturningV2<T, future<stop_iteration>>;

template<typename T>
concept FlattenedConsumerFilterV2 =
    requires(T filter, const dht::decorated_key& dk, const mutation_fragment_v2& mf) {
        { filter(dk) } -> std::same_as<bool>;
        { filter(mf) } -> std::same_as<bool>;
        { filter.on_end_of_stream() } -> std::same_as<void>;
    };
