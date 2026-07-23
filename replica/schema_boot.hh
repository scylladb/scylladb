/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstddef>
#include <utility>

#include <seastar/core/future-util.hh>

namespace replica::schema_boot {

inline constexpr size_t max_concurrent_keyspace_schema_partitions = 1;
inline constexpr size_t max_concurrent_keyspace_population = 2;
inline constexpr size_t max_concurrent_tables_per_keyspace = 8;
inline constexpr size_t max_concurrent_views_per_keyspace = 8;

template <typename Range, typename Func>
seastar::future<> for_each_keyspace_schema_partition(Range&& range, Func&& func) {
    return seastar::max_concurrent_for_each(
            std::forward<Range>(range),
            max_concurrent_keyspace_schema_partitions,
            std::forward<Func>(func));
}

template <typename Range, typename Func>
seastar::future<> for_each_keyspace_population(Range&& range, Func&& func) {
    return seastar::max_concurrent_for_each(
            std::forward<Range>(range),
            max_concurrent_keyspace_population,
            std::forward<Func>(func));
}

template <typename Range, typename Func>
seastar::future<> for_each_table_in_keyspace(Range&& range, Func&& func) {
    return seastar::max_concurrent_for_each(
            std::forward<Range>(range),
            max_concurrent_tables_per_keyspace,
            std::forward<Func>(func));
}

template <typename Range, typename Func>
seastar::future<> for_each_view_in_keyspace(Range&& range, Func&& func) {
    return seastar::max_concurrent_for_each(
            std::forward<Range>(range),
            max_concurrent_views_per_keyspace,
            std::forward<Func>(func));
}

} // namespace replica::schema_boot
