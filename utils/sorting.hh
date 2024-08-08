    /*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <stdexcept>
#include <vector>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include "utils/stall_free.hh"

namespace utils {

template <typename Container, typename T>
concept VerticiesContainer = requires(const Container& c) {
    c.begin();
    c.end();
    c.size();
    std::same_as<typename Container::value_type, T>;
};

/**
 * Topological sort a DAG using Kahn's algorithm.
 *
 * @param vertices      Contains all vertices of the graph
 * @param adjacency_map Entry {k, v} means there is an k->v edge in the graph
 * @return a vector of topologically sorted vertices. 
        If there is an edge k->v, then vertex k will be before vertex v.
 * @throws std::runtime_error when a graph has any cycle.
 */
template<typename T, typename Compare = std::less<T>, typename Container>
requires VerticiesContainer<Container, T>
seastar::future<std::vector<T>> topological_sort(const Container& vertices, const std::multimap<T, T, Compare>& adjacency_map) {
    std::map<T, size_t, Compare> ref_count_map; // Contains counters how many edges point (reference) to a vertex
    std::vector<T> sorted;
    sorted.reserve(vertices.size());

    for (auto& edge: adjacency_map) {
        ref_count_map[edge.second]++;
    }

    // Push to result vertices which reference counter == 0
    for (auto& v: vertices) {
        if (!ref_count_map.contains(v)) {
            sorted.emplace_back(v);
        }
    }

    // Iterate over already sorted vertices, 
    // decrease counter of vertices to which sorted vertices points,
    // push to the end of `sorted` vector vertices which counter == 0.
    for (size_t i = 0; i < sorted.size(); i++) {
        auto [it_begin, it_end] = adjacency_map.equal_range(sorted[i]);
        
        while (it_begin != it_end) {
            auto d = it_begin->second;
            if (--ref_count_map[d] == 0) {
                sorted.push_back(d);
            }
            ++it_begin;
        }
        co_await seastar::coroutine::maybe_yield();
    }
    co_await utils::clear_gently(ref_count_map);

    if (sorted.size() != vertices.size()) {
        throw std::runtime_error("Detected cycle in the graph.");
    }
    co_return sorted;
}

}
