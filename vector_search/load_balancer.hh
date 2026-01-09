/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/shared_ptr.hh>
#include <vector>

namespace vector_search {

template <class T, typename RandomNumberEngine>
class load_balancer {
public:
    load_balancer(std::vector<seastar::lw_shared_ptr<T>> container, RandomNumberEngine& g)
        : _container(std::move(container))
        , _g(g) {
    }

    seastar::lw_shared_ptr<T> next() {
        if (_container.empty()) {
            return nullptr;
        }
        return pop(randomize_index());
    }

private:
    using distribution = std::uniform_int_distribution<std::size_t>;

    size_t randomize_index() {
        return _dist(_g, distribution::param_type(0, _container.size() - 1));
    }

    seastar::lw_shared_ptr<T> pop(size_t index) {
        auto ret = _container[index];
        std::swap(_container[index], _container.back());
        _container.pop_back();
        return ret;
    }

    std::vector<seastar::lw_shared_ptr<T>> _container;
    RandomNumberEngine& _g;
    distribution _dist;
};

} // namespace vector_search
