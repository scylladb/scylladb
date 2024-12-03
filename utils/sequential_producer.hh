/*
 * Copyright (C) 2021 ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <functional>
#include <seastar/core/shared_future.hh>
#include <stdexcept>

/// Invokes a factory to produce an object, but sequentially: only one fiber at a time may be executing the
/// factory.  Any other fiber requesting the object will wait for the existing factory invocation to finish, then
/// copy the result.
///
/// TODO: Move to Seastar.
template<typename T>
class sequential_producer {
  public:
    using factory_t = std::function<seastar::future<T>()>;

  private:
    factory_t _factory;
    seastar::shared_future<T> _churning; ///< Resolves when the previous _factory call completes.

  public:
    sequential_producer(factory_t&& f) : _factory(std::move(f))
    {
        clear();
    }

    seastar::future<T> operator()() {
        if (_churning.available()) {
            _churning = _factory();
        }
        return _churning.get_future();
    }

    void clear() {
        _churning = seastar::make_exception_future<T>(
                std::logic_error("initial future used in sequential_producer"));
    }
};
