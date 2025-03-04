/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <stdexcept>
#include <optional>

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>

#include "seastarx.hh"
#include "utils/phased_barrier.hh"
#include "utils/on_internal_error.hh"

namespace utils {

/// This class provides an interface for pluggable async services
template <typename T>
class pluggable {
    shared_ptr<T> _service;
    utils::phased_barrier _phaser;

public:
    class permit {
        utils::phased_barrier::operation _op;
        T* _service = nullptr;
    public:
        permit() = default;
        permit(permit&&) = default;
        permit(utils::phased_barrier::operation op, T* s) noexcept
            : _op(std::move(op))
            , _service(s)
        {}

        permit& operator=(permit&&) = default;

        operator bool() const noexcept {
            return _service != nullptr;
        }
    
        T* get() noexcept {
            return _service;
        }
    
        T* operator->() noexcept {
            return get();
        }
    
        T& operator*() noexcept {
            return *get();
        }
    };

    operator bool() const noexcept {
        return plugged();
    }

    bool plugged() const noexcept {
        return bool(_service);
    }

    permit get_permit() {
        if (auto* p = _service.get()) {
            return permit(_phaser.start(), p);
        }
        return permit();
    }

    void plug(shared_ptr<T> service) {
        if (plugged()) {
            on_internal_error("service is already plugged-in");
        }
        if (_phaser.is_closed()) {
            on_internal_error("service plugin is closed");
        }
        _service = std::move(service);
    }

    future<> unplug() {
        if (plugged()) {
            auto s = std::move(_service);
            co_await _phaser.advance_and_await();
        }
    }

    future<> close() {
        if (plugged()) {
            auto s = std::move(_service);
            co_await _phaser.close();
        }
    }
};

} // namespace utils
