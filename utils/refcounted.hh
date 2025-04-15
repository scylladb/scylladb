/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdint>
#include <cassert>

#pragma once

namespace utils {

// Aborts the program if destroyed with existing references.
class refcounted {
    mutable uint64_t _count = 0;
public:
    refcounted() = default;
    refcounted(refcounted&&) = delete;
    ~refcounted() noexcept {
        assert(_count == 0);
    }
    template <typename T>
    class ref {
        T& _target;
    public:
        ref(T& t) noexcept : _target(t) {
            static_cast<const refcounted&>(_target)._count += 1;
        }
        ~ref() noexcept {
            static_cast<const refcounted&>(_target)._count -= 1;
        }
        T* operator->() const noexcept {
            return &_target;
        }
        T& operator*() const noexcept {
            return _target;
        }
    };
};

}
