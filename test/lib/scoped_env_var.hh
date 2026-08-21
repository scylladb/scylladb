/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <cstdlib>
#include <optional>
#include <string>

// RAII helper that overrides a single environment variable and restores its
// previous state on destruction (or on release()). If the variable was not set
// before, it is unset again; otherwise the previous value is restored.
class scoped_env_var {
    std::string _name;
    std::optional<std::string> _saved;
    bool _active = false;

    void save(std::string name) {
        release();
        _name = std::move(name);
        if (const char* prev = ::getenv(_name.c_str())) {
            _saved = prev;
        } else {
            _saved.reset();
        }
        _active = true;
    }

public:
    scoped_env_var() = default;

    // Override `name` with `value`.
    scoped_env_var(const std::string& name, const std::string& value) {
        set(name, value);
    }

    scoped_env_var(scoped_env_var&& o) noexcept
            : _name(std::move(o._name)), _saved(std::move(o._saved)), _active(o._active) {
        o._active = false;
    }
    scoped_env_var& operator=(scoped_env_var&& o) noexcept {
        if (this != &o) {
            release();
            _name = std::move(o._name);
            _saved = std::move(o._saved);
            _active = o._active;
            o._active = false;
        }
        return *this;
    }
    scoped_env_var(const scoped_env_var&) = delete;
    scoped_env_var& operator=(const scoped_env_var&) = delete;

    ~scoped_env_var() { release(); }

    // Save the current value of `name` and set it to `value`.
    void set(const std::string& name, const std::string& value) {
        save(name);
        ::setenv(_name.c_str(), value.c_str(), 1);
    }

    // Save the current value of `name` and remove it from the environment.
    void unset(const std::string& name) {
        save(name);
        ::unsetenv(_name.c_str());
    }

    // Restore the variable to the state saved when it was last set/unset.
    void release() noexcept {
        if (!_active) {
            return;
        }
        if (_saved) {
            ::setenv(_name.c_str(), _saved->c_str(), 1);
        } else {
            ::unsetenv(_name.c_str());
        }
        _active = false;
    }
};
