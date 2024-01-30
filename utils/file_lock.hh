/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <memory>
#include <ostream>
#include <filesystem>
#include <seastar/core/sstring.hh>
#include <seastar/core/future.hh>

#include "seastarx.hh"

namespace fs = std::filesystem;

namespace utils {
    class file_lock {
    public:
        file_lock() = delete;
        file_lock(const file_lock&) = delete;
        file_lock(file_lock&&) noexcept;
        ~file_lock();

        file_lock& operator=(file_lock&&) = default;

        static future<file_lock> acquire(fs::path);

        fs::path path() const;
        sstring to_string() const {
            return path().native();
        }
    private:
        class impl;
        file_lock(fs::path);
        std::unique_ptr<impl> _impl;
    };
}

