/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <filesystem>
#include <fmt/format.h>

namespace fs = std::filesystem;

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
class tmpdir {
    fs::path _path;

private:
    void remove() noexcept;

    class sweeper {
        const tmpdir& _tmpd;
    public:
        sweeper(const tmpdir& t) noexcept : _tmpd(t) {}
        ~sweeper();
    };

public:
    tmpdir();

    tmpdir(tmpdir&& other) noexcept;
    tmpdir(const tmpdir&) = delete;
    void operator=(tmpdir&& other) noexcept;
    void operator=(const tmpdir&) = delete;

    ~tmpdir();

    const fs::path& path() const noexcept { return _path; }
    sweeper make_sweeper() const noexcept { return sweeper(*this); }
};
