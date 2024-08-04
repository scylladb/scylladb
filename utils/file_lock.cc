/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "utils/assert.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/posix.hh>
#include <unistd.h>
#include <fcntl.h>

#include "file_lock.hh"

class utils::file_lock::impl {
public:
    impl(fs::path path)
            : _path(std::move(path)), _fd(
                    file_desc::open(_path.native(), O_RDWR | O_CREAT | O_CLOEXEC,
                    S_IRWXU)) {
        if (::lockf(_fd.get(), F_TLOCK, 0) != 0) {
            throw std::system_error(errno, std::system_category(),
                        "Could not acquire lock: " + _path.native());
        }
    }
    impl(impl&&) = default;
    ~impl() {
        if (!_path.empty()) {
            ::unlink(_path.c_str());
        }
        SCYLLA_ASSERT(_fd.get() != -1);
        auto r = ::lockf(_fd.get(), F_ULOCK, 0);
        SCYLLA_ASSERT(r == 0);
    }
    fs::path _path;
    file_desc _fd;
};

utils::file_lock::file_lock(fs::path path)
    : _impl(std::make_unique<impl>(std::move(path)))
{}

utils::file_lock::file_lock(file_lock&& f) noexcept
    : _impl(std::move(f._impl))
{}

utils::file_lock::~file_lock()
{}

fs::path utils::file_lock::path() const {
    return _impl ? _impl->_path : "";
}

future<utils::file_lock> utils::file_lock::acquire(fs::path path) {
    // meh. not really any future stuff here. but pretend, for the
    // day when a future version of lock etc is added.
    try {
        return make_ready_future<file_lock>(file_lock(path));
    } catch (...) {
        return make_exception_future<utils::file_lock>(std::current_exception());
    }
}
