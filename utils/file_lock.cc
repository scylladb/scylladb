/*
 * Copyright 2014 Cloudius Systems
 */


#include <core/reactor.hh>
#include <core/posix.hh>
#include <unistd.h>
#include <fcntl.h>

#include "file_lock.hh"

class utils::file_lock::impl {
public:
    impl(sstring path)
            : _path(std::move(path)), _fd(
                    file_desc::open(_path, O_RDWR | O_CREAT | O_CLOEXEC,
                    S_IRWXU)) {
        if (::lockf(_fd.get(), F_TLOCK, 0) != 0) {
            throw std::system_error(errno, std::system_category(), "Could not acquire lock: " + _path);
        }
    }
    impl(impl&&) = default;
    ~impl() {
        if (!_path.empty()) {
            ::unlink(_path.c_str());
        }
        assert(_fd.get() != -1);
        ::lockf(_fd.get(), F_ULOCK, 0);
    }
    sstring
        _path;
    file_desc
        _fd;
};

utils::file_lock::file_lock(sstring path)
    : _impl(std::make_unique<impl>(std::move(path)))
{}

utils::file_lock::file_lock(file_lock&& f)
    : _impl(std::move(f._impl))
{}

utils::file_lock::~file_lock()
{}

sstring utils::file_lock::path() const {
    return _impl ? _impl->_path : "";
}

future<utils::file_lock> utils::file_lock::acquire(sstring path) {
    // meh. not really any future stuff here. but pretend, for the
    // day when a future version of lock etc is added.
    try {
        return make_ready_future<file_lock>(file_lock(path));
    } catch (...) {
        return make_exception_future<utils::file_lock>(std::current_exception());
    }
}

std::ostream& utils::operator<<(std::ostream& out, const file_lock& f) {
    return out << "file lock '" << f.path() << "'";
}
