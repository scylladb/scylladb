/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef FILE_DESC_HH_
#define FILE_DESC_HH_

#include "sstring.hh"
#include <unistd.h>
#include <assert.h>
#include <utility>
#include <fcntl.h>
#include <sys/ioctl.h>

inline void throw_system_error_on(bool condition);

template <typename T>
inline void throw_kernel_error(T r);


class file_desc {
    int _fd;
public:
    file_desc() = delete;
    file_desc(const file_desc&) = delete;
    file_desc(file_desc&& x) : _fd(x._fd) { x._fd = -1; }
    ~file_desc() { if (_fd != -1) { ::close(_fd); } }
    void operator=(const file_desc&) = delete;
    file_desc& operator=(file_desc&& x) {
        if (this != &x) {
            std::swap(_fd, x._fd);
            if (x._fd != -1) {
                x.close();
            }
        }
        return *this;
    }
    void close() {
        assert(_fd != -1);
        auto r = ::close(_fd);
        throw_system_error_on(r == -1);
        _fd = -1;
    }
    int get() { return _fd; }
    static file_desc open(sstring name, int flags, mode_t mode = 0) {
        int fd = ::open(name.c_str(), flags, mode);
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    static file_desc socket(int family, int type, int protocol = 0) {
        int fd = ::socket(family, type, protocol);
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    int ioctl(int request) {
        return ioctl(request, 0);
    }
    int ioctl(int request, int value) {
        int r = ::ioctl(_fd, request, value);
        throw_system_error_on(r == -1);
        return r;
    }
    template <class X>
    int ioctl(int request, X& data) {
        int r = ::ioctl(_fd, request, &data);
        throw_system_error_on(r == -1);
        return r;
    }
    template <class X>
    int ioctl(int request, X&& data) {
        int r = ::ioctl(_fd, request, &data);
        throw_system_error_on(r == -1);
        return r;
    }
    template <class X>
    int setsockopt(int level, int optname, X& data) {
        int r = ::setsockopt(_fd, level, optname, &data, sizeof(data));
        throw_system_error_on(r == -1);
        return r;
    }
    int setsockopt(int level, int optname, const char* data) {
        int r = ::setsockopt(_fd, level, optname, data, strlen(data) + 1);
        throw_system_error_on(r == -1);
        return r;
    }
private:
    file_desc(int fd) : _fd(fd) {}
 };

inline
void throw_system_error_on(bool condition) {
    if (condition) {
        throw std::system_error(errno, std::system_category());
    }
}

template <typename T>
inline
void throw_kernel_error(T r) {
    static_assert(std::is_signed<T>::value, "kernel error variables must be signed");
    if (r < 0) {
        throw std::system_error(-r, std::system_category());
    }
}

#endif /* FILE_DESC_HH_ */
