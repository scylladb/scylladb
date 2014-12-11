/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef FILE_DESC_HH_
#define FILE_DESC_HH_

#include "sstring.hh"
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <utility>
#include <fcntl.h>
#include <sys/ioctl.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <signal.h>
#include <system_error>
#include <boost/optional.hpp>
#include <pthread.h>
#include <memory>
#include "net/api.hh"

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
    int get() const { return _fd; }
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
    static file_desc eventfd(unsigned initval, int flags) {
        int fd = ::eventfd(initval, flags);
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    static file_desc epoll_create(int flags = 0) {
        int fd = ::epoll_create1(flags);
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    static file_desc timerfd_create(int clockid, int flags) {
        int fd = ::timerfd_create(clockid, flags);
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    static file_desc signalfd(const sigset_t& mask, int flags) {
        int fd = ::signalfd(-1, &mask, flags);
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    static file_desc temporary(sstring directory);
    file_desc dup() const {
        int fd = ::dup(get());
        throw_system_error_on(fd == -1);
        return file_desc(fd);
    }
    file_desc accept(sockaddr& sa, socklen_t& sl, int flags = 0) {
        auto ret = ::accept4(_fd, &sa, &sl, flags);
        throw_system_error_on(ret == -1);
        return file_desc(ret);
    }
    void truncate(size_t size) {
        auto ret = ::ftruncate(_fd, size);
        throw_system_error_on(ret);
    }
    int ioctl(int request) {
        return ioctl(request, 0);
    }
    int ioctl(int request, int value) {
        int r = ::ioctl(_fd, request, value);
        throw_system_error_on(r == -1);
        return r;
    }
    int ioctl(int request, unsigned int value) {
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
    int setsockopt(int level, int optname, X&& data) {
        int r = ::setsockopt(_fd, level, optname, &data, sizeof(data));
        throw_system_error_on(r == -1);
        return r;
    }
    int setsockopt(int level, int optname, const char* data) {
        int r = ::setsockopt(_fd, level, optname, data, strlen(data) + 1);
        throw_system_error_on(r == -1);
        return r;
    }
    size_t size() {
        struct stat buf;
        auto r = ::fstat(_fd, &buf);
        throw_system_error_on(r == -1);
        return buf.st_size;
    }
    boost::optional<size_t> read(void* buffer, size_t len) {
        auto r = ::read(_fd, buffer, len);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    boost::optional<ssize_t> recv(void* buffer, size_t len, int flags) {
        auto r = ::recv(_fd, buffer, len, flags);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { r };
    }
    boost::optional<size_t> recvmsg(msghdr* mh, int flags) {
        auto r = ::recvmsg(_fd, mh, flags);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    boost::optional<size_t> send(const void* buffer, size_t len, int flags) {
        auto r = ::send(_fd, buffer, len, flags);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    boost::optional<size_t> sendto(socket_address& addr, const void* buf, size_t len, int flags) {
        auto r = ::sendto(_fd, buf, len, flags, &addr.u.sa, sizeof(addr.u.sas));
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    boost::optional<size_t> sendmsg(const msghdr* msg, int flags) {
        auto r = ::sendmsg(_fd, msg, flags);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    void bind(sockaddr& sa, socklen_t sl) {
        auto r = ::bind(_fd, &sa, sl);
        throw_system_error_on(r == -1);
    }
    socket_address get_address() {
        socket_address addr;
        auto len = (socklen_t) sizeof(addr.u.sas);
        auto r = ::getsockname(_fd, &addr.u.sa, &len);
        throw_system_error_on(r == -1);
        return addr;
    }
    void listen(int backlog) {
        auto fd = ::listen(_fd, backlog);
        throw_system_error_on(fd == -1);
    }
    boost::optional<size_t> write(const void* buf, size_t len) {
        auto r = ::write(_fd, buf, len);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    boost::optional<size_t> writev(const iovec *iov, int iovcnt) {
        auto r = ::writev(_fd, iov, iovcnt);
        if (r == -1 && errno == EAGAIN) {
            return {};
        }
        throw_system_error_on(r == -1);
        return { size_t(r) };
    }
    void timerfd_settime(int flags, const itimerspec& its) {
        auto r = ::timerfd_settime(_fd, flags, &its, NULL);
        throw_system_error_on(r == -1);
    }

    void *map(size_t size, unsigned flags, bool shared, size_t offset) {
        void *x = mmap(NULL, size, flags, shared ? MAP_SHARED : MAP_PRIVATE, _fd, offset);
        throw_system_error_on(x == nullptr);
        return x;
    }

    void *map_shared_rw(size_t size, size_t offset) {
        return map(size, PROT_READ | PROT_WRITE, true, offset);
    }

    void *map_shared_ro(size_t size, size_t offset) {
        return map(size, PROT_READ, true, offset);
    }

    void *map_private_rw(size_t size, size_t offset) {
        return map(size, PROT_READ | PROT_WRITE, false, offset);
    }
    void *map_private_ro(size_t size, size_t offset) {
        return map(size, PROT_READ, false, offset);
    }

private:
    file_desc(int fd) : _fd(fd) {}
 };

struct mmap_deleter {
    size_t _size;
    void operator()(void* ptr) const;
};

using mmap_area = std::unique_ptr<char[], mmap_deleter>;

mmap_area mmap_anonymous(void* addr, size_t length, int prot, int flags);

class posix_thread {
public:
    class attr;
private:
    // must allocate, since this class is moveable
    std::unique_ptr<std::function<void ()>> _func;
    pthread_t _pthread;
    bool _valid = true;
    mmap_area _stack;
private:
    static void* start_routine(void* arg);
public:
    posix_thread(std::function<void ()> func);
    posix_thread(attr a, std::function<void ()> func);
    posix_thread(posix_thread&& x);
    ~posix_thread();
    void join();
public:
    class attr {
    public:
        struct stack_size { size_t size = 0; };
        attr() = default;
        template <typename... A>
        attr(A... a) {
            set(std::forward<A>(a)...);
        }
        void set() {}
        template <typename A, typename... Rest>
        void set(A a, Rest... rest) {
            set(std::forward<A>(a));
            set(std::forward<Rest>(rest)...);
        }
        void set(stack_size ss) { _stack_size = ss; }
    private:
        stack_size _stack_size;
        friend class posix_thread;
    };
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

inline
sigset_t make_sigset_mask(int signo) {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, signo);
    return set;
}

void pin_this_thread(unsigned cpu_id);

#endif /* FILE_DESC_HH_ */
