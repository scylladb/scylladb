/*
 * reactor.hh
 *
 *  Created on: Aug 1, 2014
 *      Author: avi
 */

#ifndef REACTOR_HH_
#define REACTOR_HH_

#include <memory>
#include <libaio.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unordered_map>
#include <netinet/ip.h>
#include <cstring>
#include <cassert>

class socket_address;
class reactor;
class pollable_fd;

struct ipv4_addr {
    uint8_t host[4];
    uint16_t port;
};

socket_address make_ipv4_address(ipv4_addr addr);

class socket_address {
private:
    union {
        ::sockaddr_storage sas;
        ::sockaddr sa;
        ::sockaddr_in in;
    } u;
    friend socket_address make_ipv4_address(ipv4_addr addr);
    friend class reactor;
};

template <typename T>
class promise;

template <typename T>
struct future_state {
    virtual ~future_state();
    promise<T>* promise;
    bool value_valid = false;
    bool ex_valid = false;
    union {
        T value;
        std::exception_ptr ex;
    } u;
    void set(const T& value);
    void set(T&& value);
    void set_exception(std::exception_ptr ex);
    T get() {
        while (promise) {
            promise->wait();
        }
        if (ex) {
            std::rethrow_exception(ex);
        }
        return std::move(u.value);
    }
};

template <typename T>
class future {
    std::unique_ptr<future_state<T>> _state;
public:
    T get() {
        return _state.get();
    }
    template <typename Func>
    void then(Func func) {

    }
};

class reactor {
    class task;
public:
    int _epollfd;
    io_context_t _io_context;
private:
    class task {
    public:
        virtual ~task() {}
        virtual void run() = 0;
    };
    template <typename Func>
    class lambda_task : public task {
        Func _func;
    public:
        lambda_task(Func func) : _func(func) {}
        virtual void run() { _func(); }
    };

    template <typename Func>
    std::unique_ptr<task>
    make_task(Func func) {
        return std::make_unique<lambda_task<Func>>(func);
    }

    void epoll_add_in(pollable_fd& fd, std::unique_ptr<task> t);
    void epoll_add_out(pollable_fd& fd, std::unique_ptr<task> t);
    void abort_on_error(int ret);
public:
    reactor();
    ~reactor();

    std::unique_ptr<pollable_fd> listen(socket_address sa);

    template <typename Func>
    void accept(pollable_fd& listenfd, Func with_pfd_sockaddr);

    future<std::unique_ptr<pollable_fd>> accept(pollable_fd& listen_fd)

    future<size_t> read_some(pollable_fd& fd, void* buffer, size_t size);
    template <typename Func>
    void read_some(pollable_fd& fd, void* buffer, size_t len, Func with_len);

    void run();

    friend class pollable_fd;
};

class pollable_fd {
protected:
    explicit pollable_fd(int fd) : fd(fd) {}
    pollable_fd(const pollable_fd&) = delete;
    void operator=(const pollable_fd&) = delete;
    int fd;
    int events = 0;
    std::unique_ptr<reactor::task> pollin;
    std::unique_ptr<reactor::task> pollout;
    friend class reactor;
};

template <typename Func>
inline
void reactor::accept(pollable_fd& listenfd, Func with_pfd_sockaddr) {
    auto lfd = listenfd.fd;
    epoll_add_in(listenfd, make_task([=] {
        socket_address sa;
        socklen_t sl = sizeof(&sa.u.sas);
        int fd = ::accept4(lfd, &sa.u.sa, &sl, SOCK_NONBLOCK | SOCK_CLOEXEC);
        assert(fd != -1);
        auto pfd = std::unique_ptr<pollable_fd>(new pollable_fd(fd));
        with_pfd_sockaddr(std::move(pfd), sa);
    }));
}

template <typename Func>
void reactor::read_some(pollable_fd& fd, void* buffer, size_t len, Func with_len) {
    auto rfd = fd.fd;
    epoll_add_in(fd, make_task([=] {
        ssize_t r = ::recv(rfd, buffer, len, 0);
        assert(r != -1);
        with_len(len);
    }));
}


#endif /* REACTOR_HH_ */
