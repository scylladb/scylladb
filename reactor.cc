/*
 * reactor.cc
 *
 *  Created on: Aug 1, 2014
 *      Author: avi
 */

#include "reactor.hh"
#include <cassert>
#include <unistd.h>
#include <fcntl.h>

reactor::reactor()
    : _epollfd(epoll_create1(EPOLL_CLOEXEC)) {
    assert(_epollfd != -1);
}

reactor::~reactor() {
    ::close(_epollfd);
}

future<void> reactor::get_epoll_future(pollable_fd_state& pfd,
        promise<void> pollable_fd_state::*pr, int event) {
    auto ctl = pfd.events ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    pfd.events |= event;
    pfd.*pr = promise<void>();
    ::epoll_event eevt;
    eevt.events = pfd.events;
    eevt.data.ptr = &pfd;
    int r = ::epoll_ctl(_epollfd, ctl, pfd.fd, &eevt);
    assert(r == 0);
    return (pfd.*pr).get_future();
}

future<void> reactor::readable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollin, EPOLLIN);
}

future<void> reactor::writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollout, EPOLLOUT);
}

void reactor::forget(pollable_fd_state& fd) {
    if (fd.events) {
        ::epoll_ctl(_epollfd, EPOLL_CTL_DEL, fd.fd, nullptr);
    }
}

pollable_fd
reactor::listen(socket_address sa, listen_options opts) {
    int fd = ::socket(sa.u.sa.sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    assert(fd != -1);
    if (opts.reuse_address) {
        int opt = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    }
    int r = ::bind(fd, &sa.u.sa, sizeof(sa.u.sas));
    assert(r != -1);
    ::listen(fd, 100);
    return pollable_fd(fd);
}

void reactor::run() {
    std::vector<std::unique_ptr<task>> current_tasks;
    while (true) {
        while (!_pending_tasks.empty()) {
            std::swap(_pending_tasks, current_tasks);
            for (auto&& tsk : current_tasks) {
                tsk->run();
                tsk.reset();
            }
            current_tasks.clear();
        }
        std::array<epoll_event, 128> eevt;
        int nr = ::epoll_wait(_epollfd, eevt.data(), eevt.size(), -1);
        assert(nr != -1);
        for (int i = 0; i < nr; ++i) {
            auto& evt = eevt[i];
            auto pfd = reinterpret_cast<pollable_fd_state*>(evt.data.ptr);
            auto events = evt.events;
            std::unique_ptr<task> t_in, t_out;
            if (events & EPOLLIN) {
                pfd->events &= ~EPOLLIN;
                pfd->pollin.set_value();
                pfd->pollin = promise<void>();
            }
            if (events & EPOLLOUT) {
                pfd->events &= ~EPOLLOUT;
                pfd->pollout.set_value();
                pfd->pollout = promise<void>();
            }
            evt.events = pfd->events;
            auto op = evt.events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
            ::epoll_ctl(_epollfd, op, pfd->fd, &evt);
        }
    }
}

socket_address make_ipv4_address(ipv4_addr addr) {
    socket_address sa;
    sa.u.in.sin_family = AF_INET;
    sa.u.in.sin_port = htons(addr.port);
    std::memcpy(&sa.u.in.sin_addr, addr.host, 4);
    return sa;
}

reactor the_reactor;
