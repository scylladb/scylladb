/*
 * test-reactor.cc
 *
 *  Created on: Aug 2, 2014
 *      Author: avi
 */

#include "reactor.hh"
#include <iostream>

struct test {
    reactor r;
    std::unique_ptr<pollable_fd> listener;
    struct connection {
        connection(reactor& r, std::unique_ptr<pollable_fd> fd) : r(r), fd(std::move(fd)) {}
        reactor& r;
        std::unique_ptr<pollable_fd> fd;
        char buffer[8192];
        void copy_data() {
            r.read_some(*fd, buffer, sizeof(buffer)).then([this] (future<size_t> fut) {
                auto n = fut.get();
                if (n) {
                    r.write_all(*fd, buffer, n).then([this, n] (future<size_t> fut) {
                        if (fut.get() == n) {
                            copy_data();
                        }
                    });
                } else {
                    delete this;
                }
            });
        }
    };
    void new_connection(accept_result&& accepted) {
        auto c = new connection(r, std::move(std::get<0>(accepted)));
        c->copy_data();
    }
    void start_accept() {
        r.accept(*listener).then([this] (future<accept_result> fut) {
            new_connection(fut.get());
            start_accept();
        });
    }
};

int main(int ac, char** av)
{
    test t;
    ipv4_addr addr{{}, 10000};
    listen_options lo;
    lo.reuse_address = true;
    t.listener = t.r.listen(make_ipv4_address(addr), lo);
    t.start_accept();
    t.r.run();
    return 0;
}

