/*
 * test-reactor.cc
 *
 *  Created on: Aug 2, 2014
 *      Author: avi
 */

#include "reactor.hh"
#include <iostream>

struct test {
    std::unique_ptr<pollable_fd> listener;
    struct connection {
        connection(std::unique_ptr<pollable_fd> fd) : fd(std::move(fd)) {}
        std::unique_ptr<pollable_fd> fd;
        char buffer[8192];
        void copy_data() {
            fd->read_some(buffer, sizeof(buffer)).then([this] (size_t n) {
                if (n) {
                    fd->write_all(buffer, n).then([this, n] (size_t w) {
                        if (w == n) {
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
        auto c = new connection(std::move(std::get<0>(accepted)));
        c->copy_data();
    }
    void start_accept() {
        the_reactor.accept(*listener).then([this] (accept_result&& ar) {
            new_connection(std::move(ar));
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
    t.listener = the_reactor.listen(make_ipv4_address(addr), lo);
    t.start_accept();
    the_reactor.run();
    return 0;
}

