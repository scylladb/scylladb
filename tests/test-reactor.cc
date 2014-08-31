/*
 * test-reactor.cc
 *
 *  Created on: Aug 2, 2014
 *      Author: avi
 */

#include "core/reactor.hh"
#include <iostream>

struct test {
    test(pollable_fd l) : listener(std::move(l)) {}
    pollable_fd listener;
    struct connection {
        connection(pollable_fd fd) : fd(std::move(fd)) {}
        pollable_fd fd;
        char buffer[8192];
        void copy_data() {
            fd.read_some(buffer, sizeof(buffer)).then([this] (size_t n) {
                if (n) {
                    fd.write_all(buffer, n).then([this, n] (size_t w) {
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
    void new_connection(pollable_fd fd, socket_address sa) {
        auto c = new connection(std::move(fd));
        c->copy_data();
    }
    void start_accept() {
        listener.accept().then([this] (pollable_fd fd, socket_address sa) {
            new_connection(std::move(fd), std::move(sa));
            start_accept();
        });
    }
};

int main(int ac, char** av)
{
    ipv4_addr addr{{}, 10000};
    listen_options lo;
    lo.reuse_address = true;
    test t(the_reactor.listen(make_ipv4_address(addr), lo));
    t.start_accept();
    the_reactor.run();
    return 0;
}

