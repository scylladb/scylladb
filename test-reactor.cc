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
    void on_accept(std::unique_ptr<pollable_fd> pfd, socket_address sa) {
        std::cout << "got connection\n";
        r.accept(*listener, [=] (std::unique_ptr<pollable_fd> pfd, socket_address sa) {
            on_accept(std::move(pfd), sa);
        });
    }

int main(int ac, char** av)
{
    test t;
    ipv4_addr addr{{}, 10000};
    t.listener = r.listen(make_ipv4_address(addr));
    r.accept(*listener, [&]
    r.run();
}

