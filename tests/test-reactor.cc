/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2014 Cloudius Systems
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
                    fd.write_all(buffer, n).then([this] {
                        copy_data();
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
    ipv4_addr addr{10000};
    listen_options lo;
    lo.reuse_address = true;
    test t(engine().posix_listen(make_ipv4_address(addr), lo));
    t.start_accept();
    engine().run();
    return 0;
}

