/*
 * Copyright 2014 Cloudius Systems
 */

#include "reactor.hh"
#include <iostream>
#include <algorithm>

class http_server {
    reactor& _r;
    std::vector<std::unique_ptr<pollable_fd>> _listeners;
public:
    http_server(reactor& r) : _r(r) {}
    void listen(ipv4_addr addr) {
        listen_options lo;
        lo.reuse_address = true;
        do_accepts(_r.listen(make_ipv4_address(addr), lo));
    }
    void do_accepts(std::unique_ptr<pollable_fd> lfd) {
        auto l = lfd.get();
        l->accept().then([this, lfd = std::move(lfd)] (future<accept_result> res) mutable {
            accept_result ar = res.get();
            auto fd = std::move(std::get<0>(ar));
            auto addr = std::get<1>(ar);
            (new connection(std::move(fd), addr))->read();
            do_accepts(std::move(lfd));
        });
    }
    class connection {
        std::unique_ptr<pollable_fd> _fd;
        socket_address _addr;
        input_stream_buffer<char> _read_buf;
        static constexpr size_t limit = 4096;
        using tmp_buf = temporary_buffer<char>;
    public:
        connection(std::unique_ptr<pollable_fd>&& fd, socket_address addr)
            : _fd(std::move(fd)), _addr(addr), _read_buf(*_fd, 8192) {}
        void read() {
            _read_buf.read_until(limit, '\n').then([this] (future<tmp_buf> fut_start_line) {
                auto start_line = fut_start_line.get();
                std::cout << std::string(start_line.begin(), start_line.end());
            });
        }
    };
};

int main(int ac, char** av) {
    reactor r;
    http_server server(r);
    server.listen({{}, 10000});
    r.run();
    return 0;

}


