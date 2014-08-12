/*
 * Copyright 2014 Cloudius Systems
 */

#include "reactor.hh"
#include "sstring.hh"
#include <iostream>
#include <algorithm>
#include <regex>

sstring to_sstring(const std::csub_match& sm) {
    return sstring(sm.first, sm.second);
}

static constexpr auto re_opt = std::regex::ECMAScript | std::regex::optimize;
static std::regex start_line_re { "([A-Z]+) (\\S+) HTTP/([0-9]\\.[0-9])\\r\\n", re_opt };

class http_server {
    std::vector<std::unique_ptr<pollable_fd>> _listeners;
public:
    void listen(ipv4_addr addr) {
        listen_options lo;
        lo.reuse_address = true;
        do_accepts(the_reactor.listen(make_ipv4_address(addr), lo));
    }
    void do_accepts(std::unique_ptr<pollable_fd> lfd) {
        auto l = lfd.get();
        l->accept().then([this, lfd = std::move(lfd)] (future<accept_result> res) mutable {
            accept_result ar = res.get();
            auto fd = std::move(std::get<0>(ar));
            auto addr = std::get<1>(ar);
            (new connection(*this, std::move(fd), addr))->read();
            do_accepts(std::move(lfd));
        });
    }
    class connection {
        http_server& _server;
        std::unique_ptr<pollable_fd> _fd;
        socket_address _addr;
        input_stream_buffer<char> _read_buf;
        static constexpr size_t limit = 4096;
        using tmp_buf = temporary_buffer<char>;
    public:
        connection(http_server& server, std::unique_ptr<pollable_fd>&& fd, socket_address addr)
            : _server(server), _fd(std::move(fd)), _addr(addr), _read_buf(*_fd, 8192) {}
        void read() {
            _read_buf.read_until(limit, '\n').then([this] (future<tmp_buf> fut_start_line) {
                auto start_line = fut_start_line.get();
                std::cmatch match;
                if (!std::regex_match(start_line.begin(), start_line.end(), match, start_line_re)) {
                    std::cout << "no match\n";
                    delete this;
                    return;
                }
                sstring method = to_sstring(match[1]);
                sstring url = to_sstring(match[2]);
                sstring version = to_sstring(match[3]);
                std::cout << "start line: " << method << " | " << url << " | " << version << "\n";
            });
        }
    };
};

int main(int ac, char** av) {
    http_server server;
    server.listen({{}, 10000});
    the_reactor.run();
    return 0;
}


