/*
 * Copyright 2014 Cloudius Systems
 */

#include "reactor.hh"
#include "sstring.hh"
#include <iostream>
#include <algorithm>
#include <regex>
#include <unordered_map>

sstring to_sstring(const std::csub_match& sm) {
    return sstring(sm.first, sm.second);
}

static std::string tchar = "[-!#$%&'\\*\\+.^_`|~0-9A-Za-z]";
static std::string token = tchar + "+";
static constexpr auto re_opt = std::regex::ECMAScript | std::regex::optimize;
static std::regex start_line_re { "([A-Z]+) (\\S+) HTTP/([0-9]\\.[0-9])\\r\\n", re_opt };
static std::regex header_re { "(" + token + ")\\s*:\\s*(.*\\S)\\s*\\r\\n", re_opt };
static std::regex header_cont_re { "\\s+(.*\\S)\\s*\\r\\n", re_opt };

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
        sstring _method;
        sstring _url;
        sstring _version;
        sstring _response_line;
        sstring _last_header_name;
        sstring _response_body;
        std::unordered_map<sstring, sstring> _headers;
        std::unordered_map<sstring, sstring> _response_headers;
        output_stream_buffer<char> _write_buf;
    public:
        connection(http_server& server, std::unique_ptr<pollable_fd>&& fd, socket_address addr)
            : _server(server), _fd(std::move(fd)), _addr(addr), _read_buf(*_fd, 8192)
            , _write_buf(*_fd, 8192) {}
        void read() {
            _read_buf.read_until(limit, '\n').then([this] (future<tmp_buf> fut_start_line) {
                auto start_line = fut_start_line.get();
                std::cmatch match;
                if (!std::regex_match(start_line.begin(), start_line.end(), match, start_line_re)) {
                    std::cout << "no match\n";
                    return bad();
                }
                _method = to_sstring(match[1]);
                _url = to_sstring(match[2]);
                _version = to_sstring(match[3]);
                if (_method != "GET") {
                    return bad();
                }
                std::cout << "start line: " << _method << " | " << _url << " | " << _version << "\n";
                _read_buf.read_until(limit, '\n').then([this] (future<tmp_buf> header) {
                    parse_header(std::move(header));
                });
            });
        }
        void parse_header(future<tmp_buf> f_header) {
            auto header = f_header.get();
            if (header.size() == 2 && header[0] == '\r' && header[1] == '\n') {
                return generate_response();
            }
            std::cmatch match;
            if (std::regex_match(header.begin(), header.end(), match, header_re)) {
                sstring name = to_sstring(match[1]);
                sstring value = to_sstring(match[2]);
                std::cout << "found header: " << name << "=" << value << ".\n";
                _headers[name] = std::move(value);
                _last_header_name = std::move(name);
            } else if (std::regex_match(header.begin(), header.end(), match, header_cont_re)) {
                _headers[_last_header_name] += " ";
                _headers[_last_header_name] += to_sstring(match[1]);
                std::cout << "found header: " << _last_header_name << "=" << _headers[_last_header_name] << ".\n";
            } else {
                return bad();
            }
            _read_buf.read_until(limit, '\n').then([this] (future<tmp_buf> header) {
                parse_header(std::move(header));
            });
        }
        void bad() {
            _response_line = "HTTP/1.1 400 BAD REQUEST\r\n\r\n";
            respond();
        }
        void respond() {
            _write_buf.write(_response_line.begin(), _response_line.size()).then(
                    [this] (future<size_t> n) mutable {
                write_response_headers(_response_headers.begin()).then(
                        [this] (future<size_t> done) {
                    write_body().then(
                            [this] (future<size_t> done) {
                        _write_buf.flush().then(
                                [this] (future<bool> done) {
                            delete this;
                        });
                    });
                });
            });
        }
        future<size_t> write_response_headers(std::unordered_map<sstring, sstring>::iterator hi) {
            if (hi == _response_headers.end()) {
                return _write_buf.write("\r\n", 2);
            }
            promise<size_t> pr;
            auto fut = pr.get_future();
            _write_buf.write(hi->first.begin(), hi->first.size()).then(
                    [hi, this, pr = std::move(pr)] (future<size_t> done) mutable {
                _write_buf.write(": ", 2).then(
                        [hi, this, pr = std::move(pr)] (future<size_t> done) mutable {
                    _write_buf.write(hi->second.begin(), hi->second.size()).then(
                            [hi, this, pr = std::move(pr)] (future<size_t> done) mutable {
                        _write_buf.write("\r\n", 2).then(
                                [hi, this, pr = std::move(pr)] (future<size_t> done) mutable {
                            write_response_headers(++hi).then(
                                    [this, pr = std::move(pr)] (future<size_t> done) mutable {
                                pr.set_value(done.get());
                            });
                        });
                    });
                });
            });
            return fut;
        }
        void generate_response() {
            _response_line = "HTTP/1.1 200 OK\r\n\r\n";
            _response_headers["Content-Type"] = "text/html";
            _response_body = "<html><head><title>this is the future</title></head><body><p>Future!!</p></body></html>";
            respond();
        }
        future<size_t> write_body() {
            return _write_buf.write(_response_body.begin(), _response_body.size());
        }
    };
};

int main(int ac, char** av) {
    http_server server;
    server.listen({{}, 10000});
    the_reactor.run();
    return 0;
}


