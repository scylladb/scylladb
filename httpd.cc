/*
 * Copyright 2014 Cloudius Systems
 */

#include "reactor.hh"
#include "sstring.hh"
#include <iostream>
#include <algorithm>
#include <regex>
#include <unordered_map>
#include <queue>

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
        l->accept().then([this, lfd = std::move(lfd)] (accept_result&& ar) mutable {
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
        output_stream_buffer<char> _write_buf;
        bool _eof = false;
        static constexpr size_t limit = 4096;
        using tmp_buf = temporary_buffer<char>;
        struct request {
            sstring _method;
            sstring _url;
            sstring _version;
            std::unordered_map<sstring, sstring> _headers;
        };
        sstring _last_header_name;
        struct response {
            sstring _response_line;
            sstring _body;
            std::unordered_map<sstring, sstring> _headers;
        };
        std::unique_ptr<request> _req;
        std::unique_ptr<response> _resp;
        std::queue<std::unique_ptr<response>> _pending_responses;
    public:
        connection(http_server& server, std::unique_ptr<pollable_fd>&& fd, socket_address addr)
            : _server(server), _fd(std::move(fd)), _addr(addr), _read_buf(*_fd, 8192)
            , _write_buf(*_fd, 8192) {}
        void read() {
            _read_buf.read_until(limit, '\n').then([this] (tmp_buf start_line) {
                if (!start_line.size()) {
                    _eof = true;
                    maybe_done();
                    return;
                }
                std::cmatch match;
                if (!std::regex_match(start_line.begin(), start_line.end(), match, start_line_re)) {
                    return bad(std::move(_req));
                }
                _req = std::make_unique<request>();
                _req->_method = to_sstring(match[1]);
                _req->_url = to_sstring(match[2]);
                _req->_version = to_sstring(match[3]);
                if (_req->_method != "GET") {
                    return bad(std::move(_req));
                }
                _read_buf.read_until(limit, '\n').then([this] (tmp_buf header) {
                    parse_header(std::move(header));
                });
            });
        }
        void parse_header(tmp_buf header) {
            if (header.size() == 2 && header[0] == '\r' && header[1] == '\n') {
                generate_response(std::move(_req));
                read();
                return;
            }
            std::cmatch match;
            if (std::regex_match(header.begin(), header.end(), match, header_re)) {
                sstring name = to_sstring(match[1]);
                sstring value = to_sstring(match[2]);
                _req->_headers[name] = std::move(value);
                _last_header_name = std::move(name);
            } else if (std::regex_match(header.begin(), header.end(), match, header_cont_re)) {
                _req->_headers[_last_header_name] += " ";
                _req->_headers[_last_header_name] += to_sstring(match[1]);
            } else {
                return bad(std::move(_req));
            }
            _read_buf.read_until(limit, '\n').then([this] (tmp_buf header) {
                parse_header(std::move(header));
            });
        }
        void bad(std::unique_ptr<request> req) {
            auto resp = std::make_unique<response>();
            resp->_response_line = "HTTP/1.1 400 BAD REQUEST\r\n\r\n";
            respond(std::move(resp));
        }
        void respond(std::unique_ptr<response> resp) {
            if (!_resp) {
                _resp = std::move(resp);
                start_response();
            } else {
                _pending_responses.push(std::move(resp));
            }
        }
        void start_response() {
            _resp->_headers["Content-Length"] = to_sstring(_resp->_body.size());
            _write_buf.write(_resp->_response_line.begin(), _resp->_response_line.size()).then(
                    [this] (size_t n) mutable {
                write_response_headers(_resp->_headers.begin()).then(
                        [this] (size_t done) {
                    _write_buf.write("\r\n", 2).then(
                            [this] (size_t done) mutable {
                        write_body().then(
                                [this] (size_t done) {
                            _write_buf.flush().then(
                                    [this] (bool done) {
                                _resp.reset();
                                if (!_pending_responses.empty()) {
                                    _resp = std::move(_pending_responses.front());
                                    _pending_responses.pop();
                                    start_response();
                                } else {
                                    maybe_done();
                                }
                            });
                        });
                    });
                });
            });
        }
        future<size_t> write_response_headers(std::unordered_map<sstring, sstring>::iterator hi) {
            promise<size_t> pr;
            auto fut = pr.get_future();
            if (hi == _resp->_headers.end()) {
                pr.set_value(0);
                return fut;
            }
            _write_buf.write(hi->first.begin(), hi->first.size()).then(
                    [hi, this, pr = std::move(pr)] (size_t done) mutable {
                _write_buf.write(": ", 2).then(
                        [hi, this, pr = std::move(pr)] (size_t done) mutable {
                    _write_buf.write(hi->second.begin(), hi->second.size()).then(
                            [hi, this, pr = std::move(pr)] (size_t done) mutable {
                        _write_buf.write("\r\n", 2).then(
                                [hi, this, pr = std::move(pr)] (size_t done) mutable {
                            write_response_headers(++hi).then(
                                    [this, pr = std::move(pr)] (size_t done) mutable {
                                pr.set_value(done);
                            });
                        });
                    });
                });
            });
            return fut;
        }
        void generate_response(std::unique_ptr<request> req) {
            auto resp = std::make_unique<response>();
            resp->_response_line = "HTTP/1.1 200 OK\r\n";
            resp->_headers["Content-Type"] = "text/html";
            resp->_body = "<html><head><title>this is the future</title></head><body><p>Future!!</p></body></html>";
            respond(std::move(resp));
        }
        future<size_t> write_body() {
            return _write_buf.write(_resp->_body.begin(), _resp->_body.size());
        }
        void maybe_done() {
            if (_eof && !_req && !_resp && _pending_responses.empty()) {
                delete this;
            }
        }
    };
};

int main(int ac, char** av) {
    http_server server;
    server.listen({{}, 10000});
    the_reactor.run();
    return 0;
}


