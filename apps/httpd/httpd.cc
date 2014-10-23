/*
 * Copyright 2014 Cloudius Systems
 */

#include "apps/httpd/request_parser.hh"
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/app-template.hh"
#include "core/circular_buffer.hh"
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <queue>
#include <bitset>
#include <limits>
#include <cctype>

class http_server {
    std::vector<server_socket> _listeners;
public:
    void listen(ipv4_addr addr) {
        listen_options lo;
        lo.reuse_address = true;
        _listeners.push_back(engine.listen(make_ipv4_address(addr), lo));
        do_accepts(_listeners.size() - 1);
    }
    void do_accepts(int which) {
        _listeners[which].accept().then([this, which] (connected_socket fd, socket_address addr) mutable {
            (new connection(*this, std::move(fd), addr))->read().rescue([this] (auto get_ex) {
                try {
                    get_ex();
                } catch (std::exception& ex) {
                    std::cout << "request error " << ex.what() << "\n";
                }
            });
            do_accepts(which);
        }).rescue([] (auto get_ex) {
            try {
                get_ex();
            } catch (std::exception& ex) {
                std::cout << "accept failed: " << ex.what() << "\n";
            }
        });
    }
    class connection {
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        bool _eof = false;
        static constexpr size_t limit = 4096;
        using tmp_buf = temporary_buffer<char>;
        using request = http_request;
        struct response {
            sstring _response_line;
            sstring _body;
            std::unordered_map<sstring, sstring> _headers;
        };
        http_request_parser _parser;
        std::unique_ptr<request> _req;
        std::unique_ptr<response> _resp;
        std::queue<std::unique_ptr<response>,
            circular_buffer<std::unique_ptr<response>>> _pending_responses;
    public:
        connection(http_server& server, connected_socket&& fd, socket_address addr)
            : _fd(std::move(fd)), _read_buf(_fd.input())
            , _write_buf(_fd.output()) {}
        future<> read() {
            _parser.init();
            return _read_buf.consume(_parser).then([this] {
                if (_parser.eof()) {
                    maybe_done();
                    return make_ready_future<>();
                }
                _req = _parser.get_parsed_request();
                generate_response(std::move(_req));
                read().rescue([this] (auto get_ex) mutable {
                    try {
                        get_ex();
                    } catch (std::exception& ex) {
                        std::cout << "read failed with " << ex.what() << "\n";
                        this->maybe_done();
                    }
                });
                return make_ready_future<>();
            });
        }
        future<> bad(std::unique_ptr<request> req) {
            auto resp = std::make_unique<response>();
            resp->_response_line = "HTTP/1.1 400 BAD REQUEST\r\n\r\n";
            respond(std::move(resp));
            _eof = true;
            throw std::runtime_error("failed to parse request");
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
                    [this] {
                return write_response_headers(_resp->_headers.begin());
            }).then([this] {
                return _write_buf.write("\r\n", 2);
            }).then([this] {
                return write_body();
            }).then([this] {
                return _write_buf.flush();
            }).then([this] {
                _resp.reset();
                if (!_pending_responses.empty()) {
                    _resp = std::move(_pending_responses.front());
                    _pending_responses.pop();
                    start_response();
                } else {
                    maybe_done();
                }
            });
        }
        future<> write_response_headers(std::unordered_map<sstring, sstring>::iterator hi) {
            if (hi == _resp->_headers.end()) {
                return make_ready_future<>();
            }
            return _write_buf.write(hi->first.begin(), hi->first.size()).then(
                    [this] {
                return _write_buf.write(": ", 2);
            }).then([hi, this] {
                return _write_buf.write(hi->second.begin(), hi->second.size());
            }).then([this] {
                return _write_buf.write("\r\n", 2);
            }).then([hi, this] () mutable {
                return write_response_headers(++hi);
            });
        }
        void generate_response(std::unique_ptr<request> req) {
            auto resp = std::make_unique<response>();
            resp->_response_line = "HTTP/1.1 200 OK\r\n";
            resp->_headers["Content-Type"] = "text/html";
            resp->_body = "<html><head><title>this is the future</title></head><body><p>Future!!</p></body></html>";
            respond(std::move(resp));
        }
        future<> write_body() {
            return _write_buf.write(_resp->_body.begin(), _resp->_body.size());
        }
        void maybe_done() {
            if ((_eof || _read_buf.eof()) && !_req && !_resp && _pending_responses.empty()) {
                delete this;
            }
        }
    };
};

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "HTTP Server port") ;
    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
        for(unsigned c = 0; c < smp::count; c++) {
            smp::submit_to(c, [port] () mutable {static thread_local http_server server; server.listen({port});});
        }
    });
}
