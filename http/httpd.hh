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
 * Copyright 2015 Cloudius Systems
 */

#ifndef APPS_HTTPD_HTTPD_HH_
#define APPS_HTTPD_HTTPD_HH_

#include "http/request_parser.hh"
#include "http/request.hh"
#include "core/reactor.hh"
#include "core/sstring.hh"
#include "core/app-template.hh"
#include "core/circular_buffer.hh"
#include "core/distributed.hh"
#include "core/queue.hh"
#include "core/future-util.hh"
#include "core/scollectd.hh"
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <queue>
#include <bitset>
#include <limits>
#include <cctype>
#include <vector>
#include "reply.hh"
#include "http/routes.hh"

namespace httpd {

class http_server;
class http_stats;

using namespace std::chrono_literals;

class http_stats {
    std::vector<scollectd::registration> _regs;
public:
    http_stats(http_server& server);
};

class http_server {
    std::vector<server_socket> _listeners;
    http_stats _stats { *this };
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _requests_served = 0;
    sstring _date = http_date();
    timer<> _date_format_timer { [this] {_date = http_date();} };
public:
    routes _routes;

    http_server() {
        _date_format_timer.arm_periodic(1s);
    }
    future<> listen(ipv4_addr addr) {
        listen_options lo;
        lo.reuse_address = true;
        _listeners.push_back(engine().listen(make_ipv4_address(addr), lo));
        do_accepts(_listeners.size() - 1);
        return make_ready_future<>();
    }
    void do_accepts(int which) {
        _listeners[which].accept().then(
                [this, which] (connected_socket fd, socket_address addr) mutable {
                    auto conn = new connection(*this, std::move(fd), addr);
                    conn->process().then_wrapped([this, conn] (auto&& f) {
                                delete conn;
                                try {
                                    f.get();
                                } catch (std::exception& ex) {
                                    std::cout << "request error " << ex.what() << "\n";
                                }
                            });
                    do_accepts(which);
                }).then_wrapped([] (auto f) {
            try {
                f.get();
            } catch (std::exception& ex) {
                std::cout << "accept failed: " << ex.what() << "\n";
            }
        });
    }
    class connection {
        http_server& _server;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
        static constexpr size_t limit = 4096;
        using tmp_buf = temporary_buffer<char>;
        http_request_parser _parser;
        std::unique_ptr<request> _req;
        std::unique_ptr<reply> _resp;
        // null element marks eof
        queue<std::unique_ptr<reply>> _replies { 10 };bool _done = false;
    public:
        connection(http_server& server, connected_socket&& fd,
                socket_address addr)
                : _server(server), _fd(std::move(fd)), _read_buf(_fd.input()), _write_buf(
                        _fd.output()) {
            ++_server._total_connections;
            ++_server._current_connections;
        }
        ~connection() {
            --_server._current_connections;
        }
        future<> process() {
            // Launch read and write "threads" simultaneously:
            return when_all(read(), respond()).then(
                    [] (std::tuple<future<>, future<>> joined) {
                        // FIXME: notify any exceptions in joined?
                        return make_ready_future<>();
                    });
        }
        future<> read() {
            return do_until([this] {return _done;}, [this] {
                return read_one();
            }).then_wrapped([this] (future<> f) {
                // swallow error
                // FIXME: count it?
                    return _replies.push_eventually( {});
                });
        }
        future<> read_one() {
            _parser.init();
            return _read_buf.consume(_parser).then([this] {
                if (_parser.eof()) {
                    _done = true;
                    return make_ready_future<>();
                }
                ++_server._requests_served;
                _req = _parser.get_parsed_request();
                return _replies.not_full().then([this] {
                            _done = generate_reply(std::move(_req));
                        });
            });
        }
        future<> respond() {
            return _replies.pop_eventually().then(
                    [this] (std::unique_ptr<reply> resp) {
                        if (!resp) {
                            // eof
                            return make_ready_future<>();
                        }
                        _resp = std::move(resp);
                        return start_response().then([this] {
                                    return respond();
                                });
                    });
        }
        future<> start_response() {
            _resp->_headers["Server"] = "Seastar httpd";
            _resp->_headers["Date"] = _server._date;
            _resp->_headers["Content-Length"] = to_sstring(
                    _resp->_content.size());
            return _write_buf.write(_resp->_response_line.begin(),
                    _resp->_response_line.size()).then([this] {
                return write_reply_headers(_resp->_headers.begin());
            }).then([this] {
                return _write_buf.write("\r\n", 2);
            }).then([this] {
                return write_body();
            }).then([this] {
                return _write_buf.flush();
            }).then([this] {
                _resp.reset();
            });
        }
        future<> write_reply_headers(
                std::unordered_map<sstring, sstring>::iterator hi) {
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
                return write_reply_headers(++hi);
            });
        }
        bool generate_reply(std::unique_ptr<request> req) {
            auto resp = std::make_unique<reply>();
            bool conn_keep_alive = false;
            bool conn_close = false;
            auto it = req->_headers.find("Connection");
            if (it != req->_headers.end()) {
                if (it->second == "Keep-Alive") {
                    conn_keep_alive = true;
                } else if (it->second == "Close") {
                    conn_close = true;
                }
            }
            bool should_close;
            // TODO: Handle HTTP/2.0 when it releases
            resp->set_version(req->_version);

            if (req->_version == "1.0") {
                if (conn_keep_alive) {
                    resp->_headers["Connection"] = "Keep-Alive";
                }
                should_close = !conn_keep_alive;
            } else if (req->_version == "1.1") {
                should_close = conn_close;
            } else {
                // HTTP/0.9 goes here
                should_close = true;
            }
            _server._routes.handle(req->_url, *(req.get()), *(resp.get()));
            // Caller guarantees enough room
            _replies.push(std::move(resp));
            return should_close;
        }
        future<> write_body() {
            return _write_buf.write(_resp->_content.begin(),
                    _resp->_content.size());
        }
    };
    uint64_t total_connections() const {
        return _total_connections;
    }
    uint64_t current_connections() const {
        return _current_connections;
    }
    uint64_t requests_served() const {
        return _requests_served;
    }
    static sstring http_date() {
        auto t = ::time(nullptr);
        struct tm tm;
        gmtime_r(&t, &tm);
        char tmp[100];
        strftime(tmp, sizeof(tmp), "%d %b %Y %H:%M:%S GMT", &tm);
        return tmp;
    }
};
}

#endif /* APPS_HTTPD_HTTPD_HH_ */
