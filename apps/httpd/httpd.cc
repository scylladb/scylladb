/*
 * Copyright 2014 Cloudius Systems
 */

#include "apps/httpd/request_parser.hh"
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

using namespace std::chrono_literals;

class http_server;
class http_stats;

class http_stats {
    std::vector<scollectd::registration> _regs;
public:
    http_stats(http_server& server);
};

class http_server {
    std::vector<server_socket> _listeners;
    http_stats _stats{*this};
    uint64_t _total_connections = 0;
    uint64_t _current_connections = 0;
    uint64_t _requests_served = 0;
    sstring _date = http_date();
    timer<> _date_format_timer{[this] { _date = http_date(); }};
public:
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
        _listeners[which].accept().then([this, which] (connected_socket fd, socket_address addr) mutable {
            auto conn = new connection(*this, std::move(fd), addr);
            conn->process().rescue([this, conn] (auto&& get_ex) {
                delete conn;
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
        http_server& _server;
        connected_socket _fd;
        input_stream<char> _read_buf;
        output_stream<char> _write_buf;
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
        // null element marks eof
        queue<std::unique_ptr<response>> _responses { 10 };
    public:
        connection(http_server& server, connected_socket&& fd, socket_address addr)
            : _server(server), _fd(std::move(fd)), _read_buf(_fd.input())
            , _write_buf(_fd.output()) {
            ++_server._total_connections;
            ++_server._current_connections;
        }
        ~connection() {
            --_server._current_connections;
        }
        future<> process() {
            // Launch read and write "threads" simultaneously:
            return when_all(read(), respond()).then([] (std::tuple<future<>, future<>> joined) {
                // FIXME: notify any exceptions in joined?
                return make_ready_future<>();
            });
        }
        future<> read() {
            _parser.init();
            return _read_buf.consume(_parser).then([this] {
                if (_parser.eof()) {
                    return _responses.push_eventually({});
                }
                ++_server._requests_served;
                _req = _parser.get_parsed_request();
                return _responses.not_full().then([this] {
                    bool close = generate_response(std::move(_req));
                    if (close) {
                        return _responses.push_eventually({});
                    } else {
                        return read();
                    }
                });
            });
        }
        future<> respond() {
            return _responses.pop_eventually().then([this] (std::unique_ptr<response> resp) {
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
            _resp->_headers["Content-Length"] = to_sstring(_resp->_body.size());
            return _write_buf.write(_resp->_response_line.begin(), _resp->_response_line.size()).then(
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
        bool generate_response(std::unique_ptr<request> req) {
            auto resp = std::make_unique<response>();
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
            if (req->_version == "1.0") {
                resp->_response_line = "HTTP/1.0 200 OK\r\n";
                if (conn_keep_alive) {
                    resp->_headers["Connection"] = "Keep-Alive";
                }
                should_close = !conn_keep_alive;
            } else if (req->_version == "1.1") {
                resp->_response_line = "HTTP/1.1 200 OK\r\n";
                should_close = conn_close;
            } else {
                // HTTP/0.9 goes here
                resp->_response_line = "HTTP/1.0 200 OK\r\n";
                should_close = true;
            }
            resp->_headers["Content-Type"] = "text/html";
            resp->_body = "<html><head><title>this is the future</title></head><body><p>Future!!</p></body></html>";
            // Caller guarantees enough room
            _responses.push(std::move(resp));
            return should_close;
        }
        future<> write_body() {
            return _write_buf.write(_resp->_body.begin(), _resp->_body.size());
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

http_stats::http_stats(http_server& server)
    : _regs{
        scollectd::add_polled_metric(
            scollectd::type_instance_id("httpd", scollectd::per_cpu_plugin_instance,
                    "connections", "http-connections"),
            scollectd::make_typed(scollectd::data_type::DERIVE,
                    [&server] { return server.total_connections(); })),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("httpd", scollectd::per_cpu_plugin_instance,
                    "current_connections", "current"),
            scollectd::make_typed(scollectd::data_type::GAUGE,
                    [&server] { return server.current_connections(); })),
        scollectd::add_polled_metric(
            scollectd::type_instance_id("httpd", scollectd::per_cpu_plugin_instance,
                    "http_requests", "served"),
            scollectd::make_typed(scollectd::data_type::DERIVE,
                    [&server] { return server.requests_served(); })),
    } {
}

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    app_template app;
    app.add_options()
        ("port", bpo::value<uint16_t>()->default_value(10000), "HTTP Server port") ;
    return app.run(ac, av, [&] {
        auto&& config = app.configuration();
        uint16_t port = config["port"].as<uint16_t>();
        auto server = new distributed<http_server>;
        server->start().then([server = std::move(server), port] () mutable {
            server->invoke_on_all(&http_server::listen, ipv4_addr{port});
        }).then([port] {
            std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
        });
    });
}
