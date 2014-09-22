/*
 * Copyright 2014 Cloudius Systems
 */

#include "core/reactor.hh"
#include "core/sstring.hh"
#include <iostream>
#include <algorithm>
#include <unordered_map>
#include <queue>
#include <bitset>
#include <limits>
#include <cctype>

class char_filter {
    using uchar = unsigned char;
public:
    template <typename Pred>
    explicit char_filter(Pred pred) {
        for (unsigned i = 0; i <= std::numeric_limits<uchar>::max(); ++i) {
            _filter.set(i, pred(i));
        }
    }
    bool operator()(char v) const { return _filter.test(uchar(v)); }
private:
    std::bitset<1 << std::numeric_limits<uchar>::digits> _filter;
};

char_filter tchar([](char c) {
    return std::isalnum(c) || std::string("-!#$%&'\\*\\+.^_`|~").find_first_of(c) != std::string::npos;
});
char_filter op_char([](char c) { return std::isupper(c); });
char_filter sp_char([](char c) { return std::isspace(c); });
char_filter nsp_char([](char c) { return !std::isspace(c); });
char_filter digit_char([](char c) { return std::isdigit(c); });

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
        connection(http_server& server, connected_socket&& fd, socket_address addr)
            : _fd(std::move(fd)), _read_buf(_fd.input())
            , _write_buf(_fd.output()) {}
        future<> read() {
            return _read_buf.read_until(limit, '\n').then([this] (tmp_buf start_line) {
                if (!start_line.size()) {
                    _eof = true;
                    maybe_done();
                    return make_ready_future<>();
                }
                _req = std::make_unique<request>();
                size_t pos = 0;
                size_t end = start_line.size();
                while (pos < end && op_char(start_line[pos])) {
                    ++pos;
                }
                if (pos == 0) {
                    return bad(std::move(_req));
                }
                _req->_method = sstring(start_line.begin(), pos);
                if (pos == end || start_line[pos++] != ' ') {
                    return bad(std::move(_req));
                }
                auto url = pos;
                while (pos < end && nsp_char(start_line[pos])) {
                    ++pos;
                }
                _req->_url = sstring(start_line.begin() + url, pos - url);
                if (pos == end || start_line[pos++] != ' ') {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != 'H') {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != 'T') {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != 'T') {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != 'P') {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != '/') {
                    return bad(std::move(_req));
                }
                auto ver = pos;
                if (pos == end || !digit_char(start_line[pos++])) {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != '.') {
                    return bad(std::move(_req));
                }
                if (pos == end || !digit_char(start_line[pos++])) {
                    return bad(std::move(_req));
                }
                _req->_version = sstring(start_line.begin() + ver, pos - ver);
                if (pos == end || start_line[pos++] != '\r') {
                    return bad(std::move(_req));
                }
                if (pos == end || start_line[pos++] != '\n') {
                    return bad(std::move(_req));
                }
                if (pos != end) {
                    return bad(std::move(_req));
                }
                if (_req->_method != "GET") {
                    return bad(std::move(_req));
                }
                return _read_buf.read_until(limit, '\n').then([this] (tmp_buf header) {
                    return parse_header(std::move(header));
                });
            });
        }
        future<> parse_header(tmp_buf header) {
            if (header.size() == 2 && header[0] == '\r' && header[1] == '\n') {
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
            }
            size_t pos = 0;
            size_t end = header.size();
            if (end < 2 || header[end-2] != '\r' || header[end-1] != '\n') {
                return bad(std::move(_req));
            }
            while (tchar(header[pos])) {
                ++pos;
            }
            if (pos) {
                sstring name = sstring(header.begin(), pos);
                while (pos != end && sp_char(header[pos])) {
                    ++pos;
                }
                if (pos == end || header[pos++] != ':') {
                    return bad(std::move(_req));
                }
                while (pos != end && sp_char(header[pos])) {
                    ++pos;
                }
                while (pos != end && sp_char(header[end-1])) {
                    --end;
                }
                sstring value = sstring(header.begin() + pos, end - pos);
                _req->_headers[name] = std::move(value);
                _last_header_name = std::move(name);
            } else {
                while (sp_char(header[pos])) {
                    ++pos;
                }
                while (pos != end && sp_char(header[end-1])) {
                    --end;
                }
                if (!pos || end == pos) {
                    return bad(std::move(_req));
                }
                _req->_headers[_last_header_name] += " ";
                _req->_headers[_last_header_name] += sstring(header.begin() + pos, end - pos);
            }
            return _read_buf.read_until(limit, '\n').then([this] (tmp_buf header) {
                return parse_header(std::move(header));
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
                    [this] (size_t n) mutable {
                return write_response_headers(_resp->_headers.begin());
            }).then([this] (size_t done) {
                return _write_buf.write("\r\n", 2);
            }).then([this] (size_t done) mutable {
                return write_body();
            }).then([this] (size_t done) {
                return _write_buf.flush();
            }).then([this] (bool done) {
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
        future<size_t> write_response_headers(std::unordered_map<sstring, sstring>::iterator hi) {
            if (hi == _resp->_headers.end()) {
                return make_ready_future<size_t>(0);
            }
            return _write_buf.write(hi->first.begin(), hi->first.size()).then(
                    [hi, this] (size_t done) mutable {
                return _write_buf.write(": ", 2);
            }).then([hi, this] (size_t done) mutable {
                return _write_buf.write(hi->second.begin(), hi->second.size());
            }).then([hi, this] (size_t done) mutable {
                return _write_buf.write("\r\n", 2);
            }).then([hi, this] (size_t done) mutable {
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
    namespace bpo = boost::program_options;
    bpo::options_description opts;
    opts.add_options()
       ("help", "show help message")
       ;
    opts.add(reactor::get_options_description());
    bpo::variables_map configuration;
    bpo::store(bpo::command_line_parser(ac, av).options(opts).run(), configuration);
    bpo::notify(configuration);
    if (configuration.count("help")) {
        std::cout << opts << "\n";
        return 1;
    }
    engine.configure(configuration);
    engine.when_started().then([&server] {
        uint16_t port = 10000;
        std::cout << "Seastar HTTP server listening on port " << port << " ...\n";
        server.listen({{}, port});
    });
    engine.run();
    return 0;
}


