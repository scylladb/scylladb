/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "s3/s3.hh"
#include "to_string.hh"
#include "utils/phased_barrier.hh"
#include "utils/memory_data_sink.hh"

#include <seastar/core/reactor.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/gate.hh>
#include <seastar/util/log.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/iostream.hh>
#include <seastar/http/response_parser.hh>
#include <boost/algorithm/string/trim.hpp>

#include <unordered_map>

using namespace seastar;

namespace s3 {

seastar::logger log("s3");

struct s3_http_response : public http_response {
    uint64_t content_length;
    temporary_buffer<char> content;

    sstring get_header(const sstring& name) {
        auto it = _headers.find(name);
        if (it == _headers.end()) {
            throw_with_backtrace<std::runtime_error>(format("Expected header not found: {}", name));
        }
        return boost::trim_copy(it->second);
    }

    std::string_view content_as_string() const {
        return std::string_view(content.begin(), content.size());
    }
};

std::ostream& operator<<(std::ostream& out, s3_http_response& r) {
    return out << "http_response{"
        << "status=" << r._status_code
        << ", headers=" << r._headers
        << ", body=" << std::string_view(r.content.begin(), r.content.size())
        << "}";
}

struct http_content_writer {
    size_t size;
    noncopyable_function<future<>(output_stream<char>&)> writer;
};

// Keep headers alive around async operation.
static
future<s3_http_response> make_http_request(connection_ptr con,
                                        const sstring& method,
                                        const sstring& path,
                                        const std::unordered_map<sstring, sstring>& headers,
                                        std::optional<http_content_writer> content) {
    log.debug("conn {}: {} {} {}", fmt::ptr(&*con), method, path, headers);

    sstring method_ = method;
    sstring path_ = path;
    auto& out = con->out();

    co_await out.write(method_);
    co_await out.write(" ");
    co_await out.write(path_);
    co_await out.write(" HTTP/1.1\r\n");

    for (auto&& [key, value] : headers) {
        co_await out.write(key);
        co_await out.write(": ");
        co_await out.write(value);
        co_await out.write("\r\n");
    }

    if (content) {
        co_await out.write("Content-length: ");
        co_await out.write(to_sstring(content->size));
        co_await out.write("\r\n");
        co_await out.write("\r\n");
        co_await content->writer(out);
    } else {
        co_await out.write("\r\n");
    }

    co_await out.flush();

    auto& in = con->in();
    http_response_parser parser;
    parser.init();
    co_await in.consume(parser);

    auto parsed_response = parser.get_parsed_response();
    if (!parsed_response) {
        co_await coroutine::return_exception(std::runtime_error("Parsing HTTP response failed"));
    }
    s3_http_response response{std::move(*parsed_response)};

    if (response._status_code != 204) { // 204 = No Content
        response.content_length = [&response] {
            auto it = response._headers.find("Content-Length");
            return it != response._headers.end() ? std::atoi(it->second.c_str()) : 0;
        }();
        response.content = co_await in.read_exactly(response.content_length);
    }

    if (response._status_code < 200 || response._status_code >= 300) {
        co_await coroutine::return_exception(std::runtime_error(format("request failed: {}, {}", response._status_code, (const char*)response.content.get())));
    }
    co_return response;
}

// Keep headers alive around async operation.
static
future<s3_http_response> make_http_request(connection_ptr con,
                                        const sstring& method,
                                        const sstring& path,
                                        const std::unordered_map<sstring, sstring>& headers = {},
                                        std::optional<sstring> content = std::nullopt) {
    if (!content) {
        std::optional<http_content_writer> wr = std::nullopt;
        return make_http_request(con, method, path, headers, std::move(wr));
    }
    auto content_size = content->size();
    return make_http_request(con, method, path, headers, http_content_writer{content_size,
         [content = std::move(content)] (output_stream<char>& out) {
             return out.write(*content);
         }}
    );
}

sstring parse_xml_tag(std::string_view xml, const sstring& name) {
    auto i = xml.find(name);
    if (i == std::string_view::npos) {
        throw_with_backtrace<std::runtime_error>(format("Response does not contain <{}>: {}", name, xml));
    }
    i += name.size();
    if (i == xml.size() || xml[i] != '>') {
        throw_with_backtrace<std::runtime_error>(format("Response does not contain <{}>: {}", name, xml));
    }
    ++i;
    auto e = xml.find(name, i);
    if (e == std::string_view::npos) {
        throw_with_backtrace<std::runtime_error>(format("Response does not contain </{}>: {}", name, xml));
    }
    if (e == i || xml[e - 1] != '/') {
        throw_with_backtrace<std::runtime_error>(format("Response does not contain </{}>: {}", name, xml));
    }
    --e;
    if (e == i || xml[e - 1] != '<') {
        throw_with_backtrace<std::runtime_error>(format("Response does not contain </{}>: {}", name, xml));
    }
    --e;
    return sstring(xml.begin() + i, xml.begin() + e);
}

future<> connection::close() {
    _closed = true;
    co_await _out.close();
    co_await _in.close();
}

class s3_file_impl : public file_impl {
    client_ptr _client;
    sstring _path;
private:
    [[noreturn]] void unsupported(const char* name) {
        throw_with_backtrace<std::logic_error>(format("unsupported operation: {}", name));
    }
public:
    s3_file_impl(client_ptr c, sstring path)
        : _client(std::move(c))
        , _path(std::move(path))
    { }

    // unsupported
    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override { unsupported(__FUNCTION__); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override { unsupported(__FUNCTION__); }
    virtual future<> flush(void) override { unsupported(__FUNCTION__); }
    virtual future<> truncate(uint64_t length) override { unsupported(__FUNCTION__); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { unsupported(__FUNCTION__); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { unsupported(__FUNCTION__); }
    virtual subscription<directory_entry> list_directory(std::function<future<>(directory_entry)>) override { unsupported(__FUNCTION__); }

    virtual future<struct stat> stat(void) override {
        unsupported(__FUNCTION__);
    }

    virtual future<uint64_t> size(void) override {
        return _client->get_size(_path);
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }

    virtual std::unique_ptr<seastar::file_handle_impl> dup() override {
        unsupported(__FUNCTION__); // FIXME
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t size, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object(_path, offset, size);
        co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), buf.size(), buf.release());
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        unsupported(__FUNCTION__); // FIXME
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        unsupported(__FUNCTION__); // FIXME
    }
};

connection_ptr::~connection_ptr() {
    if (!_ptr || !_ptr.owned() || _ptr->closed()) {
        return;
    }
    auto ptr = std::move(_ptr);
    log.debug("conn {}: not closed, closing in background", fmt::ptr(&*ptr));
    (void)ptr->close()
        .then([ptr] {
            log.debug("conn {}: closed", fmt::ptr(&*ptr));
        }).handle_exception([ptr](auto ep) {
            log.error("conn {}: failed to close: {}", fmt::ptr(&*ptr), ep);
        });
}

class client_impl : public client, public enable_shared_from_this<client_impl> {
    connection_factory_ptr _connection_factory;
    std::unordered_map<sstring, sstring> _headers;
protected:
    future<connection_ptr> new_connection() {
        return _connection_factory->connect();
    }

    template <typename Func, typename FuncResult = std::invoke_result_t<Func, connection_ptr>>
    futurize_t<FuncResult>
    do_with_connection(Func func) {
        return new_connection().then([func = std::move(func), this] (connection_ptr con) mutable {
            return futurize_invoke(std::move(func), con)
                    .then_wrapped([con, this] (auto&& f) mutable {
                        if (!f.failed()) {
                            _connection_factory->take_back(con);
                            return std::move(f);
                        } else {
                            return con->close()
                                .handle_exception([con] (auto ep) mutable {
                                    log.error("conn {}: failed to close: {}", fmt::ptr(&*con), ep);
                                }).then([con, f = std::move(f)] () mutable {
                                    return std::move(f);
                                });
                        }
                    });
        });
    }
private:
    class s3_upload_data_sink : public data_sink_impl {
        // "Each part must be at least 5 MB in size, except the last part."
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
        static constexpr size_t minimum_part_size = 5*1024*1024;

        struct part {
            sstring etag;
        };

        client_ptr _client;
        client_impl& _client_impl;
        const sstring _path;
        sstring _upload_id; // non-empty when upload started.
        uint64_t _part_number = 0;
        circular_buffer<temporary_buffer<char>> _current_part;
        seastar::gate _uploads;
        utils::chunked_vector<part> _parts;
        utils::phased_barrier _uploads_barrier;
        semaphore _flush_sem{2}; // allow for two concurrent uploads
        bool _upload_done = false; // true if the upload was completed or aborted on the server side
    private:
        size_t current_part_size() const {
            size_t res = 0;
            for (auto&& buf : _current_part) {
                res += buf.size();
            }
            return res;
        }

        future<> maybe_flush() {
            if (current_part_size() >= minimum_part_size) {
                auto units = co_await get_units(_flush_sem, 1);
                if (!upload_started()) {
                    co_await start_upload();
                }
                do_flush(std::move(units));
            }
            co_return;
        }

        void do_flush(std::optional<semaphore_units<>> units = std::nullopt) {
            size_t size = current_part_size();
            auto part_number = _part_number++;
            _parts.emplace_back();

            http_content_writer wr{size, [part = std::move(_current_part)] (output_stream<char>& out) mutable -> future<> {
                for (auto&& buf : part) {
                    co_await out.write(buf.begin(), buf.size());
                }
            }};

            // The operation is tracked under _uploads_barrier
            (void)_client_impl.do_with_connection([part_number, wr = std::move(wr), this] (connection_ptr con) mutable -> future<> {
                // UploadPart: https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
                auto part_number_ = part_number;
                s3_http_response resp = co_await make_http_request(con, "PUT",
                    format("{}?partNumber={}&uploadId={}", _path, part_number, _upload_id), _client_impl._headers, std::move(wr));
                _parts[part_number_].etag = resp.get_header("ETag");
            }).finally([op = _uploads_barrier.start(), units = std::move(units)] {});
        }

        future<> start_upload() {
            _upload_id = co_await _client_impl.do_with_connection([this] (connection_ptr con) -> future<sstring> {
                // CreateMultipartUpload: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateMultipartUpload.html
                s3_http_response resp = co_await make_http_request(con, "POST", _path + "?uploads", _client_impl._headers);
                co_return parse_xml_tag(resp.content_as_string(), "UploadId");
            });
        }

        // Aborts the upload.
        // Static so that the can be invoked from the destructor and doesn't depend on the instance to be alive.
        // No flushes can be in progress after this in called.
        static future<> do_abort_upload(client_ptr c, client_impl& impl, sstring path, sstring upload_id) {
            // AbortMultipartUpload: https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
            co_await impl.do_with_connection([&impl, path, upload_id] (connection_ptr con) mutable -> future<> {
                co_await make_http_request(con, "DELETE", format("{}?uploadId={}", path, upload_id), impl._headers);
            });
            log.debug("Upload ({}) of {} aborted.", upload_id, path);
            // keep c alive until here
        }

        // Aborts the upload.
        // Precondition: !upload_active()
        future<> abort_upload() {
            log.debug("Aborting upload {} of {}", _upload_id, _path);
            co_await do_abort_upload(_client, _client_impl, _path, _upload_id);
            _upload_done = true;
        }

        // Completes the upload.
        // Precondition: upload_active()
        // No flushes can be in progress when this in called.
        future<> complete_upload_on_server() {
            // CompleteMultipartUpload: https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html
            memory_data_sink_buffers bufs;
            {
                output_stream<char> out(data_sink(std::make_unique<memory_data_sink>(bufs)));
                co_await out.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n");
                co_await out.write("<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">");
                uint64_t id = 0;
                for (part& p : _parts) {
                    co_await out.write("<Part><ETag>");
                    co_await out.write(p.etag);
                    co_await out.write("</ETag><PartNumber>");
                    co_await out.write(to_sstring(id++));
                    co_await out.write("</PartNumber></Part>");
                }
                co_await out.write("</CompleteMultipartUpload>");
                co_await out.close();
            }

            size_t size = 0;
            for (auto&& buf : bufs.buffers()) {
                size += buf.size();
            }

            http_content_writer wr{size, [bufs = std::move(bufs)] (output_stream<char>& out) mutable -> future<> {
                for (auto&& buf : bufs.buffers()) {
                    co_await out.write(buf.begin(), buf.size());
                }
            }};

            log.debug("Upload {} of {} finalizing, size={}, parts={}", _upload_id, _path, size, _parts.size());

            co_await _client_impl.do_with_connection([this, wr = std::move(wr)] (connection_ptr con) mutable -> future<> {
                co_await make_http_request(con, "POST", format("{}?uploadId={}", _path, _upload_id), _client_impl._headers, std::move(wr));
            });

            log.debug("Upload {} of {} completed", _upload_id, _path);
            _upload_done = true;
        }

        bool upload_started() const {
            return !_upload_id.empty();
        }

        bool upload_active() const {
            return !_upload_done && upload_started();
        }
    public:
        s3_upload_data_sink(client_ptr client, client_impl& impl, sstring path)
            : _client(client)
            , _client_impl(impl)
            , _path(std::move(path))
        { }

        ~s3_upload_data_sink() {
            assert(_uploads_barrier.operations_in_progress() == 0);
            if (upload_active()) {
                log.warn("Stream destroyed without explicit close() for upload {} of {}, aborting in the background", _upload_id, _path);
                (void)do_abort_upload(_client, _client_impl, _path, _upload_id);
            }
        }

        future<> put(net::packet data) override {
            throw_with_backtrace<std::runtime_error>(format("unsupported: {}", __FUNCTION__));
        }

        future<> put(temporary_buffer<char> buf) override {
            _current_part.emplace_back(std::move(buf));
            return maybe_flush();
        }

        future<> put(std::vector<temporary_buffer<char>> data) override {
            for (auto&& buf : data) {
                _current_part.emplace_back(std::move(buf));
            }
            return maybe_flush();
        }

        future<> finalize_upload() {
            if (!upload_started()) {
                // FIXME: Use a single PUT if upload not started yet.
                co_await start_upload();
            }
            do_flush();
            co_await _uploads_barrier.advance_and_await();
            co_await complete_upload_on_server();
        }

        future<> close() override {
            return finalize_upload().finally([this] {
                if (upload_active()) {
                    return abort_upload();
                }
                return make_ready_future<>();
            });
        }

        size_t buffer_size() const noexcept override {
            return 128*1024;
        }
    };
public:
    client_impl(connection_factory_ptr cf)
        : _connection_factory(std::move(cf))
    {
        _headers["Host"] = _connection_factory->host_name();
        _headers["Connection"] = "Keep-Alive";
    }

    future<> remove_file(const sstring& path) override {
        co_await do_with_connection([path, this] (auto con) {
            return make_http_request(con, "DELETE", path, _headers).discard_result();
        });
    }

    future<data_sink> upload(const seastar::sstring& path) override {
        co_return data_sink(std::make_unique<s3_upload_data_sink>(shared_from_this(), *this, path));
    }

    future<temporary_buffer<char>> get_object(const seastar::sstring& path, uint64_t offset, size_t length) override {
        return do_with_connection([path, offset, length, this] (auto con) -> future<temporary_buffer<char>> {
            auto headers = _headers;
            headers["Range"] = format("bytes={}-{}", offset, offset + length - 1);
            s3_http_response resp = co_await make_http_request(con, "GET", path, headers);
            co_return std::move(resp.content);
        });
    }

    future<uint64_t> get_size(const seastar::sstring& path) override {
        return do_with_connection([this, path] (connection_ptr con) -> future<uint64_t> {
            s3_http_response resp = co_await make_http_request(con, "HEAD", path, _headers);
            co_return resp.content_length;
        });
    }

    future<file> open(const sstring& path) override {
        co_return file(make_shared<s3_file_impl>(shared_from_this(), path));
    }
};

seastar::shared_ptr<client> make_client(connection_factory_ptr cf) {
    return seastar::make_shared<client_impl>(std::move(cf));
}

class basic_connection_factory : public connection_factory {
    seastar::socket_address _addr;
    sstring _host;
    seastar::socket _s = seastar::make_socket();
    circular_buffer<connection_ptr> _pool;
public:
    basic_connection_factory(sstring host, uint16_t port)
        : _addr(seastar::ipv4_addr(host, port))
        , _host(std::move(host))
    { }

    sstring host_name() override {
        return _host;
    }

    future<connection_ptr> connect() override {
        if (!_pool.empty()) {
            auto con = _pool.front();
            _pool.pop_front();
            log.debug("conn {}: returning from pool", fmt::ptr(&*con));
            co_return con;
        }
        auto s = seastar::make_socket();
        auto cs = co_await s.connect(_addr);
        auto con = connection_ptr(seastar::make_lw_shared<connection>(std::move(cs), std::move(s)));
        log.debug("conn {}: connected to {}", fmt::ptr(&*con), _addr);
        co_return con;
    }

    void take_back(connection_ptr con) override {
        log.debug("conn {}: returning to pool ({} total)", fmt::ptr(&*con), _pool.size() + 1);
        _pool.emplace_back(std::move(con));
    }
};

connection_factory_ptr make_basic_connection_factory(sstring host, uint16_t port) {
    return seastar::make_shared<basic_connection_factory>(std::move(host), port);
}

}
