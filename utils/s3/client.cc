/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <rapidxml.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/http/request.hh>
#include "utils/s3/client.hh"
#include "utils/memory_data_sink.hh"
#include "utils/chunked_vector.hh"
#include "log.hh"

namespace utils {

inline size_t iovec_len(const std::vector<iovec>& iov)
{
    size_t ret = 0;
    for (auto&& e : iov) {
        ret += e.iov_len;
    }
    return ret;
}

}

namespace s3 {

static logging::logger s3l("s3");

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_await util::skip_entire_stream(in);
}

client::client(socket_address addr, private_tag)
        : _addr(std::move(addr))
        , _host(to_sstring(_addr))
        , _http(_addr)
{
}

shared_ptr<client> client::make(socket_address addr) {
    return seastar::make_shared<client>(std::move(addr), private_tag{});
}

future<uint64_t> client::get_object_size(sstring object_name) {
    s3l.trace("HEAD {}", object_name);
    auto req = http::request::make("HEAD", _host, object_name);
    uint64_t len = 0;
    co_await _http.make_request(std::move(req), [&len] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        len = rep.content_length;
        return make_ready_future<>(); // it's HEAD with no body
    });
    co_return len;
}

future<temporary_buffer<char>> client::get_object_contiguous(sstring object_name, std::optional<range> range) {
    auto req = http::request::make("GET", _host, object_name);
    http::reply::status_type expected = http::reply::status_type::ok;
    if (range) {
        auto range_header = format("bytes={}-{}", range->off, range->off + range->len - 1);
        s3l.trace("GET {} contiguous range='{}'", object_name, range_header);
        req._headers["Range"] = std::move(range_header);
        expected = http::reply::status_type::partial_content;
    } else {
        s3l.trace("GET {} contiguous", object_name);
    }

    size_t off = 0;
    std::optional<temporary_buffer<char>> ret;
    co_await _http.make_request(std::move(req), [&off, &ret, &object_name] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        auto in = std::move(in_);
        ret = temporary_buffer<char>(rep.content_length);
        s3l.trace("Consume {} bytes for {}", ret->size(), object_name);
        co_await in.consume([&off, &ret] (temporary_buffer<char> buf) mutable {
            if (buf.empty()) {
                return make_ready_future<consumption_result<char>>(stop_consuming(std::move(buf)));
            }

            size_t to_copy = std::min(ret->size() - off, buf.size());
            if (to_copy > 0) {
                std::copy_n(buf.get(), to_copy, ret->get_write() + off);
                off += to_copy;
            }
            return make_ready_future<consumption_result<char>>(continue_consuming());
        });
    }, expected);
    ret->trim(off);
    s3l.trace("Consumed {} bytes of {}", off, object_name);
    co_return std::move(*ret);
}

future<> client::put_object(sstring object_name, temporary_buffer<char> buf) {
    s3l.trace("PUT {}", object_name);
    auto req = http::request::make("PUT", _host, object_name);
    auto len = buf.size();
    req.write_body("bin", len, [buf = std::move(buf)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        try {
            co_await out.write(buf.get(), buf.size());
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::put_object(sstring object_name, ::memory_data_sink_buffers bufs) {
    s3l.trace("PUT {} (buffers)", object_name);
    auto req = http::request::make("PUT", _host, object_name);
    auto len = bufs.size();
    req.write_body("bin", len, [bufs = std::move(bufs)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        try {
            for (const auto& buf : bufs.buffers()) {
                co_await out.write(buf.get(), buf.size());
            }
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::delete_object(sstring object_name) {
    s3l.trace("DELETE {}", object_name);
    auto req = http::request::make("DELETE", _host, object_name);
    co_await _http.make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

class client::upload_sink : public data_sink_impl {
    // "Each part must be at least 5 MB in size, except the last part."
    // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
    static constexpr size_t minimum_part_size = 5 << 20;
    static constexpr int flush_concurrency = 3;

    shared_ptr<client> _client;
    http::experimental::client& _http;
    sstring _object_name;
    memory_data_sink_buffers _bufs;
    sstring _upload_id;
    utils::chunked_vector<sstring> _part_etags;
    semaphore _flush_sem{flush_concurrency};

    future<> start_upload();
    future<> finalize_upload();
    future<> maybe_flush();
    future<> do_flush();
    future<> upload_part(unsigned part_number, memory_data_sink_buffers bufs);
    future<> abort_upload();

    bool upload_started() const noexcept {
        return !_upload_id.empty();
    }

public:
    upload_sink(shared_ptr<client> cln, sstring object_name)
        : _client(std::move(cln))
        , _http(_client->_http)
        , _object_name(std::move(object_name))
    {
    }

    virtual future<> put(net::packet) override {
        throw_with_backtrace<std::runtime_error>("s3 put(net::packet) unsupported");
    }

    virtual future<> put(temporary_buffer<char> buf) override {
        _bufs.put(std::move(buf));
        return maybe_flush();
    }

    virtual future<> put(std::vector<temporary_buffer<char>> data) override {
        for (auto&& buf : data) {
            _bufs.put(std::move(buf));
        }
        return maybe_flush();
    }

    virtual future<> flush() override {
        return finalize_upload();
    }

    virtual future<> close() override;

    virtual size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }
};

future<> client::upload_sink::maybe_flush() {
    if (_bufs.size() >= minimum_part_size) {
        co_await do_flush();
    }
}

future<> client::upload_sink::do_flush() {
    if (!upload_started()) {
        co_await start_upload();
    }
    auto pn = _part_etags.size();
    _part_etags.emplace_back();
    co_await upload_part(pn, std::move(_bufs));
}

sstring parse_multipart_upload_id(sstring& body) {
    rapidxml::xml_document<> doc;
    try {
        doc.parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse initiate multipart upload response: {}", e.what());
        // The caller is supposed to check the upload-id to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc.first_node("InitiateMultipartUploadResult");
    auto uploadid_node = root_node->first_node("UploadId");
    return uploadid_node->value();
}

static constexpr std::string_view multipart_upload_complete_header =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
        "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

static constexpr std::string_view multipart_upload_complete_entry =
        "<Part><ETag>{}</ETag><PartNumber>{}</PartNumber></Part>";

static constexpr std::string_view multipart_upload_complete_trailer =
        "</CompleteMultipartUpload>";

unsigned prepare_multipart_upload_parts(const utils::chunked_vector<sstring>& etags) {
    unsigned ret = multipart_upload_complete_header.size();

    unsigned nr = 1;
    for (auto& etag : etags) {
        if (etag.empty()) {
            return 0;
        }
        ret += multipart_upload_complete_entry.size() - 4 + etag.size() + format("{}", nr).size();
        nr++;
    }
    ret += multipart_upload_complete_trailer.size();
    return ret;
}

future<> dump_multipart_upload_parts(output_stream<char> out, const utils::chunked_vector<sstring>& etags) {
    std::exception_ptr ex;
    try {
        co_await out.write(multipart_upload_complete_header.data(), multipart_upload_complete_header.size());

        unsigned nr = 1;
        for (auto& etag : etags) {
            assert(!etag.empty());
            co_await out.write(format(multipart_upload_complete_entry.data(), etag, nr));
            nr++;
        }
        co_await out.write(multipart_upload_complete_trailer.data(), multipart_upload_complete_trailer.size());
        co_await out.flush();
    } catch (...) {
        ex = std::current_exception();
    }
    co_await out.close();
    if (ex) {
        co_await coroutine::return_exception_ptr(std::move(ex));
    }
}

future<> client::upload_sink::start_upload() {
    s3l.trace("POST uploads {}", _object_name);
    auto rep = http::request::make("POST", _client->_host, _object_name);
    rep.query_parameters["uploads"] = "";
    co_await _http.make_request(std::move(rep), [this] (const http::reply& rep, input_stream<char>&& in_) -> future<> {
        auto in = std::move(in_);
        auto body = co_await util::read_entire_stream_contiguous(in);
        _upload_id = parse_multipart_upload_id(body);
        if (_upload_id.empty()) {
            co_await coroutine::return_exception(std::runtime_error("cannot initiate upload"));
        }
        s3l.trace("created uploads for {} -> id = {}", _object_name, _upload_id);
    });
}

future<> client::upload_sink::upload_part(unsigned part_number, memory_data_sink_buffers bufs) {
    s3l.trace("PUT part {} {} bytes in {} buffers (upload id {})", part_number, bufs.size(), bufs.buffers().size(), _upload_id);
    auto req = http::request::make("PUT", _client->_host, _object_name);
    req._headers["Content-Length"] = format("{}", bufs.size());
    req.query_parameters["partNumber"] = format("{}", part_number + 1);
    req.query_parameters["uploadId"] = _upload_id;
    req.write_body("bin", bufs.size(), [this, part_number, bufs = std::move(bufs)] (output_stream<char>&& out_) mutable -> future<> {
        auto out = std::move(out_);
        std::exception_ptr ex;
        s3l.trace("upload {} part data (upload id {})", part_number, _upload_id);
        try {
            for (auto&& buf : bufs.buffers()) {
                co_await out.write(buf.get(), buf.size());
            }
            co_await out.flush();
        } catch (...) {
            ex = std::current_exception();
        }
        co_await out.close();
        if (ex) {
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
    });

    // Do upload in the background so that several parts could go in parallel.
    // The semaphore is used for two things -- control the concurrency and let
    // the finalize_upload() wait in any background activity before checking
    // the progress.
    //
    // In case part upload goes wrong and doesn't happen, the _part_etags[part]
    // is not set, so the finalize_upload() sees it and aborts the whole thing.
    auto units = co_await get_units(_flush_sem, 1);
    (void)_http.make_request(std::move(req), [this, part_number] (const http::reply& rep, input_stream<char>&& in_) mutable -> future<> {
        auto etag = rep.get_header("ETag");
        s3l.trace("uploaded {} part data -> etag = {} (upload id {})", part_number, etag, _upload_id);
        _part_etags[part_number] = std::move(etag);
        return make_ready_future<>();
    }).handle_exception([this, part_number] (auto ex) {
        // ... the exact exception only remains in logs
        s3l.warn("couldn't upload part {}: {} (upload id {})", part_number, ex, _upload_id);
    }).finally([units = std::move(units)] {});
}

future<> client::upload_sink::abort_upload() {
    s3l.trace("DELETE upload {}", _upload_id);
    auto req = http::request::make("DELETE", _client->_host, _object_name);
    req.query_parameters["uploadId"] = std::move(_upload_id);
    co_await _http.make_request(std::move(req), ignore_reply, http::reply::status_type::no_content);
}

future<> client::upload_sink::finalize_upload() {
    if (_bufs.size() == 0) {
        co_return;
    }

    co_await do_flush();

    s3l.trace("wait for {} parts to complete (upload id {})", _part_etags.size(), _upload_id);
    co_await _flush_sem.wait(flush_concurrency);

    unsigned parts_xml_len = prepare_multipart_upload_parts(_part_etags);
    if (parts_xml_len == 0) {
        co_await abort_upload();
        co_await coroutine::return_exception(std::runtime_error("couldn't upload parts"));
    }

    s3l.trace("POST upload completion {} parts (upload id {})", _part_etags.size(), _upload_id);
    auto req = http::request::make("POST", _client->_host, _object_name);
    req.query_parameters["uploadId"] = std::move(_upload_id);
    req.write_body("xml", parts_xml_len, [this] (output_stream<char>&& out) -> future<> {
        return dump_multipart_upload_parts(std::move(out), _part_etags);
    });
    co_await _http.make_request(std::move(req), ignore_reply);
}

future<> client::upload_sink::close() {
    if (upload_started()) {
        s3l.warn("closing incomplete multipart upload -> aborting");
        co_await abort_upload();
    } else {
        s3l.trace("closing multipart upload");
    }
}

data_sink client::make_upload_sink(sstring object_name) {
    return data_sink(std::make_unique<upload_sink>(shared_from_this(), std::move(object_name)));
}

class client::readable_file : public file_impl {
    shared_ptr<client> _client;
    http::experimental::client& _http;
    sstring _object_name;

    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation on s3 readable file");
    }

public:
    readable_file(shared_ptr<client> cln, sstring object_name)
        : _client(std::move(cln))
        , _http(_client->_http)
        , _object_name(std::move(object_name))
    {
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override { unsupported(); }

    virtual future<> flush(void) override { return make_ready_future<>(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { return make_ready_future<>(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { return make_ready_future<>(); }

    class readable_file_handle_impl final : public file_handle_impl {
        socket_address _addr;
        sstring _object_name;

    public:
        readable_file_handle_impl(socket_address addr, sstring object_name)
                : _addr(std::move(addr))
                , _object_name(std::move(object_name))
        {}

        virtual std::unique_ptr<file_handle_impl> clone() const override {
            return std::make_unique<readable_file_handle_impl>(_addr, _object_name);
        }

        virtual shared_ptr<file_impl> to_file() && override {
            return make_shared<readable_file>(client::make(std::move(_addr)), std::move(_object_name));
        }
    };

    virtual std::unique_ptr<file_handle_impl> dup() override {
        return std::make_unique<readable_file_handle_impl>(_client->_addr, _object_name);
    }

    virtual future<uint64_t> size(void) override {
        return _client->get_object_size(_object_name);
    }

    virtual future<struct stat> stat(void) override {
        auto size = co_await _client->get_object_size(_object_name);
        struct stat ret;
        ret.st_nlink = 1;
        ret.st_mode = S_IFREG | S_IRUSR | S_IRGRP | S_IROTH;
        ret.st_size = size;
        ret.st_blksize = 1 << 10; // huh?
        ret.st_blocks = size >> 9;
        co_return ret;
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, len });
        std::copy_n(buf.get(), buf.size(), reinterpret_cast<uint8_t*>(buffer));
        co_return buf.size();
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ pos, utils::iovec_len(iov) });
        uint64_t off = 0;
        for (auto& v : iov) {
            auto sz = std::min(v.iov_len, buf.size() - off);
            if (sz == 0) {
                break;
            }
            std::copy_n(buf.get() + off, sz, reinterpret_cast<uint8_t*>(v.iov_base));
            off += sz;
        }
        co_return off;
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        auto buf = co_await _client->get_object_contiguous(_object_name, range{ offset, range_size });
        co_return temporary_buffer<uint8_t>(reinterpret_cast<uint8_t*>(buf.get_write()), buf.size(), buf.release());
    }

    virtual future<> close() override {
        return make_ready_future<>();
    }
};

file client::make_readable_file(sstring object_name) {
    return file(make_shared<readable_file>(shared_from_this(), std::move(object_name)));
}

future<> client::close() {
    co_await _http.close();
}

} // s3 namespace
