/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "object_storage.hh"
#include "gcp_credentials.hh"

#include <algorithm>
#include <numeric>
#include <deque>

#include <boost/regex.hpp>

#include <seastar/core/gate.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/http/client.hh>
#include <seastar/util/short_streams.hh>

#include "utils/rest/client.hh"
#include "utils/http.hh"
#include "utils/overloaded_functor.hh"

static logger gcp_storage("gcp_storage");

static constexpr uint64_t min_gcp_storage_chunk_size =  256*1024;
static constexpr uint64_t default_gcp_storage_chunk_size =  8*1024*1024;

static constexpr char GCP_OBJECT_SCOPE_READ_ONLY[] = "https://www.googleapis.com/auth/devstorage.read_only";
static constexpr char GCP_OBJECT_SCOPE_READ_WRITE[] = "https://www.googleapis.com/auth/devstorage.read_write";
static constexpr char GCP_OBJECT_SCOPE_FULL_CONTROL[] = "https://www.googleapis.com/auth/devstorage.full_control";

static constexpr char STORAGE_APIS_URI[] = "https://storage.googleapis.com";
static constexpr char APPLICATION_JSON[] = "application/json";
static constexpr char LOCATION[] = "Location";
static constexpr char CONTENT_RANGE[] = "Content-Range";
static constexpr char RANGE[] = "Range";

using namespace std::string_literals;
using namespace utils::gcp;

static bool storage_scope_implies(const scopes_type& scopes, const scopes_type& check_for) {
    if (default_scopes_implies_other_scope(scopes, check_for)) {
        return true;
    }
    if (scopes_contains_scope(check_for, GCP_OBJECT_SCOPE_READ_ONLY)) {
        return scopes_contains_scope(scopes, GCP_OBJECT_SCOPE_READ_WRITE) 
            || scopes_contains_scope(scopes, GCP_OBJECT_SCOPE_FULL_CONTROL)
            ;
    }
    if (scopes_contains_scope(check_for, GCP_OBJECT_SCOPE_READ_WRITE)) {
        return scopes_contains_scope(scopes, GCP_OBJECT_SCOPE_FULL_CONTROL);
    }
    return false;
}

class utils::gcp::storage::client::object_data_sink : public data_sink_impl {
    shared_ptr<impl> _impl;
    std::string _bucket;
    std::string _object_name;
    rjson::value _metadata;
    std::string _session_path;
    std::string _content_type;
    std::deque<temporary_buffer<char>> _buffers;
    uint64_t _accumulated = 0;
    bool _closed = false;
    bool _completed = false;
    seastar::named_gate _gate;
    seastar::semaphore _semaphore;
    std::exception_ptr _exception;

public:
    object_data_sink(shared_ptr<impl> i, std::string_view bucket, std::string_view object_name, rjson::value metadata)
        : _impl(i)
        , _bucket(bucket)
        , _object_name(object_name)
        , _metadata(std::move(metadata))
        , _semaphore(1)
    {}
    future<> put(net::packet data) override {
        return fallback_put(std::move(data));
    }
    future<> put(std::vector<temporary_buffer<char>> data) override {
        for (auto&& buf : data) {
            co_await put(std::move(buf));
        }
    }
    future<> put(temporary_buffer<char> buf) override {
        _buffers.emplace_back(std::move(buf));
        co_await maybe_do_upload(false);
    }
    future<> flush() override {
        return maybe_do_upload(true);
    }
    future<> close() override {
        if (!std::exchange(_closed, true)) {
            co_await flush();
            co_await _gate.close();
            try {
                if (!_exception && !_completed) {
                    co_await check_upload();
                }
            } catch (...) {
                _exception = std::current_exception();
            }
            if (auto ex = std::exchange(_exception, {})) {
                co_await remove_upload();
                std::rethrow_exception(ex);
            }
        }
    }

    size_t buffer_size() const noexcept override {
        return default_gcp_storage_chunk_size;
    }

    future<> acquire_session();
    future<> do_single_upload(std::deque<temporary_buffer<char>>, size_t offset, size_t len, bool final);
    future<> check_upload();
    future<> remove_upload();
    future<> maybe_do_upload(bool force) {
        auto total = std::accumulate(_buffers.begin(), _buffers.end(), size_t{}, [](size_t s, auto& buf) {
            return s + buf.size();
        });
        if (total == 0) {
            co_return;
        }
        // GCP only allows upload of less than 256k on last chunk
        if (total < min_gcp_storage_chunk_size && !_closed) {
            co_return;
        }
        // avoid uploading unless we accumulate enough data. GCP docs says to
        // try to keep uploads to 8MB or more.
        if (force || total >= default_gcp_storage_chunk_size) {
            auto bufs = std::exchange(_buffers, {});
            auto start = _accumulated;
            auto final = _closed;

            if (!final) {
                // can only write in multiples of 256.
                auto rem = total % min_gcp_storage_chunk_size;
                total -= rem;
                while (rem) {
                    auto& buf = bufs.back();
                    if (buf.size() > rem) {
                        auto keep = buf.size() - rem;
                        _buffers.emplace_back(buf.share(keep, rem));
                        buf.trim(keep);
                        break;
                    } else {
                        rem -= buf.size();
                        _buffers.emplace_back(std::move(buf));
                        bufs.pop_back();
                    }
                }
            }
            assert(std::accumulate(bufs.begin(), bufs.end(), size_t{}, [](size_t s, auto& buf) { return s + buf.size(); }) == total);
            _accumulated += total;
            // allow this in background
            (void)do_single_upload(std::move(bufs), start, total, final);
        }
    }
};

class utils::gcp::storage::client::object_data_source : public data_source_impl {
    shared_ptr<impl> _impl;
    std::string _bucket;
    std::string _object_name;
    std::string _session_path;
    uint64_t _generation = 0;
    uint64_t _size = 0;
    uint64_t _position = 0;
    std::deque<temporary_buffer<char>> _buffers;
public:
    object_data_source(shared_ptr<impl> i, std::string_view bucket, std::string_view object_name)
        : _impl(i)
        , _bucket(bucket)
        , _object_name(object_name)
    {}
    future<temporary_buffer<char>> get() override;
    future<temporary_buffer<char>> skip(uint64_t n) override;
    future<> read_info();
};

using body_writer = std::function<future<>(output_stream<char>&&)>;
using writer_and_size = std::pair<body_writer, size_t>;
using body_variant = std::variant<std::string, writer_and_size>;
using handler_func_ex = rest::handler_func_ex;
using headers_type = std::vector<rest::key_value>;

using namespace rest;

class utils::gcp::storage::client::impl {
    std::string _endpoint;
    std::optional<google_credentials> _credentials;
    seastar::http::experimental::client _client;
    shared_ptr<seastar::tls::certificate_credentials> _certs;
public:
    impl(const utils::http::url_info&, std::optional<google_credentials>, shared_ptr<seastar::tls::certificate_credentials> creds);
    impl(std::string_view endpoint, std::optional<google_credentials>, shared_ptr<seastar::tls::certificate_credentials> creds);

    future<> send_with_retry(const std::string& path, const std::string& scope, body_variant, std::string_view content_type, handler_func_ex, httpclient::method_type op, key_values headers = {});
    future<> send_with_retry(const std::string& path, const std::string& scope, body_variant, std::string_view content_type, rest::httpclient::handler_func, httpclient::method_type op, key_values headers = {});
    future<rest::httpclient::result_type> send_with_retry(const std::string& path, const std::string& scope, body_variant, std::string_view content_type, httpclient::method_type op, key_values headers = {});

    future<> close();
};

utils::gcp::storage::client::impl::impl(const utils::http::url_info& url, std::optional<google_credentials> c, shared_ptr<seastar::tls::certificate_credentials> certs)
    : _endpoint(url.host)
    , _credentials(std::move(c))
    , _client(std::make_unique<utils::http::dns_connection_factory>(url.host, url.port, url.is_https(), gcp_storage, certs), 100, seastar::http::experimental::client::retry_requests::yes)
{}

utils::gcp::storage::client::impl::impl(std::string_view endpoint, std::optional<google_credentials> c, shared_ptr<seastar::tls::certificate_credentials> certs)
    : impl(utils::http::parse_simple_url(endpoint.empty() ? STORAGE_APIS_URI : endpoint), std::move(c), std::move(certs))
{}

using status_type = seastar::http::reply::status_type;

static std::string get_gcp_error_message(std::string_view body) {
    if (!body.empty()) {
        try {
            auto json = rjson::parse(body);
            if (auto* error = rjson::find(json, "error")) {
                if (auto msg = rjson::get_opt<std::string>(*error, "message")) {
                    return *msg;
                }
            }
        } catch (...) {
        }
    }
    return "no info";
}

static future<std::string> get_gcp_error_message(input_stream<char>& in) {
    auto s = co_await util::read_entire_stream_contiguous(in);
    co_return get_gcp_error_message(s);
}

utils::gcp::storage::storage_error::storage_error(const std::string& msg)
    : std::runtime_error(msg)
    , _status(-1)
{}

utils::gcp::storage::storage_error::storage_error(int status, const std::string& msg)
    : std::runtime_error(fmt::format("{}: {}", status, msg))
    , _status(status)
{}

using namespace seastar::http;
using namespace std::chrono_literals;

static constexpr auto max_retries = 4;

static future<> backoff(int n) {
    // exponential backoff
    return seastar::sleep((1 << (std::max(n, 1) - 1)) * 300ms);
}

/**
 * Performs a REST post/put/get with credential refresh/retry.
 */
future<> 
utils::gcp::storage::client::impl::send_with_retry(const std::string& path, const std::string& scope, body_variant body, std::string_view content_type, handler_func_ex handler, httpclient::method_type op, key_values headers) {
    int retries = 0;
    bool do_backoff = false;

    for (;;) {
        rest::request_wrapper req(_endpoint);
        req.target(path);
        req.method(op);

        if (do_backoff) {
            co_await backoff(retries);
        }

        if (_credentials) {
            try {
                co_await _credentials->refresh(scope, &storage_scope_implies, _certs);
                req.add_header(utils::gcp::AUTHORIZATION, format_bearer(_credentials->token));
            } catch (...) {
                gcp_storage.error("Error refreshing credentials: {}", std::current_exception());
                std::throw_with_nested(permission_error("Error refreshing credentials"));
            }
        }

        for (auto& [k,v] : headers) {
            req.add_header(k, v);
        }

        if (!content_type.empty()) {
            req.add_header(httpclient::CONTENT_TYPE_HEADER, content_type);
        }

        std::visit(overloaded_functor {
            [&](const std::string& s) { req.content(s); },
            [&](const writer_and_size& ws) { req.content(ws.first, ws.second); }
        }, body);

        // GCP storage requires this even if content is empty
        req.add_header("Content-Length", std::to_string(req.request().content_length));

        gcp_storage.trace("Sending: {}", redacted_request_type {
            req.request(),
            bearer_filter()
        });

        try {
            co_await rest::simple_send(_client, req, [&handler](const seastar::http::reply& res, seastar::input_stream<char>& in) -> future<> {
                gcp_storage.trace("Result: {}", res);
                if (res._status == status_type::unauthorized) {
                    throw permission_error(int(res._status), co_await get_gcp_error_message(in));
                } else if (res._status == status_type::request_timeout || reply::classify_status(res._status) == reply::status_class::server_error) {
                    throw storage_error(int(res._status), co_await get_gcp_error_message(in));
                }
                co_await handler(res, in);
            });
            break;
        } catch (storage_error& e) {
            gcp_storage.debug("{}: Got unexpected response: {}", _endpoint, e.what());
            auto s = status_type(e.status());
            do_backoff = false;
            switch (s) {
            default:
                if (reply::classify_status(s) != reply::status_class::server_error) {
                    break;
                }
                [[fallthrough]];
            case status_type::request_timeout:
                do_backoff = true;
                [[fallthrough]];
            case status_type::unauthorized:
                if (retries++ < max_retries) {
                    continue; // retry loop. 
                }
                break;
            }
            throw;
        } catch (...) {
            // network, whatnot. maybe add retries here as well, but should really 
            // be on seastar level
            throw;
        }
    }
}

future<> 
utils::gcp::storage::client::impl::send_with_retry(const std::string& path, const std::string& scope, body_variant body, std::string_view content_type, rest::httpclient::handler_func f, httpclient::method_type op, key_values headers) {
    co_await send_with_retry(path, scope, std::move(body), content_type, [f](const seastar::http::reply& rep, seastar::input_stream<char>& in) -> future<> {
        // ensure these are on our coroutine frame.
        auto& resp_handler = f;
        auto result = co_await util::read_entire_stream_contiguous(in);
        resp_handler(rep, result);
    }, op, headers);
}

future<rest::httpclient::result_type> 
utils::gcp::storage::client::impl::send_with_retry(const std::string& path, const std::string& scope, body_variant body, std::string_view content_type, httpclient::method_type op, key_values headers) {
    rest::httpclient::result_type res;
    co_await send_with_retry(path, scope, std::move(body), content_type, [&res](const seastar::http::reply& r, std::string_view body)  {
        gcp_storage.trace("{}", body);
        res.reply._status = r._status;
        res.reply._content = sstring(body);
        res.reply._headers = r._headers;
        res.reply._version = r._version;
    }, op, headers);
    co_return res;
}

future<> utils::gcp::storage::client::impl::close() {
    co_await _client.close();
}

// Get an upload session for the given object
// See https://cloud.google.com/storage/docs/resumable-uploads
// See https://cloud.google.com/storage/docs/performing-resumable-uploads
future<> utils::gcp::storage::client::object_data_sink::acquire_session() {
    std::string body;
    if (!_metadata.IsNull()) {
        body = rjson::print(_metadata);
    }
    auto path = fmt::format("/upload/storage/v1/b/{}/o?uploadType=resumable&name={}"
        , _bucket
        , _object_name
    );

    auto reply = co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_READ_WRITE
        , std::move(body)
        , APPLICATION_JSON
        , httpclient::method_type::POST
    );

    if (reply.result() != status_type::ok) {
        throw failed_operation(int(reply.result()), get_gcp_error_message(reply.body()));
    }
    std::string location = reply.reply._headers[LOCATION];
    gcp_storage.debug("Upload {}/{} -> session uri {}", _bucket, _object_name, location);
    _session_path = utils::http::parse_simple_url(location).path;
}

static const boost::regex range_ex("bytes=(\\d+)-(\\d+)");

static bool parse_response_range(const seastar::http::reply& r, uint64_t& first, uint64_t& last) {
    auto& res_headers = r._headers;
    auto i = res_headers.find(RANGE);
    if (i == res_headers.end()) {
        return false;
    }

    boost::smatch m;
    std::string tmp(i->second);
    if (!boost::regex_match(tmp, m, range_ex)) {
        return false;
    }
    first = std::stoull(m[1].str());
    last = std::stoull(m[2].str());

    return true;
}

// Write a chunk to the dest object
// See https://cloud.google.com/storage/docs/resumable-uploads
// See https://cloud.google.com/storage/docs/performing-resumable-uploads
future<> utils::gcp::storage::client::object_data_sink::do_single_upload(std::deque<temporary_buffer<char>> bufs, size_t offset, size_t len, bool final) {
    // Ensure to block close from completing
    auto h = _gate.hold();
    // Enforce our concurrency constraints
    auto sem_units = co_await seastar::get_units(_semaphore, 1);

    // our file range. if the sink was closed, we can set the
    // final size, otherwise, leave it open (*)
    auto last = offset + std::max(len, size_t(1)) - 1; // inclusive.
    auto end = offset + len;

    for (;;) {
        auto range = fmt::format("bytes {}-{}/{}"
            , offset // first byte
            , last // last byte
            , final ? std::to_string(end) : "*"s
        );

        try {
            if (_session_path.empty()) {
                co_await acquire_session();
            }

            gcp_storage.debug("{}:{} write range {}-{}", _bucket, _object_name, offset, offset+len);

            auto res = co_await _impl->send_with_retry(_session_path
                , GCP_OBJECT_SCOPE_READ_WRITE
                , std::make_pair([&](output_stream<char>&& os_in) -> future<> {
                    auto os = std::move(os_in);
                    for (auto& buf : bufs) {
                        co_await os.write(buf.share());
                    }
                    co_await os.flush();
                    co_await os.close();
                }, len)
                , ""s // no content type
                , httpclient::method_type::PUT
                , rest::key_values({ { CONTENT_RANGE, range } })
            );

            switch (res.result()) {
            case status_type::ok:
            case status_type::created:
                _completed = true;
                gcp_storage.debug("{}:{} completed ({} bytes)", _bucket, _object_name, offset+len);
                co_return; // done and happy
            default:
                if (int(res.result()) == 308) {
                    uint64_t first = 0, new_last = 0;
                    if (parse_response_range(res.reply, first, new_last) && last != new_last) {
                        auto written = (new_last + 1) - offset;

                        gcp_storage.debug("{}:{} partial upload ({} bytes)", _bucket, _object_name, written);

                        if (!final && (len - written) < min_gcp_storage_chunk_size) {
                            written = len - std::min(min_gcp_storage_chunk_size, len);
                        }

                        auto to_remove = written;
                        while (to_remove) {
                            auto& buf = bufs.front();
                            auto size = std::min(to_remove, buf.size());
                            buf.trim_front(size);
                            if (buf.empty()) {
                                bufs.pop_front();
                            }
                            to_remove -= size;
                        }
                        offset += written;
                        len -= written;
                        auto total = std::accumulate(bufs.begin(), bufs.end(), size_t{}, [](size_t s, auto& buf) {
                            return s + buf.size();
                        });
                        assert(len == total);
                        continue;
                    }
                    // incomplete. ok for partial
                    gcp_storage.debug("{}:{} chunk {}:{} done", _bucket, _object_name, offset, offset+len);
                    co_return;
                }
                throw failed_upload_error(int(res.result()), get_gcp_error_message(res.body()));
            }
        } catch (...) {
            _exception = std::current_exception();
            gcp_storage.warn("Exception in upload of {}:{} ({}/{}): {}"
                , _bucket
                , _object_name
                , offset
                , len
                , _exception
            );
            break;
        }
    }
}

// Check/close the final object.
future<> utils::gcp::storage::client::object_data_sink::check_upload() {
    // Now we know the final size. Set it in range
    auto range = fmt::format("bytes */{}", _accumulated);

    auto res = co_await _impl->send_with_retry(_session_path
        , GCP_OBJECT_SCOPE_READ_WRITE
        , ""s
        , APPLICATION_JSON
        , httpclient::method_type::PUT
        , rest::key_values({ { CONTENT_RANGE, range } })
    );

    switch (res.result()) {
    case status_type::ok:
    case status_type::created:
        _completed = true;
        gcp_storage.debug("{}:{} completed ({})", _bucket, _object_name, _accumulated);
        co_return; // done and happy
    default:
        throw failed_upload_error(int(res.result()), fmt::format("{}:{} incomplete. ({}): {}"
            , _bucket, _object_name, res.reply._headers[RANGE]
            , get_gcp_error_message(res.body())
        ));
    }
}

// https://cloud.google.com/storage/docs/performing-resumable-uploads#cancel-upload
future<> utils::gcp::storage::client::object_data_sink::remove_upload() {
    if (_completed || _session_path.empty()) {
        co_return;
    }

    gcp_storage.debug("Removing incomplete upload {}:{} ()", _bucket, _object_name, _session_path);

    auto res = co_await _impl->send_with_retry(_session_path
        , GCP_OBJECT_SCOPE_READ_WRITE
        , ""s
        , APPLICATION_JSON
        , httpclient::method_type::DELETE
    );

    switch (int(res.result())) {
    case 499: // not in enum yet
        gcp_storage.debug("Upload of {}:{} removed ({})", _bucket, _object_name, _session_path);
        co_return; // done and happy
    default: {
        auto msg = get_gcp_error_message(res.body());
        gcp_storage.warn("Failed to remove broken upload of {}:{} ({})", _bucket, _object_name, msg);
        if (!_exception) {
            throw failed_upload_error(int(res.result()), fmt::format("{}:{} incomplete. ({}): {}"
                , _bucket, _object_name, res.reply._headers[RANGE]
                , msg
            ));
        }
    }
    }
}

// Read a single buffer from the source object
future<temporary_buffer<char>> utils::gcp::storage::client::object_data_source::get() {
    // If we don't know the source size yet, get the info from server
    if (_size == 0) {
        co_await read_info();
    }

    // If we don't have buffers to give, try getting one from server
    if (_buffers.empty()) {
        auto to_read = std::min(_size - _position, uint64_t(default_gcp_storage_chunk_size));

        // to_read == 0 -> eof
        if (to_read != 0) {
            gcp_storage.debug("Reading object {}:{} ({}-{}/{})", _bucket, _object_name, _position, _position+to_read, _size);

            // Ensure we read from the same generation as we queried in read_info. Note: mock server ignores this.
            auto path = fmt::format("/storage/v1/b/{}/o/{}?ifGenerationMatch={}&alt=media", _bucket, _object_name, _generation);
            auto range = fmt::format("bytes={}-{}", _position, _position+to_read-1); // inclusive range

            co_await _impl->send_with_retry(path
                , GCP_OBJECT_SCOPE_READ_ONLY
                , ""s
                , ""s
                , [&](const seastar::http::reply& rep, seastar::input_stream<char>& in) -> future<> {
                    if (rep._status != status_type::ok && rep._status != status_type::partial_content) {
                        throw failed_operation(fmt::format("Could not read object {}: {} ({}/{})", _bucket, _object_name, _position, _size));
                    }
                    auto old = _position;
                    // ensure these are on our coroutine frame.
                    auto bufs = co_await util::read_entire_stream(in);
                    for (auto&& buf : bufs) {
                        _position += buf.size();
                        _buffers.emplace_back(std::move(buf));
                    }
                    gcp_storage.debug("Read object {}:{} ({}-{}/{})", _bucket, _object_name, old, _position, _size);
                }
                , httpclient::method_type::GET
                , rest::key_values({ { RANGE, range } })
            );
        }
    }

    // Now, either buffers have data, or we are EOF.
    if (!_buffers.empty()) {
        auto res = std::move(_buffers.front());
        _buffers.pop_front();
        co_return res;
    }

    co_return temporary_buffer<char>{};
}

future<temporary_buffer<char>> utils::gcp::storage::client::object_data_source::skip(uint64_t n) {
    // If we don't know the source size yet, get the info from server
    if (_size == 0) {
        co_await read_info();
    }
    // First, skip any data we have in cache
    while (n > 0 && !_buffers.empty()) {
        auto m = std::min(n, _buffers.front().size());
        _buffers.front().trim_front(m);
        if (_buffers.front().empty()) {
            _buffers.pop_front();
        }
        n -= m;
    }
    // Now, if we have more to skip, just adjust our position.
    auto skip = std::min(_size - _position, n);
    _position += skip;

    // And get the next buffer
    co_return co_await get();
}

future<> utils::gcp::storage::client::object_data_source::read_info() {
    gcp_storage.debug("Read info {}:{}", _bucket, _object_name);

    auto path = fmt::format("/storage/v1/b/{}/o/{}", _bucket, _object_name);

    auto res = co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_READ_ONLY
        , ""s
        , ""s
        , httpclient::method_type::GET
    );

    if (res.result() != status_type::ok) {
        throw failed_operation(fmt::format("Could not query object {}:{} {}", _bucket, _object_name, res.result()));
    }

    auto item = rjson::parse(std::move(res.body()));
    // Ensure we got the info we asked for/expect
    if (rjson::get<std::string>(item, "kind") != "storage#object"s) {
        throw failed_operation("Malformed query object reply");
    }

    _size = std::stoull(rjson::get<std::string>(item, "size"));
    _generation = std::stoull(rjson::get<std::string>(item, "generation"));
}

utils::gcp::storage::client::client(std::string_view endpoint, std::optional<google_credentials> c, shared_ptr<seastar::tls::certificate_credentials> certs)
    : _impl(seastar::make_shared<impl>(endpoint, std::move(c), std::move(certs)))
{}

utils::gcp::storage::client::~client() = default;


future<> utils::gcp::storage::client::create_bucket(std::string_view project, rjson::value meta) {
    gcp_storage.debug("Create bucket {}:{}", project, rjson::get(meta, "name"));

    auto path = fmt::format("/storage/v1/b?project={}", project);
    auto body = rjson::print(meta);

    auto res = co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_FULL_CONTROL
        , body
        , APPLICATION_JSON
        , httpclient::method_type::POST
    );

    switch (res.result()) {
    case status_type::ok:
    case status_type::created:
        co_return; // done and happy
    default:
        throw failed_operation(fmt::format("Could not create bucket {}: {}", rjson::get(meta, "name"), res.result()));
    }
}

future<> utils::gcp::storage::client::create_bucket(std::string_view project, std::string_view bucket, std::string_view region, std::string_view storage_class) {
    // Construct metadata. Could fmt::format, but this is somewhat safer.
    rjson::value meta = rjson::empty_object();
    rjson::add(meta, "name", std::string(bucket));
    rjson::add(meta, "location", std::string(region.empty() ? "US" : region));
    rjson::add(meta, "storageClass", std::string(storage_class.empty() ? "STANDARD" : storage_class));

    rjson::value uniformBucketLevelAccess = rjson::empty_object();
    rjson::add(uniformBucketLevelAccess, "enabled", true);
    rjson::value iamConfiguration = rjson::empty_object();
    rjson::add(iamConfiguration, "uniformBucketLevelAccess", std::move(uniformBucketLevelAccess));
    rjson::add(meta, "iamConfiguration", std::move(iamConfiguration));

    co_await create_bucket(project, std::move(meta));
}

future<> utils::gcp::storage::client::delete_bucket(std::string_view bucket) {
    gcp_storage.debug("Delete bucket {}", bucket);

    auto path = fmt::format("/storage/v1/b/{}", bucket);

    auto res = co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_FULL_CONTROL
        , ""s
        , ""s
        , httpclient::method_type::DELETE
    );

    switch (res.result()) {
    case status_type::ok: // mock server sends wrong code, but seems acceptable
    case status_type::no_content:
        co_return; // done and happy
    default:
        throw failed_operation(fmt::format("Could not delete bucket {}: {}", bucket, res.result()));
    }
}

// See https://cloud.google.com/storage/docs/listing-objects
// TODO: maybe make a generator? However, we don't have a streaming 
// json parsing routine as such, so however we do this, we need to
// read all data from network, etc. Thus there is not all that much
// point in it. Return chunked_vector to avoid large alloc, but keep it
// in one object... for now...
future<utils::chunked_vector<utils::gcp::storage::object_info>> utils::gcp::storage::client::list_objects(std::string_view bucket, std::string_view prefix) {
    gcp_storage.debug("List bucket {} (prefix={})", bucket, prefix);

    auto path = fmt::format("/storage/v1/b/{}/o", bucket);

    if (!prefix.empty()) {
        path += fmt::format("?prefix={}", prefix);
    }

    utils::chunked_vector<utils::gcp::storage::object_info> result;

    co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_READ_ONLY
        , ""s
        , ""s
        , [&](const seastar::http::reply& rep, seastar::input_stream<char>& in) -> future<> {
            if (rep._status != status_type::ok) {
                throw failed_operation(fmt::format("Could not list bucket {}: {} ({})", bucket, rep._status
                    , co_await get_gcp_error_message(in)
                ));
            }

            // ensure these are on our coroutine frame.
            auto bufs = co_await util::read_entire_stream(in);
            auto root = rjson::parse(std::move(bufs));

            if (rjson::get<std::string>(root, "kind") != "storage#objects"s) {
                throw failed_operation("Malformed list object reply");
            }

            auto items = rjson::find(root, "items");
            if (!items) {
                co_return;
            }
            if (!items->IsArray()) {
                throw failed_operation("Malformed list object items");
            }
            for (auto& item : items->GetArray()) {
                object_info info;

                info.name = rjson::get<std::string>(item, "name");
                info.content_type = rjson::get_opt<std::string>(item, "contentType").value_or(""s);
                info.size = std::stoull(rjson::get<std::string>(item, "size"));
                info.generation = std::stoull(rjson::get<std::string>(item, "generation"));
                result.emplace_back(std::move(info));
            }

        }
        , httpclient::method_type::GET
    );

    co_return result;
}

// See https://cloud.google.com/storage/docs/deleting-objects
future<> utils::gcp::storage::client::delete_object(std::string_view bucket, std::string_view object_name) {
    gcp_storage.debug("Delete object {}:{}", bucket, object_name);

    auto path = fmt::format("/storage/v1/b/{}/o/{}", bucket, object_name);

    auto res = co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_READ_WRITE
        , ""s
        , ""s
        , httpclient::method_type::DELETE
    );

    switch (res.result()) {
    case status_type::ok:
    case status_type::no_content:
        gcp_storage.debug("Deleted {}:{}", bucket, object_name);
        co_return; // done and happy
    default:
        throw failed_operation(fmt::format("Could not delete object {}:{}: {} ({})", bucket, object_name, res.result()
            , get_gcp_error_message(res.body())
        ));
    }
}

// See https://cloud.google.com/storage/docs/copying-renaming-moving-objects
// GCP does not support moveTo across buckets.
future<> utils::gcp::storage::client::rename_object(std::string_view bucket, std::string_view object_name, std::string_view new_bucket, std::string_view new_name) {
    co_await copy_object(bucket, object_name, new_bucket, new_name);
    co_await delete_object(bucket, object_name);
}

// See https://cloud.google.com/storage/docs/copying-renaming-moving-objects
future<> utils::gcp::storage::client::rename_object(std::string_view bucket, std::string_view object_name, std::string_view new_name) {
    gcp_storage.debug("Move object {}:{} -> {}", bucket, object_name, new_name);

    auto path = fmt::format("/storage/v1/b/{}/o/{}/moveTo/o/{}", bucket, object_name, new_name);
    auto res = co_await _impl->send_with_retry(path
        , GCP_OBJECT_SCOPE_READ_WRITE
        , ""s
        , ""s
        , httpclient::method_type::PUT
    );

    switch (res.result()) {
    case status_type::ok:
    case status_type::created:
        gcp_storage.debug("Moved {}:{} to {}", bucket, object_name, new_name);
        co_return; // done and happy
    default:
        throw failed_operation(fmt::format("Could not rename object {}:{}: {} ({})", bucket, object_name, res.result()
            , get_gcp_error_message(res.body())
        ));
    }
}

// See https://cloud.google.com/storage/docs/copying-renaming-moving-objects
// Copying an object in GCP can only process a certain amount of data in one call
// Must keep doing it until all data is copied, and check response.
future<> utils::gcp::storage::client::copy_object(std::string_view bucket, std::string_view object_name, std::string_view new_bucket, std::string_view to_name) {
    auto path = fmt::format("/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}", bucket, object_name, new_bucket, to_name);
    std::string body;

    for (;;) {
        auto res = co_await _impl->send_with_retry(path
            , GCP_OBJECT_SCOPE_READ_WRITE
            , body
            , APPLICATION_JSON
            , httpclient::method_type::PUT
        );

        if (res.result() != status_type::ok) {
            throw failed_operation(fmt::format("Could not copy object {}:{}: {} ({})", bucket, object_name, res.result()
                , get_gcp_error_message(res.body())
            ));
        }

        auto resp = rjson::parse(res.body());
        if (rjson::get<bool>(resp, "done")) {
            gcp_storage.debug("Copied {}:{} to {}:{}", bucket, object_name, new_bucket, to_name);
            co_return; // done and happy
        }

        auto token = rjson::get<std::string>(resp, "rewriteToken");
        auto written = rjson::get<uint64_t>(resp, "totalBytesRewritten");
        auto size = rjson::get<uint64_t>(resp, "objectSize");

        // Call 2+ must include the rewriteToken
        body = fmt::format("{{\"rewriteToken\": \"{}\"}}", token);

        gcp_storage.debug("Partial copy of {}:{} to {}:{} ({}/{})", bucket, object_name, new_bucket, to_name, written, size);
    }
}

future<> utils::gcp::storage::client::copy_object(std::string_view bucket, std::string_view object_name, std::string_view to_name) {
    co_await copy_object(bucket, object_name, bucket, to_name);
}

seastar::data_sink utils::gcp::storage::client::create_upload_sink(std::string_view bucket, std::string_view object_name, rjson::value metadata) const {
    return seastar::data_sink(std::make_unique<object_data_sink>(_impl, bucket, object_name, std::move(metadata)));
}

seastar::data_source utils::gcp::storage::client::create_download_source(std::string_view bucket, std::string_view object_name) const {
    return seastar::data_source(std::make_unique<object_data_source>(_impl, bucket, object_name));
}

future<> utils::gcp::storage::client::close() {
    return _impl->close();
}

const std::string utils::gcp::storage::client::DEFAULT_ENDPOINT = "https://storage.googleapis.com";
