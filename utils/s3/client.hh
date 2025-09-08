/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/units.hh>
#include <seastar/http/client.hh>
#include <filesystem>
#include "utils/lister.hh"
#include "utils/s3/creds.hh"
#include "credentials_providers/aws_credentials_provider_chain.hh"
#include "retryable_http_client.hh"
#include "utils/s3/client_fwd.hh"

using namespace seastar;
class memory_data_sink_buffers;

namespace s3 {

using s3_clock = std::chrono::steady_clock;

class range {
    friend struct fmt::formatter<range>;

public:
    range() = delete;
    constexpr range(uint64_t offset, uint64_t len) : _offset(offset), _length(len) {
        uint64_t t;
        if (__builtin_add_overflow(_offset, len, &t)) [[unlikely]] {
            throw std::overflow_error(fmt::format("Range offset {} + length {} exceeds maximum uint64_t", _offset, len));
        }
        if (_offset + len > _max_object_size) [[unlikely]] {
            throw std::overflow_error(fmt::format("Range offset {} + length {} exceeds maximum object size {}", _offset, len, _max_object_size));
        }
    }
    explicit constexpr range(uint64_t offset) : _offset(offset) {}
    constexpr range& operator+=(uint64_t offset) {
        uint64_t t;
        if (__builtin_add_overflow(_offset, offset, &t)) [[unlikely]] {
            throw std::overflow_error(fmt::format("Range offset {} + length {} exceeds maximum uint64_t", _offset, offset));
        }
        if (_offset + offset > _max_object_size) [[unlikely]] {
            throw std::overflow_error(fmt::format("Range offset {} + length {} exceeds maximum object size {}", _offset, offset, _max_object_size));
        }

        _offset += offset;
        if (_length != _max_object_size) [[unlikely]] {
            if (__builtin_sub_overflow(_length, offset, &t)) {
                throw std::underflow_error(fmt::format("Range length {} - offset {} exceeds minimum uint64_t", _length, offset));
            }
            _length -= offset;
        }
        return *this;
    }
    std::partial_ordering operator<=>(const range& other) const noexcept = default;
    [[nodiscard]] sstring to_header_string() const noexcept {
        return fmt::format("bytes={}-{}", _offset, _length != _max_object_size ? fmt::format("{}", _offset + _length - 1) : "");
    }
    [[nodiscard]] uint64_t offset() const noexcept { return _offset; }
    [[nodiscard]] uint64_t length() const noexcept { return _length; }

    // 5TiB is the largest object size supported by S3
    static constexpr uint64_t _max_object_size{5_TiB};

private:
    uint64_t _offset;
    uint64_t _length{_max_object_size};
};
static constexpr range full_range{0};

struct tag {
    std::string key;
    std::string value;
    auto operator<=>(const tag&) const = default;
};
using tag_set = std::vector<tag>;

struct stats {
    uint64_t size;
    std::time_t last_modified;
};

struct filler_exception final : std::runtime_error {
    explicit filler_exception(const char* msg) : std::runtime_error(msg) {}
};

future<> ignore_reply(const http::reply& rep, input_stream<char>&& in_);
[[noreturn]] void map_s3_client_exception(std::exception_ptr ex);

class client : public enable_shared_from_this<client> {
    class multipart_upload;
    class copy_s3_object;
    class upload_sink_base;
    class upload_sink;
    class upload_jumbo_sink;
    class chunked_download_source;
    class download_source;
    class do_upload_file;
    class readable_file;
    std::string _host;
    endpoint_config_ptr _cfg;
    semaphore _creds_sem;
    timer<seastar::lowres_clock> _creds_invalidation_timer;
    timer<seastar::lowres_clock> _creds_update_timer;
    aws_credentials _credentials;
    aws::aws_credentials_provider_chain _creds_provider_chain;

    struct io_stats {
        uint64_t ops = 0;
        uint64_t bytes = 0;
        std::chrono::duration<double> duration = std::chrono::duration<double>(0);

        void update(uint64_t len, std::chrono::duration<double> lat) {
            ops++;
            bytes += len;
            duration += lat;
        }
    };
    struct group_client {
        aws::retryable_http_client retryable_client;
        io_stats read_stats;
        io_stats write_stats;
        uint64_t prefetch_bytes = 0;
        seastar::metrics::metric_groups metrics;
        group_client(std::unique_ptr<http::experimental::connection_factory> f, unsigned max_conn, const aws::retry_strategy& retry_strategy);
        void register_metrics(std::string class_name, std::string host);
    };
    std::unordered_map<seastar::scheduling_group, group_client> _https;
    using global_factory = std::function<shared_ptr<client>(std::string)>;
    global_factory _gf;
    semaphore& _memory;
    std::unique_ptr<aws::retry_strategy> _retry_strategy;

    struct private_tag {};

    future<semaphore_units<>> claim_memory(size_t mem, seastar::abort_source* as);

    future<> update_credentials_and_rearm();
    future<> authorize(http::request&);
    group_client& find_or_create_client();
    future<> make_request(http::request req, http::experimental::client::reply_handler handle = ignore_reply, std::optional<http::reply::status_type> expected = std::nullopt, seastar::abort_source* = nullptr);
    using reply_handler_ext = noncopyable_function<future<>(group_client&, const http::reply&, input_stream<char>&& body)>;
    future<> make_request(http::request req, reply_handler_ext handle, std::optional<http::reply::status_type> expected = std::nullopt, seastar::abort_source* = nullptr);
    future<> get_object_header(sstring object_name, http::experimental::client::reply_handler handler, seastar::abort_source* = nullptr);
public:

    client(std::string host, endpoint_config_ptr cfg, semaphore& mem, global_factory gf, private_tag, std::unique_ptr<aws::retry_strategy> rs = nullptr);
    static shared_ptr<client> make(std::string endpoint, endpoint_config_ptr cfg, semaphore& memory, global_factory gf = {});

    future<uint64_t> get_object_size(sstring object_name, seastar::abort_source* = nullptr);
    future<stats> get_object_stats(sstring object_name, seastar::abort_source* = nullptr);
    future<tag_set> get_object_tagging(sstring object_name, seastar::abort_source* = nullptr);
    future<> put_object_tagging(sstring object_name, tag_set tagging, seastar::abort_source* = nullptr);
    future<> delete_object_tagging(sstring object_name, seastar::abort_source* = nullptr);
    future<temporary_buffer<char>> get_object_contiguous(sstring object_name, range download_range = s3::full_range, seastar::abort_source* = nullptr);
    future<> put_object(sstring object_name, temporary_buffer<char> buf, seastar::abort_source* = nullptr);
    future<> put_object(sstring object_name, ::memory_data_sink_buffers bufs, seastar::abort_source* = nullptr);
    future<> copy_object(sstring source_object, sstring target_object, std::optional<size_t> part_size = {}, std::optional<tag> tag = {}, seastar::abort_source* = nullptr);
    future<> delete_object(sstring object_name, seastar::abort_source* = nullptr);

    file make_readable_file(sstring object_name, seastar::abort_source* = nullptr);
    data_sink make_upload_sink(sstring object_name, seastar::abort_source* = nullptr);
    data_sink make_upload_jumbo_sink(sstring object_name, std::optional<unsigned> max_parts_per_piece = {}, seastar::abort_source* = nullptr);
    data_source make_download_source(sstring object_name, range download_range = s3::full_range, seastar::abort_source* = nullptr);
    data_source make_chunked_download_source(sstring object_name, range range = s3::full_range, seastar::abort_source* = nullptr);
    /// upload a file with specified path to s3
    ///
    /// @param path the path to the file
    /// @param object_name object name for the created object in S3
    /// @param tag an optional tag
    /// @param part_size the size of each part of the multipart upload
    future<> upload_file(std::filesystem::path path,
                         sstring object_name,
                         std::optional<tag> tag = {},
                         std::optional<size_t> max_part_size = {},
                         seastar::abort_source* = nullptr);
    future<> upload_file(std::filesystem::path path,
                         sstring object_name,
                         upload_progress& up,
                         seastar::abort_source* = nullptr);

    future<> update_config(endpoint_config_ptr);

    struct handle {
        std::string _host;
        global_factory _gf;
    public:
        handle(const client& cln)
                : _host(cln._host)
                , _gf(cln._gf)
        {}

        shared_ptr<client> to_client() && {
            return _gf(std::move(_host));
        }
    };

    class bucket_lister final : public abstract_lister::impl {
        shared_ptr<client> _client;
        sstring _bucket;
        sstring _prefix;
        sstring _max_keys;
        std::optional<future<>> _opt_done_fut;
        lister::filter_type _filter;
        seastar::queue<std::optional<directory_entry>> _queue;

        future<> start_listing();

    public:

        bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix = "", size_t objects_per_page = 64, size_t entries_batch = 512 / sizeof(std::optional<directory_entry>));
        bucket_lister(shared_ptr<client> client, sstring bucket, sstring prefix, lister::filter_type filter, size_t objects_per_page = 64, size_t entries_batch = 512 / sizeof(std::optional<directory_entry>));

        future<std::optional<directory_entry>> get() override;
        future<> close() noexcept override;
    };

    future<> close();
};

} // namespace s3

template <>
struct fmt::formatter<s3::range> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const s3::range& range, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{}-{}", range._offset, range._length != s3::range::_max_object_size ? fmt::format("{}", range._length) : "∞");
    }
};
