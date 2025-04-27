/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "client_utils.hh"

#include "utils/assert.hh"
#include "utils/exceptions.hh"
#include "utils/input_stream.hh"
#include "utils/log.hh"
#include "utils/s3/aws_error.hh"
#include <seastar/core/iostream.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/http/exception.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/short_streams.hh>

#if __has_include(<rapidxml.h>)
#include <rapidxml.h>
#else
#include <rapidxml/rapidxml.hpp>
#endif

namespace s3 {

extern logging::logger s3l;

seastar::sstring parse_multipart_upload_id(seastar::sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse initiate multipart upload response: {}", e.what());
        // The caller is supposed to check the upload-id to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc->first_node("InitiateMultipartUploadResult");
    auto uploadid_node = root_node->first_node("UploadId");
    return uploadid_node->value();
}

unsigned prepare_multipart_upload_parts(const utils::chunked_vector<seastar::sstring>& etags) {
    unsigned ret = multipart_upload_complete_header.size();

    unsigned nr = 1;
    for (auto& etag : etags) {
        if (etag.empty()) {
            // 0 here means some part failed to upload, see comment in upload_part()
            // Caller checks it an aborts the multipart upload altogether
            return 0;
        }
        // length of the format string - four braces + length of the etag + length of the number
        ret += multipart_upload_complete_entry.size() - 4 + etag.size() + seastar::format("{}", nr).size();
        nr++;
    }
    ret += multipart_upload_complete_trailer.size();
    return ret;
}

seastar::future<> dump_multipart_upload_parts(seastar::output_stream<char> out, const utils::chunked_vector<seastar::sstring>& etags) {
    std::exception_ptr ex;
    try {
        co_await out.write(multipart_upload_complete_header.data(), multipart_upload_complete_header.size());

        unsigned nr = 1;
        for (auto& etag : etags) {
            SCYLLA_ASSERT(!etag.empty());
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
        co_await seastar::coroutine::return_exception_ptr(std::move(ex));
    }
}

seastar::sstring parse_multipart_copy_upload_etag(seastar::sstring& body) {
    auto doc = std::make_unique<rapidxml::xml_document<>>();
    try {
        doc->parse<0>(body.data());
    } catch (const rapidxml::parse_error& e) {
        s3l.warn("cannot parse multipart copy upload response: {}", e.what());
        // The caller is supposed to check the etag to be empty
        // and handle the error the way it prefers
        return "";
    }
    auto root_node = doc->first_node("CopyPartResult");
    auto etag_node = root_node->first_node("ETag");
    return etag_node->value();
}

seastar::sstring format_range_header(const range& range) {
    auto end_bytes = range.off + range.len - 1;
    if (end_bytes < range.off) {
        throw std::overflow_error("End of the range exceeds 64-bits");
    }
    return seastar::format("bytes={}-{}", range.off, end_bytes);
}

seastar::future<> ignore_reply(const seastar::http::reply& rep, seastar::input_stream<char>&& in_) {
    auto in = std::move(in_);
    co_await util::skip_entire_stream(in);
}

[[noreturn]] void map_s3_client_exception(std::exception_ptr ex) {
    seastar::memory::scoped_critical_alloc_section alloc;

    try {
        std::rethrow_exception(std::move(ex));
    } catch (const aws::aws_exception& e) {
        int error_code;
        switch (e.error().get_error_type()) {
        case aws::aws_error_type::HTTP_NOT_FOUND:
        case aws::aws_error_type::RESOURCE_NOT_FOUND:
        case aws::aws_error_type::NO_SUCH_BUCKET:
        case aws::aws_error_type::NO_SUCH_KEY:
        case aws::aws_error_type::NO_SUCH_UPLOAD:
            error_code = ENOENT;
            break;
        case aws::aws_error_type::HTTP_FORBIDDEN:
        case aws::aws_error_type::HTTP_UNAUTHORIZED:
        case aws::aws_error_type::ACCESS_DENIED:
            error_code = EACCES;
            break;
        default:
            error_code = EIO;
        }
        throw storage_io_error{error_code, format("S3 request failed. Code: {}. Reason: {}", e.error().get_error_type(), e.what())};
    } catch (const httpd::unexpected_status_error& e) {
        auto status = e.status();

        if (http::reply::classify_status(status) == http::reply::status_class::redirection || status == http::reply::status_type::not_found) {
            throw storage_io_error {ENOENT, format("S3 object doesn't exist ({})", status)};
        }
        if (status == http::reply::status_type::forbidden || status == http::reply::status_type::unauthorized) {
            throw storage_io_error {EACCES, format("S3 access denied ({})", status)};
        }

        throw storage_io_error {EIO, format("S3 request failed with ({})", status)};
    } catch (...) {
        auto e = std::current_exception();
        throw storage_io_error {EIO, format("S3 error ({})", e)};
    }
}

} // namespace s3
