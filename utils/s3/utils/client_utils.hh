/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/chunked_vector.hh"
#include <seastar/core/iostream.hh>
#include <seastar/core/sstring.hh>

namespace seastar::http {
struct reply;
}
namespace s3 {
using s3_clock = std::chrono::steady_clock;

struct range {
    uint64_t off;
    size_t len;
};

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


static constexpr std::string_view multipart_upload_complete_header =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"
        "<CompleteMultipartUpload xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";

static constexpr std::string_view multipart_upload_complete_entry =
        "<Part><ETag>{}</ETag><PartNumber>{}</PartNumber></Part>";

static constexpr std::string_view multipart_upload_complete_trailer =
        "</CompleteMultipartUpload>";

// "Each part must be at least 5 MB in size, except the last part."
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
static constexpr size_t aws_minimum_part_size = 5 * 1024 * 1024;
// "Part numbers can be any number from 1 to 10,000, inclusive."
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html
static constexpr unsigned aws_maximum_parts_in_piece = 10'000;

seastar::sstring parse_multipart_upload_id(seastar::sstring& body);

unsigned prepare_multipart_upload_parts(const utils::chunked_vector<seastar::sstring>& etags);

seastar::future<> dump_multipart_upload_parts(seastar::output_stream<char> out, const utils::chunked_vector<seastar::sstring>& etags);

seastar::sstring parse_multipart_copy_upload_etag(seastar::sstring& body);

seastar::sstring format_range_header(const range& range);

seastar::future<> ignore_reply(const seastar::http::reply& rep, seastar::input_stream<char>&& in_);

[[noreturn]] void map_s3_client_exception(std::exception_ptr ex);

} // namespace s3

template <>
struct fmt::formatter<s3::tag> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const s3::tag& tag, fmt::format_context& ctx) const {
        return fmt::format_to(ctx.out(), "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
                              tag.key, tag.value);
    }
};