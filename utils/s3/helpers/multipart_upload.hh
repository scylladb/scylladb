/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/chunked_vector.hh"
#include "utils/memory_data_sink.hh"
#include "utils/s3/client.hh"
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>

namespace seastar {
class abort_source;
}
namespace s3 {
struct tag;

class client::multipart_upload {
protected:
    seastar::shared_ptr<client> _client;
    seastar::sstring _object_name;
    seastar::sstring _upload_id;
    utils::chunked_vector<seastar::sstring> _part_etags;
    seastar::named_gate _bg_flushes;
    std::optional<tag> _tag;
    seastar::abort_source* _as;

    seastar::future<> start_upload();
    seastar::future<> finalize_upload();
    seastar::future<> upload_part(memory_data_sink_buffers bufs);
    seastar::future<> upload_part(std::unique_ptr<upload_sink> source);
    seastar::future<> abort_upload();

    bool upload_started() const noexcept;

    multipart_upload(seastar::shared_ptr<client> cln, seastar::sstring object_name, std::optional<tag> tag, seastar::abort_source* as);

public:
    unsigned parts_count() const noexcept { return _part_etags.size(); }
};

} // s3
