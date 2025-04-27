/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "multipart_upload.hh"
#include "utils/s3/client.hh"

namespace s3 {

class client::upload_sink_base : public multipart_upload, public data_sink_impl {
public:
    upload_sink_base(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag, seastar::abort_source* as);

    virtual future<> put(net::packet) override;

    virtual future<> close() override;

    virtual size_t buffer_size() const noexcept override;
};

class client::upload_sink final : public upload_sink_base {
    memory_data_sink_buffers _bufs;
    future<> maybe_flush();

public:
    upload_sink(shared_ptr<client> cln, sstring object_name, std::optional<tag> tag = {}, seastar::abort_source* as = nullptr);

    virtual future<> put(temporary_buffer<char> buf) override;

    virtual future<> put(std::vector<temporary_buffer<char>> data) override;

    virtual future<> flush() override;
};

class client::upload_jumbo_sink final : public upload_sink_base {
    static constexpr tag piece_tag = { .key = "kind", .value = "piece" };

    const unsigned _maximum_parts_in_piece;
    std::unique_ptr<upload_sink> _current;

    future<> maybe_flush();

public:
    upload_jumbo_sink(shared_ptr<client> cln, sstring object_name, std::optional<unsigned> max_parts_per_piece, seastar::abort_source* as);

    virtual future<> put(temporary_buffer<char> buf) override;

    virtual future<> put(std::vector<temporary_buffer<char>> data) override;

    virtual future<> flush() override;

    virtual future<> close() override;
};

} // s3
