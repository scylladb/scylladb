/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/s3/client.hh"
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>

namespace s3 {

class client::download_source final : public seastar::data_source_impl {
    shared_ptr<client> _client;
    sstring _object_name;
    seastar::abort_source* _as;
    range _range;

    struct external_body {
        input_stream<char>& b;
        promise<> done;
        external_body(input_stream<char>& b_) noexcept : b(b_), done() {}
    };

    std::optional<external_body> _body;
    named_gate _bg;

    future<external_body> request_body();

public:
    download_source(shared_ptr<client> cln, sstring object_name, std::optional<range> range, seastar::abort_source* as);

    virtual future<temporary_buffer<char>> get() override;
    virtual future<> close() override;
};

} // s3
