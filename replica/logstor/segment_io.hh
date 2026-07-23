/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <concepts>
#include <functional>
#include <optional>
#include <span>
#include <vector>

#include <seastar/core/fstream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "replica/logstor/ondisk.hh"

namespace replica::logstor {

struct segment_header {
    segment_kind kind;
    segment_sequence segment_seq;

    struct mixed {};
    struct full {
        table_id table;
        dht::token first_token;
        dht::token last_token;
    };

    std::variant<mixed, full> v;
};

using want_data = seastar::bool_class<class want_data_tag>;

using record_header_consumer = std::function<want_data(log_location, const log_record_header&)>;
using record_consumer = std::function<future<>(log_location, log_record)>;
using record_bytes_consumer = std::function<future<>(log_location, const log_record_header&, log_record_bytes_view)>;
using segment_header_consumer = std::function<future<>(const segment_header&)>;
using streamed_buffer_consumer = std::function<future<>(bytes_view)>;

template <typename Consumer>
concept record_consumer_like =
    (std::invocable<Consumer&, log_location, log_record> &&
     std::same_as<std::invoke_result_t<Consumer&, log_location, log_record>, future<>>) ||
    (std::invocable<Consumer&, log_location, const log_record_header&, log_record_bytes_view> &&
     std::same_as<std::invoke_result_t<Consumer&, log_location, const log_record_header&, log_record_bytes_view>, future<>>);

future<std::optional<segment_header>> read_segment_header(seastar::input_stream<char>& in);

log_record deserialize_log_record(simple_memory_input_stream);
future<log_record> read_log_record(seastar::input_stream<char>& in, log_location loc);

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_header_consumer on_segment_header,
        record_header_consumer on_record_header,
        record_bytes_consumer on_record);

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_header_consumer on_segment_header,
        record_header_consumer on_record_header,
        record_consumer on_record);

// Rewrites the initial streamed logstor buffer header to the local segment sequence and
// forwards subsequent bytes unchanged. This preserves the current branch's streaming
// behavior, which only rewrites the first streamed buffer header.
class streamed_segment_rewriter {
    log_segment_id _target_segment;
    segment_sequence _target_seq;
    streamed_buffer_consumer _on_buffer;
    std::vector<char> _pending_data;
    std::optional<size_t> _initial_header_size;
    bool _header_rewritten = false;

    ondisk::buffer_header read_buffer_header() const;
    void maybe_parse_initial_header();
    void rewrite_buffer_header();
    future<> flush_pending_data();

public:
    streamed_segment_rewriter(log_segment_id target_segment, segment_sequence target_seq, streamed_buffer_consumer on_buffer);

    future<> put(std::span<temporary_buffer<char>> data);
    future<> close();

    size_t buffer_size() const noexcept;
};

}
