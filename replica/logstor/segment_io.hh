/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <functional>
#include <optional>
#include <seastar/core/fstream.hh>
#include "replica/logstor/write_buffer.hh"

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
using segment_header_consumer = std::function<future<>(const segment_header&)>;

future<std::optional<segment_header>> read_segment_header(seastar::input_stream<char>& in);

log_record deserialize_log_record(simple_memory_input_stream);
future<log_record> read_log_record(seastar::input_stream<char>& in, log_location loc);

future<> scan_segment(seastar::input_stream<char>& in,
        log_segment_id segment_id,
        size_t segment_size,
        segment_header_consumer on_segment_header,
        record_header_consumer on_record_header,
        record_consumer on_record);

}
