/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Benchmarks of rjson parsing, printing and copying, on documents shaped
// like Alternator requests and responses. Only the public rjson API is
// used, so the same benchmark measures any rjson implementation.

#include <seastar/testing/perf_tests.hh>
#include <seastar/core/iostream.hh>

#include "utils/rjson.hh"

namespace {

// Alternator request bodies arrive as a list of buffers, in sizes the
// network stack happened to deliver.
constexpr size_t chunk_size = 16 * 1024;

std::string string_attr(size_t i, size_t len) {
    return fmt::format(R"("attr{}":{{"S":"{}"}})", i, std::string(len, 'a' + i % 26));
}

std::string item(size_t key, size_t attrs, size_t attr_len) {
    std::string s = fmt::format(R"({{"p":{{"S":"user{}"}},"c":{{"N":"{}"}})", key, key * 7);
    for (size_t i = 0; i < attrs; ++i) {
        s += ',';
        s += string_attr(i, attr_len);
    }
    s += '}';
    return s;
}

// A PutItem request with an item of about 1 KB.
std::string put_item() {
    return fmt::format(R"({{"TableName":"usertable","Item":{},"ReturnValues":"NONE"}})", item(12345, 10, 90));
}

// A BatchWriteItem request with 25 items of about 1 KB.
std::string batch_write() {
    std::string s = R"({"RequestItems":{"usertable":[)";
    for (size_t i = 0; i < 25; ++i) {
        if (i) {
            s += ',';
        }
        s += fmt::format(R"({{"PutRequest":{{"Item":{}}}}})", item(i, 10, 90));
    }
    s += "]}}";
    return s;
}

// A PutItem request of an item close to the 400 KB limit, with many
// small attributes.
std::string big_item() {
    return fmt::format(R"({{"TableName":"usertable","Item":{}}})", item(1, 4000, 80));
}

// A 4 MB request dominated by a few long string values.
std::string long_strings() {
    std::string s = R"({"TableName":"usertable","Item":{"p":{"S":"k"})";
    for (size_t i = 0; i < 4; ++i) {
        s += fmt::format(R"(,"blob{}":{{"B":"{}"}})", i, std::string(1024 * 1024, 'A' + i));
    }
    s += "}}";
    return s;
}

// A flat array of small scalars, the worst case for DOM memory
// amplification (#31314).
std::string flat_array() {
    std::string s = R"({"unused":[)";
    for (size_t i = 0; i < 256 * 1024; ++i) {
        s += i ? ",0" : "0";
    }
    s += "]}";
    return s;
}

// JSON numbers of every class rjson distinguishes.
std::string numbers() {
    std::string s = R"({"v":[)";
    for (size_t i = 0; i < 16 * 1024; ++i) {
        s += fmt::format("{}{},{},{},{}.{}e-3", i ? "," : "", i, -int64_t(i) - 1, uint64_t(1) << 63 | i, i, i * 37);
    }
    s += "]}";
    return s;
}

// A GetItem response wrapping a big Alternator item of mixed types.
std::string get_item_response() {
    std::string s = R"({"Item":{"p":{"S":"k"},"list":{"L":[)";
    for (size_t i = 0; i < 500; ++i) {
        s += fmt::format(R"({}{{"M":{{"n":{{"N":"{}.5"}},"b":{{"BOOL":true}},"s":{{"S":"line\n\"{}\"\t"}}}}}})", i ? "," : "", i, i);
    }
    s += "]}}}";
    return s;
}

rjson::chunked_content make_chunks(std::string_view s) {
    rjson::chunked_content ret;
    for (size_t pos = 0; pos < s.size(); pos += chunk_size) {
        auto part = s.substr(pos, chunk_size);
        ret.emplace_back(part.data(), part.size());
    }
    return ret;
}

class null_data_sink : public data_sink_impl {
public:
    future<> put(std::span<temporary_buffer<char>>) override {
        return make_ready_future<>();
    }
    future<> close() override {
        return make_ready_future<>();
    }
    size_t buffer_size() const noexcept override {
        return 128 * 1024;
    }
};

struct document {
    std::string text;
    rjson::chunked_content chunks;
    rjson::value parsed;

    explicit document(std::string s)
        : text(std::move(s))
        , chunks(make_chunks(text))
        , parsed(rjson::parse(text))
    {}

    // A fresh copy of the chunks; parse() consumes the chunks it is given.
    rjson::chunked_content shared_chunks() {
        rjson::chunked_content ret;
        ret.reserve(chunks.size());
        for (auto& c : chunks) {
            ret.push_back(c.share());
        }
        return ret;
    }
};

struct rjson_test {
    document put_item_doc{put_item()};
    document batch_write_doc{batch_write()};
    document big_item_doc{big_item()};
    document long_strings_doc{long_strings()};
    document flat_array_doc{flat_array()};
    document numbers_doc{numbers()};
    document get_item_response_doc{get_item_response()};
};

void parse(document& d) {
    perf_tests::do_not_optimize(rjson::parse(d.text));
}

void parse_chunked(document& d) {
    auto chunks = d.shared_chunks();
    perf_tests::do_not_optimize(rjson::parse(std::move(chunks)));
}

void print(document& d) {
    perf_tests::do_not_optimize(rjson::print(d.parsed));
}

future<> print_stream(document& d) {
    output_stream<char> os(data_sink(std::make_unique<null_data_sink>()));
    co_await rjson::print(d.parsed, os);
    co_await os.close();
}

void copy(document& d) {
    perf_tests::do_not_optimize(rjson::copy(d.parsed));
}

} // anonymous namespace

#define RJSON_PERF_TESTS(doc) \
    PERF_TEST_F(rjson_test, parse_##doc) { parse(doc##_doc); } \
    PERF_TEST_F(rjson_test, parse_chunked_##doc) { parse_chunked(doc##_doc); } \
    PERF_TEST_F(rjson_test, print_##doc) { print(doc##_doc); } \
    PERF_TEST_F(rjson_test, print_stream_##doc) { return print_stream(doc##_doc); } \
    PERF_TEST_F(rjson_test, copy_##doc) { copy(doc##_doc); }

RJSON_PERF_TESTS(put_item)
RJSON_PERF_TESTS(batch_write)
RJSON_PERF_TESTS(big_item)
RJSON_PERF_TESTS(long_strings)
RJSON_PERF_TESTS(flat_array)
RJSON_PERF_TESTS(numbers)
RJSON_PERF_TESTS(get_item_response)
