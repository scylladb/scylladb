/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include "test/lib/scylla_test_case.hh"

#include <atomic>
#include <numeric>
#include <seastar/core/iostream.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

#include "utils/rjson.hh"

namespace {

rjson::chunked_content split(std::string_view s, std::vector<size_t> cuts) {
    rjson::chunked_content ret;
    size_t pos = 0;
    cuts.push_back(s.size());
    for (size_t cut : cuts) {
        if (cut > pos) {
            ret.emplace_back(s.data() + pos, cut - pos);
            pos = cut;
        }
    }
    return ret;
}

std::string parse_error_message(std::string_view s) {
    try {
        rjson::parse(s);
    } catch (const rjson::error& e) {
        return e.what();
    }
    return "no error";
}

class string_data_sink : public data_sink_impl {
    std::string& _out;
public:
    explicit string_data_sink(std::string& out) : _out(out) {}
    future<> put(std::span<temporary_buffer<char>> bufs) override {
        for (auto& b : bufs) {
            _out.append(b.get(), b.size());
        }
        return make_ready_future<>();
    }
    future<> close() override { return make_ready_future<>(); }
    size_t buffer_size() const noexcept override { return 4096; }
};

future<std::string> print_to_stream(const rjson::value& v) {
    std::string out;
    output_stream<char> os(data_sink(std::make_unique<string_data_sink>(out)));
    co_await rjson::print(v, os);
    co_await os.close();
    co_return out;
}

// A document using every kind of value, with strings long enough to be
// referenced rather than copied.
const std::string mixed_document = R"({"null":null,"t":true,"f":false,"int":-12,"uint":4294967296,"neg":-9223372036854775808,)"
    R"("dbl":1.5,"exp":1e21,"small":1e-7,"esc":"a\"b\\c\/d\b\f\n\r\t\u0001é😀",)"
    R"("long":"0123456789012345678901234567890123456789012345678901234567890123456789",)"
    R"("long_esc":"0123456789012345678901234567890123456789012345678901234567890123456789\n",)"
    R"("arr":[1,[2,[3,{}]],[],{"k":"v"}],"obj":{"a":{"b":{"c":"d"}}},"empty":""})";

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_print_is_compact) {
    auto v = rjson::parse(" { \"a\" : [ 1 , 2 ] , \"b\" : { } } ");
    BOOST_REQUIRE_EQUAL(rjson::print(v), R"({"a":[1,2],"b":{}})");
}

BOOST_AUTO_TEST_CASE(test_roundtrip) {
    auto v = rjson::parse(mixed_document);
    BOOST_REQUIRE_EQUAL(rjson::print(v), R"({"null":null,"t":true,"f":false,"int":-12,"uint":4294967296,"neg":-9223372036854775808,)"
        R"("dbl":1.5,"exp":1e21,"small":1e-7,"esc":"a\"b\\c/d\b\f\n\r\t\u0001é😀",)"
        R"("long":"0123456789012345678901234567890123456789012345678901234567890123456789",)"
        R"("long_esc":"0123456789012345678901234567890123456789012345678901234567890123456789\n",)"
        R"("arr":[1,[2,[3,{}]],[],{"k":"v"}],"obj":{"a":{"b":{"c":"d"}}},"empty":""})");
    BOOST_REQUIRE(rjson::parse(rjson::print(v)) == v);
}

// The types of numbers follow RapidJSON: an integer has every integer type
// which can hold it, a number with a fraction or exponent is a double.
BOOST_AUTO_TEST_CASE(test_number_types) {
    struct expected {
        std::string_view text;
        bool is_int, is_uint, is_int64, is_uint64, is_double;
    };
    for (auto e : std::initializer_list<expected>{
            {"0", true, true, true, true, false},
            {"-0", true, true, true, true, false},
            {"2147483647", true, true, true, true, false},
            {"2147483648", false, true, true, true, false},
            {"4294967296", false, false, true, true, false},
            {"9223372036854775808", false, false, false, true, false},
            {"18446744073709551615", false, false, false, true, false},
            {"18446744073709551616", false, false, false, false, true},
            {"-2147483648", true, false, true, false, false},
            {"-2147483649", false, false, true, false, false},
            {"-9223372036854775808", false, false, true, false, false},
            {"-9223372036854775809", false, false, false, false, true},
            {"1.0", false, false, false, false, true},
            {"1e2", false, false, false, false, true},
            {"-0.0", false, false, false, false, true},
    }) {
        auto v = rjson::parse(e.text);
        BOOST_TEST_CONTEXT(e.text) {
            BOOST_REQUIRE(v.IsNumber());
            BOOST_REQUIRE_EQUAL(v.IsInt(), e.is_int);
            BOOST_REQUIRE_EQUAL(v.IsUint(), e.is_uint);
            BOOST_REQUIRE_EQUAL(v.IsInt64(), e.is_int64);
            BOOST_REQUIRE_EQUAL(v.IsUint64(), e.is_uint64);
            BOOST_REQUIRE_EQUAL(v.IsDouble(), e.is_double);
        }
    }
    BOOST_REQUIRE_EQUAL(rjson::parse("-9223372036854775808").GetInt64(), std::numeric_limits<int64_t>::min());
    BOOST_REQUIRE_EQUAL(rjson::parse("18446744073709551615").GetUint64(), std::numeric_limits<uint64_t>::max());
    BOOST_REQUIRE_EQUAL(rjson::parse("12").GetDouble(), 12.0);
    BOOST_REQUIRE_THROW(rjson::parse("1.5").GetInt(), rjson::error);
    BOOST_REQUIRE_THROW(rjson::parse("\"1\"").GetInt64(), rjson::error);
    BOOST_REQUIRE_EQUAL(rjson::parse("1e-400").GetDouble(), 0.0);
    BOOST_REQUIRE_EQUAL(rjson::parse("0.1").GetDouble(), 0.1);
    BOOST_REQUIRE_EQUAL(rjson::parse("2.2250738585072011e-308").GetDouble(), 2.2250738585072011e-308);
}

// Doubles are printed like RapidJSON's Writer prints them: the shortest
// representation, always with a fraction or an exponent.
BOOST_AUTO_TEST_CASE(test_double_format) {
    for (auto [d, text] : std::initializer_list<std::pair<double, std::string_view>>{
            {0.0, "0.0"},
            {-0.0, "-0.0"},
            {1.0, "1.0"},
            {-1.5, "-1.5"},
            {100.0, "100.0"},
            {1e20, "100000000000000000000.0"},
            {1e21, "1e21"},
            {1.5e21, "1.5e21"},
            {123.456, "123.456"},
            {0.000001, "0.000001"},
            {0.00000123, "0.00000123"},
            {1e-7, "1e-7"},
            {1.25e-7, "1.25e-7"},
            {0.1, "0.1"},
            {1.7976931348623157e308, "1.7976931348623157e308"},
            {5e-324, "5e-324"},
            {3.0e100, "3e100"},
    }) {
        BOOST_REQUIRE_EQUAL(rjson::print(rjson::value(d)), text);
    }
    BOOST_REQUIRE_THROW(rjson::print(rjson::value(std::numeric_limits<double>::infinity())), rjson::error);
    BOOST_REQUIRE_THROW(rjson::print(rjson::value(std::numeric_limits<double>::quiet_NaN())), rjson::error);
}

BOOST_AUTO_TEST_CASE(test_string_escapes) {
    std::string s = "q\"b\\s/\x01\x1f\x7f\b\f\n\r\t\xc3\xa9";
    BOOST_REQUIRE_EQUAL(rjson::print(rjson::from_string(s)), R"("q\"b\\s/\u0001\u001F)" "\x7f" R"(\b\f\n\r\t)" "\xc3\xa9\"");
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(rjson::parse(rjson::print(rjson::from_string(s)))), s);
    // A string with a null character.
    std::string with_null("a\0b", 3);
    BOOST_REQUIRE_EQUAL(rjson::print(rjson::from_string(with_null)), R"("a\u0000b")");
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(rjson::parse(R"("a\u0000b")")), with_null);
    // Characters which need escaping, written as \u escapes.
    BOOST_REQUIRE_EQUAL(rjson::print(rjson::parse(R"(["\u0022\u005c\u001f", "\u0022\u005c\u001f0123456789"])")),
        R"(["\"\\\u001F","\"\\\u001F0123456789"])");
}

// Errors keep RapidJSON's messages and offsets.
BOOST_AUTO_TEST_CASE(test_parse_errors) {
    for (auto [text, message] : std::initializer_list<std::pair<std::string_view, std::string_view>>{
            {"", "The document is empty. at 0"},
            {"  ", "The document is empty. at 2"},
            {"{broken json", "Missing a name for object member. at 1"},
            {"[1,]", "Invalid value. at 3"},
            {"[1 2]", "Missing a comma or ']' after an array element. at 3"},
            {"[1", "Missing a comma or ']' after an array element. at 2"},
            {"[", "Invalid value. at 1"},
            {R"({"a" 1})", "Missing a colon after a name of object member. at 5"},
            {R"({"a":1 "b":2})", "Missing a comma or '}' after an object member. at 7"},
            {R"({"a":)", "Invalid value. at 5"},
            {"01", "The document root must not be followed by other values. at 1"},
            {"[01]", "Missing a comma or ']' after an array element. at 2"},
            {"1 2", "The document root must not be followed by other values. at 2"},
            {"\"abc", "Missing a closing quotation mark in string. at 4"},
            {R"("\x")", "Invalid escape character in string. at 1"},
            {R"("\u12G4")", "Incorrect hex digit after \\u escape in string. at 1"},
            {R"("\uD800")", "The surrogate pair in string is invalid. at 1"},
            {R"("\uD800A")", "The surrogate pair in string is invalid. at 1"},
            {R"("\uDC00")", "The surrogate pair in string is invalid. at 1"},
            {R"("abc\)", "Invalid escape character in string. at 4"},
            {R"("\uD800\)", "The surrogate pair in string is invalid. at 1"},
            {R"("\u12)", "Incorrect hex digit after \\u escape in string. at 1"},
            {"\"a\x01\"", "Invalid encoding in string. at 2"},
            {"1.", "Miss fraction part in number. at 2"},
            {"1.e5", "Miss fraction part in number. at 2"},
            {"1e", "Miss exponent in number. at 2"},
            {"1e+", "Miss exponent in number. at 3"},
            {"1e400", "Number too big to be stored in double. at 0"},
            {"-", "Invalid value. at 1"},
            {"-a", "Invalid value. at 1"},
            {"tru", "Invalid value. at 3"},
            {"nul1", "Invalid value. at 3"},
            {"x", "Invalid value. at 0"},
            // A NUL character is taken for the end of input.
            {std::string_view(" \0[1]", 5), "The document is empty. at 1"},
            {std::string_view("[1,\0]", 5), "Invalid value. at 3"},
            {std::string_view("\"a\0\"", 4), "Missing a closing quotation mark in string. at 2"},
    }) {
        BOOST_TEST_CONTEXT(text) {
            BOOST_REQUIRE_EQUAL(parse_error_message(text), "Parsing JSON failed: " + std::string(message));
        }
    }
    try {
        rjson::parse(R"({"a":[1,]})");
        BOOST_FAIL("expected a parse error");
    } catch (const rjson::parse_error& e) {
        BOOST_REQUIRE_EQUAL(e.offset(), 8);
    }
    BOOST_REQUIRE_EQUAL(rjson::print(rjson::parse(std::string_view("[1] \0 x", 7))), "[1]");
    BOOST_REQUIRE(!rjson::try_parse("[1,"));
    BOOST_REQUIRE(rjson::try_parse("[1]"));
}

// A UTF-8 byte order mark is skipped in contiguous input, as RapidJSON did.
BOOST_AUTO_TEST_CASE(test_byte_order_mark) {
    BOOST_REQUIRE_EQUAL(rjson::print(rjson::parse("\xEF\xBB\xBF[1]")), "[1]");
}

// Splitting the input into buffers anywhere doesn't change the result,
// including in the middle of strings, escapes, numbers and literals.
BOOST_AUTO_TEST_CASE(test_chunk_boundaries) {
    const std::string expected = rjson::print(rjson::parse(mixed_document));
    for (size_t i = 0; i <= mixed_document.size(); ++i) {
        BOOST_TEST_CONTEXT(i) {
            BOOST_REQUIRE_EQUAL(rjson::print(rjson::parse(split(mixed_document, {i}))), expected);
        }
    }
    std::vector<size_t> every_byte(mixed_document.size());
    std::iota(every_byte.begin(), every_byte.end(), 0);
    BOOST_REQUIRE_EQUAL(rjson::print(rjson::parse(split(mixed_document, every_byte))), expected);
    BOOST_REQUIRE_THROW(rjson::parse(rjson::chunked_content()), rjson::parse_error);
    // Errors report offsets in the whole input.
    std::string bad = R"({"a":[1,2,x]})";
    for (size_t i = 0; i <= bad.size(); ++i) {
        try {
            rjson::parse(split(bad, {i}));
            BOOST_FAIL("expected a parse error");
        } catch (const rjson::parse_error& e) {
            BOOST_REQUIRE_EQUAL(e.offset(), 10);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_nesting_limit) {
    auto nested = [] (size_t depth) {
        return std::string(depth, '[') + std::string(depth, ']');
    };
    BOOST_REQUIRE_NO_THROW(rjson::parse(nested(rjson::default_max_nested_level)));
    BOOST_REQUIRE_EXCEPTION(rjson::parse(nested(rjson::default_max_nested_level + 1)), rjson::error, [] (const rjson::error& e) {
        return std::string_view(e.what()) == "Max nested level reached: 78";
    });
    BOOST_REQUIRE_THROW(rjson::parse(nested(10), 9), rjson::error);
    // Printing has the same limit. Neither parsing nor printing recurses.
    auto deep = rjson::parse(nested(10000), 10000);
    BOOST_REQUIRE_THROW(rjson::print(deep), rjson::error);
    BOOST_REQUIRE_EQUAL(rjson::print(deep, 10000), nested(10000));
}

BOOST_AUTO_TEST_CASE(test_containers) {
    // Large containers are split into segments.
    auto arr = rjson::empty_array();
    for (int i = 0; i < 100000; ++i) {
        arr.PushBack(rjson::value(i));
    }
    BOOST_REQUIRE_EQUAL(arr.Size(), 100000);
    for (int i = 0; i < 100000; i += 997) {
        BOOST_REQUIRE_EQUAL(arr[i].GetInt(), i);
    }
    arr.Erase(arr.Begin() + 5000);
    BOOST_REQUIRE_EQUAL(arr.Size(), 99999);
    BOOST_REQUIRE_EQUAL(arr[4999].GetInt(), 4999);
    BOOST_REQUIRE_EQUAL(arr[5000].GetInt(), 5001);
    int64_t sum = 0;
    for (auto& v : arr.GetArray()) {
        sum += v.GetInt();
    }
    BOOST_REQUIRE_EQUAL(sum, int64_t(99999) * 100000 / 2 - 5000);
    BOOST_REQUIRE_THROW(arr[99999], rjson::error);
    BOOST_REQUIRE(rjson::copy(arr) == arr);
    BOOST_REQUIRE(rjson::parse(rjson::print(arr)) == arr);

    auto obj = rjson::empty_object();
    for (int i = 0; i < 5000; ++i) {
        rjson::add_with_string_name(obj, fmt::format("member{}", i), rjson::value(i));
    }
    BOOST_REQUIRE_EQUAL(obj.MemberCount(), 5000);
    BOOST_REQUIRE_EQUAL(rjson::get(obj, "member4321").GetInt(), 4321);
    BOOST_REQUIRE(!rjson::find(obj, "member5000"));
    // remove_member() moves the last member into the place of the removed one.
    BOOST_REQUIRE(rjson::remove_member(obj, "member10"));
    BOOST_REQUIRE(!rjson::remove_member(obj, "member10"));
    BOOST_REQUIRE_EQUAL(rjson::to_string_view((obj.MemberBegin() + 10)->name), "member4999");
    // EraseMember() keeps the order.
    auto it = obj.EraseMember(obj.MemberBegin());
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(it->name), "member1");
    BOOST_REQUIRE_EQUAL(obj.MemberCount(), 4998);
    rjson::replace_with_string_name(obj, "member1", rjson::from_string("x"));
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(obj["member1"]), "x");
    BOOST_REQUIRE_THROW(obj["nonexistent"], rjson::error);
    BOOST_REQUIRE(rjson::copy(obj) == obj);
    BOOST_REQUIRE(rjson::parse(rjson::print(obj)) == obj);

    // Misuse throws rather than crashes.
    BOOST_REQUIRE_THROW(arr.MemberCount(), rjson::error);
    BOOST_REQUIRE_THROW(obj.Size(), rjson::error);
    BOOST_REQUIRE_THROW(rjson::push_back(obj, rjson::value(1)), rjson::error);
    BOOST_REQUIRE_THROW(rjson::add(arr, "a", rjson::value(1)), rjson::error);
    BOOST_REQUIRE_THROW(rjson::empty_array().PopBack(), rjson::error);
}

BOOST_AUTO_TEST_CASE(test_equality) {
    BOOST_REQUIRE(rjson::parse(R"({"a":1,"b":[1,2]})") == rjson::parse(R"({"b":[1,2],"a":1})"));
    BOOST_REQUIRE(rjson::parse("1") == rjson::parse("1.0"));
    BOOST_REQUIRE(!(rjson::parse("[1,2]") == rjson::parse("[2,1]")));
    BOOST_REQUIRE(!(rjson::parse("\"1\"") == rjson::parse("1")));
    BOOST_REQUIRE(!(rjson::parse("-1") == rjson::parse("18446744073709551615")));
}

BOOST_AUTO_TEST_CASE(test_move_and_assignment) {
    auto v = rjson::parse(R"({"a":{"b":"0123456789012345678901234567890123456789012345678901234567890123456789"}})");
    rjson::value moved(std::move(v));
    BOOST_REQUIRE(v.IsNull());
    // Assigning a part of a value to the value itself.
    moved = std::move(moved["a"]);
    BOOST_REQUIRE_EQUAL(rjson::print(moved), R"({"b":"0123456789012345678901234567890123456789012345678901234567890123456789"})");
    rjson::copyable_value c1(rjson::copy(moved));
    rjson::copyable_value c2 = c1;
    BOOST_REQUIRE(c1 == c2);
}

// Long strings parsed from chunked_content reference its buffers, which
// stay alive as long as the strings do.
SEASTAR_THREAD_TEST_CASE(test_strings_referencing_input) {
    std::string long_string(200, 'x');
    std::string doc = R"({"a":")" + long_string + R"(","b":")" + long_string + R"("})";
    rjson::value v = rjson::parse(split(doc, {}));
    rjson::value b = std::move(rjson::get(v, "b"));
    rjson::value copy_of_b = rjson::copy(b);
    v = rjson::value();
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(b), long_string);
    b = rjson::value();
    BOOST_REQUIRE_EQUAL(rjson::to_string_view(copy_of_b), long_string);
    BOOST_REQUIRE_EQUAL(rjson::print(copy_of_b), "\"" + long_string + "\"");

    // A value may be released on another shard than the one which parsed
    // it, which then frees the buffers on the shard which owns them.
    if (smp::count > 1) {
        auto parsed = make_foreign(std::make_unique<rjson::value>(rjson::parse(split(doc, {doc.size() / 2}))));
        auto printed = smp::submit_to(1, [p = std::move(parsed)] () mutable {
            auto local = rjson::copy(*p);
            *p = rjson::value();
            // local holds the last references to the buffers.
            return rjson::print(local);
        }).get();
        BOOST_REQUIRE_EQUAL(printed, doc);

        std::atomic<int> freed_on = -1;
        rjson::chunked_content chunks;
        chunks.emplace_back(doc.data(), doc.size(), make_deleter([&freed_on] { freed_on = this_shard_id(); }));
        parsed = make_foreign(std::make_unique<rjson::value>(rjson::parse(std::move(chunks))));
        smp::submit_to(1, [p = std::move(parsed)] () mutable {
            *p = rjson::value();
        }).get();
        while (freed_on == -1) {
            seastar::thread::yield();
        }
        BOOST_REQUIRE_EQUAL(freed_on, 0);
    }
}

SEASTAR_TEST_CASE(test_parse_gently) {
    std::string doc = "[";
    for (int i = 0; i < 100000; ++i) {
        doc += fmt::format("{}{{\"k\":{},\"s\":\"{}\"}}", i ? "," : "", i, std::string(i % 100, 'a'));
    }
    doc += "]";
    auto v = co_await rjson::parse_gently(split(doc, {1000, 5000, 100000, 200000}));
    BOOST_REQUIRE_EQUAL(rjson::print(v), doc);
    BOOST_REQUIRE_THROW(co_await rjson::parse_gently(split("[1,", {})), rjson::parse_error);
}

SEASTAR_TEST_CASE(test_print_to_stream) {
    auto v = rjson::parse(mixed_document);
    BOOST_REQUIRE_EQUAL(co_await print_to_stream(v), rjson::print(v));
    // Large enough to need several buffers.
    auto arr = rjson::empty_array();
    for (int i = 0; i < 10000; ++i) {
        arr.PushBack(rjson::copy(v));
    }
    BOOST_REQUIRE_EQUAL(co_await print_to_stream(arr), rjson::print(arr));
    co_await rjson::destroy_gently(std::move(arr));
    BOOST_REQUIRE(arr.IsNull());
}

SEASTAR_TEST_CASE(test_print_with_extra_array) {
    utils::chunked_vector<rjson::value> items;
    for (int i = 0; i < 3; ++i) {
        items.push_back(rjson::parse(fmt::format(R"({{"i":{}}})", i)));
    }
    for (auto [obj, expected] : std::initializer_list<std::pair<std::string_view, std::string_view>>{
            {"{}", R"({"Items":[{"i":0},{"i":1},{"i":2}]})"},
            {R"({"Count":3})", R"({"Count":3,"Items":[{"i":0},{"i":1},{"i":2}]})"},
    }) {
        std::string out;
        output_stream<char> os(data_sink(std::make_unique<string_data_sink>(out)));
        co_await rjson::print_with_extra_array(rjson::parse(obj), "Items", items, os);
        co_await os.close();
        BOOST_REQUIRE_EQUAL(out, expected);
    }
}

namespace {

// Records the events of parse_sax().
struct recording_handler {
    std::vector<std::string> events;
    bool stop_at_key = false;
    bool add(std::string e) { events.push_back(std::move(e)); return true; }
    bool Null() { return add("null"); }
    bool Bool(bool b) { return add(fmt::format("bool {}", b)); }
    bool Int(int i) { return add(fmt::format("int {}", i)); }
    bool Uint(unsigned u) { return add(fmt::format("uint {}", u)); }
    bool Int64(int64_t i) { return add(fmt::format("int64 {}", i)); }
    bool Uint64(uint64_t u) { return add(fmt::format("uint64 {}", u)); }
    bool Double(double d) { return add(fmt::format("double {}", d)); }
    bool String(const char* s, rjson::size_type len, bool) { return add(fmt::format("string {}", std::string_view(s, len))); }
    bool Key(const char* s, rjson::size_type len, bool) { return !stop_at_key && add(fmt::format("key {}", std::string_view(s, len))); }
    bool StartObject() { return add("{"); }
    bool EndObject(rjson::size_type n) { return add(fmt::format("}} {}", n)); }
    bool StartArray() { return add("["); }
    bool EndArray(rjson::size_type n) { return add(fmt::format("] {}", n)); }
};

} // anonymous namespace

BOOST_AUTO_TEST_CASE(test_parse_sax) {
    recording_handler h;
    std::vector<std::string> bufs{R"({"a":[null,true,1,-1,42949)", R"(67296,-2147483649,1.5,"x"]})"};
    size_t next = 0;
    auto source = [&] {
        if (next == bufs.size()) {
            return temporary_buffer<char>();
        }
        auto& b = bufs[next++];
        return temporary_buffer<char>(b.data(), b.size());
    };
    rjson::parse_sax(source, h);
    BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt::join(h.events, "|")),
        "{|key a|[|null|bool true|uint 1|int -1|uint64 4294967296|int64 -2147483649|double 1.5|string x|] 8|} 1");

    recording_handler stopping;
    stopping.stop_at_key = true;
    next = 0;
    BOOST_REQUIRE_EXCEPTION(rjson::parse_sax(source, stopping), rjson::parse_error, [] (const rjson::parse_error& e) {
        return e.offset() == 1;
    });
}

BOOST_AUTO_TEST_CASE(test_streaming_writer) {
    std::ostringstream os;
    rjson::streaming_writer w(os);
    w.StartObject();
    w.Key("a");
    w.StartArray();
    w.Null();
    w.Bool(false);
    w.Int(-1);
    w.Uint64(18446744073709551615u);
    w.Double(0.5);
    w.Double(std::numeric_limits<double>::quiet_NaN());
    w.String("q\"\n");
    w.RawNumber("1.50");
    w.EndArray();
    w.Key("b");
    w.RawValue("{\"x\":1}", rjson::type::kObjectType);
    w.EndObject();
    BOOST_REQUIRE_EQUAL(os.str(), R"({"a":[null,false,-1,18446744073709551615,0.5,null,"q\"\n",1.50],"b":{"x":1}})");

    std::ostringstream os2;
    rjson::streaming_writer bad(os2);
    bad.StartObject();
    BOOST_REQUIRE_THROW(bad.Null(), rjson::error);
    BOOST_REQUIRE_THROW(bad.EndArray(), rjson::error);
    std::ostringstream os3;
    rjson::streaming_writer two_roots(os3);
    two_roots.Null();
    BOOST_REQUIRE_THROW(two_roots.Null(), rjson::error);
}

BOOST_AUTO_TEST_CASE(test_type_helper) {
    auto v = rjson::empty_object();
    rjson::add(v, "s", std::string("str"));
    rjson::add(v, "i", 5);
    rjson::add(v, "u", uint64_t(7));
    rjson::add(v, "b", true);
    rjson::add(v, "d", 2.5);
    BOOST_REQUIRE_EQUAL(rjson::print(v), R"({"s":"str","i":5,"u":7,"b":true,"d":2.5})");
    BOOST_REQUIRE_EQUAL(rjson::get<std::string>(v, "s"), "str");
    BOOST_REQUIRE_EQUAL(rjson::get<int>(v, "i"), 5);
    BOOST_REQUIRE_EQUAL(rjson::get<uint64_t>(v, "u"), 7);
    BOOST_REQUIRE_EQUAL(rjson::get<bool>(v, "b"), true);
    BOOST_REQUIRE_EQUAL(*rjson::get_opt<double>(v, "d"), 2.5);
    BOOST_REQUIRE(!rjson::get_opt<int>(v, "nonexistent"));
    BOOST_REQUIRE_THROW(rjson::get<int>(v, "s"), rjson::malformed_value);
    BOOST_REQUIRE_THROW(rjson::get<int>(v, "nonexistent"), rjson::missing_value);
}
