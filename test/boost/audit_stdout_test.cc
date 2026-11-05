/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <climits>

#undef SEASTAR_TESTING_MAIN
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>
#include <sys/stat.h>

#include "audit/audit_stdout_storage_helper.hh"
#include "db/config.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/tmpdir.hh"

BOOST_AUTO_TEST_SUITE(audit_stdout_test)

namespace {

const socket_address test_addr(ipv4_addr("127.0.0.1", 9042));

sstring make_fifo(const tmpdir& dir) {
    auto path = (dir.path() / "stdout").native();
    throw_system_error_on(::mkfifo(path.c_str(), 0600) == -1, "mkfifo");
    return path;
}

// A FIFO standing in for the standard output, with its read end open.
struct test_pipe {
    tmpdir dir;
    sstring path;
    input_stream<char> in;

    test_pipe()
        : path(make_fifo(dir))
        , in(make_pipe_input_stream(file_desc::open(path, O_RDONLY | O_NONBLOCK)))
    {}
};

audit::audit_sink_set test_sinks() {
    audit::audit_sink_set sinks;
    sinks.set(audit::audit_sink::standard_output);
    return sinks;
}

// The capacity of a new pipe, which depends on the page size.
size_t pipe_capacity() {
    static const size_t capacity = [] {
        int fds[2];
        throw_system_error_on(::pipe(fds) == -1, "pipe");
        auto read_end = file_desc::from_fd(fds[0]);
        auto write_end = file_desc::from_fd(fds[1]);
        auto capacity = ::fcntl(fds[0], F_GETPIPE_SZ);
        throw_system_error_on(capacity == -1, "fcntl");
        return size_t(capacity);
    }();
    return capacity;
}

// Every tenth query is longer than a pipe can hold, the others are around PIPE_BUF.
sstring test_query(unsigned shard, int i) {
    size_t length = i % 10 == 0 ? pipe_capacity() + 4096 : (i % 3) * 3000;
    return seastar::format("{}:{}:{}", shard, i, sstring(length, char('a' + i % 26)));
}

sstring test_record(std::string_view query) {
    return seastar::format(R"(scylla-audit: node="{}", category="DML", cl="ONE", error="false", keyspace="ks", query="{}", client_ip="{}", table="t", username="user")",
            test_addr, query, test_addr);
}

sstring test_record(unsigned shard, int i) {
    return test_record(test_query(shard, i));
}

// Query 0 is longer than a pipe can hold, the others are all equally long.
sstring memory_test_query(unsigned shard, int i) {
    return seastar::format("{}:{:03}:{}", shard, i, sstring(i == 0 ? pipe_capacity() + 4096 : pipe_capacity() / 4, 'a'));
}

// The writes of one shard that are not finished yet, with the records they write.
struct unfinished_writes {
    std::vector<std::unique_ptr<audit::audit_info>> infos;
    std::vector<future<>> writes;

    future<> stop() {
        return make_ready_future<>();
    }
};

struct fill_result {
    int taken;
    sstring error;
};

// Writes the records of memory_test_query() until one fails at once, and
// returns how many were taken before it and its error.
fill_result write_until_rejected(audit::audit_stdout_storage_helper& helper, unfinished_writes& unfinished) {
    constexpr int max_records = 100;
    for (int i = 0; i < max_records; ++i) {
        auto& info = *unfinished.infos.emplace_back(std::make_unique<audit::audit_info>(audit::statement_category::DML, "ks", "t", false));
        info.set_query_string(memory_test_query(this_shard_id(), i));
        auto write = helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "user", false);
        if (write.failed()) {
            return {i, seastar::format("{}", write.get_exception())};
        }
        unfinished.writes.push_back(std::move(write));
    }
    return {max_records, ""};
}

future<> write_test_record(audit::audit_stdout_storage_helper& helper, int i) {
    audit::audit_info info(audit::statement_category::DML, "ks", "t", false);
    info.set_query_string(test_query(this_shard_id(), i));
    co_await helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "user", false);
}

future<> write_test_records(audit::audit_stdout_storage_helper& helper, int count) {
    co_await coroutine::parallel_for_each(std::views::iota(0, count), [&helper] (int i) {
        return write_test_record(helper, i);
    });
}

// Returns the lines of `in` up to its end, without line breaks.
future<std::vector<std::string>> read_all_lines(input_stream<char>& in) {
    std::vector<std::string> lines;
    std::string unterminated;
    while (auto buf = co_await in.read()) {
        auto searched = unterminated.size();
        unterminated.append(buf.get(), buf.size());
        size_t start = 0;
        for (auto end = unterminated.find('\n', searched); end != std::string::npos; end = unterminated.find('\n', start)) {
            lines.emplace_back(unterminated, start, end - start);
            start = end + 1;
        }
        unterminated.erase(0, start);
    }
    co_return lines;
}

// Returns the first `count` lines of `in`, without line breaks.
future<std::vector<std::string>> read_lines(input_stream<char>& in, size_t count) {
    std::vector<std::string> lines;
    std::string unterminated;
    while (lines.size() < count) {
        auto buf = co_await in.read();
        if (buf.empty()) {
            throw std::runtime_error(fmt::format("EOF after {} lines", lines.size()));
        }
        auto searched = unterminated.size();
        unterminated.append(buf.get(), buf.size());
        size_t start = 0;
        for (auto end = unterminated.find('\n', searched); end != std::string::npos; end = unterminated.find('\n', start)) {
            lines.emplace_back(unterminated, start, end - start);
            start = end + 1;
        }
        unterminated.erase(0, start);
    }
    co_return lines;
}

}

// Records from all shards reach the pipe whole and, per shard, in the order
// they were written, also when they are longer than PIPE_BUF or the pipe capacity.
SEASTAR_THREAD_TEST_CASE(test_records_do_not_interleave) {
    test_pipe pipe;
    db::config cfg;
    sharded<audit::audit_stdout_storage_helper> helpers;
    helpers.start(pipe.path).get();
    auto stop_helpers = defer([&helpers] noexcept {
        helpers.stop().get();
    });
    helpers.invoke_on_all([&cfg] (audit::audit_stdout_storage_helper& helper) {
        return helper.start(cfg);
    }).get();

    constexpr int records_per_shard = 100;
    auto lines = read_lines(pipe.in, this_smp_shard_count() * records_per_shard);
    helpers.invoke_on_all([] (audit::audit_stdout_storage_helper& helper) {
        return write_test_records(helper, records_per_shard);
    }).get();

    std::vector<int> next_record(this_smp_shard_count());
    for (const auto& line : lines.get()) {
        auto shard_pos = line.find(R"(query=")") + 7;
        auto record_pos = line.find(':', shard_pos) + 1;
        auto shard = std::stoul(line.substr(shard_pos, record_pos - shard_pos - 1));
        BOOST_REQUIRE_LT(shard, this_smp_shard_count());
        auto record = std::stoi(line.substr(record_pos, line.find(':', record_pos) - record_pos));
        BOOST_REQUIRE_EQUAL(record, next_record[shard]++);
        BOOST_REQUIRE(line.ends_with(test_record(shard, record)));
    }
}

// While nobody reads the pipe, the writes wait for it without blocking the shards.
SEASTAR_THREAD_TEST_CASE(test_waits_for_reader) {
    test_pipe pipe;
    db::config cfg;
    sharded<audit::audit_stdout_storage_helper> helpers;
    helpers.start(pipe.path).get();
    auto stop_helpers = defer([&helpers] noexcept {
        helpers.stop().get();
    });
    helpers.invoke_on_all([&cfg] (audit::audit_stdout_storage_helper& helper) {
        return helper.start(cfg);
    }).get();

    constexpr int records_per_shard = 50;
    auto writes = helpers.invoke_on_all([] (audit::audit_stdout_storage_helper& helper) {
        return write_test_records(helper, records_per_shard);
    });
    seastar::sleep(std::chrono::milliseconds(100)).get();
    BOOST_REQUIRE(!writes.available());

    auto lines = read_lines(pipe.in, this_smp_shard_count() * records_per_shard).get();
    writes.get();
    BOOST_REQUIRE_EQUAL(lines.size(), this_smp_shard_count() * records_per_shard);
}

// While nobody reads the pipe, writes give up after the timeout, then at once,
// and stopping gives up too. The records still reach the pipe once it is read.
SEASTAR_THREAD_TEST_CASE(test_gives_up_on_stalled_reader) {
    constexpr auto timeout = std::chrono::milliseconds(500);
    test_pipe pipe;
    db::config cfg;
    sharded<audit::audit_stdout_storage_helper> helpers;
    helpers.start(pipe.path, timeout).get();
    auto stop_helpers = defer([&helpers] noexcept {
        helpers.stop().get();
    });
    helpers.invoke_on_all([&cfg] (audit::audit_stdout_storage_helper& helper) {
        return helper.start(cfg);
    }).get();

    // record 0 is longer than the pipe can hold
    BOOST_REQUIRE_EXCEPTION(helpers.invoke_on_all([] (audit::audit_stdout_storage_helper& helper) {
        return write_test_record(helper, 0);
    }).get(), audit::audit_exception, exception_predicate::message_contains("did not write the record within"));

    seastar::sleep(timeout / 4).get();
    auto rejected_at = std::chrono::steady_clock::now();
    BOOST_REQUIRE_EXCEPTION(helpers.invoke_on_all([] (audit::audit_stdout_storage_helper& helper) {
        return write_test_record(helper, 1);
    }).get(), audit::audit_exception, exception_predicate::message_contains("has not finished a write"));
    BOOST_REQUIRE_LT(std::chrono::steady_clock::now() - rejected_at, timeout);

    helpers.stop().get();

    auto lines = read_all_lines(pipe.in).get();
    BOOST_REQUIRE_EQUAL(lines.size(), this_smp_shard_count());
    for (const auto& line : lines) {
        auto shard_pos = line.find(R"(query=")") + 7;
        auto shard = std::stoul(line.substr(shard_pos, line.find(':', shard_pos) - shard_pos));
        BOOST_REQUIRE(line.ends_with(test_record(shard, 0)));
    }
}

// While nobody reads the pipe, each shard takes records up to the memory
// limit. The record that does not fit fails at once and never reaches the
// pipe, and once the pipe is read, the shard takes records again.
SEASTAR_THREAD_TEST_CASE(test_limits_unwritten_memory) {
    const size_t max_unwritten_memory = pipe_capacity() * 4;
    test_pipe pipe;
    db::config cfg;
    sharded<audit::audit_stdout_storage_helper> helpers;
    helpers.start(pipe.path, std::chrono::minutes(1), max_unwritten_memory).get();
    auto stop_helpers = defer([&helpers] noexcept {
        helpers.stop().get();
    });
    helpers.invoke_on_all([&cfg] (audit::audit_stdout_storage_helper& helper) {
        return helper.start(cfg);
    }).get();
    sharded<unfinished_writes> unfinished;
    unfinished.start().get();
    auto stop_unfinished = defer([&unfinished] noexcept {
        unfinished.stop().get();
    });

    // record 0 is longer than the pipe can hold, so no record is written
    auto results = helpers.map([&unfinished] (audit::audit_stdout_storage_helper& helper) {
        return write_until_rejected(helper, unfinished.local());
    }).get();
    int taken = 0;
    for (const auto& result : results) {
        BOOST_REQUIRE_GT(result.taken, 1);
        BOOST_REQUIRE_NE(result.error.find("has no room for the record"), sstring::npos);
        taken += result.taken;
    }

    auto lines = read_lines(pipe.in, taken).get();
    unfinished.invoke_on_all([] (unfinished_writes& shard_writes) -> future<> {
        for (auto& write : shard_writes.writes) {
            co_await std::move(write);
        }
    }).get();
    std::vector<int> next_record(this_smp_shard_count());
    std::vector<size_t> taken_memory(this_smp_shard_count());
    std::vector<size_t> last_record_size(this_smp_shard_count());
    for (const auto& line : lines) {
        auto shard_pos = line.find(R"(query=")") + 7;
        auto shard = std::stoul(line.substr(shard_pos, line.find(':', shard_pos) - shard_pos));
        BOOST_REQUIRE_LT(shard, this_smp_shard_count());
        BOOST_REQUIRE(line.ends_with(test_record(memory_test_query(shard, next_record[shard]++))));
        last_record_size[shard] = line.size() + 1;
        taken_memory[shard] += last_record_size[shard];
    }
    for (unsigned shard = 0; shard < this_smp_shard_count(); ++shard) {
        BOOST_REQUIRE_EQUAL(next_record[shard], results[shard].taken);
        BOOST_REQUIRE_LE(taken_memory[shard], max_unwritten_memory);
        // the rejected record is as long as the last one taken
        BOOST_REQUIRE_GT(taken_memory[shard] + last_record_size[shard], max_unwritten_memory);
    }

    auto more_lines = read_lines(pipe.in, this_smp_shard_count());
    helpers.invoke_on_all([] (audit::audit_stdout_storage_helper& helper) -> future<> {
        audit::audit_info info(audit::statement_category::DML, "ks", "t", false);
        info.set_query_string(memory_test_query(this_shard_id(), 999));
        co_await helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "user", false);
    }).get();
    for (const auto& line : more_lines.get()) {
        auto shard_pos = line.find(R"(query=")") + 7;
        auto shard = std::stoul(line.substr(shard_pos, line.find(':', shard_pos) - shard_pos));
        BOOST_REQUIRE(line.ends_with(test_record(memory_test_query(shard, 999))));
    }
}

// A record that the memory limit rejects leaves no future behind that fails
// unhandled once the timeout passes.
SEASTAR_THREAD_TEST_CASE(test_rejected_record_leaves_no_failed_future) {
    constexpr auto timeout = std::chrono::milliseconds(500);
    test_pipe pipe;
    db::config cfg;
    sharded<audit::audit_stdout_storage_helper> helpers;
    helpers.start(pipe.path, timeout, pipe_capacity() * 4).get();
    auto stop_helpers = defer([&helpers] noexcept {
        helpers.stop().get();
    });
    helpers.invoke_on_all([&cfg] (audit::audit_stdout_storage_helper& helper) {
        return helper.start(cfg);
    }).get();
    sharded<unfinished_writes> unfinished;
    unfinished.start().get();
    auto stop_unfinished = defer([&unfinished] noexcept {
        unfinished.stop().get();
    });
    auto abandoned_failed_futures = [&helpers] {
        return helpers.map([] (audit::audit_stdout_storage_helper&) {
            return engine().abandoned_failed_futures();
        }).get();
    };

    auto abandoned_before = abandoned_failed_futures();
    auto results = helpers.map([&unfinished] (audit::audit_stdout_storage_helper& helper) {
        return write_until_rejected(helper, unfinished.local());
    }).get();
    int taken = 0;
    for (const auto& result : results) {
        BOOST_REQUIRE_NE(result.error.find("has no room for the record"), sstring::npos);
        taken += result.taken;
    }

    seastar::sleep(timeout * 2).get();
    auto timed_out = unfinished.map([] (unfinished_writes& shard_writes) {
        int count = 0;
        for (auto& write : shard_writes.writes) {
            if (write.failed() && seastar::format("{}", write.get_exception()).find("did not write the record within") != sstring::npos) {
                ++count;
            }
        }
        return count;
    }).get();
    auto abandoned_after = abandoned_failed_futures();
    for (unsigned shard = 0; shard < this_smp_shard_count(); ++shard) {
        BOOST_REQUIRE_EQUAL(timed_out[shard], results[shard].taken);
        BOOST_REQUIRE_EQUAL(abandoned_after[shard], abandoned_before[shard]);
    }

    // drain the pipe, so that no write outlives the helpers
    read_lines(pipe.in, taken).get();
}

// A record longer than the memory limit is taken only while no other record of
// its shard waits to be written.
SEASTAR_THREAD_TEST_CASE(test_takes_record_longer_than_limit) {
    const size_t max_unwritten_memory = pipe_capacity() * 4;
    test_pipe pipe;
    audit::audit_stdout_storage_helper helper(pipe.path, std::chrono::minutes(1), max_unwritten_memory);
    helper.start(db::config()).get();
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });
    auto write = [&helper] (const sstring& query) {
        audit::audit_info info(audit::statement_category::DML, "ks", "t", false);
        info.set_query_string(query);
        return helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "user", false);
    };
    auto first_query = memory_test_query(this_shard_id(), 0);
    auto long_query = sstring(max_unwritten_memory + 4096, 'b');

    // the first record is longer than the pipe can hold, so it waits
    auto first = write(first_query);
    BOOST_REQUIRE_EXCEPTION(write(long_query).get(), audit::audit_exception,
            exception_predicate::message_contains("has no room for the record"));

    auto lines = read_lines(pipe.in, 2);
    first.get();
    write(long_query).get();
    auto written = lines.get();
    BOOST_REQUIRE(written[0].ends_with(test_record(first_query)));
    BOOST_REQUIRE(written[1].ends_with(test_record(long_query)));
}

// A batch of more buffers than one write to the pipe takes reaches the pipe
// whole and in order.
SEASTAR_THREAD_TEST_CASE(test_writes_batch_of_many_buffers) {
    constexpr size_t chunk_size = 64 * 1024;
    constexpr int records = 2 * IOV_MAX;
    test_pipe pipe;
    audit::audit_stdout_storage_helper helper(pipe.path, std::chrono::minutes(5), records * chunk_size * 2);
    helper.start(db::config()).get();
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });
    // Query 0 is longer than a pipe can hold, so the others wait for it in one batch.
    auto query = [] (int i) {
        return seastar::format("{:04}:{}", i, sstring(i == 0 ? pipe_capacity() + 4096 : chunk_size, 'c'));
    };

    // The batch merges adjacent buffers, so a spacer after each record keeps
    // its buffers apart, as they are when memory is fragmented.
    std::vector<temporary_buffer<char>> spacers;
    std::vector<future<>> writes;
    for (int i = 0; i < records; ++i) {
        audit::audit_info info(audit::statement_category::DML, "ks", "t", false);
        info.set_query_string(query(i));
        writes.push_back(helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "user", false));
        spacers.emplace_back(chunk_size);
    }

    auto lines = read_all_lines(pipe.in);
    for (auto& write : writes) {
        write.get();
    }
    helper.stop().get();
    auto written = lines.get();
    BOOST_REQUIRE_EQUAL(written.size(), size_t(records));
    for (int i = 0; i < records; ++i) {
        BOOST_REQUIRE(written[i].ends_with(test_record(query(i))));
    }
}

// Another shard cannot write before shard 0 has opened the stream.
SEASTAR_THREAD_TEST_CASE(test_fails_before_start) {
    if (this_smp_shard_count() < 2) {
        std::cerr << "Cannot run test " << get_name() << " with this_smp_shard_count() < 2" << std::endl;
        return;
    }
    test_pipe pipe;
    sharded<audit::audit_stdout_storage_helper> helpers;
    helpers.start(pipe.path).get();
    auto stop_helpers = defer([&helpers] noexcept {
        helpers.stop().get();
    });

    BOOST_REQUIRE_EXCEPTION(helpers.invoke_on(1, [] (audit::audit_stdout_storage_helper& helper) {
        return write_test_record(helper, 1);
    }).get(), audit::audit_exception, exception_predicate::message_contains("is not started"));
}

SEASTAR_THREAD_TEST_CASE(test_escapes_fields) {
    test_pipe pipe;
    audit::audit_stdout_storage_helper helper(pipe.path);
    helper.start(db::config()).get();
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });

    auto lines = read_lines(pipe.in, 2);
    audit::audit_info info(audit::statement_category::DML, "ks", "t", false);
    info.set_query_string(std::string_view("a\"b\\c\nd\re\tf\x1bg\x7fh\xc2\x9bi\xc2\x85j\xc2\xa0k\x9bl\xc2"));
    helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "us\"er\n", false).get();
    helper.write_login(test_sinks(), "x\ny", test_addr, test_addr, true).get();

    auto written = lines.get();
    BOOST_REQUIRE(written[0].ends_with(R"(keyspace="ks", query="a\"b\\c\nd\re\tf\x1bg\x7fh\u009bi\u0085j)" "\xc2\xa0" R"(k\x9bl\xc2", client_ip="127.0.0.1:9042", table="t", username="us\"er\n")"));
    BOOST_REQUIRE(written[1].ends_with(R"(category="AUTH", cl="", error="true", keyspace="", query="", client_ip="127.0.0.1:9042", table="", username="x\ny")"));
}

// A byte that does not belong to well-formed UTF-8 is escaped alone, and the
// decoding resumes right after it.
SEASTAR_THREAD_TEST_CASE(test_escapes_invalid_utf8) {
    test_pipe pipe;
    audit::audit_stdout_storage_helper helper(pipe.path);
    helper.start(db::config()).get();
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });

    auto lines = read_lines(pipe.in, 1);
    audit::audit_info info(audit::statement_category::DML, "ks", "t", false);
    // 2-, 3- and 4-byte sequences cut short by ASCII, overlong forms of each
    // length, a surrogate, a code point past U+10FFFF, and well-formed 3- and
    // 4-byte sequences
    info.set_query_string(std::string_view("\xc2" "A|\xe2\x82" "A|\xf0\x9f\x98" "A|\xc0\x80|\xe0\x80\x80|\xf0\x80\x80\x80|"
            "\xed\xa0\x80|\xf4\x90\x80\x80|\xe2\x82\xac|\xf0\x9f\x98\x80"));
    helper.write(test_sinks(), &info, test_addr, test_addr, db::consistency_level::ONE, "user", false).get();

    BOOST_REQUIRE(lines.get()[0].ends_with(R"(query="\xc2A|\xe2\x82A|\xf0\x9f\x98A|\xc0\x80|\xe0\x80\x80|\xf0\x80\x80\x80|)"
            R"(\xed\xa0\x80|\xf4\x90\x80\x80|)" "\xe2\x82\xac|\xf0\x9f\x98\x80"
            R"(", client_ip="127.0.0.1:9042", table="t", username="user")"));
}

// A failed login is audited with a username that nothing has validated as
// UTF-8, and escaping a long one must not take quadratic time.
SEASTAR_THREAD_TEST_CASE(test_escapes_long_invalid_field) {
    test_pipe pipe;
    audit::audit_stdout_storage_helper helper(pipe.path, std::chrono::minutes(1));
    helper.start(db::config()).get();
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });

    constexpr size_t length = 1024 * 1024;
    auto lines = read_lines(pipe.in, 1);
    auto started = std::chrono::steady_clock::now();
    helper.write_login(test_sinks(), sstring(length, '\xff'), test_addr, test_addr, true).get();
    BOOST_REQUIRE_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(20));

    std::string escaped;
    for (size_t i = 0; i < length; ++i) {
        escaped += "\\xff";
    }
    BOOST_REQUIRE(lines.get()[0].ends_with(seastar::format(R"(username="{}")", escaped)));
}

// The write fails rather than the node being killed by SIGPIPE.
SEASTAR_THREAD_TEST_CASE(test_fails_without_reader) {
    test_pipe pipe;
    audit::audit_stdout_storage_helper helper(pipe.path);
    helper.start(db::config()).get();
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });
    pipe.in.close().get();

    BOOST_REQUIRE_EXCEPTION(write_test_record(helper, 1).get(), audit::audit_exception,
            exception_predicate::message_contains("failed (writing to"));
}

SEASTAR_THREAD_TEST_CASE(test_rejects_pipe_without_reader) {
    tmpdir dir;
    audit::audit_stdout_storage_helper helper(make_fifo(dir));
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });

    BOOST_REQUIRE_EXCEPTION(helper.start(db::config()).get(), audit::audit_exception,
            exception_predicate::message_contains("nothing reads the pipe"));
}

// A file stream would overwrite what the process writes to the same file.
SEASTAR_THREAD_TEST_CASE(test_rejects_regular_file) {
    tmpdir dir;
    auto path = (dir.path() / "stdout").native();
    file_desc::open(path, O_CREAT | O_WRONLY, 0600);
    audit::audit_stdout_storage_helper helper(path);
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });

    BOOST_REQUIRE_EXCEPTION(helper.start(db::config()).get(), audit::audit_exception,
            exception_predicate::message_contains("is a regular file"));
}

// A socket, such as the systemd journal, cannot be opened again.
SEASTAR_THREAD_TEST_CASE(test_rejects_socket) {
    auto socket = file_desc::socket(AF_UNIX, SOCK_STREAM);
    audit::audit_stdout_storage_helper helper(seastar::format("/proc/self/fd/{}", socket.get()));
    auto stop_helper = defer([&helper] noexcept {
        helper.stop().get();
    });

    BOOST_REQUIRE_EXCEPTION(helper.start(db::config()).get(), audit::audit_exception,
            exception_predicate::message_contains("is a socket"));
}

// Records written to /dev/null or /dev/zero would be lost without any error.
SEASTAR_THREAD_TEST_CASE(test_rejects_dev_null) {
    for (auto path : {"/dev/null", "/dev/zero"}) {
        audit::audit_stdout_storage_helper helper(path);
        auto stop_helper = defer([&helper] noexcept {
            helper.stop().get();
        });

        BOOST_REQUIRE_EXCEPTION(helper.start(db::config()).get(), audit::audit_exception,
                exception_predicate::message_contains("it is a memory device"));
    }
}

BOOST_AUTO_TEST_SUITE_END()
