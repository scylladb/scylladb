/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "audit/audit_stdout_storage_helper.hh"

#include <algorithm>
#include <array>
#include <filesystem>
#include <sys/sysmacros.h>

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_future.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/util/defer.hh>

#include <fmt/chrono.h>

#include "utils/chunked_vector.hh"

namespace audit {

namespace {

// Passes `value` to `put` in pieces, with quotes, backslashes and ASCII control
// characters escaped, so the record stays on one line.
void put_escaped(auto&& put, std::string_view value) {
    auto put_hex_escape = [&put] (unsigned char byte) {
        constexpr std::string_view digits = "0123456789abcdef";
        std::array<char, 4> escape = {'\\', 'x', digits[byte >> 4], digits[byte & 0xf]};
        put(std::string_view(escape.data(), escape.size()));
    };
    size_t unwritten = 0;
    for (size_t i = 0; i < value.size(); ++i) {
        auto c = static_cast<unsigned char>(value[i]);
        if (c != '"' && c != '\\' && c >= 0x20 && c != 0x7f) {
            continue;
        }
        if (i > unwritten) {
            put(value.substr(unwritten, i - unwritten));
        }
        if (c == '\n') {
            put("\\n");
        } else if (c == '\r') {
            put("\\r");
        } else if (c == '\t') {
            put("\\t");
        } else if (c == '"') {
            put("\\\"");
        } else if (c == '\\') {
            put("\\\\");
        } else {
            put_hex_escape(c);
        }
        unwritten = i + 1;
    }
    if (unwritten < value.size()) {
        put(value.substr(unwritten));
    }
}

}

// Records in the order they are written, copied into chunks the batch owns.
class record_batch {
    static constexpr size_t min_chunk_size = 1024;
    static constexpr size_t max_chunk_size = 64 * 1024;

    std::vector<temporary_buffer<char>> _chunks;
    size_t _chunk_free = 0;
    utils::chunked_vector<std::string_view> _data;

    void add(std::string_view data) {
        if (!_data.empty() && _data.back().data() + _data.back().size() == data.data()) {
            _data.back() = std::string_view(_data.back().data(), _data.back().size() + data.size());
        } else {
            _data.push_back(data);
        }
    }
public:
    struct position {
        size_t chunks;
        size_t chunk_free;
        size_t data;
        size_t last_data_size;
    };

    position mark() const noexcept {
        return {_chunks.size(), _chunk_free, _data.size(), _data.empty() ? 0 : _data.back().size()};
    }

    // Drops everything appended after `pos`.
    void truncate(const position& pos) noexcept {
        _chunks.erase(_chunks.begin() + pos.chunks, _chunks.end());
        _chunk_free = pos.chunk_free;
        while (_data.size() > pos.data) {
            _data.pop_back();
        }
        if (!_data.empty()) {
            _data.back() = std::string_view(_data.back().data(), pos.last_data_size);
        }
    }

    void append(std::string_view piece) {
        while (!piece.empty()) {
            if (_chunk_free == 0) {
                auto size = std::min(max_chunk_size, min_chunk_size << std::min<size_t>(_chunks.size(), 6));
                _chunks.emplace_back(size);
                _chunk_free = size;
            }
            auto& chunk = _chunks.back();
            auto dst = chunk.get_write() + chunk.size() - _chunk_free;
            auto n = std::min(_chunk_free, piece.size());
            std::copy_n(piece.data(), n, dst);
            _chunk_free -= n;
            add(std::string_view(dst, n));
            piece.remove_prefix(n);
        }
    }

    bool empty() const noexcept {
        return _data.empty();
    }

    const utils::chunked_vector<std::string_view>& data() const noexcept {
        return _data;
    }
};

// The stream to the standard output, which only shard 0 writes to.
struct stdout_stream {
    sstring path;
    std::optional<data_sink> sink;

    future<> write(const record_batch& batch) {
        if (!sink) {
            throw audit_exception("Stdout audit backend is not started");
        }
        try {
            std::vector<temporary_buffer<char>> buffers;
            buffers.reserve(batch.data().size());
            for (std::string_view data : batch.data()) {
                // a non-owning buffer: the batch outlives the put below
                buffers.emplace_back(const_cast<char*>(data.data()), data.size(), deleter());
            }
            co_await sink->put(std::move(buffers));
        } catch (...) {
            auto error_msg = seastar::format("Stdout audit backend failed (writing to {} resulted in {}).", path, std::current_exception());
            logger.error("{}", error_msg);
            throw audit_exception(std::move(error_msg));
        }
    }
};

// Collects records into batches: while one batch is being flushed, new
// records wait for the next one.
class stdout_record_batcher : public enable_shared_from_this<stdout_record_batcher> {
public:
    using flush_func = noncopyable_function<future<>(const record_batch&)>;
private:
    std::chrono::milliseconds _timeout;
    flush_func _flush;
    record_batch _pending;
    // Engaged whenever _pending is not empty.
    std::optional<shared_promise<>> _pending_flushed;
    bool _flushing = false;
    lowres_clock::time_point _flush_started;
    future<> _flush_loop = make_ready_future<>();

    // Runs in the scheduling group of the write that starts it, as the writes
    // of the other sinks do.
    future<> flush_loop() {
        // a flush into a pipe that nobody reads can outlive the helper
        auto self = shared_from_this();
        _flushing = true;
        auto stop_flushing = defer([this] () noexcept { _flushing = false; });
        while (!_pending.empty()) {
            auto flushed = std::exchange(_pending_flushed, std::nullopt);
            auto batch = std::exchange(_pending, {});
            _flush_started = lowres_clock::now();
            try {
                co_await _flush(batch);
                flushed->set_value();
            } catch (...) {
                flushed->set_exception(std::current_exception());
            }
        }
    }

    future<> append(auto&& add, lowres_clock::time_point timeout) {
        if (!_pending_flushed) {
            _pending_flushed.emplace();
        }
        auto flushed = _pending_flushed->get_shared_future(timeout);
        auto pending_mark = _pending.mark();
        try {
            add(_pending);
            if (!_flushing) {
                _flush_loop = flush_loop();
            }
        } catch (...) {
            // drop a partly copied record
            _pending.truncate(pending_mark);
            throw;
        }
        return flushed;
    }
public:
    stdout_record_batcher(std::chrono::milliseconds timeout, flush_func flush)
        : _timeout(timeout)
        , _flush(std::move(flush))
    {}

    // Appends the pieces that `build` passes to its argument as one record. Fails once the record
    // waits longer than the timeout, or at once while the flush in progress has taken that long.
    future<> write_record(auto&& build) {
        auto now = lowres_clock::now();
        if (_flushing && now - _flush_started > _timeout) {
            return make_exception_future<>(audit_exception(seastar::format(
                    "Stdout audit backend has not finished a write for over {}", _timeout)));
        }
        auto add = [&build] (record_batch& pending) {
            build([&pending] (std::string_view piece) {
                pending.append(piece);
            });
        };
        return append(add, now + _timeout).handle_exception_type([timeout = _timeout] (const timed_out_error&) {
            return make_exception_future<>(audit_exception(seastar::format(
                    "Stdout audit backend did not write the record within {}", timeout)));
        });
    }

    // Resolves once every record written so far is flushed. Fails with
    // timed_out_error at the timeout after the batch in flight started.
    future<> close() {
        auto flush_loop = std::exchange(_flush_loop, make_ready_future<>());
        if (flush_loop.available()) {
            return flush_loop;
        }
        return with_timeout(_flush_started + _timeout, std::move(flush_loop));
    }
};

namespace {

// The batcher of shard 0, which writes to the stream. Set only on shard 0.
thread_local stdout_record_batcher* shard0_batcher = nullptr;

// `build` runs on shard 0, possibly after the caller has returned.
future<> write_on_shard0(auto build) {
    return smp::submit_to(0, [build = std::move(build)] {
        if (!shard0_batcher) {
            throw audit_exception("Stdout audit backend is not started");
        }
        return shard0_batcher->write_record(build);
    });
}

std::string_view describe(directory_entry_type type) {
    switch (type) {
    case directory_entry_type::block_device: return "a block device";
    case directory_entry_type::char_device: return "a character device";
    case directory_entry_type::directory: return "a directory";
    case directory_entry_type::fifo: return "a pipe";
    case directory_entry_type::link: return "a symbolic link";
    case directory_entry_type::regular: return "a regular file";
    case directory_entry_type::socket: return "a socket";
    case directory_entry_type::unknown: break;
    }
    return "of an unknown type";
}

// Opens `path` anew, so the non-blocking mode of the stream does not affect
// other writers of the same pipe, such as the logger.
future<data_sink> open_stream(const sstring& path) {
    stat_data st;
    try {
        st = co_await file_stat(path);
    } catch (...) {
        throw audit_exception(seastar::format("Stdout audit backend failed to inspect {}: {}", path, std::current_exception()));
    }
    if (st.type != directory_entry_type::fifo && st.type != directory_entry_type::char_device) {
        // A seastar file stream writes at explicit offsets, so it would
        // overwrite whatever else the process writes to a regular file.
        throw audit_exception(seastar::format("Stdout audit backend requires {} to be a pipe or a character device, but it is {}",
                path, describe(st.type)));
    }
    // Memory devices are no place for records: most of them, like /dev/null,
    // discard what is written to them without any error.
    if (st.type == directory_entry_type::char_device && major(st.rdev) == 1) {
        throw audit_exception(seastar::format("Stdout audit backend cannot write to {}: it is a memory device, such as /dev/null",
                path));
    }
    try {
        auto out = co_await make_pipe_output_stream(std::filesystem::path(path));
        co_return std::move(out).detach();
    } catch (const std::system_error& e) {
        std::string_view hint;
        if (st.type == directory_entry_type::fifo && e.code() == std::errc::permission_denied) {
            hint = " (only the owner of a pipe can open it again)";
        } else if (st.type == directory_entry_type::fifo && e.code() == std::errc::no_such_device_or_address) {
            hint = " (nothing reads the pipe)";
        }
        throw audit_exception(seastar::format("Stdout audit backend failed to open {}: {}{}", path, e.what(), hint));
    } catch (...) {
        throw audit_exception(seastar::format("Stdout audit backend failed to open {}: {}", path, std::current_exception()));
    }
}

// The local time, formatted at most once a second on each shard, since
// localtime_r() takes a process-wide lock.
std::string_view local_time() {
    static thread_local std::time_t formatted_at = -1;
    static thread_local std::array<char, 32> formatted;
    static thread_local size_t size = 0;
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    if (now != formatted_at) {
        tm time;
        localtime_r(&now, &time);
        size = fmt::format_to_n(formatted.data(), formatted.size(), "{:%h %e %T}", time).out - formatted.data();
        formatted_at = now;
    }
    return std::string_view(formatted.data(), size);
}

// Passes the record to `put` in pieces, ending with a line break.
void put_record(auto&& put,
                socket_address node_ip,
                std::string_view category,
                std::optional<db::consistency_level> cl,
                bool error,
                std::string_view keyspace,
                std::string_view query,
                socket_address client_ip,
                std::string_view table,
                std::string_view username) {
    fmt::memory_buffer fields;
    fmt::format_to(fmt::appender(fields), R"({} scylla-audit: node="{}", category="{}", cl="{}", error="{}", keyspace=")",
                   local_time(), node_ip, category, cl ? format("{}", *cl) : sstring(), (error ? "true" : "false"));
    put(std::string_view(fields.data(), fields.size()));
    put_escaped(put, keyspace);
    put(R"(", query=")");
    put_escaped(put, query);
    fields.clear();
    fmt::format_to(fmt::appender(fields), R"(", client_ip="{}", table=")", client_ip);
    put(std::string_view(fields.data(), fields.size()));
    put_escaped(put, table);
    put(R"(", username=")");
    put_escaped(put, username);
    put("\"\n");
}

}

audit_stdout_storage_helper::audit_stdout_storage_helper(sstring path, std::chrono::milliseconds write_timeout)
    : _stream(seastar::make_shared<stdout_stream>(std::move(path)))
    , _batcher(seastar::make_shared<stdout_record_batcher>(write_timeout, [stream = _stream] (const record_batch& batch) {
        return stream->write(batch);
    }))
{}

audit_stdout_storage_helper::~audit_stdout_storage_helper() {
    if (shard0_batcher == _batcher.get()) {
        shard0_batcher = nullptr;
    }
}

future<> audit_stdout_storage_helper::start(const db::config&) {
    if (this_shard_id() != 0) {
        co_return;
    }
    _stream->sink = co_await open_stream(_stream->path);
    shard0_batcher = _batcher.get();
}

future<> audit_stdout_storage_helper::stop() {
    if (shard0_batcher == _batcher.get()) {
        shard0_batcher = nullptr;
    }
    try {
        co_await _batcher->close();
    } catch (const timed_out_error&) {
        // The flush keeps going, and keeps the stream and the batcher alive until it ends.
        logger.warn("Stdout audit backend stopped while a write to {} is in progress", _stream->path);
        co_return;
    } catch (...) {
        logger.warn("Stdout audit backend stopped while a write to {} failed: {}", _stream->path, std::current_exception());
        co_return;
    }
    if (_stream->sink) {
        try {
            co_await _stream->sink->close();
        } catch (...) {
            logger.warn("Stdout audit backend failed to close {}: {}", _stream->path, std::current_exception());
        }
    }
}

future<> audit_stdout_storage_helper::write(audit_sink_set sinks,
                                            const audit_info* audit_info,
                                            socket_address node_ip,
                                            socket_address client_ip,
                                            std::optional<db::consistency_level> cl,
                                            const sstring& username,
                                            bool error) {
    if (!sinks.contains(audit_sink::standard_output)) {
        return make_ready_future<>();
    }
    return write_on_shard0([=, &username] (auto&& put) {
        put_record(put, node_ip, audit_info->category_string(), cl, error, audit_info->keyspace(),
                   audit_info->query(), client_ip, audit_info->table(), username);
    });
}

future<> audit_stdout_storage_helper::write_login(audit_sink_set sinks,
                                                  const sstring& username,
                                                  socket_address node_ip,
                                                  socket_address client_ip,
                                                  bool error) {
    if (!sinks.contains(audit_sink::standard_output)) {
        return make_ready_future<>();
    }
    return write_on_shard0([=, &username] (auto&& put) {
        put_record(put, node_ip, "AUTH", std::nullopt, error, "", "", client_ip, "", username);
    });
}

}
