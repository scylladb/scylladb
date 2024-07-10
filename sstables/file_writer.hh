/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <utility>

#include <seastar/core/sstring.hh>
#include <seastar/core/iostream.hh>

#include "sstables/progress_monitor.hh"
#include "bytes.hh"
#include "seastarx.hh"

namespace sstables {

class file_writer {
    output_stream<char> _out;
    writer_offset_tracker _offset;
    std::optional<sstring> _filename;
    bool _closed = false;
public:
    file_writer(output_stream<char>&& out, sstring filename) noexcept
        : _out(std::move(out))
        , _filename(std::move(filename))
    {}

    file_writer(output_stream<char>&& out) noexcept
        : _out(std::move(out))
    {}

    // Must be called in a seastar thread.
    virtual ~file_writer();
    file_writer(const file_writer&) = delete;
    file_writer(file_writer&& x) noexcept
        : _out(std::move(x._out))
        , _offset(std::move(x._offset))
        , _filename(std::move(x._filename))
        , _closed(x._closed)
    {
        x._closed = true;   // don't auto-close in destructor
    }
    // Must be called in a seastar thread.
    void write(const char* buf, size_t n) {
        _offset.offset += n;
        _out.write(buf, n).get();
    }
    // Must be called in a seastar thread.
    void write(bytes_view s) {
        _offset.offset += s.size();
        _out.write(reinterpret_cast<const char*>(s.begin()), s.size()).get();
    }
    // Must be called in a seastar thread.
    void close();

    uint64_t offset() const {
        return _offset.offset;
    }

    const writer_offset_tracker& offset_tracker() const {
        return _offset;
    }

private:
    const char* get_filename() const noexcept;
};

}
