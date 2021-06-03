/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <exception>

#include "sstables/random_access_reader.hh"
#include "utils/disk-error-handler.hh"
#include "log.hh"

namespace sstables {

extern logging::logger sstlog;

future <temporary_buffer<char>> random_access_reader::read_exactly(size_t n) noexcept {
  try {
    return _in->read_exactly(n);
  } catch (...) {
    return current_exception_as_future<temporary_buffer<char>>();
  }
}

static future<> close_if_needed(std::unique_ptr<input_stream<char>> in) {
    if (!in) {
        return make_ready_future<>();
    }
    return in->close().finally([in = std::move(in)] {});
}

future<> random_access_reader::seek(uint64_t pos) noexcept {
    try {
        auto tmp = std::make_unique<input_stream<char>>(open_at(pos));
        std::swap(tmp, _in);
        return close_if_needed(std::move(tmp));
    } catch (...) {
        return current_exception_as_future();
    }
}

future<> random_access_reader::close() noexcept {
    return futurize_invoke(close_if_needed, std::move(_in));
}

file_random_access_reader::file_random_access_reader(file f, uint64_t file_size, size_t buffer_size, unsigned read_ahead)
    : _file(std::move(f)), _file_size(file_size), _buffer_size(buffer_size), _read_ahead(read_ahead) {
    set(open_at(0));
}

input_stream<char> file_random_access_reader::open_at(uint64_t pos) {
    auto len = _file_size - pos;
    file_input_stream_options options;
    options.buffer_size = _buffer_size;
    options.read_ahead = _read_ahead;

    return make_file_input_stream(_file, pos, len, std::move(options));
}

future<> file_random_access_reader::close() noexcept {
    return random_access_reader::close().finally([this] {
        return _file.close().handle_exception([save = _file](auto ep) {
            sstlog.warn("sstable close failed: {}", ep);
            general_disk_error();
        });
    });
}

}
