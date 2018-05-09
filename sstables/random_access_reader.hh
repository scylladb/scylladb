/*
 * Copyright (C) 2018 ScyllaDB
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

#include <memory>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

#include "disk-error-handler.hh"

namespace sstables {

extern logging::logger sstlog;

class random_access_reader {
    std::unique_ptr <input_stream<char>> _in;
    seastar::gate _close_gate;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;

public:
    future <temporary_buffer<char>> read_exactly(size_t n) {
        return _in->read_exactly(n);
    }

    void seek(uint64_t pos) {
        if (_in) {
            seastar::with_gate(_close_gate, [in = std::move(_in)]() mutable {
                auto fut = in->close();
                return fut.then([in = std::move(in)] {});
            });
        }
        _in = std::make_unique < input_stream < char >> (open_at(pos));
    }

    bool eof() { return _in->eof(); }

    virtual future<> close() {
        return _close_gate.close().then([this] {
            return _in->close();
        });
    }

    virtual ~random_access_reader() {}
};

class file_random_access_reader : public random_access_reader {
    file _file;
    uint64_t _file_size;
    size_t _buffer_size;
    unsigned _read_ahead;
public:
    virtual input_stream<char> open_at(uint64_t pos) override {
        auto len = _file_size - pos;
        file_input_stream_options options;
        options.buffer_size = _buffer_size;
        options.read_ahead = _read_ahead;

        return make_file_input_stream(_file, pos, len, std::move(options));
    }

    explicit file_random_access_reader(file f, uint64_t file_size, size_t buffer_size = 8192, unsigned read_ahead = 4)
        : _file(std::move(f)), _file_size(file_size), _buffer_size(buffer_size), _read_ahead(read_ahead) {
        seek(0);
    }

    virtual future<> close() override {
        return random_access_reader::close().finally([this] {
            return _file.close().handle_exception([save = _file](auto ep) {
                sstlog.warn("sstable close failed: {}", ep);
                general_disk_error();
            });
        });
    }
};

}