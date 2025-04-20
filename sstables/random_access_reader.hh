/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/assert.hh"
#include <memory>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include "seastarx.hh"

namespace sstables {

class random_access_reader {
    std::unique_ptr <input_stream<char>> _in;
protected:
    virtual input_stream<char> open_at(uint64_t pos) = 0;

    void set(input_stream<char> in) {
        SCYLLA_ASSERT(!_in);
        _in = std::make_unique<input_stream<char>>(std::move(in));
    }

public:
    future <temporary_buffer<char>> read_exactly(size_t n) noexcept;

    future<> seek(uint64_t pos) noexcept;

    bool eof() const noexcept { return _in->eof(); }

    virtual future<> close() noexcept;

    virtual ~random_access_reader() {}
};

class file_random_access_reader : public random_access_reader {
    file _file;
    uint64_t _file_size;
    size_t _buffer_size;
    unsigned _read_ahead;
public:
    virtual input_stream<char> open_at(uint64_t pos) override;

    explicit file_random_access_reader(file f, uint64_t file_size, size_t buffer_size = 8192, unsigned read_ahead = 4);

    virtual future<> close() noexcept override;
};

}
