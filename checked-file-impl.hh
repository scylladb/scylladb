/*
 * Copyright (C) 2016 ScyllaDB
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

#pragma once

#include "seastar/core/file.hh"
#include "disk-error-handler.hh"

class checked_file_impl : public file_impl {
public:

    checked_file_impl(const io_error_handler& error_handler, file f)
            : _error_handler(error_handler), _file(f) {
        _memory_dma_alignment = f.memory_dma_alignment();
        _disk_read_dma_alignment = f.disk_read_dma_alignment();
        _disk_write_dma_alignment = f.disk_write_dma_alignment();
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->write_dma(pos, buffer, len, pc);
        });
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->write_dma(pos, iov, pc);
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->read_dma(pos, buffer, len, pc);
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->read_dma(pos, iov, pc);
        });
    }

    virtual future<> flush(void) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->flush();
        });
    }

    virtual future<struct stat> stat(void) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->stat();
        });
    }

    virtual future<> truncate(uint64_t length) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->truncate(length);
        });
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->discard(offset, length);
        });
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->allocate(position, length);
        });
    }

    virtual future<uint64_t> size(void) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->size();
        });
    }

    virtual future<> close() override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->close();
        });
    }

    // returns a handle for plain file, so make_checked_file() should be called
    // on file returned by handle.
    virtual std::unique_ptr<seastar::file_handle_impl> dup() override {
        return get_file_impl(_file)->dup();
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->list_directory(next);
        });
    }

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, const io_priority_class& pc) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->dma_read_bulk(offset, range_size, pc);
        });
    }
private:
    const io_error_handler& _error_handler;
    file _file;
};

inline file make_checked_file(const io_error_handler& error_handler, file f)
{
    return file(::make_shared<checked_file_impl>(error_handler, f));
}

future<file>
inline open_checked_file_dma(const io_error_handler& error_handler,
                             sstring name, open_flags flags,
                             file_open_options options = {})
{
    return do_io_check(error_handler, [&] {
        return open_file_dma(name, flags, options).then([&] (file f) {
            return make_ready_future<file>(make_checked_file(error_handler, f));
        });
    });
}

future<file>
inline open_checked_directory(const io_error_handler& error_handler,
                              sstring name)
{
    return do_io_check(error_handler, [&] {
        return engine().open_directory(name).then([&] (file f) {
            return make_ready_future<file>(make_checked_file(error_handler, f));
        });
    });
}
