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

    checked_file_impl(disk_error_signal_type& s, file f)
            : _signal(s) , _file(f) {}

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, const io_priority_class& pc) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->write_dma(pos, buffer, len, pc);
        });
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->write_dma(pos, iov, pc);
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, const io_priority_class& pc) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->read_dma(pos, buffer, len, pc);
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, const io_priority_class& pc) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->read_dma(pos, iov, pc);
        });
    }

    virtual future<> flush(void) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->flush();
        });
    }

    virtual future<struct stat> stat(void) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->stat();
        });
    }

    virtual future<> truncate(uint64_t length) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->truncate(length);
        });
    }

    virtual future<> discard(uint64_t offset, uint64_t length) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->discard(offset, length);
        });
    }

    virtual future<> allocate(uint64_t position, uint64_t length) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->allocate(position, length);
        });
    }

    virtual future<uint64_t> size(void) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->size();
        });
    }

    virtual future<> close() override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->close();
        });
    }

    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override {
        return do_io_check(_signal, [&] {
            return get_file_impl(_file)->list_directory(next);
        });
    }

private:
    disk_error_signal_type &_signal;
    file _file;
};

inline file make_checked_file(disk_error_signal_type& signal, file& f)
{
    return file(::make_shared<checked_file_impl>(signal, f));
}

future<file>
inline open_checked_file_dma(disk_error_signal_type& signal,
                             sstring name, open_flags flags,
                             file_open_options options)
{
    return do_io_check(signal, [&] {
        return open_file_dma(name, flags, options).then([&] (file f) {
            return make_ready_future<file>(make_checked_file(signal, f));
        });
    });
}

future<file>
inline open_checked_file_dma(disk_error_signal_type& signal,
                             sstring name, open_flags flags)
{
    return do_io_check(signal, [&] {
        return open_file_dma(name, flags).then([&] (file f) {
            return make_ready_future<file>(make_checked_file(signal, f));
        });
    });
}
