/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include "utils/disk-error-handler.hh"

#include "seastarx.hh"

class checked_file_impl : public file_impl {
public:

    checked_file_impl(const io_error_handler& error_handler, file f)
            : file_impl(*get_file_impl(f)),  _error_handler(error_handler), _file(f) {
    }

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent* intent) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->write_dma(pos, buffer, len, intent);
        });
    }

    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->write_dma(pos, iov, intent);
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent* intent) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->read_dma(pos, buffer, len, intent);
        });
    }

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent* intent) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->read_dma(pos, iov, intent);
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

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent* intent) override {
        return do_io_check(_error_handler, [&] {
            return get_file_impl(_file)->dma_read_bulk(offset, range_size, intent);
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
        return open_directory(name).then([&] (file f) {
            return make_ready_future<file>(make_checked_file(error_handler, f));
        });
    });
}
