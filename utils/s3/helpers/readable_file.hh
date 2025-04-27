/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once
#include "utils/s3/client.hh"

namespace s3 {

class client::readable_file : public file_impl {
    struct handle {
        std::string _host;
        global_factory _gf;
    public:
        handle(const client& cln)
                : _host(cln._host)
                , _gf(cln._gf)
        {}

        shared_ptr<client> to_client() && {
            return _gf(std::move(_host));
        }
    };
    shared_ptr<client> _client;
    sstring _object_name;
    std::optional<stats> _stats;
    seastar::abort_source* _as;

    [[noreturn]] void unsupported() {
        throw_with_backtrace<std::logic_error>("unsupported operation on s3 readable file");
    }

    future<> maybe_update_stats();

public:
    readable_file(shared_ptr<client> cln, sstring object_name, seastar::abort_source* as = nullptr);

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len, io_intent*) override { unsupported(); }
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override { unsupported(); }
    virtual future<> truncate(uint64_t length) override { unsupported(); }
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override { unsupported(); }

    virtual future<> flush(void) override { return make_ready_future<>(); }
    virtual future<> allocate(uint64_t position, uint64_t length) override { return make_ready_future<>(); }
    virtual future<> discard(uint64_t offset, uint64_t length) override { return make_ready_future<>(); }

    class readable_file_handle_impl final : public file_handle_impl {
        handle _h;
        sstring _object_name;

    public:
        readable_file_handle_impl(handle h, sstring object_name);

        virtual std::unique_ptr<file_handle_impl> clone() const override;

        virtual shared_ptr<file_impl> to_file() && override;
    };

    virtual std::unique_ptr<file_handle_impl> dup() override;

    virtual future<uint64_t> size(void) override;

    virtual future<struct stat> stat(void) override;

    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len, io_intent*) override;

    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov, io_intent*) override;

    virtual future<temporary_buffer<uint8_t>> dma_read_bulk(uint64_t offset, size_t range_size, io_intent*) override;

    virtual future<> close() override {
        return make_ready_future<>();
    }
};

} // s3
