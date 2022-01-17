/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "buffer_input_stream.hh"
#include "limiting_data_source.hh"

using namespace seastar;

class buffer_data_source_impl : public data_source_impl {
private:
    temporary_buffer<char> _buf;
public:
    buffer_data_source_impl(temporary_buffer<char>&& buf)
        : _buf(std::move(buf))
    {}

    buffer_data_source_impl(buffer_data_source_impl&&) noexcept = default;
    buffer_data_source_impl& operator=(buffer_data_source_impl&&) noexcept = default;

    virtual future<temporary_buffer<char>> get() override {
        return make_ready_future<temporary_buffer<char>>(std::move(_buf));
    }
    virtual future<temporary_buffer<char>> skip(uint64_t n) override {
        auto min = std::min(n, _buf.size());
        _buf.trim_front(min);
        return make_ready_future<temporary_buffer<char>>(std::move(_buf));
    }
};

input_stream<char> make_buffer_input_stream(temporary_buffer<char>&& buf) {
    return input_stream < char > {
        data_source{std::make_unique<buffer_data_source_impl>(std::move(buf))}
    };
}

input_stream<char> make_buffer_input_stream(temporary_buffer<char>&& buf,
                                            seastar::noncopyable_function<size_t()>&& limit_generator) {
    auto res = data_source{std::make_unique<buffer_data_source_impl>(std::move(buf))};
    return input_stream < char > { make_limiting_data_source(std::move(res), std::move(limit_generator)) };
}
