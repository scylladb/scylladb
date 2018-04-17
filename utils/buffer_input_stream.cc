/*
 * Copyright (C) 2017 ScyllaDB
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
