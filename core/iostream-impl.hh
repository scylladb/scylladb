/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */


#pragma once

#include "net/packet.hh"

template<typename CharType>
inline
future<> output_stream<CharType>::write(const char_type* buf) {
    return write(buf, strlen(buf));
}

template<typename CharType>
template<typename SizeType, SizeType MaxSize>
inline
future<> output_stream<CharType>::write(const basic_sstring<CharType, SizeType, MaxSize>& s) {
    return write(s.c_str(), s.size());
}

template<typename CharType>
inline
future<> output_stream<CharType>::write(const std::basic_string<CharType>& s) {
    return write(s.c_str(), s.size());
}

template<typename CharType>
future<> output_stream<CharType>::write(scattered_message<CharType> msg) {
    return write(std::move(msg).release());
}

template<typename CharType>
future<> output_stream<CharType>::write(net::packet p) {
    static_assert(std::is_same<CharType, char>::value, "packet works on char");

    if (p.len() == 0) {
        return make_ready_future<>();
    }

    assert(!_end && "Mixing buffered writes and zero-copy writes not supported yet");

    if (!_trim_to_size || p.len() <= _size) {
        // TODO: aggregate buffers for later coalescing. Currently we flush right
        // after appending the message anyway, so it doesn't matter.
        return _fd.put(std::move(p));
    }

    auto head = p.share(0, _size);
    p.trim_front(_size);
    return _fd.put(std::move(head)).then([this, p = std::move(p)] () mutable {
        return write(std::move(p));
    });
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream<CharType>::read_exactly_part(size_t n, tmp_buf out, size_t completed) {
    if (available()) {
        auto now = std::min(n - completed, available());
        std::copy(_buf.get(), _buf.get() + now, out.get_write() + completed);
        _buf.trim_front(now);
        completed += now;
    }
    if (completed == n) {
        return make_ready_future<tmp_buf>(std::move(out));
    }

    // _buf is now empty
    return _fd.get().then([this, n, out = std::move(out), completed] (auto buf) mutable {
        if (buf.size() == 0) {
            _eof = true;
            return make_ready_future<tmp_buf>(std::move(buf));
        }
        _buf = std::move(buf);
        return this->read_exactly_part(n, std::move(out), completed);
    });
}

template <typename CharType>
future<temporary_buffer<CharType>>
input_stream<CharType>::read_exactly(size_t n) {
    if (_buf.size() == n) {
        // easy case: steal buffer, return to caller
        return make_ready_future<tmp_buf>(std::move(_buf));
    } else if (_buf.size() > n) {
        // buffer large enough, share it with caller
        auto front = _buf.share(0, n);
        _buf.trim_front(n);
        return make_ready_future<tmp_buf>(std::move(front));
    } else if (_buf.size() == 0) {
        // buffer is empty: grab one and retry
        return _fd.get().then([this, n] (auto buf) mutable {
            if (buf.size() == 0) {
                _eof = true;
                return make_ready_future<tmp_buf>(std::move(buf));
            }
            _buf = std::move(buf);
            return this->read_exactly(n);
        });
    } else {
        // buffer too small: start copy/read loop
        tmp_buf b(n);
        return read_exactly_part(n, std::move(b), 0);
    }
}

template <typename CharType>
template <typename Consumer>
future<>
input_stream<CharType>::consume(Consumer& consumer) {
    if (_buf.empty() && !_eof) {
        return _fd.get().then([this, &consumer] (tmp_buf buf) {
            _buf = std::move(buf);
            _eof = _buf.empty();
            return consume(consumer);
        });
    } else {
        auto tmp = std::move(_buf);
        bool done = tmp.empty();
        consumer(std::move(tmp), [this, &done] (tmp_buf unconsumed) {
            done = true;
            if (!unconsumed.empty()) {
                _buf = std::move(unconsumed);
            }
        });
        if (!done) {
            return consume(consumer);
        } else {
            return make_ready_future<>();
        }
    }
}

// Writes @buf in chunks of _size length. The last chunk is buffered if smaller.
template <typename CharType>
future<>
output_stream<CharType>::split_and_put(temporary_buffer<CharType> buf) {
    assert(_end == 0);

    if (buf.size() < _size) {
        if (!_buf) {
            _buf = _fd.allocate_buffer(_size);
        }
        std::copy(buf.get(), buf.get() + buf.size(), _buf.get_write());
        _end = buf.size();
        return make_ready_future<>();
    }

    auto chunk = buf.share(0, _size);
    buf.trim_front(_size);
    return _fd.put(std::move(chunk)).then([this, buf = std::move(buf)] () mutable {
        return split_and_put(std::move(buf));
    });
}

template <typename CharType>
future<>
output_stream<CharType>::write(const char_type* buf, size_t n) {
    auto bulk_threshold = _end ? (2 * _size - _end) : _size;
    if (n >= bulk_threshold) {
        if (_end) {
            auto now = _size - _end;
            std::copy(buf, buf + now, _buf.get_write() + _end);
            _end = _size;
            temporary_buffer<char> tmp = _fd.allocate_buffer(n - now);
            std::copy(buf + now, buf + n, tmp.get_write());
            return flush().then([this, tmp = std::move(tmp)]() mutable {
                if (_trim_to_size) {
                    return split_and_put(std::move(tmp));
                } else {
                    return _fd.put(std::move(tmp));
                }
            });
        } else {
            temporary_buffer<char> tmp = _fd.allocate_buffer(n);
            std::copy(buf, buf + n, tmp.get_write());
            if (_trim_to_size) {
                return split_and_put(std::move(tmp));
            } else {
                return _fd.put(std::move(tmp));
            }
        }
    }

    if (!_buf) {
        _buf = _fd.allocate_buffer(_size);
    }

    auto now = std::min(n, _size - _end);
    std::copy(buf, buf + now, _buf.get_write() + _end);
    _end += now;
    if (now == n) {
        return make_ready_future<>();
    } else {
        temporary_buffer<char> next = _fd.allocate_buffer(_size);
        std::copy(buf + now, buf + n, next.get_write());
        _end = n - now;
        std::swap(next, _buf);
        return _fd.put(std::move(next));
    }
}

template <typename CharType>
future<>
output_stream<CharType>::flush() {
    if (!_end) {
        return make_ready_future<>();
    }
    _buf.trim(_end);
    _end = 0;
    return _fd.put(std::move(_buf));
}

