/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#include "fstream.hh"
#include <new>
#include <malloc.h>
#include <algorithm>

future<temporary_buffer<char>>
file_data_source_impl::get() {
    // must align allocation for dma
    auto p = ::memalign(std::min<size_t>(_buffer_size, 4096), _buffer_size);
    if (!p) {
        throw std::bad_alloc();
    }
    auto q = static_cast<char*>(p);
    temporary_buffer<char> buf(q, _buffer_size, make_free_deleter(p));
    auto old_pos = _pos;
    // dma_read needs to be aligned. It doesn't have to be page-aligned,
    // though, and we could get away with something much smaller. However, if
    // we start reading in things outside page boundaries, we will end up with
    // various pages around, some of them with overlapping ranges. Those would
    // be very challenging to cache.
    old_pos &= ~4095;
    auto front = _pos - old_pos;
    _pos += _buffer_size - front;
    return _file.dma_read(old_pos, q, _buffer_size).then(
            [buf = std::move(buf), front] (size_t size) mutable {
        buf.trim(size);
        buf.trim_front(front);
        return make_ready_future<temporary_buffer<char>>(std::move(buf));
    });
}

