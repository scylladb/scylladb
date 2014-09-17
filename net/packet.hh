/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef PACKET_HH_
#define PACKET_HH_

#include "core/deleter.hh"
#include <vector>
#include <cassert>

namespace net {

struct fragment {
    char* base;
    size_t size;
};

// Zero-copy friendly packet class
//
// For implementing zero-copy, we need a flexible destructor that can
// destroy packet data in different ways: decrementing a reference count,
// or calling a free()-like function.
//
// Moreover, we need different destructors for each set of fragments within
// a single fragment. For example, a header and trailer might need delete[]
// to be called, while the internal data needs a reference count to be
// released.  Matters are complicated in that fragments can be split
// (due to virtual/physical translation).
//
// To implement this, we associate each packet with a single destructor,
// but allow composing a packet from another packet plus a fragment to
// be added, with its own destructor, causing the destructors to be chained.
//
// The downside is that the data needed for the destructor is duplicated,
// if it is already available in the fragment itself.
//
// As an optimization, when we allocate small fragments, we allocate some
// extra space, so prepending to the packet does not require extra
// allocations.  This is useful when adding headers.
//
class packet final {
    // enough for lots of headers, not quite two cache lines:
    static constexpr size_t internal_data_size = 128 - 16;

    struct impl {
        // when destroyed, virtual destructor will reclaim resources
        std::unique_ptr<deleter> _deleter;
        std::vector<fragment> _fragments;
        unsigned _len = 0;

        impl();
        impl(const impl&) = delete;
        impl(fragment frag);
    };
    std::unique_ptr<impl> _impl;
public:
    // build empty packet
    packet();
    // move existing packet
    packet(packet&& x);
    // copy data into packet
    packet(char* data, size_t len);
    // copy data into packet
    packet(fragment frag);
    // zero-copy single fragment
    template <typename Deleter>
    packet(fragment frag, Deleter deleter);
    // zero-copy multiple fragment
    template <typename Deleter>
    packet(std::vector<fragment> frag, Deleter deleter);
    // append fragment (copying new fragment)
    packet(packet&& x, fragment frag);
    // prepend fragment (copying new fragment, with header optimization)
    packet(fragment frag, packet&& x);
    // prepend fragment (zero-copy)
    template <typename Deleter>
    packet(fragment frag, Deleter deleter, packet&& x);
    // append fragment (zero-copy)
    template <typename Deleter>
    packet(packet&& x, fragment frag, Deleter deleter);

    packet& operator=(packet&& x) {
        if (this != &x) {
            this->~packet();
            new (this) packet(std::move(x));
        }
        return *this;
    }

    unsigned len() const { return _impl->_len; }

    fragment frag(unsigned idx) const { return _impl->_fragments[idx]; }
    fragment& frag(unsigned idx) { return _impl->_fragments[idx]; }

    unsigned nr_frags() const { return _impl->_fragments.size(); }
    std::vector<fragment>& fragments() { return _impl->_fragments; }
    const std::vector<fragment>& fragments() const { return _impl->_fragments; }

    // share packet data (reference counted, non COW)
    packet share();
    packet share(size_t offset, size_t len);

    void append(packet&& p);

    void trim_front(size_t how_much);

    // get a header pointer, linearizing if necessary
    template <typename Header>
    Header* get_header(size_t offset = 0);

    // get a header pointer, linearizing if necessary
    char* get_header(size_t offset, size_t size);

    // prepend a header (default-initializing it)
    template <typename Header>
    Header* prepend_header(size_t size = sizeof(Header));

    // prepend a header (uninitialized!)
    char* prepend_uninitialized_header(size_t size);
private:
    void linearize(size_t at_frag, size_t desired_size);
    std::unique_ptr<shared_deleter> do_share();
};

inline
packet::packet(packet&& x)
    : _impl(std::move(x._impl)) {
}

inline
packet::impl::impl() : _len(0) {
}

inline
packet::impl::impl(fragment frag) : _len(frag.size) {
    auto flen = std::max(frag.size, internal_data_size);
    std::unique_ptr<char[]> buf{new char[flen]};
    std::copy(frag.base, frag.base + frag.size, buf.get() + flen - frag.size);
    _fragments.push_back({buf.get() + flen - frag.size, frag.size});
    _deleter.reset(new internal_deleter(std::unique_ptr<deleter>(),
            buf.release(), flen - frag.size));
}

inline
packet::packet()
    : _impl(new impl) {
}

inline
packet::packet(fragment frag) : _impl(new impl(frag)) {
}

inline
packet::packet(char* data, size_t size) : packet(fragment{data, size}) {
}

template <typename Deleter>
inline
packet::packet(fragment frag, Deleter d)
    : _impl(new impl()) {
    _impl->_deleter = make_deleter(std::unique_ptr<deleter>(), std::move(d));
    _impl->_fragments.push_back(frag);
    _impl->_len = frag.size;
}

template <typename Deleter>
inline
packet::packet(std::vector<fragment> frag, Deleter d)
    : _impl(new impl) {
    _impl->_deleter = make_deleter(std::unique_ptr<deleter>(), std::move(d));
    _impl->_fragments = std::move(frag);
    _impl->_len = 0;
    for (auto&& f : _impl->_fragments) {
        _impl->_len += f.size;
    }
}

inline
packet::packet(packet&& x, fragment frag)
    : _impl(std::move(x._impl)) {
    _impl->_len += frag.size;
    std::unique_ptr<char[]> buf(new char[frag.size]);
    std::copy(frag.base, frag.base + frag.size, buf.get());
    _impl->_fragments.push_back({buf.get(), frag.size});
    _impl->_deleter = make_deleter(std::move(_impl->_deleter), [buf = buf.release()] {
        delete[] buf;
    });
}

inline
packet::packet(fragment frag, packet&& x)
    : _impl(std::move(x._impl)) {
    _impl->_len += frag.size;
    // try to prepend into existing internal fragment
    auto id = dynamic_cast<internal_deleter*>(_impl->_deleter.get());
    if (id && id->free_head >= frag.size) {
        id->free_head -= frag.size;
        _impl->_fragments[0].base -= frag.size;
        _impl->_fragments[0].size += frag.size;
        std::copy(frag.base, frag.base + frag.size, _impl->_fragments[0].base);
    } else {
        // didn't work out, allocate and copy
        auto size = std::max(frag.size, internal_data_size);
        std::unique_ptr<char[]> buf(new char[size]);
        std::copy(frag.base, frag.base + frag.size, buf.get() + size - frag.size);
        _impl->_fragments.insert(_impl->_fragments.begin(),
                {buf.get() + size - frag.size, frag.size});
        _impl->_deleter.reset(new internal_deleter(std::move(_impl->_deleter),
                buf.release(), size - frag.size));
    }
}

template <typename Deleter>
inline
packet::packet(fragment frag, Deleter d, packet&& x)
    : _impl(std::move(x._impl)) {
    _impl->_len += frag.size;
    _impl->_fragments.insert(_impl->_fragments.begin(), frag);
    _impl->_deleter.reset(make_deleter(std::move(_impl->_deleter), d));
}

template <typename Deleter>
inline
packet::packet(packet&& x, fragment frag, Deleter d)
    : _impl(std::move(x._impl)) {
    _impl->_len += frag.size;
    _impl->_fragments.push_back(frag);
    _impl->_deleter.reset(make_deleter(std::move(_impl->_deleter), d));
}

inline
void packet::append(packet&& p) {
    _impl->_len += p._impl->_len;
    _impl->_fragments.reserve(_impl->_fragments.size()
                              + p._impl->_fragments.size());
    _impl->_fragments.insert(_impl->_fragments.end(),
            p._impl->_fragments.begin(), p._impl->_fragments.end());
    // preserve _deleter in front of chain in case it is an internal_deleter
    if (!p._impl->_deleter) {
        return;
    } else if (!_impl->_deleter) {
        _impl->_deleter = std::move(p._impl->_deleter);
    } else if (!_impl->_deleter->next) {
        _impl->_deleter->next = std::move(p._impl->_deleter);
    } else {
        auto chain = make_deleter(std::move(_impl->_deleter->next),
                [d = std::move(p._impl->_deleter)] {});
        _impl->_deleter->next = std::move(chain);
    }
}

inline
char* packet::get_header(size_t offset, size_t size) {
    if (offset + size > _impl->_len) {
        return nullptr;
    }
    size_t i = 0;
    while (i != _impl->_fragments.size() && offset >= _impl->_fragments[i].size) {
        offset -= _impl->_fragments[i++].size;
    }
    if (i == _impl->_fragments.size()) {
        return nullptr;
    }
    if (offset + size > _impl->_fragments[i].size) {
        linearize(i, offset + size);
    }
    return _impl->_fragments[i].base + offset;
}

template <typename Header>
inline
Header* packet::get_header(size_t offset) {
    return reinterpret_cast<Header*>(get_header(offset, sizeof(Header)));
}

inline
void packet::trim_front(size_t how_much) {
    assert(how_much <= _impl->_len);
    _impl->_len -= how_much;
    size_t i = 0;
    while (how_much && how_much >= _impl->_fragments[i].size) {
        how_much -= _impl->_fragments[i++].size;
    }
    _impl->_fragments.erase(_impl->_fragments.begin(), _impl->_fragments.begin() + i);
    if (how_much) {
        _impl->_fragments[0].base += how_much;
        _impl->_fragments[0].size -= how_much;
    }
}

template <typename Header>
Header*
packet::prepend_header(size_t size) {
    auto h = prepend_uninitialized_header(size);
    return new (h) Header{};
}

// prepend a header (uninitialized!)
inline
char* packet::prepend_uninitialized_header(size_t size) {
    if (_impl->_len == 0) {
        auto id = std::make_unique<internal_deleter>(nullptr, internal_data_size);
        _impl->_fragments.clear();
        _impl->_fragments.push_back({id->buf + id->free_head, 0});
        _impl->_deleter.reset(id.release());
    }
    _impl->_len += size;
    auto id = dynamic_cast<internal_deleter*>(_impl->_deleter.get());
    if (id && id->free_head >= size) {
        id->free_head -= size;
        _impl->_fragments[0].base -= size;
        _impl->_fragments[0].size += size;
    } else {
        // didn't work out, allocate and copy
        auto nsize = std::max(size, internal_data_size);
        std::unique_ptr<char[]> buf(new char[nsize]);
        _impl->_fragments.insert(_impl->_fragments.begin(), {buf.get() + nsize - size, size});
        _impl->_deleter.reset(new internal_deleter(std::move(_impl->_deleter), buf.release(), nsize - size));
    }
    return _impl->_fragments[0].base;
}

inline
std::unique_ptr<shared_deleter>
packet::do_share() {
    auto sd = dynamic_cast<shared_deleter*>(_impl->_deleter.get());
    if (!sd) {
        auto usd = std::make_unique<shared_deleter>(std::move(_impl->_deleter));
        sd = usd.get();
        _impl->_deleter = std::move(usd);
    }
    return std::make_unique<shared_deleter>(*sd);
}

inline
packet packet::share() {
    return share(0, _impl->_len);
}

inline
packet packet::share(size_t offset, size_t len) {
    packet n;
    n._impl->_fragments.reserve(_impl->_fragments.size());
    size_t idx = 0;
    while (offset > 0 && offset >= _impl->_fragments[idx].size) {
        offset -= _impl->_fragments[idx++].size;
    }
    while (n._impl->_len < len) {
        auto& f = _impl->_fragments[idx++];
        auto fsize = std::min(len - n._impl->_len, f.size - offset);
        n._impl->_fragments.push_back({ f.base + offset, fsize });
        n._impl->_len += fsize;
        offset = 0;
    }
    assert(!n._impl->_deleter);
    n._impl->_deleter = do_share();
    return n;
}

}

#endif /* PACKET_HH_ */
