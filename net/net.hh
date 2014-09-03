/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#ifndef NET_HH_
#define NET_HH_

#include "core/reactor.hh"
#include "ethernet.hh"
#include <unordered_map>

namespace net {

class packet;
class interface;
class device;
class l3_protocol;

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
    struct deleter {
        std::unique_ptr<deleter> next;
        deleter(std::unique_ptr<deleter> next) : next(std::move(next)) {}
        virtual ~deleter() {};
    };
    struct internal_deleter final : deleter {
        // FIXME: make buf an std::array<>?
        char* buf;
        unsigned free_head;
        internal_deleter(std::unique_ptr<deleter> next, char* buf, unsigned free_head)
            : deleter(std::move(next)), buf(buf), free_head(free_head) {}
        explicit internal_deleter(std::unique_ptr<deleter> next)
            : internal_deleter(std::move(next), new char[internal_data_size], internal_data_size) {}
        virtual ~internal_deleter() override { delete[] buf; }
    };
    template <typename Deleter>
    struct lambda_deleter final : deleter {
        Deleter del;
        lambda_deleter(std::unique_ptr<deleter> next, Deleter&& del)
            : deleter(std::move(next)), del(std::move(del)) {}
        virtual ~lambda_deleter() override { del(); }
    };
    template <typename Deleter>
    std::unique_ptr<deleter>
    make_deleter(std::unique_ptr<deleter> next, Deleter d) {
        return std::unique_ptr<deleter>(new lambda_deleter<Deleter>(std::move(next), std::move(d)));
    }
    // when destroyed, virtual destructor will reclaim resources
    std::unique_ptr<deleter> _deleter;
public:
    std::vector<fragment> fragments;
    unsigned len = 0;
public:
    // build empty packet
    packet() = default;
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

    void trim_front(size_t how_much);

    // get a header pointer, linearizing if necessary
    template <typename Header>
    Header* get_header(size_t offset);

    // get a header pointer, linearizing if necessary
    char* get_header(size_t offset, size_t size);

    // prepend a header (default-initializing it)
    template <typename Header>
    Header* prepend_header(size_t size = sizeof(Header));

    // prepend a header (uninitialized!)
    char* prepend_uninitialized_header(size_t size);
private:
    void linearize(size_t at_frag, size_t desired_size);
};

class l3_protocol {
    interface* _netif;
    uint16_t _proto_num;
public:
    explicit l3_protocol(interface* netif, uint16_t proto_num) : _netif(netif), _proto_num(proto_num) {}
    future<> send(ethernet_address to, packet p);
    future<packet, ethernet_address> receive();
private:
    void received(packet p);
    friend class interface;
};

class interface {
    std::unique_ptr<device> _dev;
    std::unordered_map<uint16_t, promise<packet, ethernet_address>> _proto_map;
    ethernet_address _hw_address;
private:
    future<packet, ethernet_address> receive(uint16_t proto_num);
    future<> send(uint16_t proto_num, ethernet_address to, packet p);
public:
    explicit interface(std::unique_ptr<device> dev);
    ethernet_address hw_address() { return _hw_address; }
    void run();
    friend class l3_protocol;
};

class device {
public:
    virtual ~device() {}
    virtual future<packet> receive() = 0;
    virtual future<> send(packet p) = 0;
    virtual ethernet_address hw_address() = 0;
};

inline
packet::packet(packet&& x)
    : _deleter(std::move(x._deleter)), fragments(std::move(x.fragments)), len(x.len) {
    x.len = 0;
}

inline
packet::packet(fragment frag) : len(frag.size) {
    auto flen = std::max(frag.size, internal_data_size);
    std::unique_ptr<char[]> buf{new char[flen]};
    std::copy(frag.base, frag.base + frag.size, buf.get() + flen - frag.size);
    fragments.push_back(frag);
    _deleter.reset(new internal_deleter(std::unique_ptr<deleter>(),
            buf.release(), flen - frag.size));
}

inline
packet::packet(char* data, size_t size) : packet(fragment{data, size}) {
}

template <typename Deleter>
inline
packet::packet(fragment frag, Deleter d)
    : _deleter(make_deleter(std::unique_ptr<deleter>(), std::move(d))) {
    fragments.push_back(frag);
    len = frag.size;
}

template <typename Deleter>
inline
packet::packet(std::vector<fragment> frag, Deleter d)
    : _deleter(make_deleter(std::unique_ptr<deleter>(), std::move(d)))
    , fragments(std::move(frag))
    , len(0) {
    for (auto&& f : fragments) {
        len += f.size;
    }
}

inline
packet::packet(packet&& x, fragment frag)
    : fragments(std::move(x.fragments))
    , len(x.len + frag.size) {
    std::unique_ptr<char[]> buf(new char[frag.size]);
    std::copy(frag.base, frag.base + frag.size, buf.get());
    _deleter = make_deleter(std::move(x._deleter), [buf = buf.release()] {
        delete[] buf;
    });
    x.len = 0;
}

inline
packet::packet(fragment frag, packet&& x)
    : _deleter(std::move(x._deleter)), len(x.len + frag.size) {
    // try to prepend into existing internal fragment
    auto id = dynamic_cast<internal_deleter*>(x._deleter.get());
    if (id && id->free_head >= frag.size) {
        id->free_head -= frag.size;
        fragments[0].base -= frag.size;
        fragments[0].size += frag.size;
        std::copy(frag.base, frag.base + frag.size, fragments[0].base);
    } else {
        // didn't work out, allocate and copy
        auto size = std::max(frag.size, internal_data_size);
        std::unique_ptr<char[]> buf(new char[size]);
        std::copy(frag.base, frag.base + frag.size, buf.get() + size - frag.size);
        fragments.reserve(x.fragments.size() + 1);
        fragments.push_back({buf.get() + size - frag.size, frag.size});
        std::copy(x.fragments.begin(), x.fragments.end(), std::back_inserter(fragments));
        x.fragments.clear();
        _deleter.reset(new internal_deleter(std::move(_deleter), buf.release(), size - frag.size));
    }
    x.len = 0;
}

template <typename Deleter>
inline
packet::packet(fragment frag, Deleter d, packet&& x) : len(x.len + frag.size) {
    fragments.reserve(x.fragments.size() + 1);
    fragments.push_back(frag);
    std::copy(x.fragments.begin(), x.fragments.end(), std::back_inserter(fragments));
    x.fragments.clear();
    _deleter.reset(make_deleter(std::move(_deleter), d));
    x.len = 0;
}

template <typename Deleter>
inline
packet::packet(packet&& x, fragment frag, Deleter d)
    : fragments(std::move(x.fragments)), len(x.len + frag.size) {
    fragments.push_back(frag);
    _deleter.reset(make_deleter(std::move(_deleter), d));
    x.len = 0;
}

inline
char* packet::get_header(size_t offset, size_t size) {
    if (offset + size > len) {
        return nullptr;
    }
    size_t i = 0;
    while (i != fragments.size() && offset >= fragments[i].size) {
        offset -= fragments[i++].size;
    }
    if (i == fragments.size()) {
        return nullptr;
    }
    if (offset + size > fragments[i].size) {
        linearize(i, offset + size);
    }
    return fragments[i].base + offset;
}

template <typename Header>
inline
Header* packet::get_header(size_t offset) {
    return reinterpret_cast<Header*>(get_header(offset, sizeof(Header)));
}

inline
void packet::trim_front(size_t how_much) {
    assert(how_much <= len);
    len -= how_much;
    size_t i = 0;
    while (how_much && how_much >= fragments[i].size) {
        how_much -= fragments[i++].size;
    }
    fragments.erase(fragments.begin(), fragments.begin() + i);
    if (how_much) {
        fragments[0].base += how_much;
        fragments[0].size -= how_much;
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
    if (len == 0) {
        auto id = std::make_unique<internal_deleter>(nullptr);
        fragments.clear();
        fragments.push_back({id->buf + id->free_head, 0});
        _deleter.reset(id.release());
    }
    len += size;
    auto id = dynamic_cast<internal_deleter*>(_deleter.get());
    if (id && id->free_head >= size) {
        id->free_head -= size;
        fragments[0].base -= size;
        fragments[0].size += size;
    } else {
        // didn't work out, allocate and copy
        auto nsize = std::max(size, internal_data_size);
        std::unique_ptr<char[]> buf(new char[nsize]);
        fragments.insert(fragments.begin(), {buf.get() + nsize - size, size});
        _deleter.reset(new internal_deleter(std::move(_deleter), buf.release(), nsize - size));
    }
    return fragments[0].base;
}

}

#endif /* NET_HH_ */
