/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

#pragma once
#include "bytes.hh"
#include "schema.hh"
#include <boost/any.hpp>

class partition_key;
class clustering_key;

namespace sstables {

class key_view {
    bytes_view _bytes;
public:
    key_view(bytes_view b) : _bytes(b) {}

    bool operator==(const key_view& k) const { return k._bytes == _bytes; }
    bool operator!=(const key_view& k) const { return !(k == *this); }

    explicit operator bytes_view() const {
        return _bytes;
    }
};

enum class composite_marker : bytes::value_type {
    start_range = -1,
    none = 0,
    end_range = 1,
};

inline void check_marker(bytes_view component, composite_marker expected) {
    auto found = composite_marker(component.back());
    if (found != expected) {
        throw runtime_exception(sprint("Unexpected marker. Found %d, expected %d\n", uint8_t(found), uint8_t(expected)));
    }
}

// Our internal representation differs slightly (in the way it serializes) from Origin.
// In order to be able to achieve read and write compatibility for sstables - so they can
// be imported and exported - we need to always convert a key to this representation.
class key {
    bytes _bytes;
public:
    key(bytes&& b) : _bytes(std::move(b)) {}
    static key from_bytes(bytes b) { return key(std::move(b)); }
    static key from_deeply_exploded(const schema& s, const std::vector<boost::any>& v);
    static key from_exploded(const schema& s, const std::vector<bytes>& v);
    static key from_exploded(const schema& s, std::vector<bytes>&& v);
    // Unfortunately, the _bytes field for the partition_key are not public. We can't move.
    static key from_partition_key(const schema& s, const partition_key& pk);

    std::vector<bytes> explode(const schema& s) const;

    operator key_view() const {
        return key_view(_bytes);
    }
    explicit operator bytes_view() const {
        return _bytes;
    }
    bytes& get_bytes() {
        return _bytes;
    }
};

bytes composite_from_clustering_key(const schema& s, const clustering_key& ck);

class composite_view {
    bytes_view _bytes;
public:
    composite_view(bytes_view b) : _bytes(b) {}

    std::vector<bytes> explode() const;

    explicit operator bytes_view() const {
        return _bytes;
    }
};
}
