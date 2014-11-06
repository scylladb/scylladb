#ifndef _MEMCACHED_HH
#define _MEMCACHED_HH

#include "core/sstring.hh"

namespace memcache {

class item_key {
private:
    sstring _key;
    size_t _hash;
public:
    item_key() = default;
    item_key(item_key&) = default;
    item_key(sstring key)
        : _key(key)
        , _hash(std::hash<sstring>()(key))
    {}
    item_key(item_key&& other)
        : _key(std::move(other._key))
        , _hash(other._hash)
    {
        other._hash = 0;
    }
    size_t hash() const {
        return _hash;
    }
    const sstring& key() const {
        return _key;
    }
    bool operator==(const item_key& other) const {
        return other._hash == _hash && other._key == _key;
    }
    void operator=(item_key&& other) {
        _key = std::move(other._key);
        _hash = other._hash;
        other._hash = 0;
    }
};

}

namespace std {

template <>
struct hash<memcache::item_key> {
    size_t operator()(const memcache::item_key& key) {
        return key.hash();
    }
};

} /* namespace std */

#endif
