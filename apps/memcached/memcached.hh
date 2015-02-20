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
#ifndef _MEMCACHED_HH
#define _MEMCACHED_HH

#include "core/sstring.hh"

namespace memcache {

class item;
class cache;

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
